use crate::{burner::Burn, pdas};
use anchor_lang::AccountDeserialize;
use chrono::Utc;
use data_credits::DelegatedDataCreditsV0;
use futures_util::StreamExt;
use helium_crypto::{Keypair, PublicKeyBinary, Sign};
use helium_proto::{
    services::{
        iot_config::{org_client::OrgClient, OrgDisableReqV1, OrgEnableReqV1},
        Channel,
    },
    Message,
};
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::{mem, sync::Arc};
use tokio::sync::Mutex;

pub struct Balances {
    pub provider: Arc<RpcClient>,
    pub balances: Arc<Mutex<HashMap<PublicKeyBinary, Balance>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum DebitError {
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
}

impl Balances {
    /// Fetch all of the current balances that have been actively burned so that
    /// we have an accurate cache.
    pub async fn new(
        pool: &Pool<Postgres>,
        sub_dao: &Pubkey,
        provider: Arc<RpcClient>,
    ) -> Result<Self, DebitError> {
        let mut burns = sqlx::query_as("SELECT * FROM pending_burns").fetch(pool);

        let mut balances = HashMap::new();

        while let Some(Burn {
            payer,
            amount: burn_amount,
            ..
        }) = burns.next().await.transpose()?
        {
            // Look up the current balance of the payer
            let balance = payer_balance(provider.as_ref(), sub_dao, &payer).await?;
            balances.insert(
                payer,
                Balance {
                    burned: burn_amount as u64,
                    balance,
                    enabled: true,
                },
            );
        }

        Ok(Self {
            provider,
            balances: Arc::new(Mutex::new(balances)),
        })
    }

    pub fn balances(&self) -> Arc<Mutex<HashMap<PublicKeyBinary, Balance>>> {
        self.balances.clone()
    }

    /// Debits the balance from the cache, returning true if there was enough
    /// balance and false otherwise.
    pub async fn debit_if_sufficient(
        &self,
        sub_dao: &Pubkey,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<BalanceSufficiency, DebitError> {
        let mut balances = self.balances.lock().await;

        let mut balance = if !balances.contains_key(payer) {
            let new_balance = payer_balance(self.provider.as_ref(), sub_dao, payer).await?;
            balances.insert(payer.clone(), Balance::new(new_balance));
            balances.get_mut(&payer).unwrap()
        } else {
            let mut balance = balances.get_mut(payer).unwrap();

            // If the balance is not sufficient, check to see if it has been increased
            if balance.balance < amount + balance.burned {
                balance.balance = payer_balance(self.provider.as_ref(), sub_dao, payer).await?;
            }

            balance
        };

        let sufficient = if balance.balance >= amount + balance.burned {
            balance.burned += amount;
            BalanceSufficiency::sufficient(&mut balance.enabled)
        } else {
            BalanceSufficiency::insufficient(&mut balance.enabled)
        };

        Ok(sufficient)
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BalanceSufficiency {
    Sufficient { enable: bool },
    Insufficient { disable: bool },
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigureOrgError {
    #[error("Rpc error: {0}")]
    RpcError(#[from] tonic::Status),
    #[error("Crypto error: {0}")]
    CryptoError(#[from] helium_crypto::Error),
}

impl BalanceSufficiency {
    pub fn sufficient(enabled: &mut bool) -> Self {
        Self::Sufficient {
            enable: !mem::replace(enabled, true),
        }
    }

    pub fn insufficient(enabled: &mut bool) -> Self {
        Self::Insufficient {
            disable: mem::replace(enabled, false),
        }
    }

    pub fn is_sufficient(&self) -> bool {
        matches!(self, BalanceSufficiency::Sufficient { .. })
    }

    pub async fn configure_org(
        self,
        client: &mut OrgClient<Channel>,
        keypair: &Keypair,
        oui: u64,
    ) -> Result<(), ConfigureOrgError> {
        match self {
            Self::Sufficient { enable: true } => {
                let mut req = OrgEnableReqV1 {
                    oui,
                    timestamp: Utc::now().timestamp_millis() as u64,
                    signer: keypair.public_key().to_vec(),
                    signature: vec![],
                };
                let signature = keypair.sign(&req.encode_to_vec())?;
                req.signature = signature;
                let _ = client.enable(req).await?;
            }
            Self::Insufficient { disable: true } => {
                let mut req = OrgDisableReqV1 {
                    oui,
                    timestamp: Utc::now().timestamp_millis() as u64,
                    signer: keypair.public_key().to_vec(),
                    signature: vec![],
                };
                let signature = keypair.sign(&req.encode_to_vec())?;
                req.signature = signature;
                let _ = client.disable(req).await?;
            }
            _ => (),
        }
        Ok(())
    }
}

pub async fn payer_balance(
    provider: &RpcClient,
    sub_dao: &Pubkey,
    payer: &PublicKeyBinary,
) -> Result<u64, DebitError> {
    let ddc_key = pdas::delegated_data_credits(sub_dao, payer);
    let account_data = provider.get_account_data(&ddc_key).await?;
    let mut account_data = account_data.as_ref();
    let ddc = DelegatedDataCreditsV0::try_deserialize(&mut account_data)?;
    let account_data = provider.get_account_data(&ddc.escrow_account).await?;
    let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;
    Ok(account_layout.amount)
}

pub struct Balance {
    pub balance: u64,
    pub burned: u64,
    pub enabled: bool,
}

impl Balance {
    pub fn new(balance: u64) -> Self {
        Self {
            balance,
            burned: 0,
            enabled: true,
        }
    }
}
