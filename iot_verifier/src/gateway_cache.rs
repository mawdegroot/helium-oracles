use crate::{helius, Settings};
use futures::stream::TryStreamExt;
use helium_crypto::PublicKeyBinary;
use helius::GatewayInfo;
use retainer::Cache;
use sqlx::PgPool;
use std::time::Duration;

pub const CACHE_TTL: u64 = 86400;
const HELIUS_DB_POOL_SIZE: usize = 100;

pub struct GatewayCache {
    pool: PgPool,
    pub cache: Cache<PublicKeyBinary, GatewayInfo>,
}

#[derive(thiserror::Error, Debug)]
#[error("error creating gateway cache: {0}")]
pub struct NewGatewayCacheError(#[from] db_store::Error);

#[derive(thiserror::Error, Debug)]
#[error("gateway not found: {0}")]
pub struct GatewayNotFound(PublicKeyBinary);

impl GatewayCache {
    pub async fn from_settings(settings: &Settings) -> Result<Self, NewGatewayCacheError> {
        let pool = settings.database.connect(HELIUS_DB_POOL_SIZE).await?;
        let cache = Cache::<PublicKeyBinary, GatewayInfo>::new();
        Ok(Self { pool, cache })
    }

    pub async fn prewarm(&self) -> anyhow::Result<()> {
        let sql = r#"SELECT address, location, elevation, gain, is_full_hotspot FROM gateways"#;
        let mut rows = sqlx::query_as::<_, GatewayInfo>(sql).fetch(&self.pool);
        while let Some(gateway) = rows.try_next().await? {
            self.cache
                .insert(
                    gateway.address.clone(),
                    gateway.clone(),
                    Duration::from_secs(CACHE_TTL),
                )
                .await;
        }
        Ok(())
    }

    pub async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<GatewayInfo, GatewayNotFound> {
        match self.cache.get(address).await {
            Some(hit) => {
                metrics::increment_counter!("oracles_iot_verifier_gateway_cache_hit");
                Ok(hit.value().clone())
            }
            _ => match GatewayInfo::resolve_gateway(&self.pool, address.as_ref()).await {
                Ok(Some(res)) => {
                    metrics::increment_counter!("oracles_iot_verifier_gateway_cache_miss");
                    self.cache
                        .insert(address.clone(), res.clone(), Duration::from_secs(CACHE_TTL))
                        .await;
                    Ok(res)
                }
                _ => Err(GatewayNotFound(address.clone())),
            },
        }
    }
}
