use futures::stream::StreamExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use serde::Serialize;
use sqlx::{types::Uuid, Row};

use crate::{
    lora_field::{DevAddrField, DevAddrRange, NetIdField},
    HELIUM_NET_ID,
};

pub mod proto {
    pub use helium_proto::services::iot_config::{OrgResV1, OrgV1};
}

#[derive(Clone, Debug, PartialEq, Serialize, sqlx::Type)]
#[sqlx(type_name = "org_status", rename_all = "snake_case")]
pub enum OrgStatus {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct Org {
    #[sqlx(try_from = "i64")]
    pub oui: u64,
    #[sqlx(rename = "owner_pubkey")]
    pub owner: PublicKeyBinary,
    #[sqlx(rename = "payer_pubkey")]
    pub payer: PublicKeyBinary,
    pub delegate_keys: Vec<PublicKeyBinary>,
    pub status: OrgStatus,
}

#[derive(Debug, Serialize)]
pub struct OrgList {
    orgs: Vec<Org>,
}

#[derive(Debug)]
pub struct OrgWithConstraints {
    pub org: Org,
    pub constraints: DevAddrRange,
}

pub async fn create_org(
    owner: PublicKeyBinary,
    payer: PublicKeyBinary,
    delegate_keys: Vec<PublicKeyBinary>,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Org, sqlx::Error> {
    let org = sqlx::query_as::<_, Org>(
        r#"
        insert into organizations (owner_pubkey, payer_pubkey, delegate_keys)
        values ($1, $2, $3)
        on conflict (owner_pubkey) do nothing
        returning *
        "#,
    )
    .bind(&owner)
    .bind(&payer)
    .bind(&delegate_keys)
    .fetch_one(db)
    .await?;

    Ok(org)
}

pub async fn insert_constraints(
    oui: u64,
    net_id: NetIdField,
    devaddr_range: &DevAddrRange,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        insert into organization_devaddr_constraints (oui, net_id, start_addr, end_addr)
        values ($1, $2, $3, $4)
        "#,
    )
    .bind(oui as i64)
    .bind(i64::from(net_id))
    .bind(i64::from(devaddr_range.start_addr))
    .bind(i64::from(devaddr_range.end_addr))
    .execute(db)
    .await
    .map(|_| ())
}

pub async fn list(db: impl sqlx::PgExecutor<'_>) -> Result<Vec<Org>, sqlx::Error> {
    Ok(sqlx::query_as::<_, Org>(
        r#"
        select * from organizations
        "#,
    )
    .fetch(db)
    .filter_map(|row| async move { row.ok() })
    .collect::<Vec<Org>>()
    .await)
}

pub async fn get(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<Org, sqlx::Error> {
    sqlx::query_as::<_, Org>(
        r#"
        select * from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await
}

pub async fn get_with_constraints(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<OrgWithConstraints, sqlx::Error> {
    let row = sqlx::query(
        r#"
        select org.owner_pubkey, org.payer_pubkey, org.delegate_keys, org.status, org_const.start_addr, org_const.end_addr
        from organizations org join organization_devaddr_constraints org_const
        on org.oui = org_const.oui
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await?;

    let start_addr = row.get::<i64, &str>("start_addr");
    let end_addr = row.get::<i64, &str>("end_addr");

    Ok(OrgWithConstraints {
        org: Org {
            oui,
            owner: row.get("owner_pubkey"),
            payer: row.get("payer_pubkey"),
            delegate_keys: row.get("delegate_keys"),
            status: row.get("status"),
        },
        constraints: DevAddrRange {
            start_addr: start_addr.into(),
            end_addr: end_addr.into(),
        },
    })
}

pub async fn get_status(oui: u64, db: impl sqlx::PgExecutor<'_>) -> Result<OrgStatus, sqlx::Error> {
    sqlx::query_scalar::<_, OrgStatus>(
        r#"
        select status from organizations where oui = $1
        "#,
    )
    .bind(oui as i64)
    .fetch_one(db)
    .await
}

pub async fn toggle_status(
    oui: u64,
    status: OrgStatus,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        update organizations
        set status = $1
        where oui = $2
        "#,
    )
    .bind(status)
    .bind(oui as i64)
    .execute(db)
    .await?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum OrgPubkeysError {
    #[error("error retrieving saved org keys: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("unable to deserialize pubkey: {0}")]
    DecodeError(#[from] helium_crypto::Error),
    #[error("Route Id parse error: {0}")]
    RouteIdParse(#[from] sqlx::types::uuid::Error),
}

pub async fn get_org_pubkeys(
    oui: u64,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgPubkeysError> {
    let org = get(oui, db).await?;

    let mut pubkeys: Vec<PublicKey> = vec![PublicKey::try_from(org.owner)?];

    let mut delegate_pubkeys: Vec<PublicKey> = org
        .delegate_keys
        .into_iter()
        .map(PublicKey::try_from)
        .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()?;

    pubkeys.append(&mut delegate_pubkeys);

    Ok(pubkeys)
}

pub async fn get_org_pubkeys_by_route(
    route_id: &str,
    db: impl sqlx::PgExecutor<'_>,
) -> Result<Vec<PublicKey>, OrgPubkeysError> {
    let uuid = Uuid::try_parse(route_id)?;

    let org = sqlx::query_as::<_, Org>(
        r#"
        select * from organizations
        join routes on organizations.oui = routes.oui
        where routes.id = $1
        "#,
    )
    .bind(uuid)
    .fetch_one(db)
    .await?;

    let mut pubkeys: Vec<PublicKey> = vec![PublicKey::try_from(org.owner)?];

    let mut delegate_keys: Vec<PublicKey> = org
        .delegate_keys
        .into_iter()
        .map(PublicKey::try_from)
        .collect::<Result<Vec<PublicKey>, helium_crypto::Error>>()?;

    pubkeys.append(&mut delegate_keys);

    Ok(pubkeys)
}

#[derive(thiserror::Error, Debug)]
pub enum NextHeliumDevAddrError {
    #[error("error retrieving next available addr: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("invalid devaddr from netid: {0}")]
    InvalidNetId(#[from] crate::lora_field::InvalidNetId),
}

#[derive(sqlx::FromRow)]
struct NextHeliumDevAddr {
    coalesce: i64,
}

pub async fn next_helium_devaddr(
    db: impl sqlx::PgExecutor<'_>,
) -> Result<DevAddrField, NextHeliumDevAddrError> {
    let helium_default_start: i64 = HELIUM_NET_ID.range_start()?.into();

    let addr = sqlx::query_as::<_, NextHeliumDevAddr>(
            r#"
            select coalesce(max(end_addr), $1) from organization_devaddr_constraints where net_id = $2
            "#,
        )
        .bind(helium_default_start)
        .bind(i64::from(HELIUM_NET_ID))
        .fetch_one(db)
        .await?
        .coalesce;

    let next_addr = if addr == helium_default_start {
        addr
    } else {
        addr + 1
    };

    tracing::info!("next helium devaddr start {addr}");

    Ok(next_addr.into())
}

impl From<proto::OrgV1> for Org {
    fn from(org: proto::OrgV1) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            delegate_keys: org
                .delegate_keys
                .into_iter()
                .map(|key| key.into())
                .collect(),
            status: OrgStatus::Enabled,
        }
    }
}

#[allow(deprecated)]
impl From<Org> for proto::OrgV1 {
    fn from(org: Org) -> Self {
        Self {
            oui: org.oui,
            owner: org.owner.into(),
            payer: org.payer.into(),
            delegate_keys: org
                .delegate_keys
                .iter()
                .map(|key| key.as_ref().into())
                .collect(),
            // Deprecated proto field; flagged above to avoid compiler warning
            nonce: 0,
        }
    }
}
