use crate::{
    gateway_updater::MessageReceiver,
    hex_density::{compute_hex_density_map, GlobalHexMap, HexDensityMap, SharedHexDensityMap},
    last_beacon::LastBeacon,
    Settings,
};
use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;
use std::collections::HashMap;

// The number in minutes within which the gateway has registered a beacon
// to the oracle for inclusion in transmit scaling density calculations
const HIP_17_INTERACTIVITY_LIMIT: i64 = 3600;

pub struct Server {
    hex_density_map: SharedHexDensityMap,
    pool: PgPool,
    refresh_offset: Duration,
    gateway_cache_receiver: MessageReceiver,
}

#[derive(Debug, thiserror::Error)]
pub enum TxScalerError {
    #[error("tx scaler db connect error")]
    DbConnect(#[from] db_store::Error),
    #[error("txn scaler error retrieving recent activity")]
    RecentActivity(#[from] sqlx::Error),
}

impl Server {
    pub async fn from_settings(
        settings: &Settings,
        pool: PgPool,
        gateway_cache_receiver: MessageReceiver,
    ) -> Result<Self, TxScalerError> {
        let mut server = Self {
            hex_density_map: SharedHexDensityMap::new(),
            pool,
            refresh_offset: settings.loader_window_max_lookback_age(),
            gateway_cache_receiver,
        };

        server.refresh_scaling_map().await?;

        Ok(server)
    }

    pub fn hex_density_map(&self) -> impl HexDensityMap {
        self.hex_density_map.clone()
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result<(), TxScalerError> {
        tracing::info!("density_scaler: starting transmit scaler process");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("density_scaler: stopping transmit scaler");
                return Ok(());
            }

            tokio::select! {
                _ = self.gateway_cache_receiver.changed() => self.refresh_scaling_map().await?,
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn refresh_scaling_map(&mut self) -> Result<(), TxScalerError> {
        let refresh_start = Utc::now() - self.refresh_offset;
        tracing::info!("density_scaler: generating hex scaling map, starting at {refresh_start:?}");
        let mut global_map = GlobalHexMap::new();
        let active_gateways = self
            .gateways_recent_activity(refresh_start)
            .await
            .map_err(sqlx::Error::from)?;
        for k in active_gateways.keys() {
            let pubkey = PublicKeyBinary::from(k.clone());
            if let Some(gateway_info) = self.gateway_cache_receiver.borrow().get(&pubkey) {
                if let Some(metadata) = &gateway_info.metadata {
                    global_map.increment_unclipped(metadata.location)
                }
            }
        }
        global_map.reduce_global();
        let new_map = compute_hex_density_map(&global_map);
        tracing::info!(
            "density_scaler: scaling factor map entries: {}",
            new_map.len()
        );
        self.hex_density_map.swap(new_map).await;
        tracing::info!(
            "density_scaler: generating hex scaling map, completed at {:?}",
            Utc::now()
        );
        Ok(())
    }

    async fn gateways_recent_activity(
        &self,
        now: DateTime<Utc>,
    ) -> Result<HashMap<Vec<u8>, DateTime<Utc>>, sqlx::Error> {
        let interactivity_deadline = now - Duration::minutes(HIP_17_INTERACTIVITY_LIMIT);
        Ok(
            LastBeacon::get_all_since(interactivity_deadline, &self.pool)
                .await?
                .into_iter()
                .map(|beacon| (beacon.id, beacon.timestamp))
                .collect::<HashMap<Vec<u8>, DateTime<Utc>>>(),
        )
    }
}
