use crate::{entropy::Entropy, poc_report::Report, Result, Settings};
use file_store::{
    file_sink, file_sink::MessageSender, file_sink_write, file_upload,
    lora_beacon_report::LoraBeaconIngestReport, lora_invalid_poc::LoraInvalidBeaconReport,
    lora_invalid_poc::LoraInvalidWitnessReport, lora_witness_report::LoraWitnessIngestReport,
    traits::IngestId, FileType,
};
use helium_proto::services::poc_lora::{
    InvalidParticipantSide, InvalidReason, LoraBeaconIngestReportV1, LoraInvalidBeaconReportV1,
    LoraInvalidWitnessReportV1, LoraWitnessIngestReportV1,
};
use std::path::Path;

use futures::stream::{self, StreamExt};
use helium_proto::Message;
use sqlx::PgPool;
use tokio::time::{self, MissedTickBehavior};

const DB_POLL_TIME: time::Duration = time::Duration::from_secs(60 * 35);
const PURGER_WORKERS: usize = 40;
const PURGER_DB_POOL_SIZE: usize = 200;

/// the period in seconds after when a beacon report in the DB will be deemed stale
// this period needs to be sufficiently long that we can be sure the beacon has had the
// opportunity to be verified and after this point extremely unlikely to ever be verified
// successfully
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const BEACON_STALE_PERIOD: i64 = 60 * 90;
/// the period in seconds after when a witness report in the DB will be deemed stale
// extend witness stale period beyond that of beacons
// witnesses are inserted into the DB up to 10 mins before beacons
// due to the loader sequentially loading witnesses then beacons
const WITNESS_STALE_PERIOD: i64 = BEACON_STALE_PERIOD + (15 * 60);
/// the period of time in seconds after which entropy will be deemed stale
/// and purged from the DB
// this value should be > that beacon stale period to allow for any beacon
// to be verified right up to the end of its stale period
// any beacon or witness using this entropy & received after this period will fail
// due to being stale
// the report itself will never be verified but instead handled by the stale purger
// this value will be added to the env var BASE_STALE_PERIOD to determine final setting
const ENTROPY_STALE_PERIOD: i64 = BEACON_STALE_PERIOD + (15 * 60);

pub struct Purger {
    pool: PgPool,
    base_stale_period: i64,
    settings: Settings,
}

impl Purger {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(PURGER_DB_POOL_SIZE).await?;
        let settings = settings.clone();
        let base_stale_period = settings.base_stale_period;
        Ok(Self {
            pool,
            settings,
            base_stale_period,
        })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting purger");

        let mut db_timer = time::interval(DB_POLL_TIME);
        db_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let store_base_path = Path::new(&self.settings.cache);
        let (lora_invalid_beacon_tx, lora_invalid_beacon_rx) = file_sink::message_channel(50);
        let (lora_invalid_witness_tx, lora_invalid_witness_rx) = file_sink::message_channel(50);
        let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
        let file_upload =
            file_upload::FileUpload::from_settings(&self.settings.output, file_upload_rx).await?;

        let mut lora_invalid_beacon_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidBeaconReport,
            store_base_path,
            lora_invalid_beacon_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        let mut lora_invalid_witness_sink = file_sink::FileSinkBuilder::new(
            FileType::LoraInvalidWitnessReport,
            store_base_path,
            lora_invalid_witness_rx,
        )
        .deposits(Some(file_upload_tx.clone()))
        .create()
        .await?;

        // spawn off the file sinks
        let shutdown2 = shutdown.clone();
        let shutdown3 = shutdown.clone();
        let shutdown4 = shutdown.clone();
        tokio::spawn(async move { lora_invalid_beacon_sink.run(&shutdown2).await });
        tokio::spawn(async move { lora_invalid_witness_sink.run(&shutdown3).await });
        tokio::spawn(async move { file_upload.run(&shutdown4).await });

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = db_timer.tick() =>
                    match self.handle_db_tick(lora_invalid_beacon_tx.clone(),lora_invalid_witness_tx.clone(), shutdown.clone()).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal purger error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping purger");
        Ok(())
    }

    async fn handle_db_tick(
        &self,
        lora_invalid_beacon_tx: MessageSender,
        lora_invalid_witness_tx: MessageSender,
        shutdown: triggered::Listener,
    ) -> Result {
        // pull stale beacons and witnesses
        // for each we have to write out an invalid report to S3
        // as these wont have previously resulted in a file going to s3
        // once the report is safely on s3 we can then proceed to purge from the db
        let beacon_stale_period = self.base_stale_period + BEACON_STALE_PERIOD;
        tracing::info!(
            "starting query get_stale_pending_beacons with stale period: {beacon_stale_period}"
        );
        let stale_beacons =
            Report::get_stale_pending_beacons(&self.pool, beacon_stale_period).await?;
        tracing::info!("completed query get_stale_pending_beacons");
        let num_stale_beacons = stale_beacons.len();
        tracing::info!("purging {:?} stale beacons", num_stale_beacons);
        let beacon_handler =
            stream::iter(stale_beacons).for_each_concurrent(PURGER_WORKERS, |report| {
                let tx = lora_invalid_beacon_tx.clone();
                async move {
                    match self.handle_purged_beacon(&report, tx).await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to purge beacon: {err:?}")
                        }
                    }
                }
            });
        tokio::select! {
            _ = beacon_handler => {
                    tracing::info!("completed purging {num_stale_beacons} stale beacons");
                },
            _ = shutdown.clone() => (),
        }

        let witness_stale_period = self.base_stale_period + WITNESS_STALE_PERIOD;
        tracing::info!(
            "starting query get_stale_pending_witnesses with stale period: {witness_stale_period}"
        );
        let stale_witnesses =
            Report::get_stale_pending_witnesses(&self.pool, witness_stale_period).await?;
        tracing::info!("completed query get_stale_pending_witnesses");
        let num_stale_witnesses = stale_witnesses.len();
        tracing::info!("purging {num_stale_witnesses} stale witnesses");
        let witness_handler =
            stream::iter(stale_witnesses).for_each_concurrent(PURGER_WORKERS, |report| {
                let tx = lora_invalid_witness_tx.clone();
                async move {
                    match self.handle_purged_witness(&report, tx).await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::warn!("failed to purge witness: {err:?}")
                        }
                    }
                }
            });
        tokio::select! {
            _ = witness_handler => {
                tracing::info!("completed purging {num_stale_witnesses} stale witnesses");
                    },
            _ = shutdown.clone() => (),
        }

        // purge any stale entropy, no need to output anything to s3 here
        let _ = Entropy::purge(&self.pool, self.base_stale_period + ENTROPY_STALE_PERIOD).await;
        Ok(())
    }

    async fn handle_purged_beacon(
        &self,
        db_beacon: &Report,
        lora_invalid_beacon_tx: MessageSender,
    ) -> Result {
        let beacon_buf: &[u8] = &db_beacon.report_data;
        let beacon_report: LoraBeaconIngestReport =
            LoraBeaconIngestReportV1::decode(beacon_buf)?.try_into()?;
        let beacon_id = beacon_report.ingest_id();
        let beacon = &beacon_report.report;
        let received_timestamp = beacon_report.received_timestamp;
        let invalid_beacon_proto: LoraInvalidBeaconReportV1 = LoraInvalidBeaconReport {
            received_timestamp,
            reason: InvalidReason::Stale,
            report: beacon.clone(),
        }
        .into();
        tracing::debug!("purging beacon with date: {received_timestamp}");
        file_sink_write!(
            "invalid_beacon",
            &lora_invalid_beacon_tx,
            invalid_beacon_proto
        )
        .await?;
        // delete the report from the DB
        Report::delete_report(&self.pool, &beacon_id).await
    }

    async fn handle_purged_witness(
        &self,
        db_witness: &Report,
        lora_invalid_witness_tx: MessageSender,
    ) -> Result {
        let witness_buf: &[u8] = &db_witness.report_data;
        let witness_report: LoraWitnessIngestReport =
            LoraWitnessIngestReportV1::decode(witness_buf)?.try_into()?;
        let witness_id = witness_report.ingest_id();
        let received_timestamp = witness_report.received_timestamp;
        let invalid_witness_report_proto: LoraInvalidWitnessReportV1 = LoraInvalidWitnessReport {
            received_timestamp,
            report: witness_report.report,
            reason: InvalidReason::Stale,
            participant_side: InvalidParticipantSide::Witness,
        }
        .into();
        tracing::debug!("purging witness with date: {received_timestamp}");
        file_sink_write!(
            "invalid_witness_report",
            &lora_invalid_witness_tx,
            invalid_witness_report_proto
        )
        .await?;

        // delete the report from the DB
        Report::delete_report(&self.pool, &witness_id).await
    }
}
