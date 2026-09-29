//! ETL Ingestion Worker daemon entrypoint.
//!
//! Listens for `merchant_upload_pending` notifications fired by the
//! merchant_uploads table's trigger. On each event: fetches the
//! merchant_uploads row, downloads the raw file from S3 by s3_key, runs
//! the shared common::data_upload parsing pipeline against it, and writes
//! the result back as parsed_preview (jsonb) + a terminal status
//! (preview_ready or parse_failed).
//!
//! This worker never writes to categories/products/suppliers/stock_movements
//! directly, commiting a confirmed upload is api-service's job

mod processor;
mod types;

use anyhow::{Context, Result};
use sqlx::postgres::PgListener;
use std::env;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ETL_WORKER_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let database_url = env::var("API_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .context("API_DATABASE_URL or DATABASE_URL must be set")?;
    let pool = common::db::connect(&database_url).await;

    let s3_client = common::s3::build_s3_client().await;
    let bucket = env::var("S3_BUCKET").context("S3_BUCKET must be set")?;

    info!("👂 Listening on channel 'merchant_upload_pending' ...");
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("merchant_upload_pending").await?;

    loop {
        match listener.recv().await {
            Ok(notification) => {
                let payload = notification.payload().to_string();
                let upload_id = match Uuid::parse_str(&payload) {
                    Ok(id) => id,
                    Err(_) => {
                        warn!("merchant_upload_pending: invalid upload_id payload: {payload}");
                        continue;
                    }
                };
                info!("🔔 Upload pending: {upload_id}");
                let pool_clone = pool.clone();
                let s3_clone = s3_client.clone();
                let bucket_clone = bucket.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        processor::process_upload(upload_id, &pool_clone, &s3_clone, &bucket_clone).await
                    {
                        error!("Processing failed for upload {upload_id}: {e:#}");
                    }
                });
            }
            Err(e) => {
                error!("Listener error: {e:#}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}
