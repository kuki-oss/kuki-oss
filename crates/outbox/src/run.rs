use std::sync::Arc;

use kuki_store::outbox::KNOWN_TOPICS;
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::error::RelayError;
use crate::kafka::{KafkaConfig, KafkaPublisher, ensure_topics};
use crate::relay::{Relay, RelayConfig};
use crate::consume::{ConsumerConfig, FanoutConsumer};

/// `NAME`, or the contents of the file named by `NAME_FILE` (Docker and Kubernetes secrets).
fn secret(name: &str) -> Result<String, RelayError> {
    if let Ok(path) = std::env::var(format!("{name}_FILE")) {
        return std::fs::read_to_string(&path)
            .map(|s| s.trim().to_owned())
            .map_err(|e| RelayError::Config(format!("reading {name}_FILE: {e}")));
    }
    std::env::var(name)
        .map_err(|_| RelayError::Config(format!("{name} (or {name}_FILE) is required")))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}

/// A pool that connects as the application role.
async fn app_pool(max_connections: u32) -> Result<PgPool, RelayError> {
    let url = secret("KUKI_DATABASE_URL")?;
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *c).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?)
}

/// The `kuki outbox` role: connect as the application role, make sure the topics exist,
/// relay until SIGINT or SIGTERM.
pub async fn run_from_env() -> Result<(), RelayError> {
    let pool = app_pool(3).await?;

    let kafka = KafkaConfig::from_env()?;
    ensure_topics(&kafka, KNOWN_TOPICS).await?;
    let publisher = KafkaPublisher::connect(&kafka)?;

    tracing::info!(brokers = %kafka.brokers, "outbox relay starting");
    Relay::new(pool, Arc::new(publisher), RelayConfig::default())
        .run(shutdown_signal())
        .await
}

/// The `kuki webhooks fanout` role: consume recommendation events from kafka and queue them
/// for subscribed receivers, until SIGINT or SIGTERM.
pub async fn run_fanout_from_env() -> Result<(), RelayError> {
    let pool = app_pool(3).await?;
    let kafka = KafkaConfig::from_env()?;
    let cfg = ConsumerConfig::from_env()?;
    ensure_topics(&kafka, KNOWN_TOPICS).await?;
    let consumer = FanoutConsumer::connect(&kafka, &cfg, pool)?;

    tracing::info!(
        brokers = %kafka.brokers,
        group = %cfg.group_id,
        topic = %cfg.topic,
        from_start = cfg.from_start,
        "recommendation event fan-out starting"
    );
    consumer.run(shutdown_signal()).await
}
