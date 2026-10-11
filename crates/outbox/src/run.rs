use std::sync::Arc;

use kuki_store::outbox::KNOWN_TOPICS;
use sqlx::postgres::PgPoolOptions;

use crate::error::RelayError;
use crate::kafka::{KafkaConfig, KafkaPublisher, ensure_topics};
use crate::relay::{Relay, RelayConfig};

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

/// The `kuki outbox` role: connect as the application role, make sure the topics exist,
/// relay until SIGINT or SIGTERM.
pub async fn run_from_env() -> Result<(), RelayError> {
    let url = secret("KUKI_DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *c).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;

    let kafka = KafkaConfig::from_env()?;
    ensure_topics(&kafka, KNOWN_TOPICS).await?;
    let publisher = KafkaPublisher::connect(&kafka)?;

    tracing::info!(brokers = %kafka.brokers, "outbox relay starting");
    Relay::new(pool, Arc::new(publisher), RelayConfig::default())
        .run(shutdown_signal())
        .await
}
