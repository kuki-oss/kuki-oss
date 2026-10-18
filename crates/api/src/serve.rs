use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::http::header;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use thiserror::Error;
use tower_http::sensitive_headers::SetSensitiveRequestHeadersLayer;
use kuki_outbox::{EventFeed, KafkaConfig};
use kuki_store::outbox::{KNOWN_TOPICS, TOPIC_RECOMMENDATIONS};
use tokio::{sync::watch, task::JoinHandle};

use crate::bao::{BaoClient, BaoHasher, BaoReadiness};
use crate::config::ApiConfig;
use crate::pg::{AllReady, PgKeyStore, PgReadiness};
use crate::ratelimit::RateLimitConfig;
use crate::routes::router;
use crate::state::{AppState, Readiness};

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("secrets backend client: {0}")]
    Bao(#[from] reqwest::Error),
    #[error("cannot listen on {addr}: {source}")]
    Listen {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("server: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration: {0}")]
    Config(String),
}

/// A pool that connects as the application role (`kuki_app`, NOBYPASSRLS).
pub async fn connect_app_pool(url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect(url)
        .await
}

pub async fn build_state(config: ApiConfig) -> Result<AppState, ServeError> {
    let pool = connect_app_pool(config.database_url.expose_str(), 10).await?;

    let bao = Arc::new(BaoClient::new(
        config.bao_addr.clone(),
        config.bao_token.clone(),
    )?);
    let hasher = Arc::new(BaoHasher {
        client: bao.clone(),
        key_name: config.bao_transit_key.clone(),
    });

    let checks: Vec<Arc<dyn Readiness>> = vec![
        Arc::new(PgReadiness(pool.clone())),
        Arc::new(BaoReadiness(bao)),
    ];

    Ok(AppState {
        config: Arc::new(config),
        pool: pool.clone(),
        keys: Arc::new(PgKeyStore(pool)),
        hasher,
        ready: Arc::new(AllReady(checks)),
        extensions: Arc::new(kuki_extension::Extensions::default()),
    })
}

struct LiveFeed {
    feed: EventFeed,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

/// Live recommendation events for GraphQL subscriptions. Off unless KUKI_KAFKA_BROKERS is set.
async fn start_live_feed() -> Result<Option<LiveFeed>, ServeError> {
    if std::env::var_os("KUKI_KAFKA_BROKERS").is_none() {
        tracing::warn!("KUKI_KAFKA_BROKERS is not set: GraphQL subscriptions are disabled");
        return Ok(None);
    }
    let cfg = |e: kuki_outbox::RelayError| ServeError::Config(e.to_string());
    let kafka = KafkaConfig::from_env().map_err(cfg)?;
    kuki_outbox::ensure_topics(&kafka, KNOWN_TOPICS).await.map_err(cfg)?;
    let (stop, stopped) = watch::channel(false);
    let (feed, task) =
        EventFeed::start(&kafka, TOPIC_RECOMMENDATIONS, 1024, stopped).map_err(cfg)?;
    tracing::info!(brokers = %kafka.brokers, "live recommendation feed started");
    Ok(Some(LiveFeed { feed, stop, task }))
}

pub async fn serve(config: ApiConfig) -> Result<(), ServeError> {
    let bind = config.bind;
    let limits = RateLimitConfig::from_env().map_err(ServeError::Config)?;
    let state = build_state(config).await?;
    let live = start_live_feed().await?;

    let mut app = router(state);
    match limits.limiter() {
        Some(limiter) => {
            tracing::info!(
                burst = limits.burst,
                refill_per_second = limits.refill_per_sec,
                "per-key rate limiting enabled (in-process; limits are per server instance)"
            );
            app = app.layer(axum::Extension(Arc::new(limiter)));
        }
        None => tracing::warn!("per-key rate limiting is disabled (KUKI_RATE_LIMIT_BURST=0)"),
    }
    if let Some(l) = &live {
        app = app.layer(axum::Extension(l.feed.clone()));
    }
    let app = app.layer(SetSensitiveRequestHeadersLayer::new([
        header::AUTHORIZATION,
    ]));

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|source| ServeError::Listen { addr: bind, source })?;
    tracing::info!(%bind, "kuki-api listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    if let Some(l) = live {
        let _ = l.stop.send(true);
        let _ = l.task.await;
    }
    Ok(())
}

pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
    tracing::info!("shutting down");
}
