//! kuki-engine daemon entrypoint.
//!
//! Listens for real-time PostgreSQL Change Data Capture (CDC) events on
//! channels, i.e, `kuki_stock_change` and `kuki_order_created`. On incoming events,
//! loads merchant context, queries SurrealDB supply graph edges, evaluates
//! the active RecommendationGenerator (Phase 0 rule or Phase 1 ML), and writes
//! resulting proposals to `kuki_recommendations`.
//!
//! TODO: Prod should not allow this to be ran as a CLI
//! CLI flags:
//!   --run-once <merchant_id>   Evaluate a merchant immediately and exit.
//!   --engine <rule|ml>         Select Phase 0 (rule) or Phase 1 (ml) generator. Default: rule.

mod auto_execution_gate;
mod generator;
mod restock_ml_model;
mod restock_rule_engine;
mod types;

use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::types::BigDecimal;
use std::str::FromStr;
use sqlx::postgres::PgListener;
use std::env;
use std::sync::Arc;
use std::collections::HashMap;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::generator::{MerchantContext, RecommendationGenerator};
use crate::restock_ml_model::RestockMLModel;
use crate::restock_rule_engine::RestockRuleEngine;
use crate::types::KukiRecommendation;

use common::surreal::SurrealClient;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("KUKI_ENGINE_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI args
    let args: Vec<String> = env::args().collect();
    let run_once_id = arg_value(&args, "--run-once");
    let engine_mode = arg_value(&args, "--engine").unwrap_or_else(|| "rule".to_string());

    // Build database pool
    let database_url = env::var("API_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .context("API_DATABASE_URL or DATABASE_URL must be set")?;
    let pool = common::db::connect(&database_url).await;

    // Build SurrealDB client
    let surreal_url = env::var("SURREAL_URL").unwrap_or_else(|_| "ws://localhost:8000".to_string());
    let surreal_user = env::var("SURREAL_USER").unwrap_or_else(|_| "root".to_string());
    let surreal_pass = env::var("SURREAL_PASS").unwrap_or_else(|_| "root".to_string());
    let surreal_ns = env::var("SURREAL_NS").unwrap_or_else(|_| "merchant".to_string());
    let surreal = common::surreal::SurrealClient::connect(
        &surreal_url,
        surreal_user,
        surreal_pass,
        &surreal_ns,
        "kuki_instance"
    )
    .await
    .context("failed to connect to SurrealDB")?;

    // Build engine registry keyed by engine id. All variants are constructed
    // at startup, since dispatch happens per-notification, not once at
    // process start. Currently the CLI flag still controls --run-once's one-
    // shot behaviour, where there's no notification payload to read a choice from
    let mut registry: HashMap<String, Arc<dyn RecommendationGenerator>> = HashMap::new();
    registry.insert("rule".to_string(), Arc::new(RestockRuleEngine) as Arc<dyn RecommendationGenerator>);
    registry.insert("ml".to_string(), Arc::new(RestockMLModel) as Arc<dyn RecommendationGenerator>);
    let registry = Arc::new(registry);

    // Still reads CLI --engine flag directly since there's no notification
    // payload here.
    if let Some(id_str) = run_once_id {
        // ── On-demand demo use-case ────────────────────────────────────────
        let merchant_id = Uuid::parse_str(&id_str)
            .with_context(|| format!("Invalid UUID: {id_str}"))?;
        let generator = registry
            .get(&engine_mode)
            .cloned()
            .unwrap_or_else(|| registry.get("rule").cloned().unwrap());
        info!("⚡ Running once for merchant {merchant_id}");
        run_for_merchant(merchant_id, &pool, &surreal, &*generator).await?;
        return Ok(());
    }

    // ── Listener mode ─────────────────────────────────────────────────
    // Payload is now JSON, not a bare UUID string
    #[derive(serde::Deserialize)]
    struct EngineEventPayload {
        merchant_id: Uuid,
        #[serde(default = "default_engine")]
        engine: String,
    }
    fn default_engine() -> String { "rule".to_string() }

    info!("👂 Listening on channel 'kuki_engine_events' ...");
    let mut listener = PgListener::connect_with(&pool).await?;
    listener.listen("kuki_engine_events").await?;

    loop {
        match listener.recv().await {
            Ok(notification) => {
                let payload_str = notification.payload().to_string();

                let event: EngineEventPayload = match serde_json::from_str(&payload_str) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("kuki_engine_events: malformed JSON payload '{payload_str}': {e}");
                        continue;
                    }
                };

                let generator = match registry.get(&event.engine) {
                    Some(g) => g.clone(),
                    None => {
                        warn!("kuki_engine_events: unknown engine '{}', falling back to 'rule'", event.engine);
                        registry.get("rule").cloned().unwrap()
                    }
                };

                info!("🔔 Event received for merchant {} (engine: {})", event.merchant_id, event.engine);
                let pool_clone = pool.clone();
                let surreal_clone = surreal.clone();
                tokio::spawn(async move {
                    if let Err(e) = run_for_merchant(event.merchant_id, &pool_clone, &surreal_clone, &*generator).await {
                        error!("Engine run failed for {}: {e:#}", event.merchant_id);
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

async fn run_for_merchant(
    merchant_id: Uuid,
    pool: &sqlx::PgPool,
    surreal: &SurrealClient,
    generator: &dyn RecommendationGenerator,
) -> Result<()> {
    let ctx = MerchantContext::load(merchant_id, pool, surreal).await?;
    info!(
        "📦 Loaded context: {} products, {} movements, {} pending recs",
        ctx.products.len(),
        ctx.stock_movements.len(),
        ctx.pending_recs.len()
    );

    let recs = generator.evaluate(&ctx).await?;
    info!("✨ {} recommendation(s) generated by [{}]", recs.len(), generator.name());

    let merchant_kind = crate::auto_execution_gate::fetch_merchant_kind(pool, merchant_id).await?;
    let gate = crate::auto_execution_gate::AutoExecutionGate;

    for mut rec in recs {
        rec.status = gate.evaluate(&rec, &merchant_kind).await;
        persist_recommendation(&rec, pool).await?;
        info!(
            "  → Persisted {} rec for product in payload, confidence: {:.2}, source: {}",
            rec.r#type.as_str(),
            rec.confidence,
            rec.source
        );
    }

    Ok(())
}

async fn persist_recommendation(rec: &KukiRecommendation, pool: &sqlx::PgPool) -> Result<()> {
    let rationale_json = serde_json::to_value(&rec.rationale)?;
    let confidence = BigDecimal::from_str(&format!("{:.6}", rec.confidence))
        .unwrap_or_else(|_| BigDecimal::from(0));

    sqlx::query(
        r#"INSERT INTO kuki_recommendations
               (id, merchant_id, type, source, payload, rationale, confidence, status, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"#,
    )
    .bind(rec.id)
    .bind(rec.merchant_id)
    .bind(rec.r#type.as_str())
    .bind(&rec.source)
    .bind(&rec.payload)
    .bind(&rationale_json)
    .bind(confidence)
    .bind(rec.status.as_str())
    .bind(Utc::now())
    .execute(pool)
    .await
    .context("Failed to persist recommendation")?;

    Ok(())
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2)
        .find(|w| w[0] == flag)
        .map(|w| w[1].clone())
}
