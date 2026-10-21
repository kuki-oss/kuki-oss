//! Entry points for the `kuki evaluate` commands. Secrets come from the environment or a `*_FILE`
//! path, never from config files, and are never logged.

use futures::FutureExt;
use std::{collections::BTreeSet, sync::Arc};

use chrono::Utc;
use kuki_extension::Trigger;
use kuki_store::{
    session::{OrgSession, with_org_tx},
    shadow,
};
use serde_json::json;
use sqlx::{PgConnection, PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::{
    builtin,
    evaluator::{Evaluator, EvaluatorOptions},
    registry::Registry,
    runner::{Runner, RunnerConfig, RunnerError},
};

fn secret(name: &str) -> Result<String, RunnerError> {
    if let Ok(v) = std::env::var(name) {
        return Ok(v);
    }
    let file = std::env::var(format!("{name}_FILE"))
        .map_err(|_| RunnerError::Config(format!("{name} (or {name}_FILE) is required")))?;
    let text = std::fs::read_to_string(&file)
        .map_err(|e| RunnerError::Config(format!("cannot read {name}_FILE: {e}")))?;
    Ok(text.trim().to_owned())
}

/// The application role: row-level security applies.
async fn app_pool(max: u32) -> Result<PgPool, RunnerError> {
    let url = secret("KUKI_DATABASE_URL")?;
    Ok(PgPoolOptions::new()
        .max_connections(max)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *c).await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?)
}

/// The privileged role, for administration only.
async fn admin_pool() -> Result<PgPool, RunnerError> {
    let url = secret("KUKI_MIGRATE_DATABASE_URL")?;
    Ok(PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?)
}

async fn resolve_org(c: &mut PgConnection, org: &str) -> Result<Uuid, RunnerError> {
    let found: Option<Uuid> = match org.parse::<Uuid>() {
        Ok(id) => {
            sqlx::query_scalar("SELECT id FROM organisations WHERE id = $1")
                .bind(id)
                .fetch_optional(c)
                .await?
        }
        Err(_) => {
            sqlx::query_scalar("SELECT id FROM organisations WHERE slug = $1")
                .bind(org)
                .fetch_optional(c)
                .await?
        }
    };
    found.ok_or_else(|| RunnerError::NotFound(format!("organisation `{org}`")))
}

fn evaluator(pool: PgPool) -> Result<Arc<Evaluator>, RunnerError> {
    // OSS has no inference, priors or LLM, so no capabilities.
    let registry =
        Registry::builtin(&BTreeSet::new()).map_err(|e| RunnerError::Config(e.to_string()))?;
    Ok(Arc::new(Evaluator::new(
        pool,
        Arc::new(registry),
        EvaluatorOptions::default(),
    )))
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

/// `kuki evaluate run`: react to changes and re-evaluate on a schedule until SIGINT or SIGTERM.
pub async fn run_from_env() -> Result<(), RunnerError> {
    let pool = app_pool(5).await?;
    let cfg = RunnerConfig::from_env()?;
    tracing::info!(
        debounce_ms = cfg.debounce.as_millis() as u64,
        tick_secs = cfg.tick.as_secs(),
        "evaluator starting"
    );
    let runner = Arc::new(Runner::new(pool.clone(), evaluator(pool)?, cfg));
    runner.run(shutdown_signal()).await;
    Ok(())
}

/// `kuki evaluate once --org X`: one full evaluation, returned as pretty JSON. The organisation is
/// resolved with the privileged URL; the evaluation itself runs as the application role.
pub async fn once_from_env(org: &str) -> Result<String, RunnerError> {
    let org_id = resolve_org(&mut *admin_pool().await?.acquire().await?, org).await?;
    let pool = app_pool(3).await?;
    let trigger = Trigger {
        organisation_id: org_id,
        topic: "manual".into(),
        entities: vec![],
    };
    let report = evaluator(pool)?.run(&trigger, Utc::now()).await?;
    serde_json::to_string_pretty(&report).map_err(|e| RunnerError::Config(e.to_string()))
}

/// `kuki evaluate install --org X`: registers the built-in decision types for the organisation.
pub async fn install_from_env(org: &str) -> Result<Vec<String>, RunnerError> {
    let pool = admin_pool().await?;
    let mut c = pool.acquire().await?;
    let org_id = resolve_org(&mut c, org).await?;
    builtin::install_decision_types(&mut c, org_id).await?;
    Ok(builtin::decision_types()
        .iter()
        .map(|d| d.name.to_owned())
        .collect())
}

/// `kuki shadow report <generator> --org X`: what a generator in shadow mode has proposed,
/// most urgent first, as pretty JSON. The organisation is resolved with the privileged URL; the
/// read runs as the application role, under row-level security.
pub async fn shadow_report_from_env(
    org: &str,
    generator: &str,
    limit: i64,
) -> Result<String, RunnerError> {
    let org_id = resolve_org(&mut *admin_pool().await?.acquire().await?, org).await?;
    let pool = app_pool(2).await?;
    let session = OrgSession {
        org_id,
        scope_path: None,
    };
    let generator = generator.to_owned();
    let rows = with_org_tx::<_, sqlx::Error, _>(&pool, &session, {
        let generator = generator.clone();
        move |c| async move { shadow::list(c, org_id, &generator, limit).await }.boxed()
    })
    .await?;
    let proposals: Vec<_> = rows
        .iter()
        .map(|r| {
            json!({
                "dedup_key": r.dedup_key,
                "decision_type": r.decision_type,
                "org_unit_id": r.org_unit_id,
                "urgency": r.urgency,
                "confidence": r.confidence,
                "summary": r.summary,
                "first_seen_at": r.first_seen_at,
                "last_seen_at": r.last_seen_at,
                "seen_count": r.seen_count,
                "body": r.body,
            })
        })
        .collect();
    serde_json::to_string_pretty(&json!({
        "organisation_id": org_id,
        "generator_id": generator,
        "count": proposals.len(),
        "proposals": proposals,
    }))
    .map_err(|e| RunnerError::Config(e.to_string()))
}
