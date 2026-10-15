//! Action executions. A resolution queues a row; the dispatcher consumes it.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::recommendation_reads::ExecutionRow;

/// The adapter of a freshly queued execution. The dispatcher assigns
/// the real claim time.
pub const UNROUTED: &str = "unrouted";

pub struct NewExecution<'a> {
    pub id: Uuid,
    pub recommendation_id: Uuid,
    pub action_type: &'a str,
    pub input: &'a serde_json::Value,
    pub idempotency_key: &'a str,
    pub created_by: &'a str,
    pub now: DateTime<Utc>,
}

/// Queues an execution. `None` means this idempotency key was already
/// queued.
pub async fn enqueue(
    c: &mut PgConnection,
    org: Uuid,
    n: &NewExecution<'_>,
) -> Result<Option<ExecutionRow>, sqlx::Error> {
    sqlx::query_as(
        "INSERT INTO action_executions
           (id, organisation_id, recommendation_id, action_type, input, idempotency_key,
            status, adapter, created_by, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'queued', $7, $8, $9)
         ON CONFLICT (organisation_id, idempotency_key) DO NOTHING
         RETURNING id, action_type, status, adapter, attempts, created_by, created_at, finished_at, result",
    )
    .bind(n.id)
    .bind(org)
    .bind(n.recommendation_id)
    .bind(n.action_type)
    .bind(n.input)
    .bind(n.idempotency_key)
    .bind(UNROUTED)
    .bind(n.created_by)
    .bind(n.now)
    .fetch_optional(c)
    .await
}

/// An execution claimed for running, in whichever organisation it belongs to.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct Claimed {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub recommendation_id: Uuid,
    pub action_type: String,
    pub input: Value,
    pub adapter: String,
    /// This attempt's number: 1 on the first claim.
    pub attempts: i32,
    pub created_by: String,
}

/// How an execution ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Final {
    Succeeded,
    Failed,
}

impl Final {
    fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Claims up to `limit` due executions for `least_seconds`, in every organisation.
/// Cross-organisation, so it goes through the SECURITY DEFINER functions of
/// migration 20261014224427, not the table.
pub async fn claim(
    c: &mut PgConnection,
    limit: i32,
    lease_seconds: f64,
) -> Result<Vec<Claimed>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, organisation_id, recommendation_id, action_type, input, adapter, attempts, created_by
           FROM execution_claim($1, $2)",
    )
    .bind(limit)
    .bind(lease_seconds)
    .fetch_all(c)
    .await
}

/// Ends a running execution. Run it in the organisation's transaction with the
/// recommendation's own transition. `false`: it was not running (or not this
/// organisation's).
pub async fn finish(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    outcome: Final,
    adapter: &str,
    result: &Value,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT execution_finish($1, $2, $3, $4, $5)")
        .bind(org)
        .bind(id)
        .bind(outcome.as_str())
        .bind(adapter)
        .bind(result)
        .fetch_one(c)
        .await
}

/// Queues a running execution again after `delay_seconds`. `false`: it was not running
pub async fn retry(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    adapter: &str,
    error: &str,
    delay_seconds: f64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT execution_retry($1, $2, $3, $4, $5)")
        .bind(org)
        .bind(id)
        .bind(adapter)
        .bind(error)
        .bind(delay_seconds)
        .fetch_one(c)
        .await
}
