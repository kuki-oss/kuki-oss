//! Action executions. A resolution queues a row; the dispatcher consumes it.

use chrono::{DateTime, Utc};
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
