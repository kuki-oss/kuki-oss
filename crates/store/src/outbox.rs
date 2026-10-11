//! The transactional outbox. A state change calls `enqueue` inside its own transaction,
//! so the event exists if and only if the change committed. The relay (kuki-outbox) drains
//! the table through four SECURITY DEFINER functions, because it works across organisations
//! and cannot run under per-tenant row-level security.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

/// All recommendation events share one topic, keyed by recommendation id, so a
/// recommendation's events stay in order on one partition.
pub const TOPIC_RECOMMENDATIONS: &str = "kuki.recommendations";

/// Topics the relay creates at start-up
pub const KNOWN_TOPICS: &[&str] = &[TOPIC_RECOMMENDATIONS];

#[derive(Debug, sqlx::FromRow)]
pub struct OutboxRow {
    pub id: i64,
    pub organisation_id: Uuid,
    pub topic: String,
    pub key: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub attempts: i32,
}

/// Call inside the transaction that makes the change. Returns the outbox id.
pub async fn enqueue(
    c: &mut PgConnection,
    org: Uuid,
    topic: &str,
    key: &str,
    payload: &serde_json::Value,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO outbox (organisation_id, topic, key, payload) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(org)
    .bind(topic)
    .bind(key)
    .bind(payload)
    .fetch_one(c)
    .await
}

/// Oldest-first batch of rows nobody holds a lease on. Rows stay leased for `lease_seconds`.
pub async fn claim(
    c: &mut PgConnection,
    limit: i32,
    lease_seconds: f64,
) -> Result<Vec<OutboxRow>, sqlx::Error> {
    let mut rows: Vec<OutboxRow> = sqlx::query_as(
        "SELECT id, organisation_id, topic, key, payload, created_at, attempts FROM outbox_claim($1, $2)",
    )
    .bind(limit)
    .bind(lease_seconds)
    .fetch_all(c)
    .await?;
    rows.sort_by_key(|r| r.id);
    Ok(rows)
}

/// Returns how many rows changed; already-delivered rows are not counted.
pub async fn mark_published(c: &mut PgConnection, ids: &[i64]) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT outbox_mark_published($1)")
        .bind(ids)
        .fetch_one(c)
        .await
}

/// Makes rows claimable again at once.
pub async fn release(c: &mut PgConnection, ids: &[i64]) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT outbox_release($1)")
        .bind(ids)
        .execute(c)
        .await?;
    Ok(())
}

/// Parks a message the broker will never accept. It is skipped from then on.
pub async fn mark_dead(c: &mut PgConnection, id: i64, error: &str) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT outbox_mark_dead($1, $2)")
        .bind(id)
        .bind(error)
        .execute(c)
        .await?;
    Ok(())
}
