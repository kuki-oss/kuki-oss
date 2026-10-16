//! The durable queue between Kafka and the receivers. Queueing runs under
//! the organisation's tenant context; claiming and settling are cross-organisation
//! and go through the `delivery_*` functions, like action executions.

use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::webhook_subscriptions as subscriptions;

/// Queue `payload` for one subscription. False when this event was
/// already queued for it (Kafka redelivered it), so the consumer is
/// idempotent.
pub async fn enqueue(
    c: &mut PgConnection,
    org: Uuid,
    subscription_id: Uuid,
    event_id: &str,
    event_type: &str,
    payload: &Value,
) -> Result<bool, sqlx::Error> {
    let r = sqlx::query(
        "INSERT INTO webhook_deliveries
            (id, organisation_id, subscription_id, event_id, event_type, payload)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (subscription_id, event_id) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(org)
    .bind(subscription_id)
    .bind(event_id)
    .bind(event_type)
    .bind(payload)
    .execute(c)
    .await?;
    Ok(r.rows_affected() == 1)
}

/// Queue the event for every enabled subscription that wants it. Returns
/// how many deliveries were newly queued. Call it inside one tenant
/// transaction per event.
pub async fn enqueue_for_event(
    c: &mut PgConnection,
    org: Uuid,
    event_id: &str,
    event_type: &str,
    payload: &Value,
) -> Result<usize, sqlx::Error> {
    let mut queued = 0;
    for s in subscriptions::matching(c, org, event_type).await? {
        if enqueue(c, org, s.id, event_id, event_type, payload).await? {
            queued += 1;
        }
    }
    Ok(queued)
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Claimed {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub subscription_id: Uuid,
    pub event_id: String,
    pub event_type: String,
    pub payload: Value,
    pub attempts: i32,
    pub url: String,
    pub secret_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ending {
    Delivered,
    Dead,
}

impl Ending {
    fn as_str(self) -> &'static str {
        match self {
            Self::Delivered => "delivered",
            Self::Dead => "dead",
        }
    }
}

/// Takes up to `limit` due deliveries across all organisations and leases
/// them for `lease_seconds`. Not run under a tenant context.
pub async fn claim(
    c: &mut PgConnection,
    limit: i32,
    lease_seconds: f64,
) -> Result<Vec<Claimed>, sqlx::Error> {
    sqlx::query_as::<_, Claimed>(
        "SELECT id, organisation_id, subscription_id, event_id, event_type, payload, attempts,
                url, secret_ref
           FROM delivery_claim($1, $2)",
    )
    .bind(limit)
    .bind(lease_seconds)
    .fetch_all(c)
    .await
}

/// False when delivery is not running or belongs to another organisation.
pub async fn finish(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    ending: Ending,
    error: Option<&str>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT delivery_finish($1, $2, $3, $4)")
        .bind(org)
        .bind(id)
        .bind(ending.as_str())
        .bind(error)
        .fetch_one(c)
        .await
}

/// Re-queues a running delivery, not to be claimed before `delay_seconds` have passed.
pub async fn retry(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    error: &str,
    delay_seconds: f64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT delivery_retry($1, $2, $3, $4)")
        .bind(org)
        .bind(id)
        .bind(error)
        .bind(delay_seconds)
        .fetch_one(c)
        .await
}
