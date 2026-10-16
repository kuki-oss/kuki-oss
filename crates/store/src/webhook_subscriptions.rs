//! Where an organisation's recommendation events are delivered. Holds a
//! *reference* to the signing secret in OpenBao KV, never the secret.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

pub const STATUS_CHANGED: &str = "recommendation.status_changed";

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Subscription {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub secret_ref: String,
    pub event_types: Vec<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

const COLS: &str = "id, name, url, secret_ref, event_types, enabled, created_at";

/// Create or replace the subscription called `name`. The id is stabe across replacements.
pub async fn upsert(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    url: &str,
    secret_ref: &str,
    event_types: &[String],
    enabled: bool,
) -> Result<Subscription, sqlx::Error> {
    sqlx::query_as::<_, Subscription>(&format!(
        "INSERT INTO webhook_subscriptions
            (id, organisation_id, name, url, secret_ref, event_types, enabled)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (organisation_id, name) DO UPDATE
            SET url = EXCLUDED.url, secret_ref = EXCLUDED.secret_ref,
                event_types = EXCLUDED.event_types, enabled = EXCLUDED.enabled,
                updated_at = now()
         RETURNING {COLS}"
    ))
    .bind(Uuid::new_v4())
    .bind(org)
    .bind(name)
    .bind(url)
    .bind(secret_ref)
    .bind(event_types)
    .bind(enabled)
    .fetch_one(c)
    .await
}

pub async fn list(c: &mut PgConnection, org: Uuid) -> Result<Vec<Subscription>, sqlx::Error> {
    sqlx::query_as::<_, Subscription>(&format!(
        "SELECT {COLS} FROM webhook_subscriptions WHERE organisation_id = $1 ORDER BY name"
    ))
    .bind(org)
    .fetch_all(c)
    .await
}

/// Enabled subscriptions that want `event_type`.
pub async fn matching(
    c: &mut PgConnection,
    org: Uuid,
    event_type: &str,
) -> Result<Vec<Subscription>, sqlx::Error> {
    sqlx::query_as::<_, Subscription>(&format!(
        "SELECT {COLS} FROM webhook_subscriptions
          WHERE organisation_id = $1 AND enabled AND $2 = ANY(event_types)
          ORDER BY name"
    ))
    .bind(org)
    .bind(event_type)
    .fetch_all(c)
    .await
}

/// Returns whether a subscription was removed. Its queued deliveries go with it.
pub async fn remove(c: &mut PgConnection, org: Uuid, name: &str) -> Result<bool, sqlx::Error> {
    let r = sqlx::query("DELETE FROM webhook_subscriptions WHERE organisation_id = $1 AND name = $2")
        .bind(org)
        .bind(name)
        .execute(c)
        .await?;
    Ok(r.rows_affected() > 0)
}
