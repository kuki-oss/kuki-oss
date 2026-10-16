//! Webhook endpoint registry (one endpoint per organisation + action types).
//! Holds a *reference* to the signing secret in OpenBao KV, never the secret.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Endpoint {
    pub id: Uuid,
    pub action_type: String,
    pub url: String,
    pub secret_ref: String,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

const COLS: &str = "id, action_type, url, secret_ref, enabled, created_at";

/// Create or replace the endpoint for `action_type`. The id is stable across replacements.
pub async fn upsert(
    c: &mut PgConnection,
    org: Uuid,
    action_type: &str,
    url: &str,
    secret_ref: &str,
    enabled: bool,
) -> Result<Endpoint, sqlx::Error> {
    sqlx::query_as::<_, Endpoint>(&format!(
        "INSERT INTO webhook_endpoints (id, organisation_id, action_type, url, secret_ref, enabled)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (organisation_id, action_type) DO UPDATE
            SET url = EXCLUDED.url, secret_ref = EXCLUDED.secret_ref,
                enabled = EXCLUDED.enabled, updated_at = now()
         RETURNING {COLS}"
    ))
    .bind(Uuid::new_v4())
    .bind(org)
    .bind(action_type)
    .bind(url)
    .bind(secret_ref)
    .bind(enabled)
    .fetch_one(c)
    .await
}

/// The enabled endpoint for `action_type`, if any.
pub async fn find_enabled(
    c: &mut PgConnection,
    org: Uuid,
    action_type: &str,
) -> Result<Option<Endpoint>, sqlx::Error> {
    sqlx::query_as::<_, Endpoint>(&format!(
        "SELECT {COLS} FROM webhook_endpoints
          WHERE organisation_id = $1 AND action_type = $2 AND enabled"
    ))
    .bind(org)
    .bind(action_type)
    .fetch_optional(c)
    .await
}

pub async fn list(c: &mut PgConnection, org: Uuid) -> Result<Vec<Endpoint>, sqlx::Error> {
    sqlx::query_as::<_, Endpoint>(&format!(
        "SELECT {COLS} FROM webhook_endpoints WHERE organisation_id = $1 ORDER BY action_type"
    ))
    .bind(org)
    .fetch_all(c)
    .await
}

/// Returns whether an endpoint was removed.
pub async fn remove(
    c: &mut PgConnection,
    org: Uuid,
    action_type: &str,
) -> Result<bool, sqlx::Error> {
    let r = sqlx::query(
        "DELETE FROM webhook_endpoints WHERE organisation_id = $1 AND action_type = $2",
    )
    .bind(org)
    .bind(action_type)
    .execute(c)
    .await?;
    Ok(r.rows_affected() > 0)
}
