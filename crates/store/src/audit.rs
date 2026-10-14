//! Append-only audit rows. Call inside the decision's `with_org_tx` transaction.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

pub struct AuditRow<'a> {
    pub id: Uuid,
    pub occurred_at: DateTime<Utc>,
    /// `key:<uuid>`, `user:<sub>` or `policy:<id>`.
    pub actor: &'a str,
    /// For example `recommendation.accepted`.
    pub action: &'a str,
    /// For example `recommendation:<uuid>`.
    pub subject: &'a str,
    pub details: &'a Value,
}

pub async fn append(c: &mut PgConnection, org: Uuid, a: &AuditRow<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_log (id, organisation_id, occurred_at, actor, action, subject, details)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(a.id)
    .bind(org)
    .bind(a.occurred_at)
    .bind(a.actor)
    .bind(a.action)
    .bind(a.subject)
    .bind(a.details)
    .execute(c)
    .await?;
    Ok(())
}
