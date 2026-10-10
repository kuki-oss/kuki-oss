//! Link versions: one current row per (type, src, dst), history behind it.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CurrentLink {
    pub id: Uuid,
    pub valid_from: DateTime<Utc>,
}

pub async fn link_current(
    c: &mut PgConnection,
    org: Uuid,
    link_type: &str,
    src: Uuid,
    dst: Uuid,
) -> sqlx::Result<Option<CurrentLink>> {
    let row = sqlx::query(
        "SELECT id, valid_from FROM links
          WHERE organisation_id = $1 AND type = $2 AND src_id = $3 AND dst_id = $4
            AND valid_to IS NULL",
    )
    .bind(org)
    .bind(link_type)
    .bind(src)
    .bind(dst)
    .fetch_optional(c)
    .await?;
    row.map(|r| {
        Ok(CurrentLink {
            id: r.try_get("id")?,
            valid_from: r.try_get("valid_from")?,
        })
    })
    .transpose()
}

/// jsonb equality in SQL (1 equals 1.0, key orders is irrelevant)
pub async fn link_is_current(c: &mut PgConnection, id: Uuid, attrs: &Value) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM links WHERE id = $1 AND valid_to IS NULL AND attrs = $2)",
    )
    .bind(id)
    .bind(attrs)
    .fetch_one(c)
    .await
}

pub struct NewLink<'a> {
    pub id: Uuid,
    pub org: Uuid,
    pub link_type: &'a str,
    pub src: Uuid,
    pub dst: Uuid,
    pub attrs: &'a Value,
    pub valid_from: DateTime<Utc>,
}

/// Closes the prior current version (if any), then inserts the new one.
pub async fn link_write_version(
    c: &mut PgConnection,
    prior: Option<Uuid>,
    v: &NewLink<'_>,
) -> sqlx::Result<()> {
    if let Some(prior) = prior {
        sqlx::query("UPDATE links SET valid_to = $3 WHERE organisation_id = $1 AND id = $2 AND valid_to IS NULL")
            .bind(v.org)
            .bind(prior)
            .bind(v.valid_from)
            .execute(&mut *c)
            .await?;
    }
    sqlx::query(
        "INSERT INTO links (id, organisation_id, type, src_id, dst_id, attrs, valid_from)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(v.id)
    .bind(v.org)
    .bind(v.link_type)
    .bind(v.src)
    .bind(v.dst)
    .bind(v.attrs)
    .bind(v.valid_from)
    .execute(c)
    .await?;
    Ok(())
}
