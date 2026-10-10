//! Registry reads (objects, event and decision types). Runs inside `with_org_tx`.

use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct ObjectTypeRow {
    pub name: String,
    pub version: i32,
    pub json_schema: serde_json::Value,
}

#[derive(Debug, sqlx::FromRow)]
pub struct EventTypeRow {
    pub name: String,
    pub version: i32,
    pub subject_type: String,
    pub json_schema: serde_json::Value,
}

#[derive(Debug, sqlx::FromRow)]
pub struct DecisionTypeRow {
    pub name: String,
    pub pack: String,
    pub payload_schema_ref: String,
    pub payload_schema: serde_json::Value,
}

fn split(after: Option<(String, i32)>) -> (Option<String>, Option<i32>) {
    match after {
        Some((n, v)) => (Some(n), Some(v)),
        None => (None, None),
    }
}

/// Every version of every object type, ordered by (name, version).
pub async fn object_types_page(
    c: &mut PgConnection,
    org: Uuid,
    after: Option<(String, i32)>,
    limit: i64,
) -> Result<Vec<ObjectTypeRow>, sqlx::Error> {
    let (n, v) = split(after);
    sqlx::query_as(
        "SELECT name, version, json_schema FROM object_types
         WHERE organisation_id = $1
           AND ($2::text IS NULL OR (name, version) > ($2::text, $3::int))
         ORDER BY name, version LIMIT $4",
    )
    .bind(org)
    .bind(n)
    .bind(v)
    .bind(limit)
    .fetch_all(c)
    .await
}

pub async fn event_types_page(
    c: &mut PgConnection,
    org: Uuid,
    after: Option<(String, i32)>,
    limit: i64,
) -> Result<Vec<EventTypeRow>, sqlx::Error> {
    let (n, v) = split(after);
    sqlx::query_as(
        "SELECT name, version, subject_type, json_schema FROM event_types
         WHERE organisation_id = $1
           AND ($2::text IS NULL OR (name, version) > ($2::text, $3::int))
         ORDER BY name, version LIMIT $4",
    )
    .bind(org)
    .bind(n)
    .bind(v)
    .bind(limit)
    .fetch_all(c)
    .await
}

pub async fn decision_types_page(
    c: &mut PgConnection,
    org: Uuid,
    after: Option<String>,
    limit: i64,
) -> Result<Vec<DecisionTypeRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT name, pack, payload_schema_ref, payload_schema FROM decision_types
         WHERE organisation_id = $1 AND ($2::text IS NULL OR name > $2::text)
         ORDER BY name LIMIT $3",
    )
    .bind(org)
    .bind(after)
    .bind(limit)
    .fetch_all(c)
    .await
}
