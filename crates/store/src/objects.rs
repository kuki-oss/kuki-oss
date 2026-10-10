//! Object versions: one current row per id (valid_to IS NULL) with history behind it.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, Row, postgres::PgRow};
use uuid::Uuid;

pub enum ObjectTypeLookup {
    Unknown,
    UnknownVersion,
    Found(Value),
}

pub async fn object_type_lookup(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    version: i32,
) -> sqlx::Result<ObjectTypeLookup> {
    let schema: Option<Value> = sqlx::query_scalar(
        "SELECT json_schema FROM object_types
          WHERE organisation_id = $1 AND name = $2 AND version = $3",
    )
    .bind(org)
    .bind(name)
    .bind(version)
    .fetch_optional(&mut *c)
    .await?;
    if let Some(s) = schema {
        return Ok(ObjectTypeLookup::Found(s));
    }
    let any: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM object_types WHERE organisation_id = $1 AND name = $2)",
    )
    .bind(org)
    .bind(name)
    .fetch_one(c)
    .await?;
    Ok(if any {
        ObjectTypeLookup::UnknownVersion
    } else {
        ObjectTypeLookup::Unknown
    })
}

/// `None` when the unit does not exist or is outside the session's scope (RLS hides it).
pub async fn org_unit_resolve(
    c: &mut PgConnection,
    org: Uuid,
    path: &str,
) -> sqlx::Result<Option<Uuid>> {
    sqlx::query_scalar(
        "SELECT id FROM org_units WHERE organisation_id = $1 AND path = $2::text::ltree",
    )
    .bind(org)
    .bind(path)
    .fetch_optional(c)
    .await
}

#[derive(Debug, Clone)]
pub struct CurrentObject {
    pub id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub object_type: String,
    pub type_version: i32,
    pub external_ids: Value,
    pub attrs: Value,
    pub valid_from: DateTime<Utc>,
}

fn read(r: &PgRow) -> sqlx::Result<CurrentObject> {
    Ok(CurrentObject {
        id: r.try_get("id")?,
        org_unit_id: r.try_get("org_unit_id")?,
        object_type: r.try_get("object_type")?,
        type_version: r.try_get("type_version")?,
        external_ids: r.try_get("external_ids")?,
        attrs: r.try_get("attrs")?,
        valid_from: r.try_get("valid_from")?,
    })
}

/// Current versions matching `id` or any one of the single-pair `{system: id}` documents.
pub async fn objects_matching(
    c: &mut PgConnection,
    org: Uuid,
    id: Option<Uuid>,
    pairs: &[Value],
) -> sqlx::Result<Vec<CurrentObject>> {
    let rows = sqlx::query(
        "SELECT id, org_unit_id, type AS object_type, type_version, external_ids, attrs, valid_from
           FROM objects
          WHERE organisation_id = $1 AND valid_to IS NULL
            AND (id = $2 OR external_ids @> ANY($3::jsonb[]))",
    )
    .bind(org)
    .bind(id)
    .bind(pairs.to_vec())
    .fetch_all(c)
    .await?;
    rows.iter().map(read).collect()
}

/// Comparison runs in SQL so jsonb equality applies (1 equals 1.0, key order is irrelevant).
pub async fn object_is_current(
    c: &mut PgConnection,
    id: Uuid,
    object_type: &str,
    type_version: i32,
    org_unit_id: Option<Uuid>,
    external_ids: &Value,
    attrs: &Value,
) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM objects
                         WHERE id = $1 AND valid_to IS NULL AND type = $2 AND type_version = $3
                           AND org_unit_id IS NOT DISTINCT FROM $4
                           AND external_ids = $5 AND attrs = $6)",
    )
    .bind(id)
    .bind(object_type)
    .bind(type_version)
    .bind(org_unit_id)
    .bind(external_ids)
    .bind(attrs)
    .fetch_one(c)
    .await
}

pub struct NewVersion<'a> {
    pub id: Uuid,
    pub org: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub object_type: &'a str,
    pub type_version: i32,
    pub external_ids: &'a Value,
    pub attrs: &'a Value,
    pub valid_from: DateTime<Utc>,
}

/// Closes the current version (if any) and inserts the new one.
pub async fn object_write_version(c: &mut PgConnection, v: &NewVersion<'_>) -> sqlx::Result<()> {
    sqlx::query("UPDATE objects SET valid_to = $3 WHERE organisation_id = $1 AND id = $2 AND valid_to IS NULL")
        .bind(v.org)
        .bind(v.id)
        .bind(v.valid_from)
        .execute(&mut *c)
        .await?;
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version,
                              external_ids, attrs, valid_from)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(v.id)
    .bind(v.org)
    .bind(v.org_unit_id)
    .bind(v.object_type)
    .bind(v.type_version)
    .bind(v.external_ids)
    .bind(v.attrs)
    .bind(v.valid_from)
    .execute(c)
    .await?;
    Ok(())
}

/// The ledger row a previous attempt wrote for this deterministic event id, if any.
pub async fn event_outcome_get(
    c: &mut PgConnection,
    org: Uuid,
    event_id: Uuid,
) -> sqlx::Result<Option<(Uuid, Value)>> {
    let row = sqlx::query(
        "SELECT subject_id, payload FROM events WHERE organisation_id = $1 AND id = $2 LIMIT 1",
    )
    .bind(org)
    .bind(event_id)
    .fetch_optional(c)
    .await?;
    row.map(|r| Ok((r.try_get("subject_id")?, r.try_get("payload")?)))
        .transpose()
}
