//! Reads the decision engine needs that the API layer has no use for.

use chrono::{DateTime, Utc};
use serde_json::Value;
use serde_json::Value as Json;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::ontology::ObjectRow;

/// The `decision` subtree of the organisation's config. `None` when the
/// session is not for `org` or the organisation does not exist; `Some({})`
/// when nothing is configured.
pub async fn decision_config(
    c: &mut PgConnection,
    org: Uuid,
) -> Result<Option<Value>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<Value>>("SELECT org_decision_config($1)")
        .bind(org)
        .fetch_one(c)
        .await
}

/// The version of one object visible at `as_of` (valid time; `valid_to`
/// is exclusive).
pub async fn object_as_of(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    as_of: DateTime<Utc>,
) -> Result<Option<ObjectRow>, sqlx::Error> {
    sqlx::query_as::<_, ObjectRow>(
        "SELECT o.id, o.org_unit_id, o.\"type\" AS kind, o.type_version, o.external_ids, \
                o.attrs, o.valid_from, o.valid_to, o.recorded_at \
           FROM objects o \
          WHERE o.organisation_id = $1 AND o.id = $2 \
            AND o.valid_from <= $3 AND (o.valid_to IS NULL OR o.valid_to > $3) \
          ORDER BY o.valid_from DESC LIMIT 1",
    )
    .bind(org)
    .bind(id)
    .bind(as_of)
    .fetch_optional(c)
    .await
}

#[derive(Debug, sqlx::FromRow)]
pub struct DecisionTypeInfo {
    pub pack: String,
    pub payload_schema_ref: String,
    pub payload_schema: Json,
}

pub async fn decision_type_info(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
) -> Result<Option<DecisionTypeInfo>, sqlx::Error> {
    sqlx::query_as(
        "SELECT pack, payload_schema_ref, payload_schema FROM decision_types
          WHERE organisation_id = $1 AND name = $2",
    )
    .bind(org)
    .bind(name)
    .fetch_optional(c)
    .await
}

/// Registers or updates a decision type. Needs a priviledged connection: `kuki_app`
/// only reads the registry.
pub async fn register_decision_type(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    pack: &str,
    schema_ref: &str,
    schema: &Json,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO decision_types (organisation_id, name, pack, payload_schema_ref, payload_schema)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (organisation_id, name) DO UPDATE
            SET pack = EXCLUDED.pack,
                payload_schema_ref = EXCLUDED.payload_schema_ref,
                payload_schema = EXCLUDED.payload_schema",
    )
    .bind(org)
    .bind(name)
    .bind(pack)
    .bind(schema_ref)
    .bind(schema)
    .execute(c)
    .await?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
pub struct PendingRow {
    pub id: Uuid,
    pub dedup_key: String,
    pub version: i32,
    /// From the stored provenance; `None` for rows written by something else.
    pub generator_id: Option<String>,
}

/// Every pending recommendation of one decision type, ordered by dedup key.
pub async fn pending_for_type(
    c: &mut PgConnection,
    org: Uuid,
    decision_type: &str,
) -> Result<Vec<PendingRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT id, dedup_key, version, provenance->>'generator_id' AS generator_id
           FROM recommendations
          WHERE organisation_id = $1 AND decision_type = $2 AND status = 'pending'
          ORDER BY dedup_key",
    )
    .bind(org)
    .bind(decision_type)
    .fetch_all(c)
    .await
}

/// Recommendations created at or after `since`, per org unit (`None` = no unit). For the review budget.
pub async fn created_counts_since(
    c: &mut PgConnection,
    org: Uuid,
    since: DateTime<Utc>,
) -> Result<Vec<(Option<Uuid>, i64)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT org_unit_id, count(*) FROM recommendations
          WHERE organisation_id = $1 AND created_at >= $2 GROUP BY org_unit_id",
    )
    .bind(org)
    .bind(since)
    .fetch_all(c)
    .await
}
