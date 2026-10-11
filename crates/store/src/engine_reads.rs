//! Reads the decision engine needs that the API layer has no use for.

use chrono::{DateTime, Utc};
use serde_json::Value;
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
