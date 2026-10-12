//! Metric reads. Valid time: the latest value with `as_of <= T`; the higher version wins a tie.

use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct MetricRow {
    pub name: String,
    pub version: i32,
    pub subject_id: Uuid,
    pub as_of: DateTime<Utc>,
    pub value: serde_json::Value,
    pub unit: Option<String>,
}

/// A scoped key only sees metrics about current objects it can see (the objects
/// policy decides).
pub async fn metric_at(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    subject: Uuid,
    as_of: Option<DateTime<Utc>>,
) -> Result<Option<MetricRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT m.name, m.version, m.subject_id, m.as_of, m.value, m.unit
           FROM metric_values m
          WHERE m.organisation_id = $1 AND m.name = $2 AND m.subject_id = $3
            AND ($4::timestamptz IS NULL OR m.as_of <= $4)
            AND (kuki_scope_path() IS NULL OR EXISTS (
                   SELECT 1 FROM objects v
                    WHERE v.organisation_id = m.organisation_id AND v.id = m.subject_id
                      AND v.valid_to IS NULL))
          ORDER BY m.as_of DESC, m.version DESC
          LIMIT 1",
    )
    .bind(org)
    .bind(name)
    .bind(subject)
    .bind(as_of)
    .fetch_optional(c)
    .await
}
