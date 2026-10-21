//! Shadow proposals: what a generator in shadow mode would have proposed.
//!
//! Kept apart from `recommendations` on purpose. Nothing that counts, lists
//! or announces recommendations reads this table, so a shadow run cannot
//! touch the inbox, the review budget or the API.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::recommendations::NewRecommendation;

/// One stored shadow proposal, as `kuki shadow report` shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct ShadowRow {
    pub dedup_key: String,
    pub decision_type: String,
    pub org_unit_id: Option<Uuid>,
    pub urgency: f64,
    pub confidence: f64,
    pub summary: String,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub seen_count: i32,
    pub body: Value,
}

/// Records that `generator_id` proposed `new` at `now`: inserts it, or refreshes the stored copy
/// and counts the run.
pub async fn upsert(
    c: &mut PgConnection,
    org: Uuid,
    generator_id: &str,
    new: &NewRecommendation,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO shadow_proposals
             (organisation_id, generator_id, dedup_key, decision_type, org_unit_id,
              rationale_summary, body, provenance, first_seen_at, last_seen_at, seen_count)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, 1)
         ON CONFLICT (organisation_id, generator_id, dedup_key) DO UPDATE SET
             decision_type     = EXCLUDED.decision_type,
             org_unit_id       = EXCLUDED.org_unit_id,
             rationale_summary = EXCLUDED.rationale_summary,
             body              = EXCLUDED.body,
             provenance        = EXCLUDED.provenance,
             last_seen_at      = EXCLUDED.last_seen_at,
             seen_count        = shadow_proposals.seen_count + 1",
    )
    .bind(org)
    .bind(generator_id)
    .bind(&new.dedup_key)
    .bind(&new.decision_type)
    .bind(new.org_unit_id)
    .bind(&new.rationale_summary)
    .bind(&new.body)
    .bind(&new.provenance)
    .bind(now)
    .execute(c)
    .await?;
    Ok(())
}

/// Deletes this generator's rows last proposed before `before`. Returns how many went.
pub async fn prune(
    c: &mut PgConnection,
    org: Uuid,
    generator_id: &str,
    before: DateTime<Utc>,
) -> Result<u64, sqlx::Error> {
    let r = sqlx::query(
        "DELETE FROM shadow_proposals
          WHERE organisation_id = $1 AND generator_id = $2 AND last_seen_at < $3",
    )
    .bind(org)
    .bind(generator_id)
    .bind(before)
    .execute(c)
    .await?;
    Ok(r.rows_affected())
}

/// The generator's shadow proposals, most urgent first.
pub async fn list(
    c: &mut PgConnection,
    org: Uuid,
    generator_id: &str,
    limit: i64,
) -> Result<Vec<ShadowRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT dedup_key, decision_type, org_unit_id,
                (body->>'urgency')::float8 AS urgency,
                (body #>> '{rationale,confidence}')::float8 AS confidence,
                rationale_summary, first_seen_at, last_seen_at, seen_count, body
           FROM shadow_proposals
          WHERE organisation_id = $1 AND generator_id = $2
          ORDER BY urgency DESC, dedup_key
          LIMIT $3",
    )
    .bind(org)
    .bind(generator_id)
    .bind(limit)
    .fetch_all(c)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(ShadowRow {
                dedup_key: r.try_get("dedup_key")?,
                decision_type: r.try_get("decision_type")?,
                org_unit_id: r.try_get("org_unit_id")?,
                urgency: r.try_get("urgency")?,
                confidence: r.try_get("confidence")?,
                summary: r.try_get("rationale_summary")?,
                first_seen_at: r.try_get("first_seen_at")?,
                last_seen_at: r.try_get("last_seen_at")?,
                seen_count: r.try_get("seen_count")?,
                body: r.try_get("body")?,
            })
        })
        .collect()
}
