//! Converted rows through the ingest pipeline, whatever the mapping's kind.

use serde::Serialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::mapping::Mapping;
use crate::{
    IngestContext, IngestError, IngestOptions, Rejected, body::RawRow, ingest_events,
    links::upsert_links, objects::upsert_objects, outcome::UpsertResult,
};

/// What a load did. `index` in each rejected row is the zero-based data row.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadSummary {
    pub kind: &'static str,
    pub batch_id: Uuid,
    pub accepted: u32,
    /// Objects and links only; events are append-only and have no update outcome.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unchanged: Option<u32>,
    pub rejected: Vec<Rejected>,
}

fn from_upsert(kind: &'static str, result: UpsertResult) -> LoadSummary {
    let (mut created, mut updated, mut unchanged) = (0, 0, 0);
    for a in result.accepted.iter().flatten() {
        match a.outcome {
            "created" => created += 1,
            "updated" => updated += 1,
            _ => unchanged += 1,
        }
    }
    LoadSummary {
        kind,
        batch_id: result.batch_id,
        accepted: result.accepted_count,
        created: Some(created),
        updated: Some(updated),
        unchanged: Some(unchanged),
        rejected: result.rejected,
    }
}

/// Sends the rows through the pipeline call that matches the mapping's kind.
pub async fn load_rows(
    pool: &PgPool,
    ctx: IngestContext,
    mapping: &Mapping,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
) -> Result<LoadSummary, IngestError> {
    Ok(match mapping {
        Mapping::Objects(_) => from_upsert("objects", upsert_objects(pool, ctx, rows, opts).await?),
        Mapping::Links(_) => from_upsert("links", upsert_links(pool, ctx, rows, opts).await?),
        Mapping::Events(_) => {
            let r = ingest_events(pool, ctx, rows, opts).await?;
            LoadSummary {
                kind: "events",
                batch_id: r.batch_id,
                accepted: r.accepted_count,
                created: None,
                updated: None,
                unchanged: None,
                rejected: r.rejected,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RejectionCode, outcome::UpsertAccepted};

    #[test]
    fn upsert_outcomes_are_counted_and_rejections_kept() {
        let accepted = |index, outcome| UpsertAccepted {
            index,
            id: Uuid::nil(),
            outcome,
        };
        let summary = from_upsert(
            "objects",
            UpsertResult {
                batch_id: Uuid::nil(),
                accepted_count: 4,
                accepted: Some(vec![
                    accepted(0, "created"),
                    accepted(1, "created"),
                    accepted(2, "updated"),
                    accepted(3, "unchanged"),
                ]),
                rejected: vec![Rejected {
                    index: 4,
                    code: RejectionCode::MalformedLine,
                    message: "bad".into(),
                    field: None,
                    quarantine_id: Uuid::nil(),
                }],
            },
        );
        assert_eq!(summary.accepted, 4);
        assert_eq!(
            (summary.created, summary.updated, summary.unchanged),
            (Some(2), Some(1), Some(1))
        );
        assert_eq!(summary.rejected.len(), 1);
    }
}
