//! Replays one quarantined row through the processor for its batch kind.

use futures::FutureExt;
use kuki_store::{
    ingest::{batch_exists, quarantine_mark_replayed},
    session::with_org_tx,
};
use kuki_types::ids;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    body::{Format, parse_rows},
    links::upsert_links_with,
    objects::{db, upsert_objects_with},
    outcome::{IngestResult, UpsertResult},
    pipeline::{IngestContext, IngestError, IngestOptions, ingest_events_with},
};

#[derive(Serialize)]
#[serde(untagged)]
pub enum ReplayResult {
    Events(IngestResult),
    Upserts(UpsertResult),
}

impl ReplayResult {
    pub fn accepted_count(&self) -> u32 {
        match self {
            Self::Events(r) => r.accepted_count,
            Self::Upserts(r) => r.accepted_count,
        }
    }
}

/// `ctx.idempotency_key` is the replay request's key. The row is processed as index 0 of a
/// one-row `replay` batch, so ids derive from that key and a retry resumes exactly.
///
/// `Ok(None)` means the row was already replayed by a different request.
/// The replay writes as live data: it carries no backfill id.
pub async fn replay_row(
    pool: &PgPool,
    ctx: IngestContext,
    quarantine_id: Uuid,
    kind: &str,
    already_replayed: bool,
    row: Value,
) -> Result<Option<ReplayResult>, IngestError> {
    let (org, session) = (ctx.org, ctx.session.clone());

    if already_replayed {
        // A crash after the row was marked but before the response was stored leaves it
        // `replayed`. The same key then resumes; any other key is a conflict.
        let batch = ids::batch_id(org, &ctx.idempotency_key);
        let ours = with_org_tx(pool, &session, move |c| {
            async move { batch_exists(c, org, batch).await }.boxed()
        })
        .await
        .map_err(db)?;
        if !ours {
            return Ok(None);
        }
    }

    let key = match kind {
        "events" => "events",
        "objects" => "objects",
        "links" => "links",
        other => {
            return Err(IngestError::invalid(
                None,
                format!("a row from a '{other}' batch cannot be replayed"),
            ));
        }
    };
    let line = serde_json::to_vec(&row).map_err(|e| IngestError::internal(e.to_string()))?;
    let rows = parse_rows(&line, Format::NdJson, key)?;

    let ctx = IngestContext {
        backfill_id: None,
        ..ctx
    };
    let opts = IngestOptions::default();
    let result = match key {
        "events" => ReplayResult::Events(
            ingest_events_with(pool, ctx, rows, &opts, Some(quarantine_id)).await?,
        ),
        "objects" => ReplayResult::Upserts(
            upsert_objects_with(pool, ctx, rows, &opts, Some(quarantine_id)).await?,
        ),
        _ => ReplayResult::Upserts(
            upsert_links_with(pool, ctx, rows, &opts, Some(quarantine_id)).await?,
        ),
    };

    if result.accepted_count() > 0 {
        with_org_tx(pool, &session, move |c| {
            async move { quarantine_mark_replayed(c, org, quarantine_id).await }.boxed()
        })
        .await
        .map_err(db)?;
    }
    Ok(Some(result))
}
