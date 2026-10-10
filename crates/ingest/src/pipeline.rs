//! parse -> validate -> resolve -> write, in chunked tenant transactions.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::FutureExt;
use kuki_store::ingest::{
    NewBatch, NewEvent, Rejection, Resolution, Subject, batch_ensure, batch_finish, events_insert,
    quarantine_insert, subject_resolve,
};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_types::ids;
use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use thiserror::Error;
use uuid::Uuid;

use crate::body::{RawRow, RowContent};
use crate::event::{EventInput, SubjectRef, parse_event};
use crate::outcome::{Accepted, IngestResult, Rejected, RejectionCode};
use crate::registry::{Registry, TypeEntry, first_violation};

#[derive(Debug, Error)]
pub enum IngestError {
    /// The whole request is unusable (400 VALIDATION_FAILED).
    #[error("{message}")]
    Invalid {
        pointer: Option<String>,
        message: String,
    },
    /// Infrastructure or registry problem (500 INTERNAL). The detail is for logs only.
    #[error("internal: {0}")]
    Internal(String),
}

impl IngestError {
    pub fn invalid(pointer: Option<String>, message: impl Into<String>) -> Self {
        Self::Invalid {
            pointer,
            message: message.into(),
        }
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

fn db(e: impl std::fmt::Display) -> IngestError {
    IngestError::Internal(e.to_string())
}

pub struct IngestContext {
    pub org: Uuid,
    pub session: OrgSession,
    /// Audit identity, e.g. `api_key:<uuid>`.
    pub source: String,
    pub idempotency_key: String,
    pub backfill_id: Option<String>,
}

pub struct IngestOptions {
    pub chunk_size: usize,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self { chunk_size: 1000 }
    }
}

#[derive(Clone)]
struct Candidate {
    index: u32,
    raw: Value,
    input: EventInput,
}

struct Pre {
    index: u32,
    raw: Value,
    code: RejectionCode,
    message: String,
    field: Option<String>,
}

enum Checked {
    Ok(NewEvent),
    Reject {
        code: RejectionCode,
        message: String,
        field: Option<String>,
    },
}

fn reject(code: RejectionCode, message: impl Into<String>, field: Option<&str>) -> Checked {
    Checked::Reject {
        code,
        message: message.into(),
        field: field.map(str::to_owned),
    }
}

pub(crate) struct Failure<'a> {
    pub index: u32,
    pub code: RejectionCode,
    pub message: &'a str,
    pub field: Option<&'a str>,
    pub raw: &'a Value,
}

/// A normal batch quarantines the row under a deterministic id. A replay keeps the
/// original row quarantined and reports its id, so no duplicate row appears.
pub(crate) async fn quarantine_row(
    c: &mut PgConnection,
    org: Uuid,
    key: &str,
    batch: Uuid,
    replay_of: Option<Uuid>,
    f: &Failure<'_>,
) -> sqlx::Result<Uuid> {
    if let Some(original) = replay_of {
        return Ok(original);
    }
    let id = ids::quarantine_id(org, key, f.index);
    quarantine_insert(
        c,
        org,
        &Rejection {
            id,
            batch_id: batch,
            index: f.index as i32,
            code: f.code.as_str(),
            message: f.message,
            field: f.field,
            raw: f.raw,
        },
    )
    .await?;
    Ok(id)
}

pub(crate) fn batch_kind(kind: &'static str, replay_of: Option<Uuid>) -> &'static str {
    if replay_of.is_some() { "replay" } else { kind }
}

struct ChunkOut {
    accepted: Vec<Accepted>,
    rejected: Vec<Rejected>,
}

pub async fn ingest_events(
    pool: &PgPool,
    ctx: IngestContext,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
) -> Result<IngestResult, IngestError> {
    ingest_events_with(pool, ctx, rows, opts, None).await
}

pub(crate) async fn ingest_events_with(
    pool: &PgPool,
    ctx: IngestContext,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
    replay_of: Option<Uuid>,
) -> Result<IngestResult, IngestError> {
    let chunk_size = opts.chunk_size.max(1);
    let ctx = Arc::new(ctx);
    let batch = ids::batch_id(ctx.org, &ctx.idempotency_key);

    // 1. Structural validation. Bad rows go to quarantine; they never fail the request.
    let mut pre: Vec<Pre> = Vec::new();
    let mut candidates: Vec<Candidate> = Vec::new();
    for row in rows {
        match row.content {
            RowContent::Malformed { text, message } => pre.push(Pre {
                index: row.index,
                raw: Value::String(text),
                code: RejectionCode::MalformedLine,
                message,
                field: None,
            }),
            RowContent::Json(raw) => match parse_event(&raw) {
                Ok(input) => candidates.push(Candidate {
                    index: row.index,
                    raw,
                    input,
                }),
                Err(fe) => pre.push(Pre {
                    index: row.index,
                    raw,
                    code: RejectionCode::SchemaInvalid,
                    message: fe.message,
                    field: fe.field,
                }),
            },
        }
    }

    // 2. The batch row, plus quarantine rows for everything rejected above.
    let mut rejected: Vec<Rejected> = pre
        .iter()
        .map(|p| Rejected {
            index: p.index,
            code: p.code,
            message: p.message.clone(),
            field: p.field.clone(),
            quarantine_id: replay_of
                .unwrap_or_else(|| ids::quarantine_id(ctx.org, &ctx.idempotency_key, p.index)),
        })
        .collect();
    {
        let ctx = ctx.clone();
        with_org_tx(pool, &ctx.session.clone(), move |c| {
            async move {
                batch_ensure(
                    c,
                    &NewBatch {
                        id: batch,
                        org: ctx.org,
                        kind: batch_kind("events", replay_of),
                        idempotency_key: &ctx.idempotency_key,
                        source: &ctx.source,
                        backfill_id: ctx.backfill_id.as_deref(),
                    },
                )
                .await?;
                for p in &pre {
                    quarantine_row(
                        c,
                        ctx.org,
                        &ctx.idempotency_key,
                        batch,
                        replay_of,
                        &Failure {
                            index: p.index,
                            code: p.code,
                            message: &p.message,
                            field: p.field.as_deref(),
                            raw: &p.raw,
                        },
                    )
                    .await?;
                }
                Ok::<_, sqlx::Error>(())
            }
            .boxed()
        })
        .await
        .map_err(db)?;
    }

    // 3. Registry: one lookup per distinct (type, version), compiled once.
    let wanted: BTreeSet<(String, i32)> = candidates
        .iter()
        .map(|c| (c.input.event_type.clone(), c.input.type_version as i32))
        .collect();
    let loaded = {
        let org = ctx.org;
        with_org_tx(pool, &ctx.session, move |c| {
            async move { Registry::fetch(c, org, wanted).await }.boxed()
        })
        .await
        .map_err(db)?
    };
    let registry = Arc::new(Registry::compile(loaded)?);

    // 4. Chunked writes. Each chunk commits before the next starts, so a failure part-way
    //    leaves earlier chunks committed and a retry with the same key simply resumes.
    let mut accepted: Vec<Accepted> = Vec::new();
    for chunk in candidates.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        let (ctx2, registry2) = (ctx.clone(), registry.clone());
        let out = with_org_tx(pool, &ctx.session, move |c| {
            async move { run_chunk(c, &ctx2, batch, &registry2, &chunk, replay_of).await }.boxed()
        })
        .await
        .map_err(db)?;
        accepted.extend(out.accepted);
        rejected.extend(out.rejected);
    }

    // 5. Counts, then the result.
    accepted.sort_by_key(|a| a.index);
    rejected.sort_by_key(|r| r.index);
    let (a, r) = (accepted.len() as i32, rejected.len() as i32);
    with_org_tx(pool, &ctx.session, move |c| {
        async move { batch_finish(c, batch, a, r).await }.boxed()
    })
    .await
    .map_err(db)?;

    Ok(IngestResult {
        batch_id: batch,
        accepted_count: accepted.len() as u32,
        accepted: Some(accepted),
        rejected,
    })
}

async fn run_chunk(
    c: &mut PgConnection,
    ctx: &IngestContext,
    batch: Uuid,
    registry: &Registry,
    chunk: &[Candidate],
    replay_of: Option<Uuid>,
) -> sqlx::Result<ChunkOut> {
    let mut events = Vec::with_capacity(chunk.len());
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();

    for cand in chunk {
        match check(c, ctx, registry, cand).await? {
            Checked::Ok(ev) => {
                accepted.push(Accepted {
                    index: cand.index,
                    event_id: ev.id,
                });
                events.push(ev);
            }
            Checked::Reject {
                code,
                message,
                field,
            } => {
                let quarantine_id = quarantine_row(
                    c,
                    ctx.org,
                    &ctx.idempotency_key,
                    batch,
                    replay_of,
                    &Failure {
                        index: cand.index,
                        code,
                        message: &message,
                        field: field.as_deref(),
                        raw: &cand.raw,
                    },
                )
                .await?;
                rejected.push(Rejected {
                    index: cand.index,
                    code,
                    message,
                    field,
                    quarantine_id,
                });
            }
        }
    }

    events_insert(
        c,
        ctx.org,
        &ctx.source,
        &ctx.idempotency_key,
        ctx.backfill_id.as_deref(),
        &events,
    )
    .await?;
    Ok(ChunkOut { accepted, rejected })
}

async fn check(
    c: &mut PgConnection,
    ctx: &IngestContext,
    registry: &Registry,
    cand: &Candidate,
) -> sqlx::Result<Checked> {
    let input = &cand.input;
    let version = input.type_version as i32; // range checked in parse_event

    // Cheap checks first: registry, then payload schema, then the database lookup.
    let (subject_type, validator) = match registry.get(&input.event_type, version) {
        Some(TypeEntry::Ready {
            subject_type,
            validator,
        }) => (subject_type, validator),
        Some(TypeEntry::Missing(RejectionCode::UnknownTypeVersion)) => {
            return Ok(reject(
                RejectionCode::UnknownTypeVersion,
                format!("event type '{}' has no version {version}", input.event_type),
                Some("/typeVersion"),
            ));
        }
        _ => {
            return Ok(reject(
                RejectionCode::UnknownEventType,
                format!("event type '{}' is not registered", input.event_type),
                Some("/type"),
            ));
        }
    };

    let payload = Value::Object(input.payload.clone());
    if let Some((field, message)) = first_violation(validator, &payload) {
        return Ok(reject(RejectionCode::SchemaInvalid, message, Some(&field)));
    }

    let resolution = match &input.subject {
        SubjectRef::Id(s) => subject_resolve(c, ctx.org, &Subject::Id(s.id)).await?,
        SubjectRef::External(s) => {
            subject_resolve(
                c,
                ctx.org,
                &Subject::External {
                    system: &s.external_id.system,
                    id: &s.external_id.id,
                },
            )
            .await?
        }
    };
    let found = match resolution {
        Resolution::Found(f) => f,
        // Also covers objects hidden by org-unit scope: existence must not leak.
        Resolution::NotFound => {
            return Ok(reject(
                RejectionCode::UnresolvedSubject,
                "no object matches the subject",
                Some("/subject"),
            ));
        }
        Resolution::Ambiguous => {
            return Ok(reject(
                RejectionCode::AmbiguousSubject,
                "more than one object matches the subject's external id",
                Some("/subject"),
            ));
        }
    };
    if &found.object_type != subject_type {
        return Ok(reject(
            RejectionCode::SchemaInvalid,
            format!(
                "event type '{}' requires a subject of type '{}', got '{}'",
                input.event_type, subject_type, found.object_type
            ),
            Some("/subject"),
        ));
    }

    Ok(Checked::Ok(NewEvent {
        id: ids::event_id(ctx.org, &ctx.idempotency_key, cand.index),
        event_type: input.event_type.clone(),
        type_version: version,
        subject_id: found.id,
        occurred_at: input.occurred_at,
        payload,
        supersedes: input.supersedes,
    }))
}
