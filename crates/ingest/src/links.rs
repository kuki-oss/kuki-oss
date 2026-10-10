//! Link upserts: versioned per (type, src, dst) and ledger-backed, like objects.

use chrono::{DateTime, Utc};
use futures::FutureExt;
use kuki_store::{
    ingest::{NewBatch, NewEvent, Resolution, Subject, batch_ensure, batch_finish, events_insert},
    links::{NewLink, link_current, link_is_current, link_write_version},
    objects::event_outcome_get,
    session::with_org_tx,
};
use kuki_types::ids;
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    body::{RawRow, RowContent},
    objects::{
        ChunkOut, Ctx, Problem, Step, bad, db, pointer, recorded_outcome, reject, reject_row,
    },
    outcome::{RejectionCode, UpsertAccepted, UpsertResult},
    pipeline::{IngestContext, IngestError, IngestOptions, batch_kind},
};

#[derive(Debug, Clone, PartialEq)]
pub enum Endpoint {
    Id(Uuid),
    External { system: String, id: String },
}

#[derive(Debug, Clone)]
pub struct LinkInput {
    pub link_type: String,
    pub src: Endpoint,
    pub dst: Endpoint,
    pub attrs: Map<String, Value>,
    pub valid_from: Option<DateTime<Utc>>,
}

fn parse_endpoint(v: Option<&Value>, at: &str) -> Result<Endpoint, Problem> {
    let obj = match v {
        Some(Value::Object(m)) => m,
        _ => return Err(bad(at, "is required and must be an object")),
    };
    if let Some(k) = obj.keys().find(|k| *k != "id" && *k != "externalId") {
        return Err(bad(format!("{at}{}", pointer(k)), "unknown field"));
    }
    match (obj.get("id"), obj.get("externalId")) {
        (Some(Value::String(s)), None) => Uuid::parse_str(s)
            .map(Endpoint::Id)
            .map_err(|_| bad(format!("{at}/id"), "must be a UUID")),
        (None, Some(Value::Object(e))) => {
            let get = |k: &str| {
                e.get(k)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
            };
            match (get("system"), get("id")) {
                (Some(system), Some(id)) if e.len() == 2 => Ok(Endpoint::External { system, id }),
                _ => Err(bad(
                    format!("{at}/externalId"),
                    "must be {system, id}, both non-empty strings",
                )),
            }
        }
        _ => Err(bad(at, "must contain exactly one of `id` or `externalId`")),
    }
}

const KNOWN: [&str; 5] = ["type", "src", "dst", "attrs", "validFrom"];

pub fn parse_link(v: &Value) -> Result<LinkInput, Problem> {
    let obj = v
        .as_object()
        .ok_or_else(|| bad("", "row must be a JSON object"))?;
    if let Some(k) = obj.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        return Err(bad(pointer(k), "unknown field"));
    }
    let link_type = obj
        .get("type")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .ok_or_else(|| {
            bad(
                "/type",
                "is required and must be a string of 1-128 characters",
            )
        })?
        .to_owned();
    let src = parse_endpoint(obj.get("src"), "/src")?;
    let dst = parse_endpoint(obj.get("dst"), "/dst")?;
    let attrs = match obj.get("attrs") {
        None => Map::new(),
        Some(Value::Object(m)) => m.clone(),
        Some(_) => return Err(bad("/attrs", "must be an object")),
    };
    let valid_from = match obj.get("validFrom") {
        None => None,
        Some(Value::String(s)) => Some(
            DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| bad("/validFrom", "must be an RFC 3339 timestamp"))?,
        ),
        Some(_) => return Err(bad("/validFrom", "must be an RFC 3339 timestamp")),
    };
    Ok(LinkInput {
        link_type,
        src,
        dst,
        attrs,
        valid_from,
    })
}

/// The row id of the version this request writes. Deterministic because the event id is.
/// TODO: move into `kuki_types::ids` with a frozen known-vector test.
fn link_id(event_id: Uuid) -> Uuid {
    Uuid::new_v5(&event_id, b"kuki.link")
}

/// Unresolvable and out-of-scope endpoints look the same, so existence does not leak.
async fn resolve(
    c: &mut PgConnection,
    org: Uuid,
    ep: &Endpoint,
    field: &str,
) -> sqlx::Result<Result<Uuid, Step>> {
    let r = match ep {
        Endpoint::Id(id) => kuki_store::ingest::subject_resolve(c, org, &Subject::Id(*id)).await?,
        Endpoint::External { system, id } => {
            kuki_store::ingest::subject_resolve(c, org, &Subject::External { system, id }).await?
        }
    };
    Ok(match r {
        Resolution::Found(f) => Ok(f.id),
        Resolution::NotFound => Err(reject(
            RejectionCode::UnresolvedSubject,
            format!("no object matches `{field}`"),
            Some(&format!("/{field}")),
        )),
        Resolution::Ambiguous => Err(reject(
            RejectionCode::AmbiguousSubject,
            format!("`{field}` matches more than one object"),
            Some(&format!("/{field}")),
        )),
    })
}

async fn process_row(c: &mut PgConnection, ctx: &Ctx, row: &RawRow) -> sqlx::Result<Step> {
    let value = match &row.content {
        RowContent::Json(v) => v,
        RowContent::Malformed { message, .. } => {
            return Ok(reject(RejectionCode::MalformedLine, message.clone(), None));
        }
    };
    let input = match parse_link(value) {
        Ok(i) => i,
        Err(p) => {
            return Ok(reject(
                RejectionCode::SchemaInvalid,
                p.message,
                Some(&p.field),
            ));
        }
    };

    // A previous attempt of this request already wrote this row: report what it recorded.
    let event_id = ids::event_id(ctx.org, &ctx.key, row.index);
    if let Some((subject, payload)) = event_outcome_get(c, ctx.org, event_id).await? {
        return Ok(Step::Done {
            id: subject,
            outcome: recorded_outcome(&payload),
        });
    }

    let src = match resolve(c, ctx.org, &input.src, "src").await? {
        Ok(id) => id,
        Err(step) => return Ok(step),
    };
    let dst = match resolve(c, ctx.org, &input.dst, "dst").await? {
        Ok(id) => id,
        Err(step) => return Ok(step),
    };
    let attrs = Value::Object(input.attrs.clone());

    let current = link_current(c, ctx.org, &input.link_type, src, dst).await?;
    if let Some(cur) = &current
        && link_is_current(c, cur.id, &attrs).await?
    {
        return Ok(Step::Done {
            id: cur.id,
            outcome: "unchanged",
        });
    }

    let (outcome, valid_from) = match &current {
        Some(cur) => {
            let vf = match input.valid_from {
                Some(t) if t > cur.valid_from => t,
                Some(_) => {
                    return Ok(reject(
                        RejectionCode::SchemaInvalid,
                        "must be later than the current version's validFrom",
                        Some("/validFrom"),
                    ));
                }
                None => std::cmp::max(
                    Utc::now(),
                    cur.valid_from + chrono::Duration::microseconds(1),
                ),
            };
            ("updated", vf)
        }
        None => ("created", input.valid_from.unwrap_or_else(Utc::now)),
    };

    // Ledger first (I5), then the projection, in the same transaction.
    let id = link_id(event_id);
    let event = NewEvent {
        id: event_id,
        event_type: "kuki.link.upserted".into(),
        type_version: 1,
        subject_id: id,
        occurred_at: Utc::now(),
        payload: json!({
            "outcome": outcome,
            "type": input.link_type,
            "srcId": src,
            "dstId": dst,
            "attrs": attrs,
            "validFrom": valid_from.to_rfc3339(),
        }),
        supersedes: None,
    };
    events_insert(c, ctx.org, &ctx.source, &ctx.key, None, &[event]).await?;
    link_write_version(
        c,
        current.as_ref().map(|c| c.id),
        &NewLink {
            id,
            org: ctx.org,
            link_type: &input.link_type,
            src,
            dst,
            attrs: &attrs,
            valid_from,
        },
    )
    .await?;
    Ok(Step::Done { id, outcome })
}

async fn run_chunk(
    c: &mut PgConnection,
    ctx: &Ctx,
    batch: Uuid,
    chunk: &[RawRow],
) -> sqlx::Result<ChunkOut> {
    let mut out = ChunkOut::default();
    for row in chunk {
        match process_row(c, ctx, row).await? {
            Step::Done { id, outcome } => out.accepted.push(UpsertAccepted {
                index: row.index,
                id,
                outcome,
            }),
            Step::Reject {
                code,
                message,
                field,
            } => out
                .rejected
                .push(reject_row(c, ctx, batch, row, code, message, field).await?),
        }
    }
    Ok(out)
}

pub async fn upsert_links(
    pool: &PgPool,
    ctx: IngestContext,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
) -> Result<UpsertResult, IngestError> {
    upsert_links_with(pool, ctx, rows, opts, None).await
}

pub(crate) async fn upsert_links_with(
    pool: &PgPool,
    ctx: IngestContext,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
    replay_of: Option<Uuid>,
) -> Result<UpsertResult, IngestError> {
    let c = Ctx {
        org: ctx.org,
        key: ctx.idempotency_key.clone(),
        source: ctx.source.clone(),
        scoped: ctx.session.scope_path.is_some(),
        replay_of,
    };
    let batch = ids::batch_id(c.org, &c.key);

    {
        let c = c.clone();
        with_org_tx(pool, &ctx.session, move |conn| {
            async move {
                batch_ensure(
                    conn,
                    &NewBatch {
                        id: batch,
                        org: c.org,
                        kind: batch_kind("links", c.replay_of),
                        idempotency_key: &c.key,
                        source: &c.source,
                        backfill_id: None,
                    },
                )
                .await
            }
            .boxed()
        })
        .await
        .map_err(db)?;
    }

    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for chunk in rows.chunks(opts.chunk_size.max(1)) {
        let (c2, owned) = (c.clone(), chunk.to_vec());
        let out = with_org_tx(pool, &ctx.session, move |conn| {
            async move { run_chunk(conn, &c2, batch, &owned).await }.boxed()
        })
        .await
        .map_err(db)?;
        accepted.extend(out.accepted);
        rejected.extend(out.rejected);
    }

    let (a, r) = (accepted.len() as i32, rejected.len() as i32);
    with_org_tx(pool, &ctx.session, move |conn| {
        async move { batch_finish(conn, batch, a, r).await }.boxed()
    })
    .await
    .map_err(db)?;

    Ok(UpsertResult {
        batch_id: batch,
        accepted_count: accepted.len() as u32,
        accepted: Some(accepted),
        rejected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> Value {
        json!({
            "type": "supplies",
            "src": { "id": "7a3c2f0e-5b4d-4c1e-9a55-1c9d2f6a8b10" },
            "dst": { "externalId": { "system": "erp", "id": "B" } }
        })
    }
    fn with(k: &str, v: Value) -> Value {
        let mut g = good();
        g[k] = v;
        g
    }
    fn field(v: Value) -> String {
        parse_link(&v).unwrap_err().field
    }

    #[test]
    fn a_well_formed_link_parses() {
        let l = parse_link(&good()).unwrap();
        assert_eq!(l.link_type, "supplies");
        assert!(matches!(l.src, Endpoint::Id(_)));
        assert_eq!(
            l.dst,
            Endpoint::External {
                system: "erp".into(),
                id: "B".into()
            }
        );
        assert!(l.attrs.is_empty() && l.valid_from.is_none());
    }

    #[test]
    fn problems_carry_json_pointers() {
        assert_eq!(field(json!(1)), "");
        assert_eq!(field(with("bogus", json!(1))), "/bogus");
        assert_eq!(field(with("type", json!(""))), "/type");
        assert_eq!(field(with("attrs", json!([]))), "/attrs");
        assert_eq!(field(with("validFrom", json!("yesterday"))), "/validFrom");
        assert_eq!(field(with("src", json!({}))), "/src");
        assert_eq!(field(with("src", json!({ "id": "nope" }))), "/src/id");
        assert_eq!(field(with("src", json!({ "extra": 1 }))), "/src/extra");
        assert_eq!(
            field(with(
                "src",
                json!({ "id": "7a3c2f0e-5b4d-4c1e-9a55-1c9d2f6a8b10", "externalId": { "system": "a", "id": "b" } })
            )),
            "/src"
        );
        assert_eq!(
            field(with("dst", json!({ "externalId": { "system": "erp" } }))),
            "/dst/externalId"
        );
        assert_eq!(
            field(with(
                "dst",
                json!({ "externalId": { "system": "e", "id": "i", "x": 1 } })
            )),
            "/dst/externalId"
        );
        let mut missing = good();
        missing.as_object_mut().unwrap().remove("dst");
        assert_eq!(field(missing), "/dst");
    }

    #[test]
    fn version_ids_are_deterministic_and_distinct() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(link_id(a), link_id(a));
        assert_ne!(link_id(a), link_id(b));
        assert_ne!(link_id(a), a);
    }
}
