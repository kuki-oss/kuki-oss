//! Object upserts: versioned, and ledger-backed (invariant I5).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use jsonschema::Validator;
use kuki_store::{
    ingest::{NewBatch, NewEvent, batch_ensure, batch_finish, events_insert},
    objects::{
        CurrentObject, NewVersion, ObjectTypeLookup, event_outcome_get, object_is_current,
        object_type_lookup, object_write_version, objects_matching, org_unit_resolve,
    },
    session::with_org_tx,
};
use kuki_types::ids;
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    body::{RawRow, RowContent},
    outcome::{Rejected, RejectionCode, UpsertAccepted, UpsertResult},
    pipeline::{Failure, IngestContext, IngestError, IngestOptions, batch_kind, quarantine_row},
    registry::first_violation,
};

#[derive(Debug, Clone)]
pub struct ObjectInput {
    pub id: Option<Uuid>,
    pub object_type: String,
    pub type_version: i32,
    pub external_ids: Map<String, Value>,
    pub org_unit_path: Option<String>,
    pub attrs: Map<String, Value>,
    pub valid_from: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq)]
pub struct Problem {
    pub field: String,
    pub message: String,
}

pub(crate) fn bad(field: impl Into<String>, message: impl Into<String>) -> Problem {
    Problem {
        field: field.into(),
        message: message.into(),
    }
}

pub(crate) fn pointer(key: &str) -> String {
    format!("/{}", key.replace('~', "~0").replace('/', "~1"))
}

fn valid_ltree(p: &str) -> bool {
    !p.is_empty()
        && p.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 256
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
}

const KNOWN: [&str; 7] = [
    "id",
    "type",
    "typeVersion",
    "externalIds",
    "orgUnitPath",
    "attrs",
    "validFrom",
];

pub fn parse_object(v: &Value) -> Result<ObjectInput, Problem> {
    let obj = v
        .as_object()
        .ok_or_else(|| bad("", "row must be a JSON object"))?;
    if let Some(k) = obj.keys().find(|k| !KNOWN.contains(&k.as_str())) {
        return Err(bad(pointer(k), "unknown field"));
    }
    let object_type = obj
        .get("type")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| bad("/type", "is required and must be a non-empty string"))?
        .to_owned();
    let type_version = obj
        .get("typeVersion")
        .and_then(Value::as_i64)
        .filter(|n| (1..=i64::from(i32::MAX)).contains(n))
        .ok_or_else(|| bad("/typeVersion", "is required and must be an integer >= 1"))?
        as i32;
    let external_ids = match obj.get("externalIds") {
        Some(Value::Object(m)) if !m.is_empty() => {
            for (k, val) in m {
                if k.is_empty() || !val.as_str().is_some_and(|s| !s.is_empty()) {
                    return Err(bad(
                        format!("/externalIds{}", pointer(k)),
                        "keys and values must be non-empty strings",
                    ));
                }
            }
            m.clone()
        }
        _ => {
            return Err(bad(
                "/externalIds",
                "is required and must be a non-empty object",
            ));
        }
    };
    let attrs = match obj.get("attrs") {
        Some(Value::Object(m)) => m.clone(),
        _ => return Err(bad("/attrs", "is required and must be an object")),
    };
    let id = match obj.get("id") {
        None => None,
        Some(Value::String(s)) => {
            Some(Uuid::parse_str(s).map_err(|_| bad("/id", "must be a UUID"))?)
        }
        Some(_) => return Err(bad("/id", "must be a UUID string")),
    };
    let org_unit_path = match obj.get("orgUnitPath") {
        None => None,
        Some(Value::String(s)) if valid_ltree(s) => Some(s.clone()),
        Some(_) => {
            return Err(bad(
                "/orgUnitPath",
                "must be dot-separated labels of letters, digits and _",
            ));
        }
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
    Ok(ObjectInput {
        id,
        object_type,
        type_version,
        external_ids,
        org_unit_path,
        attrs,
        valid_from,
    })
}

#[derive(Debug)]
enum Match {
    None,
    One(Box<CurrentObject>),
    Ambiguous,
}

/// With `id`, the only acceptable match is that exact object. Without it, exactly one object.
fn resolve_match(id: Option<Uuid>, mut rows: Vec<CurrentObject>) -> Match {
    match id {
        Some(id) => match rows.iter().position(|r| r.id == id) {
            Some(pos) if rows.len() == 1 => Match::One(Box::new(rows.remove(pos))),
            Some(_) => Match::Ambiguous,
            None if rows.is_empty() => Match::None,
            None => Match::Ambiguous,
        },
        None => match rows.len() {
            0 => Match::None,
            1 => Match::One(Box::new(rows.remove(0))),
            _ => Match::Ambiguous,
        },
    }
}

enum Lookup {
    Unknown,
    UnknownVersion,
    Ready(Validator),
}
type Cache = HashMap<(String, i32), Lookup>;

#[derive(Clone)]
pub(crate) struct Ctx {
    pub org: Uuid,
    pub key: String,
    pub source: String,
    pub scoped: bool,
    pub replay_of: Option<Uuid>,
}

pub(crate) enum Step {
    Done {
        id: Uuid,
        outcome: &'static str,
    },
    Reject {
        code: RejectionCode,
        message: String,
        field: Option<String>,
    },
}

pub(crate) fn reject(code: RejectionCode, message: impl Into<String>, field: Option<&str>) -> Step {
    Step::Reject {
        code,
        message: message.into(),
        field: field.map(str::to_owned),
    }
}

#[derive(Default)]
pub(crate) struct ChunkOut {
    pub accepted: Vec<UpsertAccepted>,
    pub rejected: Vec<Rejected>,
}

pub(crate) fn db(e: impl std::fmt::Display) -> IngestError {
    IngestError::internal(e.to_string())
}

pub(crate) fn raw_of(row: &RawRow) -> Value {
    match &row.content {
        RowContent::Json(v) => v.clone(),
        RowContent::Malformed { text, .. } => Value::String(text.clone()),
    }
}

pub(crate) async fn reject_row(
    c: &mut PgConnection,
    ctx: &Ctx,
    batch: Uuid,
    row: &RawRow,
    code: RejectionCode,
    message: String,
    field: Option<String>,
) -> sqlx::Result<Rejected> {
    let raw = raw_of(row);
    let quarantine_id = quarantine_row(
        c,
        ctx.org,
        &ctx.key,
        batch,
        ctx.replay_of,
        &Failure {
            index: row.index,
            code,
            message: &message,
            field: field.as_deref(),
            raw: &raw,
        },
    )
    .await?;
    Ok(Rejected {
        index: row.index,
        code,
        message,
        field,
        quarantine_id,
    })
}

async fn load_type(
    c: &mut PgConnection,
    org: Uuid,
    name: &str,
    version: i32,
) -> sqlx::Result<Lookup> {
    Ok(match object_type_lookup(c, org, name, version).await? {
        ObjectTypeLookup::Unknown => Lookup::Unknown,
        ObjectTypeLookup::UnknownVersion => Lookup::UnknownVersion,
        ObjectTypeLookup::Found(schema) => {
            Lookup::Ready(jsonschema::validator_for(&schema).map_err(|e| {
                // A broken registry entry is the operator's problem, not the caller's: fail the chunk.
                sqlx::Error::Protocol(format!(
                    "object type {name} v{version} has an invalid schema: {e}"
                ))
            })?)
        }
    })
}

pub(crate) fn recorded_outcome(payload: &Value) -> &'static str {
    if payload["outcome"] == "created" {
        "created"
    } else {
        "updated"
    }
}

async fn process_row(
    c: &mut PgConnection,
    ctx: &Ctx,
    cache: &mut Cache,
    row: &RawRow,
) -> sqlx::Result<Step> {
    let value = match &row.content {
        RowContent::Json(v) => v,
        RowContent::Malformed { message, .. } => {
            return Ok(reject(RejectionCode::MalformedLine, message.clone(), None));
        }
    };
    let input = match parse_object(value) {
        Ok(i) => i,
        Err(p) => {
            return Ok(reject(
                RejectionCode::SchemaInvalid,
                p.message,
                Some(&p.field),
            ));
        }
    };

    // A previous attempt of this same request already wrote this row: report what it recorded.
    let event_id = ids::event_id(ctx.org, &ctx.key, row.index);
    if let Some((subject, payload)) = event_outcome_get(c, ctx.org, event_id).await? {
        return Ok(Step::Done {
            id: subject,
            outcome: recorded_outcome(&payload),
        });
    }

    let key = (input.object_type.clone(), input.type_version);
    if !cache.contains_key(&key) {
        let l = load_type(c, ctx.org, &key.0, key.1).await?;
        cache.insert(key.clone(), l);
    }
    let validator = match &cache[&key] {
        Lookup::Unknown => {
            return Ok(reject(
                RejectionCode::UnknownObjectType,
                format!("object type '{}' is not registered", input.object_type),
                Some("/type"),
            ));
        }
        Lookup::UnknownVersion => {
            return Ok(reject(
                RejectionCode::UnknownTypeVersion,
                format!(
                    "object type '{}' has no version {}",
                    input.object_type, input.type_version
                ),
                Some("/typeVersion"),
            ));
        }
        Lookup::Ready(v) => v,
    };
    let attrs = Value::Object(input.attrs.clone());
    if let Some((field, message)) = first_violation(validator, &attrs) {
        // `first_violation` reports pointers as events see them (`/payload/...`).
        let inner = field.strip_prefix("/payload").unwrap_or(&field);
        return Ok(reject(
            RejectionCode::SchemaInvalid,
            message,
            Some(&format!("/attrs{inner}")),
        ));
    }

    let unit = match &input.org_unit_path {
        Some(p) => match org_unit_resolve(c, ctx.org, p).await? {
            Some(u) => Some(u),
            None => {
                return Ok(reject(
                    RejectionCode::OutOfScope,
                    "no such org unit in scope",
                    Some("/orgUnitPath"),
                ));
            }
        },
        None if ctx.scoped => {
            return Ok(reject(
                RejectionCode::OutOfScope,
                "this key is limited to an org-unit subtree, so `orgUnitPath` is required",
                Some("/orgUnitPath"),
            ));
        }
        None => None,
    };

    let pairs: Vec<Value> = input
        .external_ids
        .iter()
        .map(|(k, v)| json!({ k: v }))
        .collect();
    let rows = objects_matching(c, ctx.org, input.id, &pairs).await?;
    let current = match resolve_match(input.id, rows) {
        Match::None => None,
        Match::One(o) => Some(*o),
        Match::Ambiguous => {
            return Ok(reject(
                RejectionCode::AmbiguousSubject,
                "externalIds match more than one object, or a different object than `id`",
                Some("/externalIds"),
            ));
        }
    };
    if let Some(cur) = &current
        && cur.object_type != input.object_type
    {
        return Ok(reject(
            RejectionCode::SchemaInvalid,
            "an object's type cannot change",
            Some("/type"),
        ));
    }

    // External ids merge: incoming pairs win per system, existing pairs are kept.
    let mut ext = current
        .as_ref()
        .and_then(|c| c.external_ids.as_object().cloned())
        .unwrap_or_default();
    for (k, v) in &input.external_ids {
        ext.insert(k.clone(), v.clone());
    }
    let ext = Value::Object(ext);
    let target_unit = unit.or(current.as_ref().and_then(|c| c.org_unit_id));

    if let Some(cur) = &current
        && object_is_current(
            c,
            cur.id,
            &input.object_type,
            input.type_version,
            target_unit,
            &ext,
            &attrs,
        )
        .await?
    {
        return Ok(Step::Done {
            id: cur.id,
            outcome: "unchanged",
        });
    }

    let (id, outcome, valid_from) = match &current {
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
            (cur.id, "updated", vf)
        }
        None => (
            input
                .id
                .unwrap_or_else(|| ids::object_id(ctx.org, &ctx.key, row.index)),
            "created",
            input.valid_from.unwrap_or_else(Utc::now),
        ),
    };

    // Ledger first (I5), then the projection, in the same transaction.
    let payload = json!({
        "outcome": outcome,
        "type": input.object_type,
        "typeVersion": input.type_version,
        "externalIds": ext,
        "orgUnitId": target_unit,
        "attrs": attrs,
        "validFrom": valid_from.to_rfc3339(),
    });
    let event = NewEvent {
        id: event_id,
        event_type: "kuki.object.upserted".into(),
        type_version: 1,
        subject_id: id,
        occurred_at: Utc::now(),
        payload,
        supersedes: None,
    };
    events_insert(c, ctx.org, &ctx.source, &ctx.key, None, &[event]).await?;
    object_write_version(
        c,
        &NewVersion {
            id,
            org: ctx.org,
            org_unit_id: target_unit,
            object_type: &input.object_type,
            type_version: input.type_version,
            external_ids: &ext,
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
    cache: &mut Cache,
    chunk: &[RawRow],
) -> sqlx::Result<ChunkOut> {
    let mut out = ChunkOut::default();
    for row in chunk {
        match process_row(c, ctx, cache, row).await? {
            Step::Done { id, outcome } => out.accepted.push(UpsertAccepted {
                index: row.index,
                id,
                outcome,
            }),
            Step::Reject {
                code,
                message,
                field,
            } => {
                out.rejected
                    .push(reject_row(c, ctx, batch, row, code, message, field).await?);
            }
        }
    }
    Ok(out)
}

pub async fn upsert_objects(
    pool: &PgPool,
    ctx: IngestContext,
    rows: Vec<RawRow>,
    opts: &IngestOptions,
) -> Result<UpsertResult, IngestError> {
    upsert_objects_with(pool, ctx, rows, opts, None).await
}

pub async fn upsert_objects_with(
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
                        kind: batch_kind("objects", c.replay_of),
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

    let mut cache = Cache::new();
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for chunk in rows.chunks(opts.chunk_size.max(1)) {
        // The closure must own everything it touches (it is higher-ranked over the connection
        // borrow), so the validator cache goes in and comes back out with the result.
        let (c2, owned, taken) = (c.clone(), chunk.to_vec(), std::mem::take(&mut cache));
        let (out, back) = with_org_tx(pool, &ctx.session, move |conn| {
            async move {
                let mut cache = taken;
                let out = run_chunk(conn, &c2, batch, &mut cache, &owned).await?;
                Ok::<_, sqlx::Error>((out, cache))
            }
            .boxed()
        })
        .await
        .map_err(db)?;
        cache = back;
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
        json!({ "type": "item", "typeVersion": 1, "externalIds": { "erp": "A1" }, "attrs": { "name": "Bolt" } })
    }
    fn with(k: &str, v: Value) -> Value {
        let mut g = good();
        g[k] = v;
        g
    }
    fn field(v: Value) -> String {
        parse_object(&v).unwrap_err().field
    }
    fn current(id: Uuid) -> CurrentObject {
        CurrentObject {
            id,
            org_unit_id: None,
            object_type: "item".into(),
            type_version: 1,
            external_ids: json!({}),
            attrs: json!({}),
            valid_from: Utc::now(),
        }
    }

    #[test]
    fn a_well_formed_object_parses() {
        let o = parse_object(&good()).unwrap();
        assert_eq!((o.object_type.as_str(), o.type_version), ("item", 1));
        assert!(o.id.is_none() && o.valid_from.is_none() && o.org_unit_path.is_none());
        let o = parse_object(&with("validFrom", json!("2026-10-07T12:00:00+02:00"))).unwrap();
        assert_eq!(
            o.valid_from.unwrap().to_rfc3339(),
            "2026-10-07T10:00:00+00:00"
        );
    }

    #[test]
    fn problems_carry_json_pointers() {
        assert_eq!(field(json!([1])), "");
        assert_eq!(field(with("bogus", json!(1))), "/bogus");
        assert_eq!(field(with("typeVersion", json!(0))), "/typeVersion");
        assert_eq!(field(with("externalIds", json!({}))), "/externalIds");
        assert_eq!(
            field(with("externalIds", json!({ "erp": "" }))),
            "/externalIds/erp"
        );
        assert_eq!(field(with("attrs", json!([]))), "/attrs");
        assert_eq!(field(with("id", json!("nope"))), "/id");
        assert_eq!(field(with("orgUnitPath", json!("a..b"))), "/orgUnitPath");
        assert_eq!(field(with("validFrom", json!("yesterday"))), "/validFrom");
    }

    #[test]
    fn matching_requires_one_unambiguous_object() {
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(matches!(resolve_match(None, vec![]), Match::None));
        assert!(matches!(
            resolve_match(None, vec![current(a)]),
            Match::One(_)
        ));
        assert!(matches!(
            resolve_match(None, vec![current(a), current(b)]),
            Match::Ambiguous
        ));
        assert!(matches!(
            resolve_match(Some(a), vec![current(a)]),
            Match::One(_)
        ));
        assert!(matches!(resolve_match(Some(a), vec![]), Match::None));
        assert!(matches!(
            resolve_match(Some(a), vec![current(b)]),
            Match::Ambiguous
        ));
        assert!(matches!(
            resolve_match(Some(a), vec![current(a), current(b)]),
            Match::Ambiguous
        ));
    }
}
