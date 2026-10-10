//! GET /v1/quarantine and POST /v1/quarantine/{id}/replay.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use futures::FutureExt;
use kuki_ingest::{pipeline::IngestContext, replay::replay_row};
use kuki_store::{
    ingest::{QuarantineRow, quarantine_get, quarantine_list},
    session::with_org_tx,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    auth::{Principal, Scope},
    error::ApiError,
    events::{
        idempotency_finish, idempotency_gate, idempotency_key, ingest_error, json_response,
        read_body,
    },
    state::AppState,
};

const DEFAULT_PAGE: usize = 25;
const MAX_PAGE: usize = 100;
const CURSOR_PREFIX: &str = "q1.";

/// Quarantine rows carry raw input and have no org-unit policy, so a key limited to a
/// subtree must not reach them.
fn guard(p: &Principal, scope: Scope, name: &'static str) -> Result<(), ApiError> {
    if !p.scopes.contains(&scope) {
        return Err(ApiError::forbidden(name));
    }
    if p.session.scope_path.is_some() {
        return Err(ApiError::forbidden("an organisation-wide key"));
    }
    Ok(())
}

fn bad_param(name: &str, what: &str) -> ApiError {
    ApiError::validation(format!("`{name}` {what}"))
}

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    #[serde(rename = "batchId")]
    batch_id: Option<String>,
    first: Option<String>,
    after: Option<String>,
}

fn item(r: &QuarantineRow) -> Value {
    let mut v = json!({
        "id": r.id,
        "batchId": r.batch_id,
        "index": r.row_index,
        "receivedAt": r.received_at.to_rfc3339(),
        "code": r.code,
        "message": r.message,
        "status": r.status,
        "raw": r.raw,
    });
    if let Some(f) = &r.field {
        v["field"] = json!(f);
    }
    v
}

pub async fn list_quarantine(
    State(st): State<AppState>,
    principal: Principal,
    Query(q): Query<ListQuery>,
) -> Result<Response, ApiError> {
    guard(&principal, Scope::IngestRead, "ingest:read")?;

    let first = match q.first.as_deref() {
        None => DEFAULT_PAGE,
        Some(s) => s
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=MAX_PAGE).contains(n))
            .ok_or_else(|| bad_param("first", "must be an integer from 1 to 100."))?,
    };
    let batch = match q.batch_id.as_deref() {
        None => None,
        Some(s) => Some(Uuid::parse_str(s).map_err(|_| bad_param("batchId", "must be a UUID."))?),
    };
    let after = match q.after.as_deref() {
        None => None,
        Some(s) => Some(
            s.strip_prefix(CURSOR_PREFIX)
                .and_then(|h| Uuid::parse_str(h).ok())
                .ok_or_else(|| bad_param("after", "is not a valid cursor."))?,
        ),
    };

    let org = principal.session.org_id;
    let mut rows = with_org_tx(&st.pool, &principal.session, move |c| {
        async move { quarantine_list(c, org, batch, after, first as i64 + 1).await }.boxed()
    })
    .await
    .map_err(ApiError::internal)?;

    let has_next = rows.len() > first;
    rows.truncate(first);
    let end_cursor = rows
        .last()
        .map(|r| format!("{CURSOR_PREFIX}{}", r.id.simple()));
    let body = json!({
        "items": rows.iter().map(item).collect::<Vec<_>>(),
        "pageInfo": { "hasNextPage": has_next, "endCursor": end_cursor },
    });
    Ok(json_response(StatusCode::OK, &body, false))
}

/// `Ok(None)` for no body or `{}`: replay the original unchanged.
fn parse_correction(bytes: &[u8]) -> Result<Option<(String, Value)>, ApiError> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let Ok(Value::Object(m)) = serde_json::from_slice::<Value>(bytes) else {
        return Err(ApiError::validation("The body must be a JSON object."));
    };
    let mut it = m.into_iter();
    match (it.next(), it.next()) {
        (None, _) => Ok(None),
        (Some((k, row)), None) if matches!(k.as_str(), "event" | "object" | "link") => {
            Ok(Some((k, row)))
        }
        _ => Err(ApiError::validation(
            "The body may hold only one of `event`, `object` or `link`.",
        )),
    }
}

fn hash(id: Uuid, body: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"kuki.replay.v1\0");
    h.update(id.as_bytes());
    h.update((body.len() as u64).to_be_bytes());
    h.update(body);
    h.finalize().into()
}

pub async fn replay_quarantined(
    State(st): State<AppState>,
    principal: Principal,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    guard(&principal, Scope::IngestWrite, "ingest:write")?;
    let key = idempotency_key(&headers)?;
    let id = Uuid::parse_str(&id).map_err(|_| ApiError::not_found("No such quarantined row."))?;
    let bytes = read_body(&headers, body).await?;
    let correction = parse_correction(&bytes)?;

    let hash = hash(id, &bytes);
    if let Some(replay) = idempotency_gate(&st, &principal, &key, hash).await? {
        return Ok(replay);
    }

    let org = principal.session.org_id;
    let entry = with_org_tx(&st.pool, &principal.session, move |c| {
        async move { quarantine_get(c, org, id).await }.boxed()
    })
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("No such quarantined row."))?;

    let expected = match entry.kind.as_str() {
        "events" => "event",
        "objects" => "object",
        _ => "link",
    };
    let row = match correction {
        Some((k, v)) if k == expected => v,
        Some((k, _)) => {
            return Err(ApiError::validation(format!(
                "Row {id} came from a `{}` batch; send `{expected}`, not `{k}`.",
                entry.kind
            )));
        }
        None => entry.raw.clone(),
    };

    let ctx = IngestContext {
        org,
        session: principal.session.clone(),
        source: principal.actor.clone(),
        idempotency_key: key.clone(),
        backfill_id: None,
    };
    let result = replay_row(
        &st.pool,
        ctx,
        id,
        &entry.kind,
        entry.status == "replayed",
        row,
    )
    .await
    .map_err(ingest_error)?
    .ok_or_else(|| ApiError::conflict(None, "This row was already replayed."))?;

    let value = serde_json::to_value(&result).map_err(ApiError::internal)?;
    idempotency_finish(&st, &principal, &key, hash, &value).await?;
    Ok(json_response(StatusCode::ACCEPTED, &value, false))
}
