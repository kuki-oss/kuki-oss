//! POST /v1/events: idempotent, partially-successful event ingest.

use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::FutureExt;
use kuki_ingest::{
    body::{Format, parse_rows},
    pipeline::{IngestContext, IngestError, IngestOptions, ingest_events},
};
use kuki_store::{
    ingest::{Idempotency, idempotency_begin, idempotency_complete},
    session::with_org_tx,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    auth::Principal,
    error::{ApiError, FieldError},
    state::AppState,
};

/// Request body ceiling. Larger loads must be split into several requests.
pub const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const MAX_KEY_LEN: usize = 255;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsQuery {
    mode: Option<String>,
    backfill_id: Option<String>,
    #[serde(rename = "return")]
    ret: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Return {
    Full,
    Summary,
}

pub fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let v = headers
        .get("idempotency-key")
        .ok_or_else(|| ApiError::validation("The `Idempotency-Key` header is required."))?
        .to_str()
        .map_err(|_| ApiError::validation("`Idempotency-Key` must be visible ASCII."))?
        .trim();
    if v.is_empty() || v.len() > MAX_KEY_LEN || !v.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(ApiError::validation(
            "`Idempotency-Key` must be 1-255 visible ASCII characters.",
        ));
    }
    Ok(v.to_owned())
}

pub(crate) fn format_of(headers: &HeaderMap) -> Result<Format, ApiError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match ct.as_str() {
        "application/json" => Ok(Format::Json),
        "application/x-ndjson" => Ok(Format::NdJson),
        _ => Err(ApiError::validation(
            "Content-Type must be application/json or application/x-ndjson.",
        )),
    }
}

#[derive(Debug, PartialEq)]
struct Params {
    ret: Return,
    backfill: Option<String>,
}

pub(crate) fn valid_backfill_id(id: &str) -> Result<(), ApiError> {
    if id.is_empty() || id.len() > 128 || !id.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(ApiError::validation(
            "`backfillId` must be 1-128 visible ASCII characters.",
        ));
    }
    Ok(())
}

fn params(q: &EventsQuery) -> Result<Params, ApiError> {
    let ret = match q.ret.as_deref() {
        None | Some("full") => Return::Full,
        Some("summary") => Return::Summary,
        Some(_) => {
            return Err(ApiError::validation(
                "`return` must be `full` or `summary`.",
            ));
        }
    };
    let backfill = match (q.mode.as_deref(), q.backfill_id.as_deref()) {
        (None | Some("live"), None) => None,
        (None | Some("live"), Some(_)) => {
            return Err(ApiError::validation(
                "`backfillId` is only valid with `mode=backfill`.",
            ));
        }
        (Some("backfill"), Some(id)) => {
            valid_backfill_id(id)?;
            Some(id.to_owned())
        }
        (Some("backfill"), None) => {
            return Err(ApiError::validation(
                "`backfillId` is required when `mode=backfill`.",
            ));
        }
        (Some(_), _) => return Err(ApiError::validation("`mode` must be `live` or `backfill`.")),
    };
    Ok(Params { ret, backfill })
}

/// Everything that changes the meaning of the request is hashed, so a reused key with
/// different parameters is a mismatch instead of a silent replay.
fn request_hash(format: Format, q: &EventsQuery, ret: Return, body: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"kuki.events.v1\0");
    h.update(match format {
        Format::Json => &b"json"[..],
        Format::NdJson => &b"ndjson"[..],
    });
    h.update([0]);
    h.update(q.mode.as_deref().unwrap_or("live").as_bytes());
    h.update([0]);
    h.update(q.backfill_id.as_deref().unwrap_or("").as_bytes());
    h.update([0]);
    h.update(match ret {
        Return::Full => &b"full"[..],
        Return::Summary => &b"summary"[..],
    });
    h.update([0]);
    h.update((body.len() as u64).to_be_bytes());
    h.update(body);
    h.finalize().into()
}

pub(crate) fn json_response(
    status: StatusCode,
    body: &serde_json::Value,
    replayed: bool,
) -> Response {
    let mut res = (status, Json(body)).into_response();
    if replayed {
        res.headers_mut()
            .insert("idempotency-replayed", HeaderValue::from_static("true"));
    }
    res
}

pub(crate) fn ingest_error(e: IngestError) -> ApiError {
    match e {
        IngestError::Invalid { pointer, message } => {
            let err = ApiError::validation("The request body is invalid.");
            match pointer {
                Some(pointer) => err.with_field_errors(vec![FieldError { pointer, message }]),
                None => ApiError::validation(message),
            }
        }
        IngestError::Internal(cause) => ApiError::internal(cause),
    }
}

pub(crate) async fn read_body(
    headers: &HeaderMap,
    body: Body,
) -> Result<axum::body::Bytes, ApiError> {
    if let Some(len) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        && len > MAX_BODY_BYTES
    {
        return Err(ApiError::payload_too_large(MAX_BODY_BYTES));
    }
    to_bytes(body, MAX_BODY_BYTES)
        .await
        .map_err(|_| ApiError::payload_too_large(MAX_BODY_BYTES))
}

/// `Ok(Some(response))` is a stored replay; a different body under the same key is a 409.
pub(crate) async fn idempotency_gate(
    st: &AppState,
    principal: &Principal,
    key: &str,
    hash: [u8; 32],
) -> Result<Option<Response>, ApiError> {
    let org = principal.session.org_id;
    let key = key.to_owned();
    let begin = with_org_tx(&st.pool, &principal.session, move |c| {
        async move {
            idempotency_begin(c, org, &key, &hash)
                .await
                .map_err(ApiError::from)
        }
        .boxed()
    })
    .await?;
    match begin {
        Idempotency::Replay { status, response } => {
            let code = u16::try_from(status)
                .ok()
                .and_then(|s| StatusCode::from_u16(s).ok())
                .unwrap_or(StatusCode::ACCEPTED);
            Ok(Some(json_response(code, &response, true)))
        }
        Idempotency::Mismatch => Err(ApiError::conflict(
            None,
            "This Idempotency-Key was already used with a different request.",
        )),
        Idempotency::Proceed | Idempotency::Resume => Ok(None),
    }
}

pub(crate) async fn idempotency_finish(
    st: &AppState,
    principal: &Principal,
    key: &str,
    hash: [u8; 32],
    value: &serde_json::Value,
) -> Result<(), ApiError> {
    let org = principal.session.org_id;
    let (key, stored) = (key.to_owned(), value.clone());
    with_org_tx(&st.pool, &principal.session, move |c| {
        async move {
            idempotency_complete(
                c,
                org,
                &key,
                &hash,
                StatusCode::ACCEPTED.as_u16() as i32,
                &stored,
            )
            .await
            .map_err(ApiError::from)
        }
        .boxed()
    })
    .await
}

pub async fn post_events(
    State(st): State<AppState>,
    principal: Principal,
    Query(q): Query<EventsQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    if !principal.scopes.contains(&crate::auth::Scope::IngestWrite) {
        return Err(ApiError::forbidden("ingest:write"));
    }
    let key = idempotency_key(&headers)?;
    let Params { ret, backfill } = params(&q)?;
    let format = format_of(&headers)?;

    if let Some(len) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<usize>().ok())
        && len > MAX_BODY_BYTES
    {
        return Err(ApiError::payload_too_large(MAX_BODY_BYTES));
    }
    let bytes = read_body(&headers, body).await?;

    let hash = request_hash(format, &q, ret, &bytes);
    let rows = parse_rows(&bytes, format, "events").map_err(ingest_error)?;

    let org = principal.session.org_id;
    if let Some(replay) = idempotency_gate(&st, &principal, &key, hash).await? {
        return Ok(replay);
    }

    if let Some(id) = backfill.clone() {
        let status = with_org_tx(&st.pool, &principal.session, move |c| {
            async move {
                kuki_store::backfill::backfill_open(
                    c,
                    org,
                    &id,
                    kuki_store::backfill::DEFAULT_TTL_SECS,
                )
                .await
                .map_err(ApiError::from)
            }
            .boxed()
        })
        .await?;
        if status != "open" {
            return Err(ApiError::conflict(
                None,
                format!("Backfill is {status}; it no longer accepts events."),
            ));
        }
    }

    let ctx = IngestContext {
        org,
        session: principal.session.clone(),
        source: principal.actor.clone(),
        idempotency_key: key.clone(),
        backfill_id: backfill.clone(),
    };
    let mut result = ingest_events(&st.pool, ctx, rows, &IngestOptions::default())
        .await
        .map_err(ingest_error)?;
    if ret == Return::Summary {
        result.accepted = None;
    }
    let value = serde_json::to_value(&result).map_err(ApiError::internal)?;

    idempotency_finish(&st, &principal, &key, hash, &value).await?;

    Ok(json_response(StatusCode::ACCEPTED, &value, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn idempotency_key_is_required_and_bounded() {
        assert!(idempotency_key(&hdr(&[])).is_err());
        assert!(idempotency_key(&hdr(&[("idempotency-key", "")])).is_err());
        assert!(idempotency_key(&hdr(&[("idempotency-key", "has space")])).is_err());
        assert!(idempotency_key(&hdr(&[("idempotency-key", &"k".repeat(256))])).is_err());
        assert_eq!(
            idempotency_key(&hdr(&[("idempotency-key", "load-2026-10-07")])).unwrap(),
            "load-2026-10-07"
        );
    }

    #[test]
    fn content_type_selects_the_format() {
        let f = |v| format_of(&hdr(&[("content-type", v)]));
        assert_eq!(f("application/json").unwrap(), Format::Json);
        assert_eq!(f("application/json; charset=utf-8").unwrap(), Format::Json);
        assert_eq!(f("Application/X-NDJSON").unwrap(), Format::NdJson);
        assert!(f("text/plain").is_err());
        assert!(format_of(&hdr(&[])).is_err());
    }

    #[test]
    fn parameters_are_validated() {
        let q = |mode: Option<&str>, id: Option<&str>, ret: Option<&str>| EventsQuery {
            mode: mode.map(Into::into),
            backfill_id: id.map(Into::into),
            ret: ret.map(Into::into),
        };
        let p = |a, b, c| params(&q(a, b, c));
        assert_eq!(
            p(None, None, None).unwrap(),
            Params {
                ret: Return::Full,
                backfill: None
            }
        );
        assert_eq!(
            p(Some("live"), None, Some("summary")).unwrap().ret,
            Return::Summary
        );
        assert_eq!(
            p(Some("backfill"), Some("h-2024"), None)
                .unwrap()
                .backfill
                .as_deref(),
            Some("h-2024")
        );
        assert!(p(Some("backfill"), None, None).is_err());
        assert!(p(None, Some("h-2024"), None).is_err());
        assert!(p(Some("backfill"), Some(""), None).is_err());
        let binding = "x".repeat(129);
        assert!(p(Some("backfill"), Some(binding.as_str()), None).is_err());
        assert!(p(Some("nope"), None, None).is_err());
        assert!(p(None, None, Some("everything")).is_err());
        assert_eq!(p(None, None, Some("full")).unwrap().ret, Return::Full);
    }

    #[test]
    fn the_hash_covers_body_and_parameters() {
        let q = EventsQuery::default();
        let base = request_hash(Format::Json, &q, Return::Full, b"{}");
        assert_eq!(base, request_hash(Format::Json, &q, Return::Full, b"{}"));
        assert_ne!(base, request_hash(Format::Json, &q, Return::Full, b"{ }"));
        assert_ne!(base, request_hash(Format::Json, &q, Return::Summary, b"{}"));
        assert_ne!(base, request_hash(Format::NdJson, &q, Return::Full, b"{}"));
    }
}
