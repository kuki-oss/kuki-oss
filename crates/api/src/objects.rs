//! POST /v1/objects/batch: versioned, ledger-backed object upserts.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use kuki_ingest::{
    body::parse_rows,
    objects::upsert_objects,
    pipeline::{IngestContext, IngestOptions},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{
    auth::{Principal, Scope},
    error::ApiError,
    events::{
        format_of, idempotency_finish, idempotency_gate, idempotency_key, ingest_error,
        json_response, read_body,
    },
    state::AppState,
};

/// Shared by /v1/objects/batch and /v1/links/batch.
#[derive(Debug, Default, Deserialize)]
pub struct UpsertQuery {
    #[serde(rename = "return")]
    ret: Option<String>,
}

pub(crate) fn summary_only(q: &UpsertQuery) -> Result<bool, ApiError> {
    match q.ret.as_deref() {
        None | Some("full") => Ok(false),
        Some("summary") => Ok(true),
        Some(_) => Err(ApiError::validation(
            "`return` must be `full` or `summary`.",
        )),
    }
}

/// The endpoint tag is part of the hash, so a key reused on another
/// endpoint is a mismatch rather than a bogus replay.
pub(crate) fn request_hash(
    tag: &str,
    format: kuki_ingest::body::Format,
    summary: bool,
    body: &[u8],
) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(tag.as_bytes());
    h.update([0]);
    h.update([
        matches!(format, kuki_ingest::body::Format::NdJson) as u8,
        summary as u8,
        0,
    ]);
    h.update((body.len() as u64).to_be_bytes());
    h.update(body);
    h.finalize().into()
}

pub async fn post_objects(
    State(st): State<AppState>,
    principal: Principal,
    Query(q): Query<UpsertQuery>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    if !principal.scopes.contains(&Scope::IngestWrite) {
        return Err(ApiError::forbidden("ingest:write"));
    }
    let key = idempotency_key(&headers)?;
    let summary = summary_only(&q)?;
    let format = format_of(&headers)?;
    let bytes = read_body(&headers, body).await?;

    let hash = request_hash("kuki.objects.v1", format, summary, &bytes);
    let rows = parse_rows(&bytes, format, "objects").map_err(ingest_error)?;
    if let Some(replay) = idempotency_gate(&st, &principal, &key, hash).await? {
        return Ok(replay);
    }

    let ctx = IngestContext {
        org: principal.session.org_id,
        session: principal.session.clone(),
        source: principal.actor.clone(),
        idempotency_key: key.clone(),
        backfill_id: None,
    };
    let mut result = upsert_objects(&st.pool, ctx, rows, &IngestOptions::default())
        .await
        .map_err(ingest_error)?;
    if summary {
        result.accepted = None;
    }
    let value = serde_json::to_value(&result).map_err(ApiError::internal)?;
    idempotency_finish(&st, &principal, &key, hash, &value).await?;
    Ok(json_response(StatusCode::ACCEPTED, &value, false))
}
