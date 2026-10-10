//! POST /v1/links/batch: versioned, ledge-backed link upserts.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use kuki_ingest::{
    body::parse_rows,
    links::upsert_links,
    pipeline::{IngestContext, IngestOptions},
};

use crate::{
    auth::{Principal, Scope},
    error::ApiError,
    events::{
        format_of, idempotency_finish, idempotency_gate, idempotency_key, ingest_error,
        json_response, read_body,
    },
    objects::{UpsertQuery, request_hash, summary_only},
    state::AppState,
};

pub async fn post_links(
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

    let hash = request_hash("kuki.links.v1", format, summary, &bytes);
    let rows = parse_rows(&bytes, format, "links").map_err(ingest_error)?;
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
    let mut result = upsert_links(&st.pool, ctx, rows, &IngestOptions::default())
        .await
        .map_err(ingest_error)?;
    if summary {
        result.accepted = None;
    }
    let value = serde_json::to_value(&result).map_err(ApiError::internal)?;
    idempotency_finish(&st, &principal, &key, hash, &value).await?;
    Ok(json_response(StatusCode::ACCEPTED, &value, false))
}
