//! GET /v1/backfills/{id} and POST /v1/backfills/{id}/complete.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures::FutureExt;
use kuki_store::{
    backfill::{Backfill, backfill_begin_complete, backfill_get},
    session::with_org_tx,
};
use serde_json::{Value, json};

use crate::{
    auth::{Principal, Scope},
    error::ApiError,
    events::{idempotency_key, valid_backfill_id},
    state::AppState,
};

fn view(b: &Backfill) -> Value {
    json!({
        "backfillId": b.id,
        "status": b.status,
        "openedAt": b.opened_at.to_rfc3339(),
        "expiresAt": b.expires_at.to_rfc3339(),
        "eventCount": b.event_count,
    })
}

/// A malformed id cannot exist, so it is a 404 like any other unknown id.
fn checked(id: &str) -> Result<(), ApiError> {
    valid_backfill_id(id).map_err(|_| ApiError::not_found("Backfill"))
}

pub async fn get_backfill(
    State(st): State<AppState>,
    principal: Principal,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !principal.scopes.contains(&Scope::IngestRead) {
        return Err(ApiError::forbidden("ingest:read"));
    }
    checked(&id)?;
    let org = principal.session.org_id;
    let found = with_org_tx(&st.pool, &principal.session, move |c| {
        async move { backfill_get(c, org, &id).await.map_err(ApiError::from) }.boxed()
    })
    .await?;
    let b = found.ok_or_else(|| ApiError::not_found("Backfill"))?;
    Ok(Json(view(&b)))
}

/// Open -> completing. Safe to retry: an already-completing or completed backfill answers
/// 202 with its current status, and the transition itself happens at most once.
pub async fn complete_backfill(
    State(st): State<AppState>,
    principal: Principal,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if !principal.scopes.contains(&Scope::IngestWrite) {
        return Err(ApiError::forbidden("ingest:write"));
    }
    idempotency_key(&headers)?;
    checked(&id)?;
    let org = principal.session.org_id;
    let found = with_org_tx(&st.pool, &principal.session, move |c| {
        async move {
            if let Some(b) = backfill_begin_complete(c, org, &id).await? {
                return Ok::<_, ApiError>(Some(b));
            }
            Ok(backfill_get(c, org, &id).await?)
        }
        .boxed()
    })
    .await?;

    let b = found.ok_or_else(|| ApiError::not_found("Backfill"))?;
    if b.status == "expired" {
        return Err(ApiError::conflict(
            None,
            "Backfill expired before it was completed.",
        ));
    }
    let body = json!({ "backfillId": b.id, "status": b.status });
    Ok((StatusCode::ACCEPTED, Json(body)).into_response())
}
