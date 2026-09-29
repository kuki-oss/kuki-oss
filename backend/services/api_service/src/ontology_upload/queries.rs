//! Database queries for the data-upload pipeline.
//!
//! Powers the `merchantUpload` GraphQL query and is reused internally by
//! `merchant_upload::mutations` for ownership + status-transition checks
//! before every state change.

use chrono::{DateTime, Utc};
use common::errors::AppError;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::schema::types::merchant_upload::MerchantUploadGql;

/// Full row shape, kept `pub(crate)` (not `pub`) so mutations.rs can read
/// `status`/`parsed_preview`/etc. directly before deciding whether a
/// transition is valid, without a round trip through the public GQL type.
#[derive(sqlx::FromRow, Clone)]
pub(crate) struct UploadRow {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub kind: String,
    pub status: String,
    pub mapping_overrides: Option<Value>,
    pub parsed_preview: Option<Value>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn row_to_gql(r: UploadRow) -> MerchantUploadGql {
    MerchantUploadGql {
        id: r.id,
        merchant_id: r.merchant_id,
        kind: r.kind,
        status: r.status,
        mapping_overrides: r.mapping_overrides.map(async_graphql::Json),
        parsed_preview: r.parsed_preview.map(async_graphql::Json),
        error_message: r.error_message,
        created_at: r.created_at,
        updated_at: r.updated_at,
    }
}

/// `id = $1 AND merchant_id = $2` collapses "doesn't exist" and "exists
/// but isn't yours" into the same `None`, matching get_recommendation's
/// pattern — doesn't leak whether another merchant's upload exists.
pub(crate) async fn fetch_row(
    pool: &PgPool,
    merchant_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<UploadRow>, AppError> {
    let row: Option<UploadRow> = sqlx::query_as(
        r#"SELECT id, merchant_id, kind, status, mapping_overrides, parsed_preview,
                  error_message, created_at, updated_at
           FROM merchant_uploads
           WHERE id = $1 AND merchant_id = $2"#,
    )
    .bind(upload_id)
    .bind(merchant_id)
    .fetch_optional(pool)
    .await
    .map_err(AppError::Database)?;

    Ok(row)
}

/// Public: fetch a single upload for the `merchantUpload(id)` GraphQL query.
pub async fn get_upload(
    pool: &PgPool,
    merchant_id: Uuid,
    upload_id: Uuid,
) -> Result<Option<MerchantUploadGql>, AppError> {
    Ok(fetch_row(pool, merchant_id, upload_id).await?.map(row_to_gql))
}
