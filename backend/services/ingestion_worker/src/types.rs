use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Mirrors the merchant_uploads row shape needed by the worker. Only the
/// columns the worker actually reads/writes are represented here.
#[derive(Debug, sqlx::FromRow)]
pub struct MerchantUploadRow {
    pub id: Uuid,
    pub merchant_id: Uuid,
    pub kind: String,
    pub s3_key: String,
    pub status: String,
    pub mapping_overrides: Option<serde_json::Value>,
}

/// The full parsed_preview jsonb shape written back on success. Kept
/// separate from common::data_upload::ParsedRow so this worker controls
/// its own on-the-wire preview shape independent of internal parser
/// types changing later.
#[derive(Debug, Serialize, Deserialize)]
pub struct ParsedPreview {
    pub kind: String,
    pub headers: Vec<HeaderPreview>,
    pub rows: Vec<common::data_upload::ParsedRow>,
    pub row_count: usize,
    pub clean_count: usize,
    pub flagged_count: usize,
    pub skipped_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeaderPreview {
    pub raw_header: String,
    pub canonical_field: Option<String>,
    pub match_kind: String,
}
