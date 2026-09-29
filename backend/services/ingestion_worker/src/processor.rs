use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use chrono::Utc;
use sqlx::PgPool;
use std::collections::HashMap;
use uuid::Uuid;

use common::data_upload::{parse_csv_rows, resolve_mapping, UploadKind};

use crate::types::{HeaderPreview, MerchantUploadRow, ParsedPreview};

pub async fn process_upload(
    upload_id: Uuid,
    pool: &PgPool,
    s3_client: &S3Client,
    bucket: &str,
) -> Result<()> {
    let row = fetch_upload(upload_id, pool).await?;

    // Guard against races: another worker instance, or a stale/duplicate
    // notification, may have already picked this row up.
    if row.status != "pending" {
        tracing::info!(
            "Skipping upload {upload_id}: status is '{}', not 'pending'",
            row.status
        );
        return Ok(());
    }

    mark_parsing(upload_id, pool).await?;

    let kind = UploadKind::from_db_str(&row.kind)
        .with_context(|| format!("unrecognized upload kind '{}' for upload {upload_id}", row.kind))?;

    let result = run_pipeline(&row, kind, s3_client, bucket).await;

    match result {
        Ok(preview) => {
            write_preview_ready(upload_id, &preview, pool).await?;
            tracing::info!(
                "✅ Upload {upload_id}: {} clean, {} flagged, {} skipped",
                preview.clean_count,
                preview.flagged_count,
                preview.skipped_count
            );
        }
        Err(e) => {
            write_parse_failed(upload_id, &format!("{e:#}"), pool).await?;
            tracing::warn!("⚠️ Upload {upload_id} failed to parse: {e:#}");
        }
    }

    Ok(())
}

async fn run_pipeline(
    row: &MerchantUploadRow,
    kind: UploadKind,
    s3_client: &S3Client,
    bucket: &str,
) -> Result<ParsedPreview> {
    let object = s3_client
        .get_object()
        .bucket(bucket)
        .key(&row.s3_key)
        .send()
        .await
        .context("failed to fetch object from S3")?;

    let bytes = object
        .body
        .collect()
        .await
        .context("failed to read S3 object body")?
        .into_bytes();

    let overrides: HashMap<String, String> = row
        .mapping_overrides
        .as_ref()
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()
        .context("mapping_overrides was not a valid string->string map")?
        .unwrap_or_default();

    let mut reader = csv::ReaderBuilder::new()
        .flexible(true) // tolerate ragged rows; row_parser reports a
                         // per-row length mismatch rather than the csv
                         // crate hard-erroring the whole file
        .from_reader(bytes.as_ref());

    let raw_headers: Vec<String> = reader
        .headers()
        .context("failed to read CSV header row")?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let mapping = resolve_mapping(kind, &raw_headers, &overrides);

    if !mapping.is_complete() {
        let mut problems = Vec::new();
        if !mapping.missing_required.is_empty() {
            problems.push(format!(
                "missing required field(s): {}",
                mapping.missing_required.join(", ")
            ));
        }
        if !mapping.duplicate_fields.is_empty() {
            problems.push(format!(
                "duplicate mapping to field(s): {}",
                mapping.duplicate_fields.join(", ")
            ));
        }
        anyhow::bail!("column mapping incomplete: {}", problems.join("; "));
    }

    let mut raw_rows: Vec<Vec<String>> = Vec::new();
    for record in reader.records() {
        let record = record.context("failed to read a CSV data row")?;
        raw_rows.push(record.iter().map(|c| c.to_string()).collect());
    }

    let parsed_rows = parse_csv_rows(&mapping, &raw_rows);

    let clean_count = parsed_rows
        .iter()
        .filter(|r| r.status == common::data_upload::RowStatus::Clean)
        .count();
    let flagged_count = parsed_rows
        .iter()
        .filter(|r| r.status == common::data_upload::RowStatus::Flagged)
        .count();
    let skipped_count = parsed_rows
        .iter()
        .filter(|r| r.status == common::data_upload::RowStatus::Skipped)
        .count();

    let headers = mapping
        .headers
        .iter()
        .map(|h| HeaderPreview {
            raw_header: h.raw_header.clone(),
            canonical_field: h.canonical_field.map(|s| s.to_string()),
            match_kind: format!("{:?}", h.match_kind),
        })
        .collect();

    Ok(ParsedPreview {
        kind: kind.as_db_str().to_string(),
        headers,
        row_count: parsed_rows.len(),
        clean_count,
        flagged_count,
        skipped_count,
        rows: parsed_rows,
    })
}

async fn fetch_upload(upload_id: Uuid, pool: &PgPool) -> Result<MerchantUploadRow> {
    sqlx::query_as::<_, MerchantUploadRow>(
        r#"SELECT id, merchant_id, kind, s3_key, status, mapping_overrides
           FROM merchant_uploads WHERE id = $1"#,
    )
    .bind(upload_id)
    .fetch_one(pool)
    .await
    .with_context(|| format!("merchant_uploads row {upload_id} not found"))
}

async fn mark_parsing(upload_id: Uuid, pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"UPDATE merchant_uploads SET status = 'parsing', updated_at = $2 WHERE id = $1"#,
    )
    .bind(upload_id)
    .bind(Utc::now())
    .execute(pool)
    .await
    .context("failed to mark upload as parsing")?;
    Ok(())
}

async fn write_preview_ready(
    upload_id: Uuid,
    preview: &ParsedPreview,
    pool: &PgPool,
) -> Result<()> {
    let preview_json = serde_json::to_value(preview).context("failed to serialize parsed_preview")?;
    sqlx::query(
        r#"UPDATE merchant_uploads
           SET status = 'preview_ready', parsed_preview = $2, error_message = NULL, updated_at = $3
           WHERE id = $1"#,
    )
    .bind(upload_id)
    .bind(preview_json)
    .bind(Utc::now())
    .execute(pool)
    .await
    .context("failed to write parsed_preview")?;
    Ok(())
}

async fn write_parse_failed(upload_id: Uuid, error_message: &str, pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"UPDATE merchant_uploads
           SET status = 'parse_failed', error_message = $2, updated_at = $3
           WHERE id = $1"#,
    )
    .bind(upload_id)
    .bind(error_message)
    .bind(Utc::now())
    .execute(pool)
    .await
    .context("failed to write parse_failed status")?;
    Ok(())
}
