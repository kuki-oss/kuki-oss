//! Database mutations for the merchant data-upload pipeline.
//!
//! Powers initiateMerchantUpload, markMerchantUploadUploaded,
//! remapMerchantUpload, and confirmMerchantUpload. Every mutation starts
//! by fetching the row scoped to (id, merchant_id) — same ownership
//! pattern as kuki::mutations — then checks the current status is valid
//! for the requested transition before writing.

use aws_sdk_s3::presigning::PresigningConfig;
use chrono::Utc;
use common::{
    data_upload::{
        slugify,
        CanonicalValue,
        ParsedRow,
        RowStatus,
        UploadKind,
    },
    errors::AppError,
    surreal::SurrealClient,
};
use serde_json::Value;
use sqlx::{
    PgPool,
    types::BigDecimal,
};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::merchant_upload::queries::{fetch_row, get_upload};
use crate::schema::types::merchant_upload::{
    ConfirmMerchantUploadResultGql, InitiateMerchantUploadResultGql, MerchantUploadGql,
};

const PRESIGN_EXPIRY_SECS: u64 = 900; // 15 minutes

/// Create a new merchant_uploads row (status = awaiting_upload) and return
/// a presigned S3 PUT URL for the browser to upload the raw file to
/// directly.
///
/// s3_key format is `{merchant_id}/{upload_id}.csv` — hardcoded `.csv`
/// extension is deliberate for now, matching the agreed CSV-first build
/// order (xlsx/PDF are sub-step 8, not yet built). This will need
/// revisiting once xlsx/PDF uploads exist and the extension needs to vary.
pub async fn initiate_upload(
    pool: &PgPool,
    s3_client: &aws_sdk_s3::Client,
    bucket: &str,
    merchant_id: Uuid,
    kind_str: &str,
) -> Result<InitiateMerchantUploadResultGql, AppError> {
    let kind = UploadKind::from_db_str(kind_str)
        .ok_or_else(|| AppError::BadRequest(format!("invalid upload kind '{kind_str}'")))?;

    let upload_id = Uuid::new_v4();
    let s3_key = format!("{merchant_id}/{upload_id}.csv");

    sqlx::query(
        r#"INSERT INTO merchant_uploads (id, merchant_id, kind, s3_key, status)
           VALUES ($1, $2, $3, $4, 'awaiting_upload')"#,
    )
    .bind(upload_id)
    .bind(merchant_id)
    .bind(kind.as_db_str())
    .bind(&s3_key)
    .execute(pool)
    .await
    .map_err(AppError::Database)?;

    let presigning_config = PresigningConfig::expires_in(Duration::from_secs(PRESIGN_EXPIRY_SECS))
        .map_err(|e| {
            tracing::error!(error = ?e, "failed to build S3 pre-signing config");
            AppError::InternalServerError
        })?;

    let presigned = s3_client
        .put_object()
        .bucket(bucket)
        .key(&s3_key)
        .presigned(presigning_config)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, "failed to presign S3 PUT");
            AppError::InternalServerError
        })?;

    let upload = get_upload(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)?;

    Ok(InitiateMerchantUploadResultGql {
        upload,
        upload_url: presigned.uri().to_string(),
    })
}

/// Flip status: awaiting_upload -> pending. Fires the merchant_uploads
/// trigger, which notifies the ETL worker.
pub async fn mark_uploaded(
    pool: &PgPool,
    merchant_id: Uuid,
    upload_id: Uuid,
) -> Result<MerchantUploadGql, AppError> {
    let existing = fetch_row(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)?;

    if existing.status != "awaiting_upload" {
        return Err(AppError::BadRequest(format!(
            "upload is '{}', expected 'awaiting_upload'",
            existing.status
        )));
    }

    sqlx::query(
        r#"UPDATE merchant_uploads SET status = 'pending', updated_at = $1
           WHERE id = $2 AND merchant_id = $3"#,
    )
    .bind(Utc::now())
    .bind(upload_id)
    .bind(merchant_id)
    .execute(pool)
    .await
    .map_err(AppError::Database)?;

    get_upload(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)
}

/// Replace mapping_overrides wholesale and reset status to `pending`, so
/// the worker re-parses the *same* S3 object with the new mapping. Valid
/// from preview_ready or parse_failed — a visitor fixing either a bad
/// mapping or a previously-failed parse takes the same path.
pub async fn remap_upload(
    pool: &PgPool,
    merchant_id: Uuid,
    upload_id: Uuid,
    mapping_overrides: HashMap<String, String>,
) -> Result<MerchantUploadGql, AppError> {
    let existing = fetch_row(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)?;

    if !matches!(existing.status.as_str(), "preview_ready" | "parse_failed") {
        return Err(AppError::BadRequest(format!(
            "upload is '{}', expected 'preview_ready' or 'parse_failed'",
            existing.status
        )));
    }

    let overrides_json: Value = serde_json::to_value(&mapping_overrides).map_err(|e| {
        tracing::error!(error = ?e, "failed to serialize mapping_overrides");
        AppError::InternalServerError
    })?;

    sqlx::query(
        r#"UPDATE merchant_uploads
           SET mapping_overrides = $1, status = 'pending', parsed_preview = NULL,
               error_message = NULL, updated_at = $2
           WHERE id = $3 AND merchant_id = $4"#,
    )
    .bind(overrides_json)
    .bind(Utc::now())
    .bind(upload_id)
    .bind(merchant_id)
    .execute(pool)
    .await
    .map_err(AppError::Database)?;

    get_upload(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)
}

/// Mirrors just the fields confirm actually needs from the ETL worker's
/// on-disk parsed_preview shape. A local, minimal type rather than
/// depending on etl-ingestion-worker as a library, serde ignores the
/// extra fields (headers, counts) the worker also writes.
#[derive(serde::Deserialize)]
struct StoredPreview {
    kind: String,
    rows: Vec<ParsedRow>,
}

/// Commits a confirmed upload's parsed rows into the real
/// categories/products/suppliers/stock_movements tables (branching by
/// kind), plus the SurrealDB supplier->product edge for supplier-kind
/// uploads. Runs inside a single Postgres transaction so a partial
/// failure never leaves half-commited rows.
pub async fn confirm_upload(
    pool: &PgPool,
    surreal: &SurrealClient,
    merchant_id: Uuid,
    upload_id: Uuid,
) -> Result<ConfirmMerchantUploadResultGql, AppError> {
    let existing = fetch_row(pool, merchant_id, upload_id)
        .await?
        .ok_or(AppError::NotFound)?;

    if existing.status != "preview_ready" {
        return Err(AppError::BadRequest(format!(
            "upload is '{}', expected 'preview_ready'",
            existing.status
        )));
    }

    let preview_json = existing.parsed_preview.clone().ok_or_else(|| {
        tracing::error!(upload_id = %upload_id, "upload is preview_ready but parsed_preview is empty");
        AppError::InternalServerError
    })?;

    let preview: StoredPreview = serde_json::from_value(preview_json).map_err(|e| {
        tracing::error!(upload_id = %upload_id, error = ?e, "parsed_preview did not match expected shape");
        AppError::InternalServerError
    })?;

    let kind = UploadKind::from_db_str(&preview.kind).ok_or_else(|| {
        tracing::error!(upload_id = %upload_id, kind = %preview.kind, "unrecognized kind in parsed_preview");
        AppError::InternalServerError
    })?;

    // Only clean/flagged rows carry committable values, skipped rows
    // have no values and must not be written
    let committable: Vec<&ParsedRow> = preview
        .rows
        .iter()
        .filter(|r| !matches!(r.status, RowStatus::Skipped))
        .collect();

    let mut tx = pool.begin().await.map_err(AppError::Database)?;

    let commit_result = match kind {
        UploadKind::Supplier => commit_supplier_rows(&mut tx, surreal, merchant_id, &committable).await,
        UploadKind::Demand => commit_demand_rows(&mut tx, merchant_id, &committable).await,
    };

    match commit_result {
        Ok(()) => {
            sqlx::query(
                r#"UPDATE merchant_uploads SET status = 'confirmed', updated_at = $1
                   WHERE id = $2 AND merchant_id = $3"#,
            )
            .bind(Utc::now())
            .bind(upload_id)
            .bind(merchant_id)
            .execute(&mut *tx)
            .await
            .map_err(AppError::Database)?;

            tx.commit().await.map_err(AppError::Database)?;

            let upload = get_upload(pool, merchant_id, upload_id).await?.ok_or(AppError::NotFound)?;
            Ok(ConfirmMerchantUploadResultGql {
                upload,
                message: "Upload confirmed and commited.".to_string(),
            })
        }
        Err(e) => {
            // tx dropped without commit() here, rolls back automatically
            tracing::error!(upload_id = %upload_id, error = ?e, "confirm commit failed");
            let _ = sqlx::query(
                r#"UPDATE merchant_uploads SET status = 'confirm_failed', error_message = $1, updated_at = $2
                   WHERE id = $3 AND merchant_id = $4"#,
            )
            .bind(format!("{e}"))
            .bind(Utc::now())
            .bind(upload_id)
            .bind(merchant_id)
            .execute(pool)
            .await;

            Err(e)
        }
    }
}

async fn get_or_create_category(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    merchant_id: Uuid,
    cache: &mut HashMap<String, Uuid>,
    name: &str,
) -> Result<Uuid, AppError> {
    let slug = slugify(name);
    if let Some(id) = cache.get(&slug) {
        return Ok(*id);
    }

    let id: Uuid = sqlx::query_scalar(
        r#"INSERT INTO categories (merchant_id, name, slug)
           VALUES ($1, $2, $3)
           ON CONFLICT (merchant_id, slug) DO UPDATE SET name = EXCLUDED.name
           RETURNING id"#,
    )
    .bind(merchant_id)
    .bind(name)
    .bind(&slug)
    .fetch_one(&mut **tx)
    .await
    .map_err(AppError::Database)?;

    cache.insert(slug, id);
    Ok(id)
}

fn get_text(row: &ParsedRow, field: &str) -> Option<String> {
    match row.values.get(field) {
        Some(CanonicalValue::Text(s)) => Some(s.clone()),
        _ => None,
    }
}

fn get_number(row: &ParsedRow, field: &str) -> Option<f64> {
    match row.values.get(field) {
        Some(CanonicalValue::Number(n)) => Some(*n),
        _ => None,
    }
}

fn get_date(row: &ParsedRow, field: &str) -> Option<String> {
    match row.values.get(field) {
        Some(CanonicalValue::Date(d)) => Some(d.clone()),
        _ => None,
    }
}

/// KNOWN OPEN ISSUE: uploaded products are inserted with sku = '',
/// relying on the trg_products_default_sku trigger to assign a real
/// value before the (merchant_id, sku) unique constraint is checked.
/// Whether that trigger produces a DIFFERENT sku per row (not just once
/// per statement) has not been confirmed against its actual source --
/// if it doesn't, every row after the first will silently no-op via
/// ON CONFLICT DO NOTHING and RETURNING id will return no rows,
/// panicking fetch_one. Verify before relying on this in production.
async fn commit_supplier_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    surreal: &SurrealClient,
    merchant_id: Uuid,
    rows: &[&ParsedRow],
) -> Result<(), AppError> {
    let mut category_cache: HashMap<String, Uuid> = HashMap::new();
    let mut supplier_cache: HashMap<String, Uuid> = HashMap::new();

    for row in rows {
        let supplier_name = get_text(row, "supplier_name")
            .ok_or_else(|| AppError::BadRequest("row missing required supplier_name".to_string()))?;
        let lead_time_days = get_number(row, "lead_time_days")
            .ok_or_else(|| AppError::BadRequest("row missing required lead_time_days".to_string()))?;
        let product_name = get_text(row, "product_name")
            .ok_or_else(|| AppError::BadRequest("row missing required product_name".to_string()))?;
        let current_stock = get_number(row, "current_stock")
            .ok_or_else(|| AppError::BadRequest("row missing required current_stock".to_string()))?;

        let moq = get_number(row, "moq");
        let reliability = get_number(row, "reliability");
        let reorder_point = get_number(row, "reorder_point");
        let category_name = get_text(row, "product_category");

        let supplier_id = if let Some(id) = supplier_cache.get(&supplier_name) {
            *id
        } else {
            let id: Uuid = sqlx::query_scalar(
                r#"INSERT INTO suppliers (merchant_id, name, default_lead_time_days, moq)
                   VALUES ($1, $2, $3, $4)
                   RETURNING id"#,
            )
            .bind(merchant_id)
            .bind(&supplier_name)
            .bind(BigDecimal::try_from(lead_time_days).unwrap_or_default())
            .bind(moq.and_then(|m| BigDecimal::try_from(m).ok()))
            .fetch_one(&mut **tx)
            .await
            .map_err(AppError::Database)?;
            supplier_cache.insert(supplier_name.clone(), id);
            id
        };

        let category_id = match &category_name {
            Some(name) => Some(get_or_create_category(tx, merchant_id, &mut category_cache, name).await?),
            None => None,
        };

        let slug = slugify(&product_name);
        let category_path: Vec<String> = category_name.clone().into_iter().collect();

        let product_id: Uuid = sqlx::query_scalar(
            r#"INSERT INTO products (
                   merchant_id, name, slug, sku, category_id, category_path,
                   current_stock, reorder_point, preferred_supplier_id, upload_kind
               )
               VALUES ($1, $2, $3, '', $4, $5, $6, $7, $8, 'supplier')
               ON CONFLICT (merchant_id, sku) DO NOTHING
               RETURNING id"#,
        )
        .bind(merchant_id)
        .bind(&product_name)
        .bind(&slug)
        .bind(category_id)
        .bind(&category_path)
        .bind(BigDecimal::try_from(current_stock).unwrap_or_default())
        .bind(reorder_point.and_then(|r| BigDecimal::try_from(r).ok()))
        .bind(supplier_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(AppError::Database)?;

        surreal
            .upsert_supplier_node(supplier_id, &supplier_name, merchant_id)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to upsert supplier node");
                AppError::InternalServerError
            })?;

        surreal
            .upsert_product_node(product_id, &slug, "", &category_path, merchant_id)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to upsert product node");
                AppError::InternalServerError
            })?;

        // lead_time_samples = 0: self-reported, not empirically observed.
        // variance uses the same 2.0-day global default
        // RestockRuleEngine's category_fallback path uses. reliability
        // defaults to a neutral 0.5 if not supplied.
        surreal
            .upsert_supply_edge(supplier_id, &slug, lead_time_days, 2.0, reliability.unwrap_or(0.5), 0, moq)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to upsert supply edge");
                AppError::InternalServerError
            })?;
    }

    Ok(())
}

/// NOTE: current_stock is left at the schema default (0) for
/// demand-only uploads -- they never state an actual on-hand quantity,
/// only historical deltas. days_of_stock_remaining will be wrong for
/// these products until a real starting stock is supplied some other way.
async fn commit_demand_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    merchant_id: Uuid,
    rows: &[&ParsedRow],
) -> Result<(), AppError> {
    let mut category_cache: HashMap<String, Uuid> = HashMap::new();
    let mut product_cache: HashMap<String, Uuid> = HashMap::new();

    for row in rows {
        let product_name = get_text(row, "product_name")
            .ok_or_else(|| AppError::BadRequest("row missing required product_name".to_string()))?;
        let event_date = get_date(row, "event_date")
            .ok_or_else(|| AppError::BadRequest("row missing required event_date".to_string()))?;
        let quantity_delta = get_number(row, "quantity_delta")
            .ok_or_else(|| AppError::BadRequest("row missing required quantity_delta".to_string()))?;

        let category_name = get_text(row, "product_category");
        let reason_raw = get_text(row, "reason");

        let category_id = match &category_name {
            Some(name) => Some(get_or_create_category(tx, merchant_id, &mut category_cache, name).await?),
            None => None,
        };

        let slug = slugify(&product_name);

        let product_id: Uuid = if let Some(id) = product_cache.get(&slug) {
            *id
        } else {
            let id: Uuid = sqlx::query_scalar(
                r#"INSERT INTO products (merchant_id, name, slug, sku, category_id, category_path, upload_kind)
                   VALUES ($1, $2, $3, '', $4, $5, 'demand')
                   ON CONFLICT (merchant_id, sku) DO UPDATE SET name = EXCLUDED.name
                   RETURNING id"#,
            )
            .bind(merchant_id)
            .bind(&product_name)
            .bind(&slug)
            .bind(category_id)
            .bind(category_name.clone().into_iter().collect::<Vec<String>>())
            .fetch_one(&mut **tx)
            .await
            .map_err(AppError::Database)?;
            product_cache.insert(slug.clone(), id);
            id
        };

        const VALID_REASONS: [&str; 5] = ["sale", "restock", "adjustment", "return", "write_off"];
        let reason = reason_raw
            .as_deref()
            .map(|r| r.to_lowercase())
            .filter(|r| VALID_REASONS.contains(&r.as_str()))
            .unwrap_or_else(|| {
                if quantity_delta < 0.0 {
                    "sale".to_string()
                } else if quantity_delta > 0.0 {
                    "restock".to_string()
                } else {
                    "adjustment".to_string()
                }
            });

        let occurred_at = chrono::NaiveDate::parse_from_str(&event_date, "%Y-%m-%d")
            .map_err(|e| {
                tracing::error!(error = ?e, event_date = %event_date, "stored event_date was not valid ISO date");
                AppError::InternalServerError
            })?
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();

        sqlx::query(
            r#"INSERT INTO stock_movements (merchant_id, product_id, delta, reason, occurred_at)
               VALUES ($1, $2, $3, $4, $5)"#,
        )
        .bind(merchant_id)
        .bind(product_id)
        .bind(BigDecimal::try_from(quantity_delta).unwrap_or_default())
        .bind(&reason)
        .bind(occurred_at)
        .execute(&mut **tx)
        .await
        .map_err(AppError::Database)?;
    }

    Ok(())
}
