use chrono::{Duration, Utc};
use common::errors::AppError;
use sqlx::PgPool;

const SANDBOX_TTL_HOURS: i64 = 48;

/// Deletes all sandbox merchants (and their dependent rows) created more
/// than SANDBOX_TTL_HOURS ago, matching the sandbox_session_id cookie's
/// own 48h Max-Age. Runs in dependency order (children first, merchants
/// last) since most merchant_id FKs are NOT ON DELETE CASCADE -- this
/// exact order was manually verified in step 1's work against a real
/// test sandbox merchant with zero FK violations.
///
/// merchant_uploads is deliberately absent from this list: it uses
/// ON DELETE CASCADE on merchant_id, so upload rows (and, transitively,
/// anything referencing them) disappear for free once the merchants row
/// is deleted below.
pub async fn delete_expired_sandbox_merchants(pool: &PgPool) -> Result<DeleteResult, AppError> {
    let mut tx = pool.begin().await.map_err(AppError::Database)?;

    const TABLES_IN_ORDER: &[&str] = &[
        "order_items",
        "purchase_order_lines",
        "product_images",
        "customer_segment_members",
        "enquiry_items",
        "kuki_recommendations",
        "stock_movements",
        "orders",
        "purchase_orders",
        "products",
        "suppliers",
        "customers",
        "customer_segments",
        "enquiries",
        "discounts",
        "categories",
        "service_categories",
        "services",
        "installations",
        "fulfillment_slots",
        "marketing_campaigns",
        "marketing_costs",
        "payments",
        "store_settings",
        "merchant_kuki_config",
    ];

    for table in TABLES_IN_ORDER {
        let query = format!(
            r#"DELETE FROM {table}
               WHERE merchant_id IN (
                   SELECT id FROM merchants
                   WHERE kind = 'sandbox' AND created_at < $1
               )"#
        );
        sqlx::query(&query)
            .bind(Utc::now() - Duration::hours(SANDBOX_TTL_HOURS))
            .execute(&mut *tx)
            .await
            .map_err(AppError::Database)?;
    }

    let deleted_merchant_ids: Vec<uuid::Uuid> = sqlx::query_scalar(
        r#"DELETE FROM merchants
           WHERE kind = 'sandbox' AND created_at < $1
           RETURNING id"#,
    )
    .bind(Utc::now() - Duration::hours(SANDBOX_TTL_HOURS))
    .fetch_all(&mut *tx)
    .await
    .map_err(AppError::Database)?;

    tx.commit().await.map_err(AppError::Database)?;

    tracing::info!(
        deleted_count = deleted_merchant_ids.len(),
        "sandbox_cleanup: deleted expired sandbox merchants"
    );

    Ok(DeleteResult {
        deleted_merchant_ids,
    })
}

pub struct DeleteResult {
    pub deleted_merchant_ids: Vec<uuid::Uuid>,
}

// ── Stale merchant_uploads sweep (unchanged from previous message) ────

/// Sweeps merchant_uploads rows stuck in awaiting_upload or parsing past
/// a reasonable TTL -- a visitor who requested a presigned URL and never
/// uploaded, or a worker that crashed mid-parse.
pub async fn sweep_stale_uploads(
    pool: &PgPool,
    stale_after_hours: i64,
) -> Result<SweepResult, AppError> {
    let cutoff = Utc::now() - Duration::hours(stale_after_hours);

    let abandoned: Vec<uuid::Uuid> = sqlx::query_scalar(
        r#"UPDATE merchant_uploads
           SET status = 'parse_failed',
               error_message = 'Upload was never completed (no file received within the expected window)',
               updated_at = now()
           WHERE status = 'awaiting_upload' AND created_at < $1
           RETURNING id"#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)?;

    let orphaned: Vec<uuid::Uuid> = sqlx::query_scalar(
        r#"UPDATE merchant_uploads
           SET status = 'pending', updated_at = now()
           WHERE status = 'parsing' AND updated_at < $1
           RETURNING id"#,
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)?;

    tracing::info!(
        abandoned_count = abandoned.len(),
        orphaned_count = orphaned.len(),
        "sandbox_cleanup: swept stale merchant_uploads rows"
    );

    Ok(SweepResult {
        abandoned_upload_ids: abandoned,
        requeued_upload_ids: orphaned,
    })
}

pub struct SweepResult {
    pub abandoned_upload_ids: Vec<uuid::Uuid>,
    pub requeued_upload_ids: Vec<uuid::Uuid>,
}
