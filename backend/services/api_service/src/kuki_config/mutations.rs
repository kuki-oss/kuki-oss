use common::errors::AppError;
use sqlx::PgPool;
use uuid::Uuid;

/// Upserts a merchant's Kuki config. Every parameter is optional
/// COALESCE against the existing row's value (or the column's own
/// DEFAULT on first insert) so a partial update, e.g., just
/// changing velocity_window_days, doesn't clobber other fields
/// back to their defaults
pub async fn upsert_kuki_config(
    pool: &PgPool,
    merchant_id: Uuid,
    velocity_window_days: Option<i32>,
    safety_buffer_multiplier: Option<f64>,
    merchant_buffer_days: Option<i32>,
) -> Result<(), AppError> {
    sqlx::query(
        r#"INSERT INTO merchant_kuki_config (merchant_id, velocity_window_days, safety_buffer_multiplier, merchant_buffer_days)
           VALUES ($1, COALESCE($2, 14), COALESCE($3, 1.5), COALESCE($4, 7))
           ON CONFLICT (merchant_id) DO UPDATE SET
             velocity_window_days = COALESCE($2, merchant_kuki_config.velocity_window_days),
             safety_buffer_multiplier = COALESCE($3, merchant_kuki_config.safety_buffer_multiplier),
             merchant_buffer_days = COALESCE($4, merchant_kuki_config.merchant_buffer_days),
             updated_at = now()"#,
    )
    .bind(merchant_id)
    .bind(velocity_window_days)
    .bind(safety_buffer_multiplier)
    .bind(merchant_buffer_days)
    .execute(pool)
    .await
    .map_err(AppError::Database)?;

    Ok(())
}

/// This is Fire-and-Forget: fires the same channel/payload shape
/// notify_kuki_engine() already emits for the organic CDC path,
/// so kuki-engine's listener doesn't need to distinguish "real
/// event" from "manual re-run button", they're identical from
/// its perspective
pub async fn trigger_kuki_evaluation(
    pool: &PgPool,
    merchant_id: Uuid,
    engine: &str,
) -> Result<(), AppError> {
    let payload = serde_json::json!({ "merchant_id": merchant_id, "engine": engine });

    sqlx::query("SELECT pg_notify('kuki_engine_events', $1)")
        .bind(payload.to_string())
        .execute(pool)
        .await
        .map_err(AppError::Database)?;

    Ok(())
}
