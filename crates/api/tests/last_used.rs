mod common;

use std::time::Duration;

use axum::http::StatusCode;
use common::*;
use sqlx::PgPool;

#[sqlx::test(migrations = "../../migrations")]
async fn authenticating_records_the_keys_last_use(admin: PgPool) {
    let org = seed_org(&admin, "last-used").await;
    let token = seed_key(&admin, org, &["ingest:read"]).await;
    let app = app(app_pool(&admin).await);

    let used = || async {
        sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
            "SELECT last_used_at FROM api_keys WHERE organisation_id = $1",
        )
        .bind(org)
        .fetch_one(&admin)
        .await
        .unwrap()
    };
    assert!(used().await.is_none());

    let (s, _) = common::viewer(&app, &token).await;
    assert_eq!(s, StatusCode::OK);

    // The write happens in a background task, so poll briefly.
    let mut seen = None;
    for _ in 0..60 {
        seen = used().await;
        if seen.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(seen.is_some(), "last_used_at was never set");
}
