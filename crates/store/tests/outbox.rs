use kuki_store::outbox::{self, TOPIC_RECOMMENDATIONS};
use serde_json::json;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

async fn put(c: &mut PgConnection, key: &str, n: i64) -> i64 {
    outbox::enqueue(
        c,
        Uuid::new_v4(),
        TOPIC_RECOMMENDATIONS,
        key,
        &json!({ "n": n }),
    )
    .await
    .unwrap()
}

fn ids(rows: &[outbox::OutboxRow]) -> Vec<i64> {
    rows.iter().map(|r| r.id).collect()
}

#[sqlx::test(migrations = "../../migrations")]
async fn claims_come_oldest_first_and_are_leased(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k1", 1).await;
    let b = put(&mut c, "k2", 2).await;
    let d = put(&mut c, "k1", 3).await;

    let rows = outbox::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!(ids(&rows), [a, b, d]);
    assert!(rows.iter().all(|r| r.attempts == 1));
    assert_eq!(rows[0].topic, TOPIC_RECOMMENDATIONS);
    assert_eq!(rows[0].key, "k1");
    assert_eq!(rows[0].payload["n"], 1);

    assert!(
        outbox::claim(&mut c, 10, 60.0).await.unwrap().is_empty(),
        "leased rows are not handed out twice"
    );

    // An expired lease (a crashed relay) makes them claimable again, and the attempt is counted.
    let again = outbox::claim(&mut c, 10, 0.0).await.unwrap();
    assert_eq!(ids(&again), [a, b, d]);
    assert!(again.iter().all(|r| r.attempts == 2));
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_batch_limit_is_respected(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k", 1).await;
    let b = put(&mut c, "k", 2).await;
    put(&mut c, "k", 3).await;
    assert_eq!(ids(&outbox::claim(&mut c, 2, 60.0).await.unwrap()), [a, b]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn released_rows_are_retried_before_newer_ones(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k", 1).await;
    let b = put(&mut c, "k", 2).await;
    let d = put(&mut c, "k", 3).await;
    outbox::claim(&mut c, 10, 60.0).await.unwrap();

    outbox::release(&mut c, &[b, d]).await.unwrap();
    let rows = outbox::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!(
        ids(&rows),
        [b, d],
        "a is still leased, b and d come back in order"
    );
    assert!(rows.iter().all(|r| r.attempts == 2));
    let _ = a;
}

#[sqlx::test(migrations = "../../migrations")]
async fn published_rows_stay_published(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k", 1).await;
    let b = put(&mut c, "k", 2).await;
    outbox::claim(&mut c, 10, 60.0).await.unwrap();

    assert_eq!(outbox::mark_published(&mut c, &[a]).await.unwrap(), 1);
    assert_eq!(
        outbox::mark_published(&mut c, &[a]).await.unwrap(),
        0,
        "already delivered"
    );
    outbox::release(&mut c, &[a]).await.unwrap();

    let rows = outbox::claim(&mut c, 10, 0.0).await.unwrap();
    assert_eq!(ids(&rows), [b], "a delivered row is never claimed again");
}

#[sqlx::test(migrations = "../../migrations")]
async fn dead_rows_are_skipped_and_explained(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k", 1).await;
    let b = put(&mut c, "k", 2).await;
    outbox::claim(&mut c, 10, 60.0).await.unwrap();

    outbox::mark_dead(&mut c, a, "message too large")
        .await
        .unwrap();
    let rows = outbox::claim(&mut c, 10, 0.0).await.unwrap();
    assert_eq!(ids(&rows), [b]);

    let (dead, err): (bool, Option<String>) =
        sqlx::query_as("SELECT dead_at IS NOT NULL, last_error FROM outbox WHERE id = $1")
            .bind(a)
            .fetch_one(&mut *c)
            .await
            .unwrap();
    assert!(dead);
    assert_eq!(err.as_deref(), Some("message too large"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_app_role_reaches_the_outbox_only_through_the_functions(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let a = put(&mut c, "k", 1).await;
    let b = put(&mut c, "k", 2).await;

    sqlx::query("SET ROLE kuki_app")
        .execute(&mut *c)
        .await
        .unwrap();

    // Row-level security hides every row from a connection with no tenant context.
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(visible, 0);
    let touched = sqlx::query("UPDATE outbox SET delivered_at = now()")
        .execute(&mut *c)
        .await
        .unwrap();
    assert_eq!(touched.rows_affected(), 0);

    // The definer functions work across organisations.
    let rows = outbox::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!(ids(&rows), [a, b]);
    assert_eq!(outbox::mark_published(&mut c, &[a, b]).await.unwrap(), 2);

    sqlx::query("RESET ROLE").execute(&mut *c).await.unwrap();
    let delivered: i64 =
        sqlx::query_scalar("SELECT count(*) FROM outbox WHERE delivered_at IS NOT NULL")
            .fetch_one(&mut *c)
            .await
            .unwrap();
    assert_eq!(delivered, 2);
}
