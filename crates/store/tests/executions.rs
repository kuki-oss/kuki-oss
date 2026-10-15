use kuki_store::executions::{self, Final};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// The unprivileged role, so row-level security and grants are really in force.
async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(c).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

/// An accepted recommendation with a queued execution.
async fn seed(admin: &PgPool, org: u128, rec: u128, exec: u128) {
    sqlx::query(
        "INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2) ON CONFLICT DO NOTHING",
    )
    .bind(u(org))
    .bind(format!("org{org}"))
    .execute(admin)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO recommendations
           (id, organisation_id, decision_type, dedup_key, status, urgency, confidence,
            rationale_summary, body, provenance)
         VALUES ($1, $2, 'work.escalate', $3, 'accepted', 0.5, 0.9, 'x', '{}', '{}')",
    )
    .bind(u(rec))
    .bind(u(org))
    .bind(format!("k{rec}"))
    .execute(admin)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO action_executions
           (id, organisation_id, recommendation_id, action_type, input, idempotency_key,
            status, adapter, created_by)
         VALUES ($1, $2, $3, 'work.escalate.notify', '{}', $4, 'queued', 'unrouted', 'user:amina')",
    )
    .bind(u(exec))
    .bind(u(org))
    .bind(u(rec))
    .bind(format!("k{exec}"))
    .execute(admin)
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn claims_are_limited_leased_and_cross_organisation(admin: PgPool) {
    seed(&admin, 1, 10, 100).await;
    seed(&admin, 2, 20, 200).await;
    let app = app_pool(&admin).await;
    let mut c = app.acquire().await.unwrap();

    assert_eq!(
        executions::claim(&mut c, 1, 60.0).await.unwrap().len(),
        1,
        "the limit is respected"
    );
    let rest = executions::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].attempts, 1);
    assert!(
        executions::claim(&mut c, 10, 60.0)
            .await
            .unwrap()
            .is_empty(),
        "both rows are under a live lease"
    );

    // A worker that died: its lease runs out and the rows are taken over, in both organisations.
    sqlx::query("UPDATE action_executions SET claimed_at = now() - interval '1 hour'")
        .execute(&admin)
        .await
        .unwrap();
    let again = executions::claim(&mut c, 10, 60.0).await.unwrap();
    assert!(again.iter().all(|r| r.attempts == 2));
    let mut orgs: Vec<Uuid> = again.iter().map(|r| r.organisation_id).collect();
    orgs.sort();
    assert_eq!(orgs, [u(1), u(2)]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn finish_and_retry_only_act_on_a_running_row_of_the_named_organisation(admin: PgPool) {
    seed(&admin, 1, 10, 100).await;
    let app = app_pool(&admin).await;
    let mut c = app.acquire().await.unwrap();
    let done = json!({ "error": "x" });

    // Queued is not running.
    assert!(
        !executions::finish(&mut c, u(1), u(100), Final::Succeeded, "a", &done)
            .await
            .unwrap()
    );

    executions::claim(&mut c, 10, 60.0).await.unwrap();
    assert!(
        !executions::finish(&mut c, u(2), u(100), Final::Succeeded, "a", &done)
            .await
            .unwrap(),
        "wrong organisation"
    );
    assert!(
        !executions::retry(&mut c, u(2), u(100), "a", "e", 0.0)
            .await
            .unwrap(),
        "wrong organisation"
    );

    // A long delay puts it back in the queue, out of reach.
    assert!(
        executions::retry(&mut c, u(1), u(100), "adapter:one", "503", 3600.0)
            .await
            .unwrap()
    );
    assert!(
        executions::claim(&mut c, 10, 60.0)
            .await
            .unwrap()
            .is_empty()
    );
    let (status, adapter, error): (String, String, Option<String>) =
        sqlx::query_as("SELECT status, adapter, last_error FROM action_executions")
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), adapter.as_str(), error.as_deref()),
        ("queued", "adapter:one", Some("503"))
    );

    sqlx::query("UPDATE action_executions SET not_before = now()")
        .execute(&admin)
        .await
        .unwrap();
    let claimed = executions::claim(&mut c, 10, 60.0).await.unwrap();
    assert_eq!((claimed.len(), claimed[0].attempts), (1, 2));

    assert!(
        executions::finish(&mut c, u(1), u(100), Final::Failed, "adapter:one", &done)
            .await
            .unwrap()
    );
    assert!(
        !executions::finish(&mut c, u(1), u(100), Final::Failed, "adapter:one", &done)
            .await
            .unwrap(),
        "already final"
    );
    assert!(
        !executions::retry(&mut c, u(1), u(100), "a", "e", 0.0)
            .await
            .unwrap(),
        "already final"
    );

    let (status, result, finished): (String, Option<Value>, bool) =
        sqlx::query_as("SELECT status, result, finished_at IS NOT NULL FROM action_executions")
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(
        (status.as_str(), result, finished),
        ("failed", Some(done), true)
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_app_role_cannot_modify_executions_directly(admin: PgPool) {
    seed(&admin, 1, 10, 100).await;
    let app = app_pool(&admin).await;

    assert!(
        sqlx::query("UPDATE action_executions SET status = 'succeeded'")
            .execute(&app)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM action_executions")
            .execute(&app)
            .await
            .is_err()
    );
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM action_executions")
        .fetch_one(&app)
        .await
        .unwrap();
    assert_eq!(visible, 0, "no tenant context, no rows");
}
