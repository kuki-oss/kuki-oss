//! The decision-type registry: the default action a pack declares round-trips, is replaced
//! on re-registration, and the database refuses a malformed one.

use kuki_store::engine_reads::{decision_type_info, register_decision_type};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

async fn seed_org(admin: &PgPool) -> Uuid {
    let id = Uuid::from_u128(1);
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, 'org1', 'org1')")
        .bind(id)
        .execute(admin)
        .await
        .unwrap();
    id
}

async fn register(
    admin: &PgPool,
    org: Uuid,
    name: &str,
    action: Option<&str>,
) -> Result<(), sqlx::Error> {
    let mut c = admin.acquire().await.unwrap();
    register_decision_type(
        &mut c,
        org,
        name,
        "kuki.base",
        "kuki.base/test.json",
        &json!({ "type": "object" }),
        action,
    )
    .await
}

async fn action(admin: &PgPool, org: Uuid, name: &str) -> Option<String> {
    let mut c = admin.acquire().await.unwrap();
    decision_type_info(&mut c, org, name)
        .await
        .unwrap()
        .expect("registered")
        .default_action_type
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_types_default_action_round_trips(admin: PgPool) {
    let org = seed_org(&admin).await;
    register(&admin, org, "work.escalate", Some("builtin:flag_review"))
        .await
        .unwrap();
    register(&admin, org, "work.other", None).await.unwrap();

    assert_eq!(
        action(&admin, org, "work.escalate").await.as_deref(),
        Some("builtin:flag_review")
    );
    assert_eq!(action(&admin, org, "work.other").await, None);
}

#[sqlx::test(migrations = "../../migrations")]
async fn re_registering_replaces_the_action_because_the_pack_is_the_source(admin: PgPool) {
    let org = seed_org(&admin).await;
    register(&admin, org, "work.escalate", Some("builtin:flag_review"))
        .await
        .unwrap();
    register(&admin, org, "work.escalate", Some("work.escalate.notify"))
        .await
        .unwrap();
    assert_eq!(
        action(&admin, org, "work.escalate").await.as_deref(),
        Some("work.escalate.notify")
    );

    register(&admin, org, "work.escalate", None).await.unwrap();
    assert_eq!(action(&admin, org, "work.escalate").await, None);
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_database_refuses_a_malformed_action_name(admin: PgPool) {
    let org = seed_org(&admin).await;
    for (i, bad) in ["", "Flag Review", "builtin:flag review", "9lives", "-x"]
        .into_iter()
        .enumerate()
    {
        let e = register(&admin, org, &format!("t{i}"), Some(bad)).await;
        assert!(e.is_err(), "{bad:?} should be refused");
    }
}
