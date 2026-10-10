mod common;

use axum::http::StatusCode;
use common::{gql, scoped_key, unit};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

fn paths(b: &Value) -> Vec<&str> {
    b["data"]["orgUnits"]["edges"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["node"]["path"].as_str().unwrap())
        .collect()
}

async fn seeded(admin: &PgPool) -> (axum::Router, Uuid) {
    let app = common::app(common::app_pool(admin).await);
    let org = common::seed_org(admin, "acme").await;
    for p in [
        "hq",
        "hq.east",
        "hq.east.nairobi",
        "hq.west",
        "hq.west.kampala",
    ] {
        unit(admin, org, p).await;
    }
    // Another organisation's units must never show up.
    let other = common::seed_org(admin, "globex").await;
    unit(admin, other, "hq.secret").await;
    (app, org)
}

#[sqlx::test(migrations = "../../migrations")]
async fn pages_forward_in_path_order(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;

    let q = "{ orgUnits(first: 2) { edges { node { path name attrs } } pageInfo { hasNextPage endCursor } } }";
    let (s, b) = gql(&app, &tok, q).await;
    assert_eq!(s, StatusCode::OK);
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(paths(&b), ["hq", "hq.east"]);
    assert_eq!(b["data"]["orgUnits"]["pageInfo"]["hasNextPage"], true);
    let end = b["data"]["orgUnits"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap()
        .to_owned();

    let q2 = format!(
        r#"{{ orgUnits(first: 2, after: "{end}") {{ edges {{ node {{ path }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
    );
    let (_, b) = gql(&app, &tok, &q2).await;
    assert_eq!(paths(&b), ["hq.east.nairobi", "hq.west"]);
    let end = b["data"]["orgUnits"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap()
        .to_owned();

    let q3 = format!(
        r#"{{ orgUnits(first: 2, after: "{end}") {{ edges {{ node {{ path }} }} pageInfo {{ hasNextPage }} }} }}"#
    );
    let (_, b) = gql(&app, &tok, &q3).await;
    assert_eq!(paths(&b), ["hq.west.kampala"]);
    assert_eq!(b["data"]["orgUnits"]["pageInfo"]["hasNextPage"], false);
}

#[sqlx::test(migrations = "../../migrations")]
async fn default_page_returns_everything_in_this_org_only(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;
    let (_, b) = gql(&app, &tok, "{ orgUnits { edges { node { path } } } }").await;
    assert_eq!(paths(&b).len(), 5);
    assert!(!paths(&b).contains(&"hq.secret"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn under_path_filters_to_a_subtree(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ orgUnits(underPath: "hq.east") { edges { node { path } } } }"#,
    )
    .await;
    assert_eq!(paths(&b), ["hq.east", "hq.east.nairobi"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn subtree_scoped_keys_only_see_their_subtree(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;

    let (_, b) = gql(&app, &tok, "{ orgUnits { edges { node { path } } } }").await;
    assert_eq!(paths(&b), ["hq.east", "hq.east.nairobi"]);

    // Asking for a subtree outside the scope intersects to empty: not an error, not a leak.
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ orgUnits(underPath: "hq.west") { edges { node { path } } } }"#,
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(paths(&b).is_empty());

    let (_, b) = gql(&app, &tok, "{ viewer { orgUnitScopePath } }").await;
    assert_eq!(b["data"]["viewer"]["orgUnitScopePath"], "hq.east");
}

#[sqlx::test(migrations = "../../migrations")]
async fn page_size_is_validated(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;

    let (s, b) = gql(&app, &tok, "{ orgUnits(first: 101) { edges { cursor } } }").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");

    let (_, b) = gql(&app, &tok, "{ orgUnits(first: 0) { edges { cursor } } }").await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");

    let (_, b) = gql(&app, &tok, "{ orgUnits(first: 100) { edges { cursor } } }").await;
    assert!(b["errors"].is_null(), "{b}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn bad_cursor_and_bad_path_are_validation_errors(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;

    for q in [
        r#"{ orgUnits(after: "garbage") { edges { cursor } } }"#,
        r#"{ orgUnits(underPath: "a b!") { edges { cursor } } }"#,
        r#"{ orgUnits(underPath: "") { edges { cursor } } }"#,
    ] {
        let (s, b) = gql(&app, &tok, q).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED",
            "{q}: {b}"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn org_units_need_decisions_read(admin: PgPool) {
    let (app, org) = seeded(&admin).await;
    let tok = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (_, b) = gql(&app, &tok, "{ orgUnits { edges { cursor } } }").await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "FORBIDDEN");
}
