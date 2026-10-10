mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

async fn seed_object(admin: &PgPool, org: Uuid, ext: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, type, type_version, external_ids, attrs, valid_from)
         VALUES ($1, $2, 'item', 1, $3, '{}', now())",
    )
    .bind(id)
    .bind(org)
    .bind(json!({ "erp": ext }))
    .execute(admin)
    .await
    .unwrap();
    id
}

/// An org with objects A and B (external ids "A" and "B") and an ingest key.
async fn links_tenant(admin: &PgPool, slug: &str) -> (Uuid, Uuid, String) {
    let org = seed_org(admin, slug).await;
    let a = seed_object(admin, org, "A").await;
    seed_object(admin, org, "B").await;
    let token = seed_key(admin, org, &["ingest:write", "ingest:read"]).await;
    (org, a, token)
}

fn by_id(id: Uuid) -> Value {
    json!({ "id": id })
}
fn ext(id: &str) -> Value {
    json!({ "externalId": { "system": "erp", "id": id } })
}
fn link(src: Value, dst: Value, attrs: Value) -> Value {
    json!({ "type": "supplies", "src": src, "dst": dst, "attrs": attrs })
}

fn links_req<'a>(token: &'a str, idem: &'a str, links: Vec<Value>) -> Req<'a> {
    Req {
        uri: "/v1/links/batch",
        token: Some(token),
        idem: Some(idem),
        content_type: "application/json",
        body: json!({ "links": links }).to_string(),
    }
}

async fn scalar(admin: &PgPool, sql: &str, org: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(org)
        .fetch_one(admin)
        .await
        .unwrap()
}

const LINK_EVENTS: &str =
    "SELECT count(*) FROM events WHERE organisation_id = $1 AND type = 'kuki.link.upserted'";

#[sqlx::test(migrations = "../../migrations")]
async fn links_are_created_updated_and_left_alone(admin: PgPool) {
    let (org, a, token) = links_tenant(&admin, "lnk-life").await;
    let app = app(app_pool(&admin).await);

    let (s, _, v) = send(
        &app,
        links_req(
            &token,
            "l-1",
            vec![link(by_id(a), ext("B"), json!({ "qty": 1 }))],
        ),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["accepted"][0]["outcome"], "created");
    let id1 = v["accepted"][0]["id"].as_str().unwrap().to_owned();

    // identical content: no write, no event, same id
    let (_, _, v) = send(
        &app,
        links_req(
            &token,
            "l-2",
            vec![link(by_id(a), ext("B"), json!({ "qty": 1 }))],
        ),
    )
    .await;
    assert_eq!(v["accepted"][0]["outcome"], "unchanged");
    assert_eq!(v["accepted"][0]["id"], id1);
    assert_eq!(scalar(&admin, LINK_EVENTS, org).await, 1);

    // changed attrs: a new version, and a new row id
    let (_, _, first) = send(
        &app,
        links_req(
            &token,
            "l-3",
            vec![link(by_id(a), ext("B"), json!({ "qty": 2 }))],
        ),
    )
    .await;
    assert_eq!(first["accepted"][0]["outcome"], "updated");
    assert_ne!(first["accepted"][0]["id"], id1);
    assert_eq!(scalar(&admin, LINK_EVENTS, org).await, 2);
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM links WHERE organisation_id = $1",
            org
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM links WHERE organisation_id = $1 AND valid_to IS NULL",
            org
        )
        .await,
        1
    );

    // the same key replays the stored response and writes nothing
    let (s, h, again) = send(
        &app,
        links_req(
            &token,
            "l-3",
            vec![link(by_id(a), ext("B"), json!({ "qty": 2 }))],
        ),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(h["idempotency-replayed"], "true");
    assert_eq!(again, first);
    assert_eq!(scalar(&admin, LINK_EVENTS, org).await, 2);

    // another endpoint's body under the same key is a conflict
    let (s, ..) = send(
        &app,
        links_req(
            &token,
            "l-3",
            vec![link(by_id(a), ext("B"), json!({ "qty": 9 }))],
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);
}

#[sqlx::test(migrations = "../../migrations")]
async fn bad_rows_are_rejected_with_specific_codes(admin: PgPool) {
    let (org, a, token) = links_tenant(&admin, "lnk-reject").await;
    seed_object(&admin, org, "DUP").await;
    seed_object(&admin, org, "DUP").await;
    let app = app(app_pool(&admin).await);

    let mut bogus = link(by_id(a), ext("B"), json!({}));
    bogus["bogus"] = json!(1);
    let rows = vec![
        link(by_id(Uuid::new_v4()), ext("B"), json!({})),
        link(by_id(a), ext("DUP"), json!({})),
        bogus,
        json!({ "type": "supplies", "src": {}, "dst": ext("B") }),
        link(by_id(a), ext("B"), json!({})),
    ];
    let (s, _, v) = send(&app, links_req(&token, "r-1", rows)).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["acceptedCount"], 1);
    let got: Vec<(&str, Option<&str>)> = v["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| (r["code"].as_str().unwrap(), r["field"].as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("UNRESOLVED_SUBJECT", Some("/src")),
            ("AMBIGUOUS_SUBJECT", Some("/dst")),
            ("SCHEMA_INVALID", Some("/bogus")),
            ("SCHEMA_INVALID", Some("/src")),
        ]
    );
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM ingest_quarantine WHERE organisation_id = $1",
            org
        )
        .await,
        4
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_same_link_twice_in_one_batch_applies_in_order(admin: PgPool) {
    let (org, a, token) = links_tenant(&admin, "lnk-dup").await;
    let app = app(app_pool(&admin).await);

    let rows = vec![
        link(by_id(a), ext("B"), json!({ "v": 1 })),
        link(by_id(a), ext("B"), json!({ "v": 2 })),
    ];
    let (_, _, v) = send(&app, links_req(&token, "d-1", rows)).await;
    assert_eq!(v["accepted"][0]["outcome"], "created");
    assert_eq!(v["accepted"][1]["outcome"], "updated");
    assert_ne!(v["accepted"][0]["id"], v["accepted"][1]["id"]);
    let v2: i64 = sqlx::query_scalar(
        "SELECT (attrs->>'v')::bigint FROM links WHERE organisation_id = $1 AND valid_to IS NULL",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(v2, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_are_isolated_and_requests_are_guarded(admin: PgPool) {
    let (org_a, a_in_a, _) = links_tenant(&admin, "lnk-a").await;
    let (org_b, _, token_b) = links_tenant(&admin, "lnk-b").await;
    let read_only = seed_key(&admin, org_b, &["ingest:read"]).await;
    let app = app(app_pool(&admin).await);

    // org B cannot link to org A's object by id
    let (_, _, v) = send(
        &app,
        links_req(
            &token_b,
            "t-1",
            vec![link(by_id(a_in_a), ext("B"), json!({}))],
        ),
    )
    .await;
    assert_eq!(v["acceptedCount"], 0);
    assert_eq!(v["rejected"][0]["code"], "UNRESOLVED_SUBJECT");
    assert_eq!(v["rejected"][0]["field"], "/src");

    // its own objects, same external ids as org A's, link fine
    let (_, _, v) = send(
        &app,
        links_req(&token_b, "t-2", vec![link(ext("A"), ext("B"), json!({}))]),
    )
    .await;
    assert_eq!(v["accepted"][0]["outcome"], "created");
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM links WHERE organisation_id = $1",
            org_a
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM links WHERE organisation_id = $1",
            org_b
        )
        .await,
        1
    );

    assert_eq!(
        send(
            &app,
            links_req(&read_only, "g-1", vec![link(ext("A"), ext("B"), json!({}))])
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );
    let mut r = links_req(&token_b, "g-2", vec![link(ext("A"), ext("B"), json!({}))]);
    r.idem = None;
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    let mut r = links_req(
        &token_b,
        "g-3",
        vec![link(ext("A"), ext("B"), json!({ "n": 1 }))],
    );
    r.uri = "/v1/links/batch?return=summary";
    let (s, _, v) = send(&app, r).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert!(v.get("accepted").is_none() || v["accepted"].is_null());
}
