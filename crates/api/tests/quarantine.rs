mod common;

use axum::http::{HeaderMap, StatusCode};
use common::*;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

async fn reject_one(app: &axum::Router, token: &str, key: &str, item: Uuid) -> String {
    let (s, _, v) = send(app, json_req(token, key, vec![event(item, json!("oops"))])).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    v["rejected"][0]["quarantineId"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn replay(
    app: &axum::Router,
    token: &str,
    id: &str,
    key: &str,
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    let uri = format!("/v1/quarantine/{id}/replay");
    let body = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    send(
        app,
        Req {
            uri: &uri,
            token: Some(token),
            idem: Some(key),
            content_type: "application/json",
            body,
        },
    )
    .await
}

async fn status_of(admin: &PgPool, id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM ingest_quarantine WHERE id = $1")
        .bind(Uuid::parse_str(id).unwrap())
        .fetch_one(admin)
        .await
        .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_corrected_row_is_replayed_into_the_ledger(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "rq-flow").await;
    let app = app(app_pool(&admin).await);
    let qid = reject_one(&app, &token, "load", item).await;

    let (s, list) = call(&app, "GET", "/v1/quarantine", &token, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    assert_eq!(list["items"][0]["id"], qid.as_str());
    assert_eq!(list["items"][0]["code"], "SCHEMA_INVALID");
    assert_eq!(list["pageInfo"]["hasNextPage"], false);

    let fix = json!({ "event": event(item, json!(5)) });
    let (s, _, r) = replay(&app, &token, &qid, "fix-1", fix.clone()).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(r["acceptedCount"], 1);
    assert!(r["rejected"].as_array().unwrap().is_empty());
    assert_eq!(count(&admin, "events", org).await, 1);
    assert_eq!(status_of(&admin, &qid).await, "replayed");
    let kind: String = sqlx::query_scalar("SELECT kind FROM ingest_batches WHERE id = $1")
        .bind(Uuid::parse_str(r["batchId"].as_str().unwrap()).unwrap())
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(kind, "replay");

    let (_, list) = call(&app, "GET", "/v1/quarantine", &token, None).await;
    assert!(list["items"].as_array().unwrap().is_empty());

    // Same key: the stored response. A different key: the row is already replayed.
    let (s, h, again) = replay(&app, &token, &qid, "fix-1", fix).await;
    assert_eq!(
        (s, h["idempotency-replayed"].to_str().unwrap()),
        (StatusCode::ACCEPTED, "true")
    );
    assert_eq!(again, r);
    let (s, _, p) = replay(&app, &token, &qid, "fix-2", Value::Null).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(p["code"], "CONFLICT");
    assert_eq!(count(&admin, "events", org).await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_row_that_is_still_invalid_stays_quarantined(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "rq-still").await;
    let app = app(app_pool(&admin).await);
    let qid = reject_one(&app, &token, "load", item).await;

    for (key, body) in [
        ("t1", json!({ "event": event(item, json!("still bad")) })),
        ("t2", Value::Null),
    ] {
        let (s, _, r) = replay(&app, &token, &qid, key, body).await;
        assert_eq!(s, StatusCode::ACCEPTED);
        assert_eq!(r["acceptedCount"], 0);
        assert_eq!(r["rejected"][0]["quarantineId"], qid.as_str());
    }
    assert_eq!(count(&admin, "ingest_quarantine", org).await, 1);
    assert_eq!(count(&admin, "events", org).await, 0);
    assert_eq!(status_of(&admin, &qid).await, "quarantined");
}

#[sqlx::test(migrations = "../../migrations")]
async fn quarantine_is_per_tenant(admin: PgPool) {
    let (_, item, token_a) = tenant(&admin, "rq-a").await;
    let (_, _, token_b) = tenant(&admin, "rq-b").await;
    let app = app(app_pool(&admin).await);
    let qid = reject_one(&app, &token_a, "load", item).await;

    let (s, list) = call(&app, "GET", "/v1/quarantine", &token_b, None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(list["items"].as_array().unwrap().is_empty());
    let (s, _, p) = replay(&app, &token_b, &qid, "steal", Value::Null).await;
    assert_eq!(
        (s, p["code"].as_str().unwrap()),
        (StatusCode::NOT_FOUND, "NOT_FOUND")
    );
    assert_eq!(status_of(&admin, &qid).await, "quarantined");
}

#[sqlx::test(migrations = "../../migrations")]
async fn listing_pages_by_cursor(admin: PgPool) {
    let (_, _, token) = tenant(&admin, "rq-page").await;
    let app = app(app_pool(&admin).await);
    let (s, ..) = send(
        &app,
        Req {
            uri: "/v1/events",
            token: Some(&token),
            idem: Some("bad-lines"),
            content_type: "application/x-ndjson",
            body: "{bad1\n{bad2\n{bad3".into(),
        },
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED);

    let (_, p1) = call(&app, "GET", "/v1/quarantine?first=2", &token, None).await;
    assert_eq!(p1["items"].as_array().unwrap().len(), 2);
    assert_eq!(p1["pageInfo"]["hasNextPage"], true);
    let cursor = p1["pageInfo"]["endCursor"].as_str().unwrap();

    let (_, p2) = call(
        &app,
        "GET",
        &format!("/v1/quarantine?first=2&after={cursor}"),
        &token,
        None,
    )
    .await;
    assert_eq!(p2["items"].as_array().unwrap().len(), 1);
    assert_eq!(p2["pageInfo"]["hasNextPage"], false);

    let mut ids: Vec<&str> = p1["items"]
        .as_array()
        .unwrap()
        .iter()
        .chain(p2["items"].as_array().unwrap())
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3);
}

#[sqlx::test(migrations = "../../migrations")]
async fn parameters_and_bodies_are_validated(admin: PgPool) {
    let (_, item, token) = tenant(&admin, "rq-valid").await;
    let app = app(app_pool(&admin).await);
    let qid = reject_one(&app, &token, "load", item).await;

    for q in [
        "first=0",
        "first=101",
        "first=x",
        "after=junk",
        "batchId=nope",
    ] {
        let (_, p) = call(&app, "GET", &format!("/v1/quarantine?{q}"), &token, None).await;
        assert_eq!(p["code"], "VALIDATION_FAILED", "{q}");
    }
    // The right key for an events row is `event`.
    let (_, _, p) = replay(&app, &token, &qid, "k1", json!({ "object": {} })).await;
    assert_eq!(p["code"], "VALIDATION_FAILED");
    let (_, _, p) = replay(&app, &token, &qid, "k2", json!({ "event": {}, "link": {} })).await;
    assert_eq!(p["code"], "VALIDATION_FAILED");

    let (s, _, _) = replay(&app, &token, &Uuid::new_v4().to_string(), "k3", Value::Null).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _, _) = send(
        &app,
        Req {
            uri: &format!("/v1/quarantine/{qid}/replay"),
            token: Some(&token),
            idem: None,
            content_type: "application/json",
            body: String::new(),
        },
    )
    .await;
    assert_ne!(s, StatusCode::ACCEPTED);
}
