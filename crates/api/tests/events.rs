mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;
use sqlx::PgPool;

const BF_EVENTS: &str = "/v1/events?mode=backfill&backfillId=history-2024";

#[sqlx::test(migrations = "../../migrations")]
async fn events_are_accepted_then_replayed(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "replay").await;
    let app = app(app_pool(&admin).await);
    let events = vec![event(item, json!(1)), event(item, json!(2))];

    let (s, h, first) = send(&app, json_req(&token, "load-1", events.clone())).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert!(h.get("idempotency-replayed").is_none());
    assert_eq!(first["acceptedCount"], 2);
    assert_eq!(first["accepted"].as_array().unwrap().len(), 2);
    assert_eq!(count(&admin, "events", org).await, 2);

    let (s, h, second) = send(&app, json_req(&token, "load-1", events)).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(h["idempotency-replayed"], "true");
    assert_eq!(second, first);
    assert_eq!(count(&admin, "events", org).await, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn reusing_a_key_with_a_different_body_is_a_conflict(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "mismatch").await;
    let app = app(app_pool(&admin).await);

    let (s, ..) = send(&app, json_req(&token, "k", vec![event(item, json!(1))])).await;
    assert_eq!(s, StatusCode::ACCEPTED);

    let (s, _, body) = send(&app, json_req(&token, "k", vec![event(item, json!(99))])).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(body["code"], "CONFLICT");
    assert_eq!(count(&admin, "events", org).await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_mixed_ndjson_load_is_partially_accepted(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "partial").await;
    let app = app(app_pool(&admin).await);
    let body = [
        event(item, json!(1)).to_string(),
        event(item, json!("not-a-number")).to_string(),
        "{not json".to_string(),
    ]
    .join("\n");

    let (s, _, v) = send(
        &app,
        Req {
            uri: "/v1/events",
            token: Some(&token),
            idem: Some("nd-1"),
            content_type: "application/x-ndjson",
            body,
        },
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["acceptedCount"], 1);
    let codes: Vec<_> = v["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["code"].as_str().unwrap())
        .collect();
    assert_eq!(codes, vec!["SCHEMA_INVALID", "MALFORMED_LINE"]);
    assert_eq!(count(&admin, "events", org).await, 1);
    assert_eq!(count(&admin, "ingest_quarantine", org).await, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn summary_mode_omits_accepted_ids(admin: PgPool) {
    let (_, item, token) = tenant(&admin, "summary").await;
    let app = app(app_pool(&admin).await);
    let mut r = json_req(&token, "s-1", vec![event(item, json!(1))]);
    r.uri = "/v1/events?return=summary";
    let (s, _, v) = send(&app, r).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["acceptedCount"], 1);
    assert!(v.get("accepted").is_none() || v["accepted"].is_null());
}

#[sqlx::test(migrations = "../../migrations")]
async fn another_tenants_subject_is_unresolved_and_nothing_leaks(admin: PgPool) {
    let (org_a, item_a, _) = tenant(&admin, "tenant-a").await;
    let (org_b, _, token_b) = tenant(&admin, "tenant-b").await;
    let app = app(app_pool(&admin).await);

    let (s, _, v) = send(
        &app,
        json_req(&token_b, "x-1", vec![event(item_a, json!(1))]),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["acceptedCount"], 0);
    assert_eq!(v["rejected"][0]["code"], "UNRESOLVED_SUBJECT");
    assert_eq!(count(&admin, "events", org_a).await, 0);
    assert_eq!(count(&admin, "events", org_b).await, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn request_level_failures_use_problem_details(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "guards").await;
    let read_only = seed_key(&admin, org, &["ingest:read"]).await;
    let app = app(app_pool(&admin).await);
    let ok = || vec![event(item, json!(1))];

    // no credentials
    let mut r = json_req(&token, "g-1", ok());
    r.token = None;
    assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED);

    // wrong scope
    let (s, _, v) = send(&app, json_req(&read_only, "g-2", ok())).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(v["code"], "FORBIDDEN");

    // missing Idempotency-Key
    let mut r = json_req(&token, "", ok());
    r.idem = None;
    let (s, _, v) = send(&app, r).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], "VALIDATION_FAILED");

    // unsupported content type
    let mut r = json_req(&token, "g-3", ok());
    r.content_type = "text/plain";
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    // mode=backfill without a backfillId
    let mut r = json_req(&token, "g-4", ok());
    r.uri = "/v1/events?mode=backfill";
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    // empty events array
    assert_eq!(
        send(&app, json_req(&token, "g-5", vec![])).await.0,
        StatusCode::BAD_REQUEST
    );

    assert_eq!(count(&admin, "events", org).await, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_backfill_tags_events_and_closes_on_completion(admin: PgPool) {
    let (org, item, token) = tenant(&admin, "bf-life").await;
    let app = app(app_pool(&admin).await);

    let mut r = json_req(
        &token,
        "bf-1",
        vec![event(item, json!(1)), event(item, json!(2))],
    );
    r.uri = BF_EVENTS;
    assert_eq!(send(&app, r).await.0, StatusCode::ACCEPTED);

    let tagged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events WHERE organisation_id = $1 AND backfill_id = 'history-2024'",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(tagged, 2);

    let (s, v) = call(&app, "GET", "/v1/backfills/history-2024", &token, None).await;
    assert_eq!(
        (s, v["status"].as_str(), v["eventCount"].as_i64()),
        (StatusCode::OK, Some("open"), Some(2))
    );

    // completion is retry-safe
    for key in ["c-1", "c-2"] {
        let (s, v) = call(
            &app,
            "POST",
            "/v1/backfills/history-2024/complete",
            &token,
            Some(key),
        )
        .await;
        assert_eq!(
            (s, v["status"].as_str()),
            (StatusCode::ACCEPTED, Some("completing"))
        );
    }

    // closed to further backfill batches, but live ingest is unaffected
    let mut r = json_req(&token, "bf-2", vec![event(item, json!(3))]);
    r.uri = BF_EVENTS;
    let (s, _, v) = send(&app, r).await;
    assert_eq!(
        (s, v["code"].as_str()),
        (StatusCode::CONFLICT, Some("CONFLICT"))
    );
    assert_eq!(
        send(
            &app,
            json_req(&token, "live-1", vec![event(item, json!(4))])
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );

    let (_, v) = call(&app, "GET", "/v1/backfills/history-2024", &token, None).await;
    assert_eq!(
        (v["status"].as_str(), v["eventCount"].as_i64()),
        (Some("completing"), Some(2))
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn backfill_requests_are_validated(admin: PgPool) {
    let (_, item, token) = tenant(&admin, "bf-guards").await;
    let app = app(app_pool(&admin).await);
    let ok = || vec![event(item, json!(1))];

    let mut r = json_req(&token, "v-1", ok());
    r.uri = "/v1/events?mode=backfill";
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    let mut r = json_req(&token, "v-2", ok());
    r.uri = "/v1/events?backfillId=orphan";
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    assert_eq!(
        call(&app, "POST", "/v1/backfills/none/complete", &token, None)
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/backfills/none/complete",
            &token,
            Some("k")
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(&app, "GET", "/v1/backfills/none", &token, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn backfills_are_per_tenant_and_expire(admin: PgPool) {
    let (org_a, item_a, token_a) = tenant(&admin, "bf-a").await;
    let (_, _, token_b) = tenant(&admin, "bf-b").await;
    let app = app(app_pool(&admin).await);

    let mut r = json_req(&token_a, "x-1", vec![event(item_a, json!(1))]);
    r.uri = BF_EVENTS;
    assert_eq!(send(&app, r).await.0, StatusCode::ACCEPTED);

    // another tenant cannot see it
    assert_eq!(
        call(&app, "GET", "/v1/backfills/history-2024", &token_b, None)
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    // a lapsed backfill reads as expired and refuses events and completion
    sqlx::query(
        "UPDATE backfills SET expires_at = now() - interval '1 minute' WHERE organisation_id = $1",
    )
    .bind(org_a)
    .execute(&admin)
    .await
    .unwrap();
    let (_, v) = call(&app, "GET", "/v1/backfills/history-2024", &token_a, None).await;
    assert_eq!(v["status"], "expired");
    let mut r = json_req(&token_a, "x-2", vec![event(item_a, json!(2))]);
    r.uri = BF_EVENTS;
    assert_eq!(send(&app, r).await.0, StatusCode::CONFLICT);
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/backfills/history-2024/complete",
            &token_a,
            Some("c")
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}
