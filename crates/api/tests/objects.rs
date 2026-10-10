mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

async fn objects_tenant(admin: &PgPool, slug: &str) -> (Uuid, String) {
    let org = seed_org(admin, slug).await;
    seed_object_type(admin, org).await;
    let token = seed_key(admin, org, &["ingest:write", "ingest:read"]).await;
    (org, token)
}

fn obj(ext: &str, name: &str) -> Value {
    json!({ "type": "item", "typeVersion": 1, "externalIds": { "erp": ext }, "attrs": { "name": name } })
}

fn objects_req<'a>(token: &'a str, idem: &'a str, objects: Vec<Value>) -> Req<'a> {
    Req {
        uri: "/v1/objects/batch",
        token: Some(token),
        idem: Some(idem),
        content_type: "application/json",
        body: json!({ "objects": objects }).to_string(),
    }
}

async fn scalar(admin: &PgPool, sql: &str, org: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(org)
        .fetch_one(admin)
        .await
        .unwrap()
}

const UPSERT_EVENTS: &str =
    "SELECT count(*) FROM events WHERE organisation_id = $1 AND type = 'kuki.object.upserted'";

#[sqlx::test(migrations = "../../migrations")]
async fn objects_are_created_updated_and_left_alone(admin: PgPool) {
    let (org, token) = objects_tenant(&admin, "obj-life").await;
    let app = app(app_pool(&admin).await);

    let (s, _, v) = send(&app, objects_req(&token, "o-1", vec![obj("A1", "Bolt")])).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["accepted"][0]["outcome"], "created");
    let id = v["accepted"][0]["id"].as_str().unwrap().to_owned();

    // identical content: no write, no event
    let (_, _, v) = send(&app, objects_req(&token, "o-2", vec![obj("A1", "Bolt")])).await;
    assert_eq!(v["accepted"][0]["outcome"], "unchanged");
    assert_eq!(v["accepted"][0]["id"], id);
    assert_eq!(scalar(&admin, UPSERT_EVENTS, org).await, 1);

    // changed attrs: a new version of the same object
    let (_, _, first) = send(&app, objects_req(&token, "o-3", vec![obj("A1", "Bolt M8")])).await;
    assert_eq!(first["accepted"][0]["outcome"], "updated");
    assert_eq!(first["accepted"][0]["id"], id);
    assert_eq!(scalar(&admin, UPSERT_EVENTS, org).await, 2);
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM objects WHERE organisation_id = $1",
            org
        )
        .await,
        2
    );
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM objects WHERE organisation_id = $1 AND valid_to IS NULL",
            org
        )
        .await,
        1
    );

    // the same key replays the stored response and writes nothing
    let (s, h, again) = send(&app, objects_req(&token, "o-3", vec![obj("A1", "Bolt M8")])).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(h["idempotency-replayed"], "true");
    assert_eq!(again, first);
    assert_eq!(scalar(&admin, UPSERT_EVENTS, org).await, 2);

    // another body under the same key is a conflict
    let (s, ..) = send(&app, objects_req(&token, "o-3", vec![obj("A1", "other")])).await;
    assert_eq!(s, StatusCode::CONFLICT);
}

#[sqlx::test(migrations = "../../migrations")]
async fn bad_rows_are_rejected_with_specific_codes(admin: PgPool) {
    let (org, token) = objects_tenant(&admin, "obj-reject").await;
    for _ in 0..2 {
        sqlx::query(
            "INSERT INTO objects (id, organisation_id, type, type_version, external_ids, attrs, valid_from)
             VALUES (gen_random_uuid(), $1, 'item', 1, '{\"erp\":\"DUP\"}', '{}', now())",
        )
        .bind(org)
        .execute(&admin)
        .await
        .unwrap();
    }
    let app = app(app_pool(&admin).await);

    let mut bogus = obj("X", "n");
    bogus["bogus"] = json!(1);
    let mut nowhere = obj("Y", "n");
    nowhere["orgUnitPath"] = json!("nowhere");
    let rows = vec![
        json!({ "type": "nope", "typeVersion": 1, "externalIds": { "erp": "X" }, "attrs": {} }),
        json!({ "type": "item", "typeVersion": 9, "externalIds": { "erp": "X" }, "attrs": {} }),
        json!({ "type": "item", "typeVersion": 1, "externalIds": { "erp": "X" }, "attrs": { "name": 5 } }),
        bogus,
        obj("DUP", "n"),
        nowhere,
        obj("OK", "fine"),
    ];
    let (s, _, v) = send(&app, objects_req(&token, "r-1", rows)).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert_eq!(v["acceptedCount"], 1);
    let rejected = v["rejected"].as_array().unwrap();
    let got: Vec<(&str, Option<&str>)> = rejected
        .iter()
        .map(|r| (r["code"].as_str().unwrap(), r["field"].as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("UNKNOWN_OBJECT_TYPE", Some("/type")),
            ("UNKNOWN_TYPE_VERSION", Some("/typeVersion")),
            ("SCHEMA_INVALID", Some("/attrs/name")),
            ("SCHEMA_INVALID", Some("/bogus")),
            ("AMBIGUOUS_SUBJECT", Some("/externalIds")),
            ("OUT_OF_SCOPE", Some("/orgUnitPath")),
        ]
    );
    assert_eq!(
        scalar(
            &admin,
            "SELECT count(*) FROM ingest_quarantine WHERE organisation_id = $1",
            org
        )
        .await,
        6
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_same_object_twice_in_one_batch_applies_in_order(admin: PgPool) {
    let (org, token) = objects_tenant(&admin, "obj-dup").await;
    let app = app(app_pool(&admin).await);

    let (_, _, v) = send(
        &app,
        objects_req(&token, "d-1", vec![obj("D1", "one"), obj("D1", "two")]),
    )
    .await;
    assert_eq!(v["accepted"][0]["outcome"], "created");
    assert_eq!(v["accepted"][1]["outcome"], "updated");
    assert_eq!(v["accepted"][0]["id"], v["accepted"][1]["id"]);
    let name: String = sqlx::query_scalar(
        "SELECT attrs->>'name' FROM objects WHERE organisation_id = $1 AND valid_to IS NULL",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(name, "two");
}

#[sqlx::test(migrations = "../../migrations")]
async fn scoped_keys_must_name_a_unit_inside_their_subtree(admin: PgPool) {
    let org = seed_org(&admin, "obj-scope").await;
    seed_object_type(&admin, org).await;
    for path in ["emea.de", "apac.jp"] {
        sqlx::query(
            "INSERT INTO org_units (id, organisation_id, path, name)
             VALUES (gen_random_uuid(), $1, $2::text::ltree, $2::text)",
        )
        .bind(org)
        .bind(path)
        .execute(&admin)
        .await
        .unwrap();
    }
    let token = seed_key_in(&admin, org, &["ingest:write"], Some("emea")).await;
    let app = app(app_pool(&admin).await);

    let mut inside = obj("S1", "n");
    inside["orgUnitPath"] = json!("emea.de");
    let mut outside = obj("S2", "n");
    outside["orgUnitPath"] = json!("apac.jp");
    let (_, _, v) = send(
        &app,
        objects_req(&token, "s-1", vec![inside, outside, obj("S3", "n")]),
    )
    .await;

    assert_eq!(v["acceptedCount"], 1);
    let codes: Vec<_> = v["rejected"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["code"].as_str().unwrap())
        .collect();
    assert_eq!(codes, vec!["OUT_OF_SCOPE", "OUT_OF_SCOPE"]);
    let placed: i64 = scalar(
        &admin,
        "SELECT count(*) FROM objects WHERE organisation_id = $1 AND org_unit_id IS NOT NULL",
        org,
    )
    .await;
    assert_eq!(placed, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_are_isolated_and_requests_are_guarded(admin: PgPool) {
    let (org_a, token_a) = objects_tenant(&admin, "obj-a").await;
    let (org_b, token_b) = objects_tenant(&admin, "obj-b").await;
    let read_only = seed_key(&admin, org_b, &["ingest:read"]).await;
    let app = app(app_pool(&admin).await);

    send(&app, objects_req(&token_a, "t-1", vec![obj("S1", "a")])).await;
    // the same external id in another tenant is a different object
    let (_, _, v) = send(&app, objects_req(&token_b, "t-1", vec![obj("S1", "b")])).await;
    assert_eq!(v["accepted"][0]["outcome"], "created");
    for org in [org_a, org_b] {
        assert_eq!(
            scalar(
                &admin,
                "SELECT count(*) FROM objects WHERE organisation_id = $1",
                org
            )
            .await,
            1
        );
    }

    assert_eq!(
        send(&app, objects_req(&read_only, "g-1", vec![obj("Z", "z")]))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut r = objects_req(&token_b, "g-2", vec![obj("Z", "z")]);
    r.idem = None;
    assert_eq!(send(&app, r).await.0, StatusCode::BAD_REQUEST);

    let mut r = objects_req(&token_b, "g-3", vec![obj("Z", "z")]);
    r.uri = "/v1/objects/batch?return=summary";
    let (s, _, v) = send(&app, r).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert!(v.get("accepted").is_none() || v["accepted"].is_null());
}
