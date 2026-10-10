mod common;

use common::gql;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

async fn setup(admin: &PgPool) -> (axum::Router, Uuid, String) {
    let app = common::app(common::app_pool(admin).await);
    let org = common::seed_org(admin, "acme").await;
    let tok = common::seed_key(admin, org, &["decisions:read"]).await;
    (app, org, tok)
}

async fn object_type(admin: &PgPool, org: Uuid, name: &str, version: i32, schema: Value) {
    sqlx::query("INSERT INTO object_types (organisation_id, name, version, json_schema) VALUES ($1, $2, $3, $4)")
        .bind(org)
        .bind(name)
        .bind(version)
        .bind(schema)
        .execute(admin)
        .await
        .unwrap();
}

async fn event_type(admin: &PgPool, org: Uuid, name: &str, version: i32, subject: &str) {
    sqlx::query(
        "INSERT INTO event_types (organisation_id, name, version, subject_type, json_schema)
         VALUES ($1, $2, $3, $4, '{\"type\":\"object\"}'::jsonb)",
    )
    .bind(org)
    .bind(name)
    .bind(version)
    .bind(subject)
    .execute(admin)
    .await
    .unwrap();
}

async fn decision_type(admin: &PgPool, org: Uuid, name: &str, pack: &str) {
    sqlx::query(
        "INSERT INTO decision_types (organisation_id, name, pack, payload_schema_ref, payload_schema)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(org)
    .bind(name)
    .bind(pack)
    .bind(format!("{pack}/{name}.json"))
    .bind(json!({"type": "object", "title": name}))
    .execute(admin)
    .await
    .unwrap();
}

fn edges<'a>(b: &'a Value, field: &str) -> &'a Vec<Value> {
    b["data"][field]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{b}"))
}

fn name_versions(b: &Value, field: &str) -> Vec<(String, i64)> {
    edges(b, field)
        .iter()
        .map(|e| {
            (
                e["node"]["name"].as_str().unwrap().to_owned(),
                e["node"]["version"].as_i64().unwrap(),
            )
        })
        .collect()
}

fn nv(items: &[(&str, i64)]) -> Vec<(String, i64)> {
    items.iter().map(|(n, v)| ((*n).to_owned(), *v)).collect()
}

fn end_cursor(b: &Value, field: &str) -> String {
    b["data"][field]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[sqlx::test(migrations = "../../migrations")]
async fn object_types_list_every_version_in_order_and_page(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    for (n, v) in [
        ("beta", 1),
        ("alpha", 2),
        ("alpha", 1),
        ("beta", 2),
        ("gamma", 1),
    ] {
        object_type(
            &admin,
            org,
            n,
            v,
            json!({"type": "object", "title": format!("{n}@{v}")}),
        )
        .await;
    }

    let q = |after: Option<&str>| {
        let a = after
            .map(|c| format!(r#", after: "{c}""#))
            .unwrap_or_default();
        format!(
            r#"{{ objectTypes(first: 2{a}) {{ edges {{ node {{ name version jsonSchema }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
        )
    };

    let (_, b1) = gql(&app, &tok, &q(None)).await;
    assert!(b1["errors"].is_null(), "{b1}");
    assert_eq!(
        name_versions(&b1, "objectTypes"),
        nv(&[("alpha", 1), ("alpha", 2)])
    );
    assert_eq!(b1["data"]["objectTypes"]["pageInfo"]["hasNextPage"], true);
    assert_eq!(
        edges(&b1, "objectTypes")[0]["node"]["jsonSchema"]["title"],
        "alpha@1"
    );

    let (_, b2) = gql(&app, &tok, &q(Some(&end_cursor(&b1, "objectTypes")))).await;
    assert_eq!(
        name_versions(&b2, "objectTypes"),
        nv(&[("beta", 1), ("beta", 2)])
    );

    let (_, b3) = gql(&app, &tok, &q(Some(&end_cursor(&b2, "objectTypes")))).await;
    assert_eq!(name_versions(&b3, "objectTypes"), nv(&[("gamma", 1)]));
    assert_eq!(b3["data"]["objectTypes"]["pageInfo"]["hasNextPage"], false);
}

#[sqlx::test(migrations = "../../migrations")]
async fn event_types_expose_the_subject_type(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    event_type(&admin, org, "stock.counted", 1, "item").await;
    event_type(&admin, org, "stock.counted", 2, "item").await;
    event_type(&admin, org, "order.placed", 1, "order").await;

    let (_, b) = gql(
        &app,
        &tok,
        "{ eventTypes { edges { node { name version subjectType jsonSchema } } pageInfo { hasNextPage } } }",
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(
        name_versions(&b, "eventTypes"),
        nv(&[
            ("order.placed", 1),
            ("stock.counted", 1),
            ("stock.counted", 2)
        ])
    );
    let e = edges(&b, "eventTypes");
    assert_eq!(e[0]["node"]["subjectType"], "order");
    assert_eq!(e[1]["node"]["subjectType"], "item");
    assert_eq!(e[0]["node"]["jsonSchema"]["type"], "object");
    assert_eq!(b["data"]["eventTypes"]["pageInfo"]["hasNextPage"], false);
}

#[sqlx::test(migrations = "../../migrations")]
async fn decision_types_page_by_name(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    decision_type(&admin, org, "reorder", "inventory").await;
    decision_type(&admin, org, "expedite", "logistics").await;
    decision_type(&admin, org, "markdown", "pricing").await;

    let q = |after: Option<&str>| {
        let a = after
            .map(|c| format!(r#", after: "{c}""#))
            .unwrap_or_default();
        format!(
            r#"{{ decisionTypes(first: 2{a}) {{ edges {{ node {{ name pack payloadSchemaRef payloadSchema }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
        )
    };

    let (_, b1) = gql(&app, &tok, &q(None)).await;
    assert!(b1["errors"].is_null(), "{b1}");
    let names: Vec<&str> = edges(&b1, "decisionTypes")
        .iter()
        .map(|e| e["node"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["expedite", "markdown"]);
    let first = &edges(&b1, "decisionTypes")[0]["node"];
    assert_eq!(first["pack"], "logistics");
    assert_eq!(first["payloadSchemaRef"], "logistics/expedite.json");
    assert_eq!(first["payloadSchema"]["title"], "expedite");
    assert_eq!(b1["data"]["decisionTypes"]["pageInfo"]["hasNextPage"], true);

    let (_, b2) = gql(&app, &tok, &q(Some(&end_cursor(&b1, "decisionTypes")))).await;
    let names: Vec<&str> = edges(&b2, "decisionTypes")
        .iter()
        .map(|e| e["node"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["reorder"]);
    assert_eq!(
        b2["data"]["decisionTypes"]["pageInfo"]["hasNextPage"],
        false
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn registries_are_per_organisation(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let other = common::seed_org(&admin, "globex").await;
    object_type(&admin, other, "theirs", 1, json!({})).await;
    event_type(&admin, other, "theirs.happened", 1, "item").await;
    decision_type(&admin, other, "theirs", "pack").await;
    object_type(&admin, org, "ours", 1, json!({})).await;

    let (_, b) = gql(
        &app,
        &tok,
        "{ objectTypes { edges { node { name version } } } }",
    )
    .await;
    assert_eq!(name_versions(&b, "objectTypes").len(), 1);
    assert_eq!(edges(&b, "objectTypes")[0]["node"]["name"], "ours");

    let (_, b) = gql(
        &app,
        &tok,
        "{ eventTypes { edges { node { name } } pageInfo { endCursor } } }",
    )
    .await;
    assert!(edges(&b, "eventTypes").is_empty());
    assert!(b["data"]["eventTypes"]["pageInfo"]["endCursor"].is_null());

    let (_, b) = gql(&app, &tok, "{ decisionTypes { edges { node { name } } } }").await;
    assert!(edges(&b, "decisionTypes").is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn guard_cursor_and_page_size_errors(admin: PgPool) {
    let (app, org, _) = setup(&admin).await;
    let writer = common::seed_key(&admin, org, &["ingest:write"]).await;
    let reader = common::seed_key(&admin, org, &["decisions:read"]).await;

    for f in ["objectTypes", "eventTypes", "decisionTypes"] {
        let q = format!("{{ {f} {{ edges {{ cursor }} }} }}");
        let (_, b) = gql(&app, &writer, &q).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "FORBIDDEN",
            "{f}: {b}"
        );

        let (_, b) = gql(
            &app,
            &reader,
            &format!(r#"{{ {f}(after: "garbage") {{ edges {{ cursor }} }} }}"#),
        )
        .await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED",
            "{f}: {b}"
        );

        let (_, b) = gql(
            &app,
            &reader,
            &format!("{{ {f}(first: 101) {{ edges {{ cursor }} }} }}"),
        )
        .await;
        assert_eq!(
            b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED",
            "{f}: {b}"
        );
    }
}
