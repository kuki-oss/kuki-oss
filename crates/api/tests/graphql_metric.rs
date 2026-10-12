mod common;

use common::{gql, scoped_key, unit};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

fn u(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

#[allow(clippy::too_many_arguments)]
async fn put_metric(
    admin: &PgPool,
    org: Uuid,
    name: &str,
    version: i32,
    subject: u128,
    as_of: &str,
    value: Value,
    unit: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO metric_values (organisation_id, name, version, subject_id, as_of, value, unit)
         VALUES ($1, $2, $3, $4, $5::text::timestamptz, $6, $7)",
    )
    .bind(org)
    .bind(name)
    .bind(version)
    .bind(Uuid::from_u128(subject))
    .bind(as_of)
    .bind(value)
    .bind(unit)
    .execute(admin)
    .await
    .unwrap();
}

async fn put_object(admin: &PgPool, org: Uuid, n: u128, unit: Option<Uuid>) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, external_ids, attrs, valid_from)
         VALUES ($1, $2, $3, 'item', 1, '{}', '{}', '2026-01-01T00:00:00Z'::timestamptz)",
    )
    .bind(Uuid::from_u128(n))
    .bind(org)
    .bind(unit)
    .execute(admin)
    .await
    .unwrap();
}

fn mq(name: &str, subject: u128, as_of: Option<&str>) -> String {
    let a = as_of
        .map(|t| format!(r#", asOf: "{t}""#))
        .unwrap_or_default();
    format!(
        r#"{{ metric(name: "{name}", subjectId: "{}"{a}) {{ name version subjectId asOf value unit }} }}"#,
        u(subject)
    )
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_latest_value_at_or_before_as_of_wins(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;
    #[allow(clippy::too_many_arguments)]
    put_metric(
        &admin,
        org,
        "open_cases",
        1,
        7,
        "2026-10-01T00:00:00Z",
        json!(3),
        Some("cases"),
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_metric(
        &admin,
        org,
        "open_cases",
        1,
        7,
        "2026-10-02T00:00:00Z",
        json!(5),
        Some("cases"),
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_metric(
        &admin,
        org,
        "open_cases",
        1,
        7,
        "2026-10-03T00:00:00Z",
        json!(8),
        Some("cases"),
    )
    .await;
    #[allow(clippy::too_many_arguments)]
    put_metric(
        &admin,
        org,
        "open_cases",
        2,
        7,
        "2026-10-03T00:00:00Z",
        json!(9),
        Some("cases"),
    )
    .await;

    let (_, b) = gql(&app, &tok, &mq("open_cases", 7, None)).await;
    assert!(b["errors"].is_null(), "{b}");
    let m = &b["data"]["metric"];
    assert_eq!(m["value"], 9, "the higher version wins at the same as_of");
    assert_eq!(m["version"], 2);
    assert_eq!(m["unit"], "cases");
    assert_eq!(m["subjectId"], u(7));
    assert_eq!(m["name"], "open_cases");
    assert!(
        m["asOf"]
            .as_str()
            .unwrap()
            .starts_with("2026-10-03T00:00:00")
    );

    let (_, b) = gql(
        &app,
        &tok,
        &mq("open_cases", 7, Some("2026-10-02T12:00:00Z")),
    )
    .await;
    assert_eq!(b["data"]["metric"]["value"], 5);
    let (_, b) = gql(
        &app,
        &tok,
        &mq("open_cases", 7, Some("2026-10-02T00:00:00Z")),
    )
    .await;
    assert_eq!(b["data"]["metric"]["value"], 5, "as_of is inclusive");
    let (_, b) = gql(
        &app,
        &tok,
        &mq("open_cases", 7, Some("2026-09-30T23:59:59Z")),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(b["data"]["metric"].is_null(), "nothing existed yet");

    let (_, b) = gql(&app, &tok, &mq("other_metric", 7, None)).await;
    assert!(b["data"]["metric"].is_null());
    let (_, b) = gql(&app, &tok, &mq("open_cases", 8, None)).await;
    assert!(b["data"]["metric"].is_null());
}

#[sqlx::test(migrations = "../../migrations")]
async fn tenants_and_subtrees_are_isolated(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let other = common::seed_org(&admin, "globex").await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    put_object(&admin, org, 11, Some(east)).await;
    put_object(&admin, org, 12, Some(west)).await;
    for n in [11, 12, 13] {
        #[allow(clippy::too_many_arguments)]
        put_metric(
            &admin,
            org,
            "load",
            1,
            n,
            "2026-10-01T00:00:00Z",
            json!(n),
            None,
        )
        .await;
    }
    #[allow(clippy::too_many_arguments)]
    put_metric(
        &admin,
        other,
        "load",
        1,
        14,
        "2026-10-01T00:00:00Z",
        json!(14),
        None,
    )
    .await;

    let all = common::seed_key(&admin, org, &["decisions:read"]).await;
    let (_, b) = gql(&app, &all, &mq("load", 12, None)).await;
    assert_eq!(b["data"]["metric"]["value"], 12);
    let (_, b) = gql(&app, &all, &mq("load", 14, None)).await;
    assert!(
        b["data"]["metric"].is_null(),
        "another organisation's metric is invisible"
    );

    let scoped = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;
    let (_, b) = gql(&app, &scoped, &mq("load", 11, None)).await;
    assert_eq!(b["data"]["metric"]["value"], 11);
    let (_, b) = gql(&app, &scoped, &mq("load", 12, None)).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(
        b["data"]["metric"].is_null(),
        "a subject outside the subtree reads as missing"
    );
    let (_, b) = gql(&app, &scoped, &mq("load", 13, None)).await;
    assert!(
        b["data"]["metric"].is_null(),
        "a subject with no visible object is hidden from scoped keys"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn arguments_are_validated_and_the_scope_is_required(admin: PgPool) {
    let app = common::app(common::app_pool(&admin).await);
    let org = common::seed_org(&admin, "acme").await;
    let tok = common::seed_key(&admin, org, &["decisions:read"]).await;

    for q in [
        r#"{ metric(name: "x", subjectId: "nope") { name } }"#.to_owned(),
        format!(
            r#"{{ metric(name: "", subjectId: "{}") {{ name }} }}"#,
            u(1)
        ),
    ] {
        let (_, b) = gql(&app, &tok, &q).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED",
            "{q}: {b}"
        );
    }

    let weak = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (_, b) = gql(&app, &weak, &mq("x", 1, None)).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "FORBIDDEN");
}
