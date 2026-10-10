mod common;

use chrono::{DateTime, Utc};
use common::{gql, scoped_key, unit};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

struct Obj {
    id: Uuid,
    kind: &'static str,
    unit: Option<Uuid>,
    ext: Value,
    attrs: Value,
    from: &'static str,
    to: Option<&'static str>,
}

impl Obj {
    fn new(n: u128) -> Self {
        Obj {
            id: Uuid::from_u128(n),
            kind: "item",
            unit: None,
            ext: json!({}),
            attrs: json!({}),
            from: "2026-01-01T00:00:00Z",
            to: None,
        }
    }
    fn kind(mut self, k: &'static str) -> Self {
        self.kind = k;
        self
    }
    fn unit(mut self, u: Uuid) -> Self {
        self.unit = Some(u);
        self
    }
    fn ext(mut self, v: Value) -> Self {
        self.ext = v;
        self
    }
    fn attrs(mut self, v: Value) -> Self {
        self.attrs = v;
        self
    }
    fn span(mut self, from: &'static str, to: Option<&'static str>) -> Self {
        self.from = from;
        self.to = to;
        self
    }
}

async fn put(admin: &PgPool, org: Uuid, o: Obj) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, external_ids, attrs, valid_from, valid_to)
         VALUES ($1, $2, $3, $4, 1, $5, $6, $7::text::timestamptz, $8::text::timestamptz)",
    )
    .bind(o.id)
    .bind(org)
    .bind(o.unit)
    .bind(o.kind)
    .bind(o.ext)
    .bind(o.attrs)
    .bind(o.from)
    .bind(o.to)
    .execute(admin)
    .await
    .unwrap();
}

async fn setup(admin: &PgPool) -> (axum::Router, Uuid, String) {
    let app = common::app(common::app_pool(admin).await);
    let org = common::seed_org(admin, "acme").await;
    let tok = common::seed_key(admin, org, &["decisions:read"]).await;
    (app, org, tok)
}

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn at(v: &Value) -> DateTime<Utc> {
    t(v.as_str().unwrap_or_else(|| panic!("not a string: {v}")))
}

fn u(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

fn one(n: u128, as_of: Option<&str>) -> String {
    let a = as_of
        .map(|t| format!(r#", asOf: "{t}""#))
        .unwrap_or_default();
    format!(
        r#"{{ ontologyObject(id: "{}"{a}) {{ id type typeVersion attrs validFrom validTo }} }}"#,
        u(n)
    )
}

fn ext_q(system: &str, id: &str, as_of: Option<&str>) -> String {
    let a = as_of
        .map(|t| format!(r#", asOf: "{t}""#))
        .unwrap_or_default();
    format!(r#"{{ ontologyObjectByExternalId(system: "{system}", id: "{id}"{a}) {{ id }} }}"#)
}

fn ids(b: &Value) -> Vec<String> {
    b["data"]["ontologyObjects"]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{b}"))
        .iter()
        .map(|e| e["node"]["id"].as_str().unwrap().to_owned())
        .collect()
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_current_version_is_the_default_and_as_of_picks_a_version(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        Obj::new(1)
            .attrs(json!({"n": 1}))
            .span("2026-01-01T00:00:00Z", Some("2026-06-01T00:00:00Z")),
    )
    .await;
    put(
        &admin,
        org,
        Obj::new(1)
            .attrs(json!({"n": 2}))
            .span("2026-06-01T00:00:00Z", None),
    )
    .await;

    let (_, b) = gql(&app, &tok, &one(1, None)).await;
    assert!(b["errors"].is_null(), "{b}");
    let o = &b["data"]["ontologyObject"];
    assert_eq!(o["attrs"]["n"], 2);
    assert!(o["validTo"].is_null());
    assert_eq!(o["type"], "item");
    assert_eq!(o["typeVersion"], 1);

    let (_, b) = gql(&app, &tok, &one(1, Some("2026-03-01T00:00:00Z"))).await;
    let o = &b["data"]["ontologyObject"];
    assert_eq!(o["attrs"]["n"], 1);
    assert_eq!(at(&o["validTo"]), t("2026-06-01T00:00:00Z"));

    // valid_to is exclusive: at the boundary the newer version applies.
    let (_, b) = gql(&app, &tok, &one(1, Some("2026-06-01T00:00:00Z"))).await;
    assert_eq!(b["data"]["ontologyObject"]["attrs"]["n"], 2);

    // Before the first version existed there is nothing to return.
    let (_, b) = gql(&app, &tok, &one(1, Some("2025-12-31T23:59:59Z"))).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(b["data"]["ontologyObject"].is_null());
}

#[sqlx::test(migrations = "../../migrations")]
async fn unknown_foreign_and_malformed_ids(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let other = common::seed_org(&admin, "globex").await;
    put(&admin, other, Obj::new(60).ext(json!({"erp": "THEIRS"}))).await;
    put(&admin, org, Obj::new(61)).await;

    let (_, b) = gql(&app, &tok, &one(999, None)).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(b["data"]["ontologyObject"].is_null());

    // Another organisation's object is indistinguishable from a missing one.
    let (_, b) = gql(&app, &tok, &one(60, None)).await;
    assert!(b["data"]["ontologyObject"].is_null());
    let (_, b) = gql(&app, &tok, &ext_q("erp", "THEIRS", None)).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(b["data"]["ontologyObjectByExternalId"].is_null());

    let (_, b) = gql(&app, &tok, r#"{ ontologyObject(id: "nope") { id } }"#).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "item", after: "garbage") { edges { cursor } } }"#,
    )
    .await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");

    let (_, b) = gql(&app, &tok, &one(61, None)).await;
    assert_eq!(b["data"]["ontologyObject"]["id"], u(61));
}

#[sqlx::test(migrations = "../../migrations")]
async fn subtree_scoped_keys_only_see_their_subtree(admin: PgPool) {
    let (app, org, _) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    put(&admin, org, Obj::new(30).unit(east)).await;
    put(&admin, org, Obj::new(31).unit(west)).await;
    put(&admin, org, Obj::new(32)).await; // org-wide
    let tok = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;

    let (_, b) = gql(&app, &tok, &one(30, None)).await;
    assert_eq!(b["data"]["ontologyObject"]["id"], u(30));
    let (_, b) = gql(&app, &tok, &one(31, None)).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(
        b["data"]["ontologyObject"].is_null(),
        "outside the subtree reads as missing"
    );
    let (_, b) = gql(&app, &tok, &one(32, None)).await;
    assert_eq!(
        b["data"]["ontologyObject"]["id"],
        u(32),
        "org-wide objects stay visible"
    );

    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "item") { edges { node { id } } } }"#,
    )
    .await;
    assert_eq!(ids(&b), [u(30), u(32)]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn lists_page_by_id_and_filter_by_type(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    for n in 41..=45 {
        put(&admin, org, Obj::new(n).kind("pageable")).await;
    }
    put(&admin, org, Obj::new(46).kind("other")).await;

    let page = |after: Option<&str>| {
        let a = after
            .map(|c| format!(r#", after: "{c}""#))
            .unwrap_or_default();
        format!(
            r#"{{ ontologyObjects(type: "pageable", first: 2{a}) {{ edges {{ node {{ id }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
        )
    };
    let end = |b: &Value| {
        b["data"]["ontologyObjects"]["pageInfo"]["endCursor"]
            .as_str()
            .unwrap()
            .to_owned()
    };

    let (_, b) = gql(&app, &tok, &page(None)).await;
    assert_eq!(ids(&b), [u(41), u(42)]);
    assert_eq!(
        b["data"]["ontologyObjects"]["pageInfo"]["hasNextPage"],
        true
    );

    let (_, b2) = gql(&app, &tok, &page(Some(&end(&b)))).await;
    assert_eq!(ids(&b2), [u(43), u(44)]);

    let (_, b3) = gql(&app, &tok, &page(Some(&end(&b2)))).await;
    assert_eq!(ids(&b3), [u(45)]);
    assert_eq!(
        b3["data"]["ontologyObjects"]["pageInfo"]["hasNextPage"],
        false
    );

    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "pageable", first: 101) { edges { cursor } } }"#,
    )
    .await;
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");
}

#[sqlx::test(migrations = "../../migrations")]
async fn lists_honour_as_of_and_org_unit_path(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    put(&admin, org, Obj::new(50).kind("geo").unit(east)).await;
    put(&admin, org, Obj::new(51).kind("geo").unit(west)).await;
    put(&admin, org, Obj::new(52).kind("geo")).await; // org-wide

    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "geo") { edges { node { id } } } }"#,
    )
    .await;
    assert_eq!(ids(&b), [u(50), u(51), u(52)]);
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "geo", orgUnitPath: "hq.east") { edges { node { id } } } }"#,
    )
    .await;
    assert_eq!(ids(&b), [u(50)], "org-wide objects have no unit to match");

    put(
        &admin,
        org,
        Obj::new(53)
            .kind("hist")
            .attrs(json!({"n": 1}))
            .span("2026-01-01T00:00:00Z", Some("2026-06-01T00:00:00Z")),
    )
    .await;
    put(
        &admin,
        org,
        Obj::new(53)
            .kind("hist")
            .attrs(json!({"n": 2}))
            .span("2026-06-01T00:00:00Z", None),
    )
    .await;
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "hist") { edges { node { attrs } } } }"#,
    )
    .await;
    assert_eq!(
        b["data"]["ontologyObjects"]["edges"][0]["node"]["attrs"]["n"],
        2
    );
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyObjects(type: "hist", asOf: "2026-03-01T00:00:00Z") { edges { node { attrs } } } }"#,
    )
    .await;
    assert_eq!(
        b["data"]["ontologyObjects"]["edges"][0]["node"]["attrs"]["n"],
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn external_id_lookup_follows_versions(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        Obj::new(70).ext(json!({"erp": "SKU-204", "wms": "W-1"})),
    )
    .await;
    put(
        &admin,
        org,
        Obj::new(71)
            .ext(json!({"erp": "OLD"}))
            .span("2026-01-01T00:00:00Z", Some("2026-06-01T00:00:00Z")),
    )
    .await;
    put(
        &admin,
        org,
        Obj::new(71)
            .ext(json!({"erp": "NEW"}))
            .span("2026-06-01T00:00:00Z", None),
    )
    .await;

    let found = |b: &Value| {
        b["data"]["ontologyObjectByExternalId"]["id"]
            .as_str()
            .map(str::to_owned)
    };

    let (_, b) = gql(&app, &tok, &ext_q("erp", "SKU-204", None)).await;
    assert_eq!(found(&b), Some(u(70)));
    let (_, b) = gql(&app, &tok, &ext_q("wms", "W-1", None)).await;
    assert_eq!(found(&b), Some(u(70)));
    let (_, b) = gql(&app, &tok, &ext_q("erp", "W-1", None)).await;
    assert_eq!(found(&b), None, "the id belongs to a different system");

    let (_, b) = gql(&app, &tok, &ext_q("erp", "OLD", None)).await;
    assert_eq!(found(&b), None);
    let (_, b) = gql(
        &app,
        &tok,
        &ext_q("erp", "OLD", Some("2026-03-01T00:00:00Z")),
    )
    .await;
    assert_eq!(found(&b), Some(u(71)));
    let (_, b) = gql(&app, &tok, &ext_q("erp", "NEW", None)).await;
    assert_eq!(found(&b), Some(u(71)));
    let (_, b) = gql(
        &app,
        &tok,
        &ext_q("erp", "NEW", Some("2026-03-01T00:00:00Z")),
    )
    .await;
    assert_eq!(found(&b), None);
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_ambiguous_external_id_is_a_conflict(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(&admin, org, Obj::new(80).ext(json!({"erp": "DUP"}))).await;
    put(&admin, org, Obj::new(81).ext(json!({"erp": "DUP"}))).await;

    let (s, b) = gql(&app, &tok, &ext_q("erp", "DUP", None)).await;
    assert_eq!(s, axum::http::StatusCode::OK);
    assert_eq!(b["errors"][0]["extensions"]["code"], "CONFLICT");
    assert!(b["data"]["ontologyObjectByExternalId"].is_null());
}

#[sqlx::test(migrations = "../../migrations")]
async fn every_object_read_needs_decisions_read(admin: PgPool) {
    let (app, org, _) = setup(&admin).await;
    put(&admin, org, Obj::new(90)).await;
    let tok = common::seed_key(&admin, org, &["ingest:write"]).await;

    for q in [
        one(90, None),
        r#"{ ontologyObjects(type: "item") { edges { cursor } } }"#.to_owned(),
        ext_q("erp", "x", None),
    ] {
        let (_, b) = gql(&app, &tok, &q).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "FORBIDDEN",
            "{q}: {b}"
        );
    }
}
