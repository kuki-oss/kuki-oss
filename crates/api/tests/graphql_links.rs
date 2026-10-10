mod common;

use common::{gql, scoped_key, unit};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn s(n: u128) -> String {
    u(n).to_string()
}

struct L {
    id: u128,
    kind: &'static str,
    src: u128,
    dst: u128,
    from: &'static str,
    to: Option<&'static str>,
}

impl L {
    fn new(id: u128, kind: &'static str, src: u128, dst: u128) -> Self {
        L {
            id,
            kind,
            src,
            dst,
            from: "2026-01-01T00:00:00Z",
            to: None,
        }
    }
    fn span(mut self, from: &'static str, to: Option<&'static str>) -> Self {
        self.from = from;
        self.to = to;
        self
    }
}

async fn put(admin: &PgPool, org: Uuid, l: L) {
    sqlx::query(
        "INSERT INTO links (id, organisation_id, type, src_id, dst_id, valid_from, valid_to)
         VALUES ($1, $2, $3, $4, $5, $6::text::timestamptz, $7::text::timestamptz)",
    )
    .bind(u(l.id))
    .bind(org)
    .bind(l.kind)
    .bind(u(l.src))
    .bind(u(l.dst))
    .bind(l.from)
    .bind(l.to)
    .execute(admin)
    .await
    .unwrap();
}

async fn object(admin: &PgPool, org: Uuid, id: u128, unit: Option<Uuid>) {
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, attrs, valid_from)
         VALUES ($1, $2, $3, 'item', 1, '{}'::jsonb, '2026-01-01T00:00:00Z')",
    )
    .bind(u(id))
    .bind(org)
    .bind(unit)
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

fn q(object: u128, args: &str) -> String {
    format!(
        r#"{{ ontologyLinks(objectId: "{}"{args}) {{ edges {{ node {{ id type srcId dstId validTo }} cursor }} pageInfo {{ hasNextPage endCursor }} }} }}"#,
        s(object)
    )
}

fn ids(b: &Value) -> Vec<String> {
    b["data"]["ontologyLinks"]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{b}"))
        .iter()
        .map(|e| e["node"]["id"].as_str().unwrap().to_owned())
        .collect()
}

fn want(ns: &[u128]) -> Vec<String> {
    ns.iter().map(|n| s(*n)).collect()
}

#[sqlx::test(migrations = "../../migrations")]
async fn direction_and_type_filters(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    // a = 10, b = 11, c = 12, d = 13
    put(&admin, org, L::new(1, "contains", 10, 11)).await;
    put(&admin, org, L::new(2, "contains", 12, 10)).await;
    put(&admin, org, L::new(3, "supplies", 10, 13)).await;
    put(&admin, org, L::new(5, "refers", 10, 10)).await; // self-link

    let run = |args: &'static str| {
        let (app, tok) = (app.clone(), tok.clone());
        async move { gql(&app, &tok, &q(10, args)).await.1 }
    };

    assert_eq!(ids(&run(", direction: OUT").await), want(&[1, 3, 5]));
    assert_eq!(ids(&run(", direction: IN").await), want(&[2, 5]));
    assert_eq!(
        ids(&run(", direction: BOTH").await),
        want(&[1, 2, 3, 5]),
        "a self-link appears once"
    );
    assert_eq!(
        ids(&run("").await),
        want(&[1, 2, 3, 5]),
        "BOTH is the default"
    );
    assert_eq!(ids(&run(r#", type: "supplies""#).await), want(&[3]));

    let b = run(", direction: OUT").await;
    let first = &b["data"]["ontologyLinks"]["edges"][0]["node"];
    assert_eq!(first["type"], "contains");
    assert_eq!(first["srcId"], s(10));
    assert_eq!(first["dstId"], s(11));
    assert!(first["validTo"].is_null());
}

#[sqlx::test(migrations = "../../migrations")]
async fn links_page_by_id(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    for (id, other) in [(1, 11), (2, 12), (3, 13), (4, 14), (5, 15)] {
        put(&admin, org, L::new(id, "contains", 10, other)).await;
    }

    let (_, b1) = gql(&app, &tok, &q(10, ", first: 2")).await;
    assert_eq!(ids(&b1), want(&[1, 2]));
    assert_eq!(b1["data"]["ontologyLinks"]["pageInfo"]["hasNextPage"], true);
    let c1 = b1["data"]["ontologyLinks"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap();

    let (_, b2) = gql(&app, &tok, &q(10, &format!(r#", first: 2, after: "{c1}""#))).await;
    assert_eq!(ids(&b2), want(&[3, 4]));
    let c2 = b2["data"]["ontologyLinks"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap();

    let (_, b3) = gql(&app, &tok, &q(10, &format!(r#", first: 2, after: "{c2}""#))).await;
    assert_eq!(ids(&b3), want(&[5]));
    assert_eq!(
        b3["data"]["ontologyLinks"]["pageInfo"]["hasNextPage"],
        false
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn as_of_follows_link_versions(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        L::new(20, "contains", 30, 31).span("2026-01-01T00:00:00Z", Some("2026-06-01T00:00:00Z")),
    )
    .await;
    put(
        &admin,
        org,
        L::new(21, "contains", 30, 31).span("2026-06-01T00:00:00Z", None),
    )
    .await;

    let at = |t: &str| format!(r#", asOf: "{t}""#);
    let (_, b) = gql(&app, &tok, &q(30, "")).await;
    assert_eq!(ids(&b), want(&[21]));
    let (_, b) = gql(&app, &tok, &q(30, &at("2026-03-01T00:00:00Z"))).await;
    assert_eq!(ids(&b), want(&[20]));
    // valid_to is exclusive: at the boundary the newer version applies.
    let (_, b) = gql(&app, &tok, &q(30, &at("2026-06-01T00:00:00Z"))).await;
    assert_eq!(ids(&b), want(&[21]));
    let (_, b) = gql(&app, &tok, &q(30, &at("2025-12-31T23:59:59Z"))).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(ids(&b).is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn scoped_keys_only_see_links_between_visible_objects(admin: PgPool) {
    let (app, org, unscoped) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    object(&admin, org, 40, Some(east)).await;
    object(&admin, org, 41, Some(east)).await;
    object(&admin, org, 42, Some(west)).await;
    object(&admin, org, 43, None).await; // org-wide
    put(&admin, org, L::new(50, "contains", 40, 41)).await;
    put(&admin, org, L::new(51, "contains", 40, 42)).await; // far end hidden
    put(&admin, org, L::new(52, "contains", 42, 40)).await; // far end hidden
    put(&admin, org, L::new(53, "contains", 40, 43)).await; // org-wide end stays visible
    let scoped = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;

    let (_, b) = gql(&app, &unscoped, &q(40, "")).await;
    assert_eq!(ids(&b), want(&[50, 51, 52, 53]));

    let (_, b) = gql(&app, &scoped, &q(40, "")).await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(ids(&b), want(&[50, 53]));

    let (_, b) = gql(&app, &scoped, &q(42, "")).await;
    assert!(
        ids(&b).is_empty(),
        "an object outside the subtree has no visible links"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn isolation_validation_and_scope(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let other = common::seed_org(&admin, "globex").await;
    put(&admin, other, L::new(60, "contains", 70, 71)).await;
    put(&admin, org, L::new(61, "contains", 70, 72)).await;

    let (_, b) = gql(&app, &tok, &q(70, "")).await;
    assert_eq!(
        ids(&b),
        want(&[61]),
        "another organisation's links never appear"
    );

    let (_, b) = gql(
        &app,
        &tok,
        r#"{ ontologyLinks(objectId: "nope") { edges { cursor } } }"#,
    )
    .await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(&app, &tok, &q(70, r#", after: "garbage""#)).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(&app, &tok, &q(70, ", first: 101")).await;
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");

    let writer = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (_, b) = gql(&app, &writer, &q(70, "")).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "FORBIDDEN");
}
