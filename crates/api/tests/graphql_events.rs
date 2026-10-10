mod common;

use chrono::{DateTime, Utc};
use common::{gql, scoped_key, unit};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

fn u(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn s(n: u128) -> String {
    u(n).to_string()
}

struct E {
    id: u128,
    kind: &'static str,
    subject: u128,
    at: &'static str,
    recorded: &'static str,
    supersedes: Option<u128>,
}

impl E {
    fn new(id: u128, subject: u128, at: &'static str) -> Self {
        E {
            id,
            kind: "stock.counted",
            subject,
            at,
            recorded: "2026-09-15T00:00:00Z",
            supersedes: None,
        }
    }
    fn kind(mut self, k: &'static str) -> Self {
        self.kind = k;
        self
    }
    fn recorded(mut self, r: &'static str) -> Self {
        self.recorded = r;
        self
    }
    fn supersedes(mut self, id: u128) -> Self {
        self.supersedes = Some(id);
        self
    }
}

async fn put(admin: &PgPool, org: Uuid, e: E) {
    sqlx::query(
        "INSERT INTO events (id, organisation_id, type, type_version, subject_id, occurred_at, recorded_at,
                             payload, supersedes, source)
         VALUES ($1, $2, $3, 1, $4, $5::text::timestamptz, $6::text::timestamptz, $7, $8, 'test')",
    )
    .bind(u(e.id))
    .bind(org)
    .bind(e.kind)
    .bind(u(e.subject))
    .bind(e.at)
    .bind(e.recorded)
    .bind(json!({ "n": e.id as u64 }))
    .bind(e.supersedes.map(u))
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

fn q(args: &str) -> String {
    let a = if args.is_empty() {
        String::new()
    } else {
        format!("({args})")
    };
    format!(
        "{{ ontologyEvents{a} {{ edges {{ node {{ id type typeVersion subjectId occurredAt recordedAt payload supersedes source }} cursor }} pageInfo {{ hasNextPage endCursor }} }} }}"
    )
}

fn ids(b: &Value) -> Vec<String> {
    b["data"]["ontologyEvents"]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{b}"))
        .iter()
        .map(|e| e["node"]["id"].as_str().unwrap().to_owned())
        .collect()
}

fn want(ns: &[u128]) -> Vec<String> {
    ns.iter().map(|n| s(*n)).collect()
}

fn t(v: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(v.as_str().unwrap())
        .unwrap()
        .with_timezone(&Utc)
}

#[sqlx::test(migrations = "../../migrations")]
async fn events_come_newest_first_and_page_through_ties(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(&admin, org, E::new(1, 100, "2026-09-01T10:00:00Z")).await;
    put(&admin, org, E::new(2, 100, "2026-09-02T10:00:00Z")).await;
    put(&admin, org, E::new(3, 100, "2026-09-03T10:00:00Z")).await;
    put(&admin, org, E::new(4, 100, "2026-09-03T10:00:00Z")).await; // same instant as 3

    let args = |after: Option<&str>| {
        let a = after
            .map(|c| format!(r#", after: "{c}""#))
            .unwrap_or_default();
        format!(r#"subjectId: "{}", first: 2{a}"#, s(100))
    };

    let (_, b1) = gql(&app, &tok, &q(&args(None))).await;
    assert!(b1["errors"].is_null(), "{b1}");
    assert_eq!(ids(&b1), want(&[4, 3]), "ties break by id, descending");
    assert_eq!(
        b1["data"]["ontologyEvents"]["pageInfo"]["hasNextPage"],
        true
    );
    let c1 = b1["data"]["ontologyEvents"]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap();

    let (_, b2) = gql(&app, &tok, &q(&args(Some(c1)))).await;
    assert_eq!(ids(&b2), want(&[2, 1]));
    assert_eq!(
        b2["data"]["ontologyEvents"]["pageInfo"]["hasNextPage"],
        false
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn filters_by_subject_type_and_time_range(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        E::new(11, 101, "2026-09-01T00:00:00Z").kind("a"),
    )
    .await;
    put(
        &admin,
        org,
        E::new(12, 101, "2026-09-02T00:00:00Z").kind("b"),
    )
    .await;
    put(
        &admin,
        org,
        E::new(13, 101, "2026-09-03T00:00:00Z").kind("a"),
    )
    .await;
    put(
        &admin,
        org,
        E::new(14, 102, "2026-09-02T00:00:00Z").kind("a"),
    )
    .await;

    let (_, b) = gql(&app, &tok, &q("")).await;
    assert_eq!(ids(&b), want(&[13, 14, 12, 11]));

    let (_, b) = gql(
        &app,
        &tok,
        &q(&format!(r#"subjectId: "{}", type: "a""#, s(101))),
    )
    .await;
    assert_eq!(ids(&b), want(&[13, 11]));

    // from is inclusive, to is exclusive.
    let range = r#"occurredFrom: "2026-09-02T00:00:00Z", occurredTo: "2026-09-03T00:00:00Z""#;
    let (_, b) = gql(&app, &tok, &q(range)).await;
    assert_eq!(ids(&b), want(&[14, 12]));

    let (_, b) = gql(
        &app,
        &tok,
        &q(r#"occurredFrom: "2026-09-04T00:00:00Z", occurredTo: "2026-09-01T00:00:00Z""#),
    )
    .await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
}

#[sqlx::test(migrations = "../../migrations")]
async fn as_of_is_ledger_time_and_corrections_are_ordinary_events(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        E::new(21, 110, "2026-09-01T00:00:00Z").recorded("2026-09-01T12:00:00Z"),
    )
    .await;
    // Arrived late.
    put(
        &admin,
        org,
        E::new(22, 110, "2026-09-02T00:00:00Z").recorded("2026-09-20T00:00:00Z"),
    )
    .await;
    // A correction of 21, recorded later still.
    put(
        &admin,
        org,
        E::new(23, 110, "2026-09-01T00:00:00Z")
            .recorded("2026-09-25T00:00:00Z")
            .supersedes(21),
    )
    .await;

    let sub = format!(r#"subjectId: "{}""#, s(110));
    let (_, b) = gql(
        &app,
        &tok,
        &q(&format!(r#"{sub}, asOf: "2026-09-10T00:00:00Z""#)),
    )
    .await;
    assert_eq!(ids(&b), want(&[21]));
    let (_, b) = gql(
        &app,
        &tok,
        &q(&format!(r#"{sub}, asOf: "2026-09-21T00:00:00Z""#)),
    )
    .await;
    assert_eq!(ids(&b), want(&[22, 21]));

    let (_, b) = gql(&app, &tok, &q(&sub)).await;
    assert_eq!(
        ids(&b),
        want(&[22, 23, 21]),
        "nothing is hidden: the correction is just another event"
    );
    let correction = &b["data"]["ontologyEvents"]["edges"][1]["node"];
    assert_eq!(correction["supersedes"], s(21));
}

#[sqlx::test(migrations = "../../migrations")]
async fn scoped_keys_only_see_events_of_visible_subjects(admin: PgPool) {
    let (app, org, unscoped) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    object(&admin, org, 120, Some(east)).await;
    object(&admin, org, 121, Some(west)).await;
    object(&admin, org, 122, None).await; // org-wide
    put(&admin, org, E::new(31, 120, "2026-09-01T00:00:00Z")).await;
    put(&admin, org, E::new(32, 121, "2026-09-02T00:00:00Z")).await;
    put(&admin, org, E::new(33, 122, "2026-09-03T00:00:00Z")).await;
    let scoped = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;

    let (_, b) = gql(&app, &unscoped, &q("")).await;
    assert_eq!(ids(&b), want(&[33, 32, 31]));

    let (_, b) = gql(&app, &scoped, &q("")).await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(ids(&b), want(&[33, 31]));

    let (_, b) = gql(&app, &scoped, &q(&format!(r#"subjectId: "{}""#, s(121)))).await;
    assert!(
        ids(&b).is_empty(),
        "a subject outside the subtree has no visible events"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn shape_isolation_and_guard(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let other = common::seed_org(&admin, "globex").await;
    put(&admin, other, E::new(40, 130, "2026-09-01T00:00:00Z")).await;
    put(&admin, org, E::new(41, 130, "2026-09-02T00:00:00Z")).await;

    let (_, b) = gql(&app, &tok, &q(&format!(r#"subjectId: "{}""#, s(130)))).await;
    assert_eq!(
        ids(&b),
        want(&[41]),
        "another organisation's events never appear"
    );
    let n = &b["data"]["ontologyEvents"]["edges"][0]["node"];
    assert_eq!(n["type"], "stock.counted");
    assert_eq!(n["typeVersion"], 1);
    assert_eq!(n["subjectId"], s(130));
    assert_eq!(n["source"], "test");
    assert_eq!(n["payload"]["n"], 41);
    assert!(n["supersedes"].is_null());
    assert_eq!(t(&n["occurredAt"]), t(&json!("2026-09-02T00:00:00Z")));
    assert_eq!(t(&n["recordedAt"]), t(&json!("2026-09-15T00:00:00Z")));

    let (_, b) = gql(&app, &tok, &q(r#"subjectId: "nope""#)).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(&app, &tok, &q(r#"after: "garbage""#)).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(&app, &tok, &q("first: 101")).await;
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");

    let writer = common::seed_key(&admin, org, &["ingest:write"]).await;
    let (_, b) = gql(&app, &writer, &q("")).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "FORBIDDEN");
}
