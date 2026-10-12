mod common;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use common::{gql, scoped_key, unit};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
struct Rec {
    n: u128,
    unit: Option<Uuid>,
    kind: &'static str,
    status: &'static str,
    urgency: f64,
    confidence: f64,
    created: String,
    updated: String,
    resolved_at: Option<String>,
    resolved_by: Option<&'static str>,
    resolution: Option<Value>,
    history: Value,
}

impl Rec {
    fn new(n: u128) -> Self {
        Rec {
            n,
            unit: None,
            kind: "work.escalate",
            status: "pending",
            urgency: 0.5,
            confidence: 0.9,
            created: "2026-10-01T10:00:00Z".into(),
            updated: "2026-10-01T10:00:00Z".into(),
            resolved_at: None,
            resolved_by: None,
            resolution: None,
            history: json!([]),
        }
    }
    fn unit(mut self, u: Uuid) -> Self {
        self.unit = Some(u);
        self
    }
    fn kind(mut self, k: &'static str) -> Self {
        self.kind = k;
        self
    }
    fn status(mut self, s: &'static str) -> Self {
        self.status = s;
        self
    }
    fn urg(mut self, u: f64) -> Self {
        self.urgency = u;
        self
    }
    fn conf(mut self, c: f64) -> Self {
        self.confidence = c;
        self
    }
    fn created(mut self, t: &str) -> Self {
        self.created = t.into();
        self
    }
    fn updated(mut self, t: &str) -> Self {
        self.updated = t.into();
        self
    }
    fn resolved(mut self, at: &str, by: &'static str) -> Self {
        self.resolved_at = Some(at.into());
        self.resolved_by = Some(by);
        self
    }
    fn resolution(mut self, v: Value) -> Self {
        self.resolution = Some(v);
        self
    }
    fn history(mut self, v: Value) -> Self {
        self.history = v;
        self
    }
}

fn body(r: &Rec) -> Value {
    json!({
        "decision_type": r.kind,
        "dedup_key": format!("k{}", r.n),
        "org_unit_id": r.unit,
        "action": {
            "proposed": true,
            "payload": { "object_id": "o-1" },
            "payload_schema_ref": "kuki.core/work.escalate.json"
        },
        "rationale": {
            "summary": "Case C-1 is overdue",
            "factors": [
                { "label": "hours overdue", "value": 5, "unit": "h",
                  "evidence": [{ "kind": "object", "id": "o-1", "as_of": "2026-10-01T00:00:00Z", "note": "due date" }] },
                { "label": "owner", "value": "unassigned", "evidence": [] }
            ],
            "confidence": r.confidence,
            "confidence_kind": "data_quality",
            "counterfactual": "if not acted on: SLA breach"
        },
        "urgency": r.urgency,
        "impact": { "value": "39.000", "unit": "KES" },
        "evidence": []
    })
}

fn provenance() -> Value {
    json!({
        "generator_id": "deadline.at_risk.t0",
        "generator_version": "1.0.0",
        "tier": "T0",
        "pack": "kuki.core",
        "ontology_version": 1,
        "config_hash": "abc",
        "as_of": "2026-10-01T09:59:00Z",
        "input_digest": "sha256:def"
    })
}

async fn put(admin: &PgPool, org: Uuid, r: Rec) {
    let b = body(&r);
    sqlx::query(
        "INSERT INTO recommendations
           (id, organisation_id, org_unit_id, decision_type, dedup_key, status, urgency, confidence,
            rationale_summary, body, provenance, revision_history, resolution,
            created_at, last_confirmed_at, updated_at, resolved_at, resolved_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7::float8, $8::float8, 'Case C-1 is overdue', $9, $10, $11, $12,
                 $13::text::timestamptz, $13::text::timestamptz, $14::text::timestamptz,
                 $15::text::timestamptz, $16)",
    )
    .bind(Uuid::from_u128(r.n))
    .bind(org)
    .bind(r.unit)
    .bind(r.kind)
    .bind(format!("k{}", r.n))
    .bind(r.status)
    .bind(r.urgency)
    .bind(r.confidence)
    .bind(b)
    .bind(provenance())
    .bind(r.history)
    .bind(r.resolution)
    .bind(r.created)
    .bind(r.updated)
    .bind(r.resolved_at)
    .bind(r.resolved_by)
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

fn u(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn at(v: &Value) -> DateTime<Utc> {
    t(v.as_str().unwrap_or_else(|| panic!("not a string: {v}")))
}

fn ago(days: i64) -> String {
    (Utc::now() - Duration::days(days)).to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn ids(b: &Value, field: &str) -> Vec<String> {
    b["data"][field]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("{b}"))
        .iter()
        .map(|e| e["node"]["id"].as_str().unwrap().to_owned())
        .collect()
}

fn list(first: Option<u32>, after: Option<&str>, args: &str) -> String {
    let f = first.map(|n| format!("first: {n}")).unwrap_or_default();
    let a = after
        .map(|c| format!(r#", after: "{c}""#))
        .unwrap_or_default();
    let inner = format!("{f}{a}{args}");
    let call = if inner
        .trim_matches(|c: char| c == ',' || c.is_whitespace())
        .is_empty()
    {
        String::new()
    } else {
        format!("({inner})")
    };
    format!(
        r#"{{ recommendations{call} {{ edges {{ node {{ id }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
    )
}

fn end(b: &Value, field: &str) -> String {
    b["data"][field]["pageInfo"]["endCursor"]
        .as_str()
        .unwrap()
        .to_owned()
}

const FULL: &str = r#"
    id organisationId orgUnitId decisionType dedupKey status urgency version
    action { proposed actionType payloadSchemaRef payload { __typename ... on GenericPayload { payload } } }
    rationale { summary confidence confidenceKind counterfactual narrative
                factors { label value unit weight direction evidence { kind id asOf note } } }
    impact { value unit }
    provenance { generatorId generatorVersion tier pack ontologyVersion configHash asOf inputDigest modelArtefact }
    resolvedBy resolution { reasonCode note payloadOverridden }
    createdAt lastConfirmedAt resolvedAt expiresAt updatedAt
    revisions { revisedAt snapshot }
    executions { id }
"#;

#[sqlx::test(migrations = "../../migrations")]
async fn a_recommendation_reads_back_in_full(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    put(
        &admin,
        org,
        Rec::new(1)
            .unit(east)
            .urg(0.85)
            .conf(0.8)
            .created("2026-10-01T10:00:00Z")
            .updated("2026-10-01T10:05:00Z"),
    )
    .await;

    let (_, b) = gql(
        &app,
        &tok,
        &format!(r#"{{ recommendation(id: "{}") {{ {FULL} }} }}"#, u(1)),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    let r = &b["data"]["recommendation"];

    assert_eq!(r["id"], u(1));
    assert_eq!(r["organisationId"], org.to_string());
    assert_eq!(r["orgUnitId"], east.to_string());
    assert_eq!(r["decisionType"], "work.escalate");
    assert_eq!(r["dedupKey"], "k1");
    assert_eq!(r["status"], "PENDING");
    assert!((r["urgency"].as_f64().unwrap() - 0.85).abs() < 1e-9);
    assert_eq!(r["version"], 1);

    assert_eq!(r["action"]["proposed"], true);
    assert!(r["action"]["actionType"].is_null());
    assert_eq!(
        r["action"]["payloadSchemaRef"],
        "kuki.core/work.escalate.json"
    );
    assert_eq!(r["action"]["payload"]["__typename"], "GenericPayload");
    assert_eq!(r["action"]["payload"]["payload"]["object_id"], "o-1");

    let ra = &r["rationale"];
    assert_eq!(ra["summary"], "Case C-1 is overdue");
    assert!((ra["confidence"].as_f64().unwrap() - 0.8).abs() < 1e-9);
    assert_eq!(ra["confidenceKind"], "DATA_QUALITY");
    assert_eq!(ra["counterfactual"], "if not acted on: SLA breach");
    assert!(ra["narrative"].is_null());
    assert_eq!(ra["factors"][0]["label"], "hours overdue");
    assert_eq!(ra["factors"][0]["value"], 5);
    assert_eq!(ra["factors"][0]["unit"], "h");
    assert!(ra["factors"][0]["weight"].is_null() && ra["factors"][0]["direction"].is_null());
    assert_eq!(ra["factors"][0]["evidence"][0]["kind"], "OBJECT");
    assert_eq!(ra["factors"][0]["evidence"][0]["id"], "o-1");
    assert_eq!(ra["factors"][0]["evidence"][0]["note"], "due date");
    assert_eq!(
        at(&ra["factors"][0]["evidence"][0]["asOf"]),
        t("2026-10-01T00:00:00Z")
    );
    assert_eq!(ra["factors"][1]["value"], "unassigned");

    assert_eq!(r["impact"]["value"], "39.000");
    assert_eq!(r["impact"]["unit"], "KES");

    let p = &r["provenance"];
    assert_eq!(p["generatorId"], "deadline.at_risk.t0");
    assert_eq!(p["tier"], "T0");
    assert_eq!(p["pack"], "kuki.core");
    assert_eq!(p["ontologyVersion"], 1);
    assert_eq!(at(&p["asOf"]), t("2026-10-01T09:59:00Z"));
    assert!(p["modelArtefact"].is_null());

    assert!(r["resolution"].is_null() && r["resolvedBy"].is_null() && r["resolvedAt"].is_null());
    assert!(r["expiresAt"].is_null());
    assert_eq!(at(&r["createdAt"]), t("2026-10-01T10:00:00Z"));
    assert_eq!(at(&r["updatedAt"]), t("2026-10-01T10:05:00Z"));
    assert_eq!(r["revisions"].as_array().unwrap().len(), 0);
    assert_eq!(r["executions"].as_array().unwrap().len(), 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn unknown_foreign_and_malformed_ids(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let other = common::seed_org(&admin, "globex").await;
    put(&admin, other, Rec::new(60)).await;
    put(&admin, org, Rec::new(61)).await;

    let one = |n: u128| format!(r#"{{ recommendation(id: "{}") {{ id }} }}"#, u(n));
    let (_, b) = gql(&app, &tok, &one(999)).await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(b["data"]["recommendation"].is_null());

    // Another organisation's row is indistinguishable from a missing one.
    let (_, b) = gql(&app, &tok, &one(60)).await;
    assert!(b["data"]["recommendation"].is_null());
    let (_, b) = gql(&app, &tok, &list(None, None, "")).await;
    assert_eq!(ids(&b, "recommendations"), [u(61)]);

    let (_, b) = gql(&app, &tok, r#"{ recommendation(id: "nope") { id } }"#).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");

    let (_, b) = gql(&app, &tok, &one(61)).await;
    assert_eq!(b["data"]["recommendation"]["id"], u(61));
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_inbox_is_urgency_then_newest_and_pages_through_ties(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        Rec::new(1).urg(0.9).created("2026-10-01T10:00:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(2).urg(0.9).created("2026-10-01T10:05:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(3).urg(0.5).created("2026-10-01T10:10:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(4).urg(0.5).created("2026-10-01T10:10:00Z"),
    )
    .await; // same urgency and time as 3
    put(
        &admin,
        org,
        Rec::new(5).urg(0.1).created("2026-10-01T09:00:00Z"),
    )
    .await;

    let (_, b) = gql(&app, &tok, &list(None, None, "")).await;
    assert_eq!(
        ids(&b, "recommendations"),
        [u(2), u(1), u(4), u(3), u(5)],
        "ties break on id, newest first"
    );

    let (_, b1) = gql(&app, &tok, &list(Some(2), None, "")).await;
    assert_eq!(ids(&b1, "recommendations"), [u(2), u(1)]);
    assert_eq!(
        b1["data"]["recommendations"]["pageInfo"]["hasNextPage"],
        true
    );
    let (_, b2) = gql(
        &app,
        &tok,
        &list(Some(2), Some(&end(&b1, "recommendations")), ""),
    )
    .await;
    assert_eq!(ids(&b2, "recommendations"), [u(4), u(3)]);
    let (_, b3) = gql(
        &app,
        &tok,
        &list(Some(2), Some(&end(&b2, "recommendations")), ""),
    )
    .await;
    assert_eq!(ids(&b3, "recommendations"), [u(5)]);
    assert_eq!(
        b3["data"]["recommendations"]["pageInfo"]["hasNextPage"],
        false
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn created_desc_orders_by_age_and_cursors_belong_to_one_order(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        Rec::new(1).urg(0.9).created("2026-10-01T10:00:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(2).urg(0.8).created("2026-10-01T10:05:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(3).urg(0.7).created("2026-10-01T10:10:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(4).urg(0.6).created("2026-10-01T10:10:00Z"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(5).urg(0.5).created("2026-10-01T09:00:00Z"),
    )
    .await;

    let by = ", orderBy: CREATED_DESC";
    let (_, b) = gql(&app, &tok, &list(None, None, by)).await;
    assert_eq!(ids(&b, "recommendations"), [u(4), u(3), u(2), u(1), u(5)]);

    let (_, b1) = gql(&app, &tok, &list(Some(2), None, by)).await;
    assert_eq!(ids(&b1, "recommendations"), [u(4), u(3)]);
    let created_cursor = end(&b1, "recommendations");
    let (_, b2) = gql(&app, &tok, &list(Some(2), Some(&created_cursor), by)).await;
    assert_eq!(ids(&b2, "recommendations"), [u(2), u(1)]);

    let (_, inbox) = gql(&app, &tok, &list(Some(2), None, "")).await;
    let inbox_cursor = end(&inbox, "recommendations");
    let (_, b) = gql(&app, &tok, &list(Some(2), Some(&inbox_cursor), by)).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    let (_, b) = gql(&app, &tok, &list(Some(2), Some(&created_cursor), "")).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
}

#[sqlx::test(migrations = "../../migrations")]
async fn filters_apply_together(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    let now = ago(1);
    put(&admin, org, Rec::new(1).unit(east).urg(0.8)).await;
    put(&admin, org, Rec::new(2).unit(west).urg(0.4).conf(0.6)).await;
    put(
        &admin,
        org,
        Rec::new(3).unit(east).kind("case.review").urg(0.6),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(4)
            .unit(east)
            .status("accepted")
            .urg(0.7)
            .resolved(&now, "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(5)
            .unit(east)
            .status("rejected")
            .urg(0.3)
            .resolved(&now, "user:amina"),
    )
    .await;

    let q = |args: &str| list(None, None, args);
    let run = |args: String| {
        let (app, tok) = (app.clone(), tok.clone());
        async move { ids(&gql(&app, &tok, &q(&args)).await.1, "recommendations") }
    };

    assert_eq!(
        run(String::new()).await,
        [u(1), u(3), u(2)],
        "pending only by default"
    );
    assert_eq!(
        run(r#"filter: { statuses: [ACCEPTED, REJECTED] }"#.into()).await,
        [u(4), u(5)]
    );
    assert_eq!(
        run(r#"filter: { statuses: [PENDING, ACCEPTED] }"#.into()).await,
        [u(1), u(4), u(3), u(2)]
    );
    assert_eq!(
        run(r#"filter: { decisionTypes: ["case.review"] }"#.into()).await,
        [u(3)]
    );
    assert_eq!(
        run(r#"filter: { orgUnitPath: "hq.east" }"#.into()).await,
        [u(1), u(3)]
    );
    assert_eq!(
        run(r#"filter: { minUrgency: 0.5 }"#.into()).await,
        [u(1), u(3)]
    );
    assert_eq!(
        run(r#"filter: { minConfidence: 0.7 }"#.into()).await,
        [u(1), u(3)]
    );
    assert_eq!(
        run(r#"filter: { decisionTypes: ["work.escalate"], orgUnitPath: "hq" }"#.into()).await,
        [u(1), u(2)]
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn low_confidence_is_hidden_below_the_display_floor(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(&admin, org, Rec::new(1).urg(0.9).conf(0.2)).await;
    put(&admin, org, Rec::new(2).urg(0.8).conf(0.3)).await; // exactly on the default floor
    put(&admin, org, Rec::new(3).urg(0.7).conf(0.9)).await;

    let summary = r#"{ recommendationSummary { pendingTotal } }"#;
    let changes = r#"{ recommendationChanges(updatedAfter: "2026-01-01T00:00:00Z") { edges { node { id } } } }"#;

    let (_, b) = gql(&app, &tok, &list(None, None, "")).await;
    assert_eq!(ids(&b, "recommendations"), [u(2), u(3)]);
    let (_, b) = gql(&app, &tok, summary).await;
    assert_eq!(
        b["data"]["recommendationSummary"]["pendingTotal"], 2,
        "the count matches the inbox"
    );
    let (_, b) = gql(&app, &tok, changes).await;
    assert_eq!(ids(&b, "recommendationChanges"), [u(2), u(3)]);

    sqlx::query(
        "UPDATE organisations SET config = jsonb_build_object('decision',
            jsonb_build_object('min_confidence_for_display', 0.5)) WHERE id = $1",
    )
    .bind(org)
    .execute(&admin)
    .await
    .unwrap();

    let (_, b) = gql(&app, &tok, &list(None, None, "")).await;
    assert_eq!(ids(&b, "recommendations"), [u(3)]);
    let (_, b) = gql(&app, &tok, summary).await;
    assert_eq!(b["data"]["recommendationSummary"]["pendingTotal"], 1);

    // A direct lookup is not an inbox: the floor does not apply.
    let (_, b) = gql(
        &app,
        &tok,
        &format!(r#"{{ recommendation(id: "{}") {{ id }} }}"#, u(1)),
    )
    .await;
    assert_eq!(b["data"]["recommendation"]["id"], u(1));
}

#[sqlx::test(migrations = "../../migrations")]
async fn subtree_scoped_keys_only_see_their_subtree(admin: PgPool) {
    let (app, org, all) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    let day = ago(1);
    put(&admin, org, Rec::new(1).unit(east).urg(0.8)).await;
    put(&admin, org, Rec::new(2).unit(west).urg(0.9)).await;
    put(
        &admin,
        org,
        Rec::new(3)
            .unit(east)
            .status("accepted")
            .resolved(&day, "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(4)
            .unit(west)
            .status("rejected")
            .resolved(&day, "user:amina"),
    )
    .await;
    let tok = scoped_key(&admin, org, &["decisions:read"], "hq.east").await;

    let (_, b) = gql(&app, &tok, &list(None, None, "")).await;
    assert_eq!(ids(&b, "recommendations"), [u(1)]);
    let (_, b) = gql(
        &app,
        &tok,
        &list(None, None, ", filter: { statuses: [ACCEPTED, REJECTED] }"),
    )
    .await;
    assert_eq!(ids(&b, "recommendations"), [u(3)]);

    let (_, b) = gql(
        &app,
        &tok,
        &format!(r#"{{ recommendation(id: "{}") {{ id }} }}"#, u(2)),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(
        b["data"]["recommendation"].is_null(),
        "outside the subtree reads as missing"
    );

    let (_, b) = gql(
        &app,
        &tok,
        r#"{ recommendationSummary { pendingTotal acceptedLast30d rejectedLast30d } }"#,
    )
    .await;
    let s = &b["data"]["recommendationSummary"];
    assert_eq!(
        (
            s["pendingTotal"].as_i64(),
            s["acceptedLast30d"].as_i64(),
            s["rejectedLast30d"].as_i64()
        ),
        (Some(1), Some(1), Some(0))
    );

    let changes = r#"{ recommendationChanges(updatedAfter: "2026-01-01T00:00:00Z") { edges { node { id } } } }"#;
    let (_, b) = gql(&app, &tok, changes).await;
    assert_eq!(ids(&b, "recommendationChanges"), [u(1), u(3)]);

    // Asking for a subtree outside the scope intersects to empty: not an error, not a leak.
    let (_, b) = gql(
        &app,
        &tok,
        &list(None, None, r#", filter: { orgUnitPath: "hq.west" }"#),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert!(ids(&b, "recommendations").is_empty());

    let (_, b) = gql(&app, &all, &list(None, None, "")).await;
    assert_eq!(ids(&b, "recommendations"), [u(2), u(1)]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_summary_counts_the_visible_population(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    let east = unit(&admin, org, "hq.east").await;
    let west = unit(&admin, org, "hq.west").await;
    put(&admin, org, Rec::new(1).unit(east)).await;
    put(&admin, org, Rec::new(2).unit(west)).await;
    put(&admin, org, Rec::new(3).unit(east).kind("case.review")).await;
    // Accepted counts the execution states too, within the last 30 days.
    put(
        &admin,
        org,
        Rec::new(4)
            .status("accepted")
            .resolved(&ago(5), "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(5)
            .status("executed")
            .resolved(&ago(10), "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(6).status("failed").resolved(&ago(1), "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(7)
            .status("accepted")
            .resolved(&ago(40), "user:amina"),
    )
    .await; // too old
    put(
        &admin,
        org,
        Rec::new(8)
            .status("expired")
            .resolved(&ago(2), "system:evaluator"),
    )
    .await; // not counted
    put(
        &admin,
        org,
        Rec::new(9)
            .status("rejected")
            .resolved(&ago(2), "user:amina"),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(10)
            .status("rejected")
            .resolved(&ago(31), "user:amina"),
    )
    .await; // too old

    let q = r#"{ recommendationSummary { pendingTotal pendingByDecisionType { decisionType count }
                  acceptedLast30d rejectedLast30d asOf } }"#;
    let (_, b) = gql(&app, &tok, q).await;
    assert!(b["errors"].is_null(), "{b}");
    let s = &b["data"]["recommendationSummary"];
    assert_eq!(s["pendingTotal"], 3);
    assert_eq!(
        s["pendingByDecisionType"],
        json!([{ "decisionType": "case.review", "count": 1 }, { "decisionType": "work.escalate", "count": 2 }])
    );
    assert_eq!(s["acceptedLast30d"], 3);
    assert_eq!(s["rejectedLast30d"], 1);
    assert!((Utc::now() - at(&s["asOf"])).num_seconds().abs() < 30);

    // A unit path excludes org-wide rows, which have no unit to match.
    let q = r#"{ recommendationSummary(orgUnitPath: "hq.east") { pendingTotal pendingByDecisionType { decisionType count }
                  acceptedLast30d rejectedLast30d } }"#;
    let (_, b) = gql(&app, &tok, q).await;
    let s = &b["data"]["recommendationSummary"];
    assert_eq!(s["pendingTotal"], 2);
    assert_eq!(
        s["pendingByDecisionType"],
        json!([{ "decisionType": "case.review", "count": 1 }, { "decisionType": "work.escalate", "count": 1 }])
    );
    assert_eq!(
        (s["acceptedLast30d"].as_i64(), s["rejectedLast30d"].as_i64()),
        (Some(0), Some(0))
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn changes_come_oldest_first_and_page_through_ties(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(&admin, org, Rec::new(1).updated("2026-10-02T10:01:00Z")).await;
    put(&admin, org, Rec::new(2).updated("2026-10-02T10:02:00Z")).await;
    put(&admin, org, Rec::new(3).updated("2026-10-02T10:02:00Z")).await;
    put(
        &admin,
        org,
        Rec::new(4)
            .status("accepted")
            .updated("2026-10-02T10:03:00Z"),
    )
    .await;
    put(&admin, org, Rec::new(5).updated("2026-10-02T09:00:00Z")).await; // before the window

    let q = |after: &str, first: u32, cursor: Option<&str>| {
        let c = cursor
            .map(|c| format!(r#", after: "{c}""#))
            .unwrap_or_default();
        format!(
            r#"{{ recommendationChanges(updatedAfter: "{after}", first: {first}{c}) {{
                  edges {{ node {{ id version updatedAt }} }} pageInfo {{ hasNextPage endCursor }} }} }}"#
        )
    };

    let (_, b1) = gql(&app, &tok, &q("2026-10-02T10:00:00Z", 2, None)).await;
    assert!(b1["errors"].is_null(), "{b1}");
    assert_eq!(ids(&b1, "recommendationChanges"), [u(1), u(2)]);
    assert_eq!(
        b1["data"]["recommendationChanges"]["pageInfo"]["hasNextPage"],
        true
    );
    assert_eq!(
        b1["data"]["recommendationChanges"]["edges"][0]["node"]["version"],
        1
    );

    let (_, b2) = gql(
        &app,
        &tok,
        &q(
            "2026-10-02T10:00:00Z",
            2,
            Some(&end(&b1, "recommendationChanges")),
        ),
    )
    .await;
    assert_eq!(
        ids(&b2, "recommendationChanges"),
        [u(3), u(4)],
        "ties on updatedAt continue by id"
    );
    assert_eq!(
        b2["data"]["recommendationChanges"]["pageInfo"]["hasNextPage"],
        false
    );

    // updatedAfter is exclusive.
    let (_, b) = gql(&app, &tok, &q("2026-10-02T10:02:00Z", 25, None)).await;
    assert_eq!(ids(&b, "recommendationChanges"), [u(4)]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn validation_and_guards(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(&admin, org, Rec::new(1)).await;

    let (s, b) = gql(&app, &tok, &list(Some(101), None, "")).await;
    assert_eq!(s, axum::http::StatusCode::OK);
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");
    let (_, b) = gql(
        &app,
        &tok,
        r#"{ recommendationChanges(updatedAfter: "2026-01-01T00:00:00Z", first: 101) { edges { cursor } } }"#,
    )
    .await;
    assert_eq!(b["errors"][0]["extensions"]["reason"], "PAGE_SIZE_EXCEEDED");

    for q in [
        list(Some(0), None, ""),
        list(None, Some("garbage"), ""),
        list(None, None, ", filter: { statuses: [] }"),
        list(None, None, ", filter: { decisionTypes: [] }"),
        list(None, None, r#", filter: { decisionTypes: [""] }"#),
        list(None, None, ", filter: { minUrgency: 1.5 }"),
        list(None, None, ", filter: { minConfidence: -0.1 }"),
        list(None, None, r#", filter: { orgUnitPath: "a b" }"#),
        r#"{ recommendationSummary(orgUnitPath: "a b") { pendingTotal } }"#.to_owned(),
        r#"{ recommendationChanges(updatedAfter: "2026-01-01T00:00:00Z", after: "garbage") { edges { cursor } } }"#
            .to_owned(),
    ] {
        let (s, b) = gql(&app, &tok, &q).await;
        assert_eq!(s, axum::http::StatusCode::OK, "{q}");
        assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED", "{q}: {b}");
    }

    let weak = common::seed_key(&admin, org, &["ingest:write"]).await;
    for q in [
        format!(r#"{{ recommendation(id: "{}") {{ id }} }}"#, u(1)),
        list(None, None, ""),
        "{ recommendationSummary { pendingTotal } }".to_owned(),
        r#"{ recommendationChanges(updatedAfter: "2026-01-01T00:00:00Z") { edges { cursor } } }"#
            .to_owned(),
    ] {
        let (_, b) = gql(&app, &weak, &q).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "FORBIDDEN",
            "{q}: {b}"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn revisions_resolutions_and_executions_are_exposed(admin: PgPool) {
    let (app, org, tok) = setup(&admin).await;
    put(
        &admin,
        org,
        Rec::new(1)
            .status("expired")
            .resolved("2026-10-03T00:00:00Z", "system:evaluator")
            .resolution(json!({ "reason": "condition_cleared", "generator": "deadline.at_risk.t0" }))
            .history(json!([{
                "version": 1, "replacedAt": "2026-10-01T09:00:00Z", "urgency": 0.4, "confidence": 0.8,
                "rationaleSummary": "old summary", "body": {}
            }])),
    )
    .await;
    put(
        &admin,
        org,
        Rec::new(2)
            .status("accepted")
            .resolved("2026-10-03T00:00:00Z", "user:amina")
            .resolution(json!({ "reason_code": "ALREADY_HANDLED", "note": "done elsewhere", "payload_overridden": true })),
    )
    .await;
    sqlx::query(
        "INSERT INTO action_executions
           (id, organisation_id, recommendation_id, action_type, input, idempotency_key, status, adapter,
            result, attempts, created_by, created_at, finished_at)
         VALUES ($1, $2, $3, 'work.escalate', '{}'::jsonb, 'idem-1', 'succeeded', 'builtin:draft',
                 $4, 1, 'user:amina', '2026-10-03T00:01:00Z', '2026-10-03T00:01:05Z')",
    )
    .bind(Uuid::from_u128(900))
    .bind(org)
    .bind(Uuid::from_u128(2))
    .bind(json!({ "ok": true }))
    .execute(&admin)
    .await
    .unwrap();

    let q = |n: u128| {
        format!(
            r#"{{ recommendation(id: "{}") {{ status resolvedBy resolvedAt
                 resolution {{ reasonCode note payloadOverridden }}
                 revisions {{ revisedAt snapshot }}
                 executions {{ id actionType status adapter attempts createdBy createdAt finishedAt result }} }} }}"#,
            u(n)
        )
    };

    let (_, b) = gql(&app, &tok, &q(1)).await;
    assert!(b["errors"].is_null(), "{b}");
    let r = &b["data"]["recommendation"];
    assert_eq!(r["status"], "EXPIRED");
    assert_eq!(r["resolvedBy"], "system:evaluator");
    assert_eq!(r["resolution"]["reasonCode"], "condition_cleared");
    assert!(r["resolution"]["note"].is_null());
    assert_eq!(r["resolution"]["payloadOverridden"], false);
    assert_eq!(
        at(&r["revisions"][0]["revisedAt"]),
        t("2026-10-01T09:00:00Z")
    );
    assert_eq!(
        r["revisions"][0]["snapshot"]["rationaleSummary"],
        "old summary"
    );
    assert_eq!(r["executions"].as_array().unwrap().len(), 0);

    let (_, b) = gql(&app, &tok, &q(2)).await;
    let r = &b["data"]["recommendation"];
    assert_eq!(r["status"], "ACCEPTED");
    assert_eq!(r["resolution"]["reasonCode"], "ALREADY_HANDLED");
    assert_eq!(r["resolution"]["note"], "done elsewhere");
    assert_eq!(r["resolution"]["payloadOverridden"], true);
    let e = &r["executions"][0];
    assert_eq!(e["id"], u(900));
    assert_eq!(e["actionType"], "work.escalate");
    assert_eq!(e["status"], "SUCCEEDED");
    assert_eq!(e["adapter"], "builtin:draft");
    assert_eq!(e["attempts"], 1);
    assert_eq!(e["createdBy"], "user:amina");
    assert_eq!(at(&e["finishedAt"]), t("2026-10-03T00:01:05Z"));
    assert_eq!(e["result"]["ok"], true);
}
