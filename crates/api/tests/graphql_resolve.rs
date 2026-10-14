mod common;

use common::{gql, scoped_key, unit};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const BOTH: [&str; 2] = ["decisions:read", "decisions:resolve"];

fn u(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

fn body(n: u128) -> Value {
    json!({
        "decision_type": "work.escalate",
        "dedup_key": format!("k{n}"),
        "org_unit_id": null,
        "action": { "proposed": true, "payload": { "object_id": "o-1" },
                    "payload_schema_ref": "kuki.core/work.escalate.json" },
        "rationale": { "summary": "Case overdue",
                       "factors": [{ "label": "hours overdue", "value": 5, "evidence": [] }],
                       "confidence": 0.9, "confidence_kind": "data_quality" },
        "urgency": 0.5,
        "impact": null,
        "evidence": []
    })
}

fn provenance() -> Value {
    json!({
        "generator_id": "deadline.at_risk.t0", "generator_version": "1.0.0", "tier": "T0",
        "pack": "kuki.core", "ontology_version": 1, "config_hash": "abc",
        "as_of": "2026-10-01T09:59:00Z", "input_digest": "sha256:def"
    })
}

async fn put(
    admin: &PgPool,
    org: Uuid,
    n: u128,
    unit: Option<Uuid>,
    status: &str,
    resolved_by: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO recommendations
           (id, organisation_id, org_unit_id, decision_type, dedup_key, status, urgency, confidence,
            rationale_summary, body, provenance, resolved_by)
         VALUES ($1, $2, $3, 'work.escalate', $4, $5, 0.5, 0.9, 'Case overdue', $6, $7, $8)",
    )
    .bind(Uuid::from_u128(n))
    .bind(org)
    .bind(unit)
    .bind(format!("k{n}"))
    .bind(status)
    .bind(body(n))
    .bind(provenance())
    .bind(resolved_by)
    .execute(admin)
    .await
    .unwrap();
}

async fn register_type(admin: &PgPool, org: Uuid) {
    sqlx::query(
        "INSERT INTO decision_types (organisation_id, name, pack, payload_schema_ref, payload_schema)
         VALUES ($1, 'work.escalate', 'kuki.core', 'kuki.core/work.escalate.json', $2)",
    )
    .bind(org)
    .bind(json!({
        "type": "object",
        "required": ["object_id"],
        "properties": { "object_id": { "type": "string" } },
        "additionalProperties": false
    }))
    .execute(admin)
    .await
    .unwrap();
}

struct Env {
    app: axum::Router,
    pool: PgPool,
    org: Uuid,
    tok: String,
}

async fn setup(admin: &PgPool) -> Env {
    let pool = common::app_pool(admin).await;
    let app = common::app(pool.clone());
    let org = common::seed_org(admin, "acme").await;
    register_type(admin, org).await;
    let tok = common::seed_key(admin, org, &BOTH).await;
    Env {
        app,
        pool,
        org,
        tok,
    }
}

const SEL: &str = r#"clientMutationId actionExecution { id }
    recommendation { id status version resolvedBy resolvedAt resolution { reasonCode note payloadOverridden } }"#;

fn accept(id: u128, extra: &str) -> String {
    format!(
        r#"mutation {{ acceptRecommendation(input: {{ recommendationId: "{}", clientMutationId: "m-1"{extra} }}) {{ {SEL} }} }}"#,
        u(id)
    )
}

const SEL_REJECT: &str = r#"clientMutationId
    recommendation { id status version resolvedBy resolvedAt resolution { reasonCode note payloadOverridden } }"#;

fn reject(id: u128, code: &str, extra: &str) -> String {
    format!(
        r#"mutation {{ rejectRecommendation(input: {{ recommendationId: "{}", reasonCode: {code}, clientMutationId: "m-1"{extra} }}) {{ {SEL_REJECT} }} }}"#,
        u(id)
    )
}

async fn principal(env: &Env) -> String {
    let (_, b) = gql(&env.app, &env.tok, "{ viewer { principalId } }").await;
    b["data"]["viewer"]["principalId"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn count(admin: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(admin)
        .await
        .unwrap()
}

async fn last_event(admin: &PgPool, org: Uuid) -> Value {
    sqlx::query_scalar(
        "SELECT payload FROM outbox WHERE organisation_id = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(org)
    .fetch_one(admin)
    .await
    .unwrap()
}

fn code(b: &Value) -> (&Value, &Value) {
    (
        &b["errors"][0]["extensions"]["code"],
        &b["errors"][0]["extensions"]["reason"],
    )
}

#[sqlx::test(migrations = "../../migrations")]
async fn accepting_records_the_resolution_announces_it_and_audits_it(admin: PgPool) {
    let env = setup(&admin).await;
    put(&admin, env.org, 1, None, "pending", None).await;
    let me = principal(&env).await;

    let (_, b) = gql(&env.app, &env.tok, &accept(1, r#", note: "ok to go""#)).await;
    assert!(b["errors"].is_null(), "{b}");
    let r = &b["data"]["acceptRecommendation"];
    assert_eq!(r["clientMutationId"], "m-1");
    assert!(r["actionExecution"].is_null(), "no executor yet");
    let rec = &r["recommendation"];
    assert_eq!(rec["id"], u(1));
    assert_eq!(rec["status"], "ACCEPTED");
    assert_eq!(rec["version"], 2);
    assert_eq!(rec["resolvedBy"], me.as_str());
    assert!(rec["resolvedAt"].is_string());
    assert_eq!(rec["resolution"]["note"], "ok to go");
    assert_eq!(rec["resolution"]["payloadOverridden"], false);
    assert!(rec["resolution"]["reasonCode"].is_null());

    let e = last_event(&admin, env.org).await;
    assert_eq!(e["type"], "recommendation.status_changed");
    assert_eq!(e["previousStatus"], "pending");
    assert_eq!(e["status"], "accepted");

    let rows: Vec<(String, String, String)> =
        sqlx::query_as("SELECT actor, action, subject FROM audit_log")
            .fetch_all(&admin)
            .await
            .unwrap();
    assert_eq!(
        rows,
        [(
            me,
            "recommendation.accepted".to_owned(),
            format!("recommendation:{}", u(1))
        )]
    );

    // The audit trail is append-only for the application role.
    assert!(
        sqlx::query("UPDATE audit_log SET actor = 'x'")
            .execute(&env.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM audit_log")
            .execute(&env.pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn rejecting_records_the_reason_code(admin: PgPool) {
    let env = setup(&admin).await;
    put(&admin, env.org, 1, None, "pending", None).await;

    let (_, b) = gql(
        &env.app,
        &env.tok,
        &reject(1, "NOT_NOW", r#", reasonText: "after quarter end""#),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    let rec = &b["data"]["rejectRecommendation"]["recommendation"];
    assert_eq!(rec["status"], "REJECTED");
    assert_eq!(rec["version"], 2);
    assert_eq!(rec["resolution"]["reasonCode"], "NOT_NOW");
    assert_eq!(rec["resolution"]["note"], "after quarter end");
    assert_eq!(b["data"]["rejectRecommendation"]["clientMutationId"], "m-1");

    assert_eq!(last_event(&admin, env.org).await["status"], "rejected");
    let action: String = sqlx::query_scalar("SELECT action FROM audit_log")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(action, "recommendation.rejected");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_payload_override_is_validated_and_stored_beside_the_original(admin: PgPool) {
    let env = setup(&admin).await;
    for n in 1..=3 {
        put(&admin, env.org, n, None, "pending", None).await;
    }

    // Breaks the schema: nothing is written.
    let (_, b) = gql(
        &env.app,
        &env.tok,
        &accept(1, r#", payloadOverride: { object_id: 5 }"#),
    )
    .await;
    assert_eq!(
        b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED",
        "{b}"
    );
    let (_, b) = gql(
        &env.app,
        &env.tok,
        &accept(1, r#", payloadOverride: { object_id: "o-9", extra: 1 }"#),
    )
    .await;
    assert_eq!(
        b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED",
        "{b}"
    );
    assert_eq!(count(&admin, "outbox").await, 0);
    assert_eq!(count(&admin, "audit_log").await, 0);
    let st: String = sqlx::query_scalar("SELECT status FROM recommendations WHERE id = $1")
        .bind(Uuid::from_u128(1))
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(st, "pending");

    // A valid, different payload: flagged, stored beside the untouched original.
    let (_, b) = gql(
        &env.app,
        &env.tok,
        &accept(2, r#", payloadOverride: { object_id: "o-2" }"#),
    )
    .await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(
        b["data"]["acceptRecommendation"]["recommendation"]["resolution"]["payloadOverridden"],
        true
    );
    let (res, original): (Value, Value) = sqlx::query_as(
        "SELECT resolution, body->'action'->'payload' FROM recommendations WHERE id = $1",
    )
    .bind(Uuid::from_u128(2))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(res["payload_override"], json!({ "object_id": "o-2" }));
    assert_eq!(original, json!({ "object_id": "o-1" }));

    // The same payload is not an override.
    let (_, b) = gql(
        &env.app,
        &env.tok,
        &accept(3, r#", payloadOverride: { object_id: "o-1" }"#),
    )
    .await;
    assert_eq!(
        b["data"]["acceptRecommendation"]["recommendation"]["resolution"]["payloadOverridden"],
        false
    );
    let has: bool = sqlx::query_scalar(
        "SELECT jsonb_exists(resolution, 'payload_override') FROM recommendations WHERE id = $1",
    )
    .bind(Uuid::from_u128(3))
    .fetch_one(&admin)
    .await
    .unwrap();
    assert!(!has);
}

#[sqlx::test(migrations = "../../migrations")]
async fn if_version_is_compare_and_set(admin: PgPool) {
    let env = setup(&admin).await;
    put(&admin, env.org, 1, None, "pending", None).await;

    let (_, b) = gql(&env.app, &env.tok, &accept(1, ", ifVersion: 7")).await;
    let (c, r) = code(&b);
    assert_eq!((c, r), (&json!("CONFLICT"), &json!("STALE_VERSION")), "{b}");
    assert_eq!(b["errors"][0]["extensions"]["currentVersion"], 1);
    assert_eq!(count(&admin, "outbox").await, 0);

    let (_, b) = gql(&env.app, &env.tok, &accept(1, ", ifVersion: 0")).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED");

    let (_, b) = gql(&env.app, &env.tok, &accept(1, ", ifVersion: 1")).await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(
        b["data"]["acceptRecommendation"]["recommendation"]["status"],
        "ACCEPTED"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn only_pending_recommendations_can_be_resolved(admin: PgPool) {
    let env = setup(&admin).await;
    put(&admin, env.org, 1, None, "expired", None).await;
    put(&admin, env.org, 2, None, "accepted", Some("user:other")).await;
    put(&admin, env.org, 3, None, "rejected", Some("user:other")).await;

    let invalid = (json!("CONFLICT"), json!("INVALID_STATE_TRANSITION"));
    for q in [
        accept(1, ""),
        reject(1, "OTHER", ""),
        accept(2, ""),          // already accepted, by someone else
        reject(2, "OTHER", ""), // accepted rows cannot be rejected
        accept(3, ""),
        reject(3, "OTHER", ""), // already rejected, by someone else
    ] {
        let (_, b) = gql(&env.app, &env.tok, &q).await;
        let (c, r) = code(&b);
        assert_eq!((c.clone(), r.clone()), invalid, "{q}: {b}");
    }
    assert_eq!(count(&admin, "outbox").await, 0);
    assert_eq!(count(&admin, "audit_log").await, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn retries_by_the_same_actor_are_idempotent(admin: PgPool) {
    let env = setup(&admin).await;
    put(&admin, env.org, 1, None, "pending", None).await;
    put(&admin, env.org, 2, None, "pending", None).await;

    for _ in 0..2 {
        let (_, b) = gql(&env.app, &env.tok, &accept(1, "")).await;
        assert!(b["errors"].is_null(), "{b}");
        assert_eq!(
            b["data"]["acceptRecommendation"]["recommendation"]["version"],
            2
        );
    }
    assert_eq!(count(&admin, "outbox").await, 1, "the retry wrote nothing");
    assert_eq!(count(&admin, "audit_log").await, 1);

    // Still a retry once execution has started.
    sqlx::query("UPDATE recommendations SET status = 'executing' WHERE id = $1")
        .bind(Uuid::from_u128(1))
        .execute(&admin)
        .await
        .unwrap();
    let (_, b) = gql(&env.app, &env.tok, &accept(1, "")).await;
    assert!(b["errors"].is_null(), "{b}");
    assert_eq!(
        b["data"]["acceptRecommendation"]["recommendation"]["status"],
        "EXECUTING"
    );

    // Someone else cannot claim an existing resolution.
    let other = common::seed_key(&admin, env.org, &BOTH).await;
    let (_, b) = gql(&env.app, &other, &accept(1, "")).await;
    assert_eq!(
        b["errors"][0]["extensions"]["reason"], "INVALID_STATE_TRANSITION",
        "{b}"
    );

    for _ in 0..2 {
        let (_, b) = gql(&env.app, &env.tok, &reject(2, "WRONG_RECOMMENDATION", "")).await;
        assert!(b["errors"].is_null(), "{b}");
    }
    assert_eq!(count(&admin, "audit_log").await, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn scope_tenancy_and_arguments(admin: PgPool) {
    let env = setup(&admin).await;
    let east = unit(&admin, env.org, "hq.east").await;
    let west = unit(&admin, env.org, "hq.west").await;
    put(&admin, env.org, 1, Some(east), "pending", None).await;
    put(&admin, env.org, 2, Some(west), "pending", None).await;
    let other = common::seed_org(&admin, "globex").await;
    put(&admin, other, 60, None, "pending", None).await;

    // Reading is not resolving.
    let reader = common::seed_key(&admin, env.org, &["decisions:read"]).await;
    for q in [accept(1, ""), reject(1, "OTHER", "")] {
        let (_, b) = gql(&env.app, &reader, &q).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "FORBIDDEN",
            "{q}: {b}"
        );
    }

    // Unknown and foreign ids look the same.
    for n in [999, 60] {
        let (_, b) = gql(&env.app, &env.tok, &accept(n, "")).await;
        assert_eq!(
            b["errors"][0]["extensions"]["code"], "NOT_FOUND",
            "{n}: {b}"
        );
    }

    // A subtree-scoped key cannot reach outside its subtree, and nothing changes there.
    let scoped = scoped_key(&admin, env.org, &BOTH, "hq.east").await;
    let (_, b) = gql(&env.app, &scoped, &accept(2, "")).await;
    assert_eq!(b["errors"][0]["extensions"]["code"], "NOT_FOUND", "{b}");
    let st: String = sqlx::query_scalar("SELECT status FROM recommendations WHERE id = $1")
        .bind(Uuid::from_u128(2))
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(st, "pending");
    let (_, b) = gql(&env.app, &scoped, &accept(1, "")).await;
    assert!(b["errors"].is_null(), "{b}");

    // Argument validation.
    let long = "x".repeat(2001);
    for q in [
        r#"mutation { acceptRecommendation(input: { recommendationId: "nope", clientMutationId: "m" }) { clientMutationId } }"#.to_owned(),
        format!(r#"mutation {{ acceptRecommendation(input: {{ recommendationId: "{}", clientMutationId: "" }}) {{ clientMutationId }} }}"#, u(2)),
        format!(r#"mutation {{ acceptRecommendation(input: {{ recommendationId: "{}", clientMutationId: "{}" }}) {{ clientMutationId }} }}"#, u(2), "m".repeat(256)),
        accept(2, &format!(r#", note: "{long}""#)),
        reject(2, "OTHER", &format!(r#", reasonText: "{long}""#)),
    ] {
        let (s, b) = gql(&env.app, &env.tok, &q).await;
        assert_eq!(s, axum::http::StatusCode::OK);
        assert_eq!(b["errors"][0]["extensions"]["code"], "VALIDATION_FAILED", "{b}");
    }
}
