use kuki_ingest::body::{Format, parse_rows};
use kuki_ingest::{IngestContext, IngestOptions, RejectionCode, ingest_events};
use kuki_store::session::OrgSession;
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

const SCHEMA: &str = r#"{"type":"object","required":["delta"],"properties":{"delta":{"type":"integer"}},"additionalProperties":false}"#;

async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET ROLE kuki_app").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect_with((*admin.connect_options()).clone())
        .await
        .unwrap()
}

async fn seed_org(admin: &PgPool, slug: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO organisations (id, slug, name) VALUES ($1, $2, $2)")
        .bind(id)
        .bind(slug)
        .execute(admin)
        .await
        .unwrap();
    id
}

async fn seed_type(admin: &PgPool, org: Uuid) {
    sqlx::query(
        "INSERT INTO event_types (organisation_id, name, version, subject_type, json_schema)
         VALUES ($1, 'stock.delta', 1, 'item', $2::jsonb)",
    )
    .bind(org)
    .bind(SCHEMA)
    .execute(admin)
    .await
    .unwrap();
}

async fn seed_object(admin: &PgPool, org: Uuid, object_type: &str, external: Value) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, type, type_version, external_ids, attrs, valid_from)
         VALUES ($1, $2, $3, 1, $4, '{}', now())",
    )
    .bind(id)
    .bind(org)
    .bind(object_type)
    .bind(external)
    .execute(admin)
    .await
    .unwrap();
    id
}

fn ctx(org: Uuid, key: &str) -> IngestContext {
    IngestContext {
        org,
        session: OrgSession {
            org_id: org,
            scope_path: None,
        },
        source: "api_key:test".into(),
        idempotency_key: key.into(),
        backfill_id: None,
    }
}

fn ev(now: &str, typ: &str, version: i64, subject: Value, payload: Value) -> String {
    json!({ "type": typ, "typeVersion": version, "subject": subject, "occurredAt": now, "payload": payload })
        .to_string()
}

async fn count(admin: &PgPool, table: &str, org: Uuid) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table} WHERE organisation_id = $1"
    ))
    .bind(org)
    .fetch_one(admin)
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_mixed_batch_accepts_good_rows_and_quarantines_the_rest(admin: PgPool) {
    let org = seed_org(&admin, "mixed").await;
    seed_type(&admin, org).await;
    let item = seed_object(&admin, org, "item", json!({ "erp": "A1" })).await;
    seed_object(&admin, org, "item", json!({ "erp": "DUP" })).await;
    seed_object(&admin, org, "item", json!({ "erp": "DUP" })).await;
    seed_object(&admin, org, "location", json!({ "erp": "LOC" })).await;
    let pool = app_pool(&admin).await;

    let now = chrono::Utc::now().to_rfc3339();
    let by_id = json!({ "id": item });
    let ext = |s: &str| json!({ "externalId": { "system": "erp", "id": s } });
    let lines = vec![
        ev(&now, "stock.delta", 1, by_id.clone(), json!({ "delta": 1 })), // 0 ok
        ev(&now, "stock.delta", 1, ext("A1"), json!({ "delta": 2 })),     // 1 ok
        ev(
            &now,
            "stock.delta",
            1,
            by_id.clone(),
            json!({ "delta": "x" }),
        ), // 2 schema
        ev(&now, "stock.nope", 1, by_id.clone(), json!({ "delta": 1 })),  // 3 type
        ev(&now, "stock.delta", 2, by_id.clone(), json!({ "delta": 1 })), // 4 version
        ev(
            &now,
            "stock.delta",
            1,
            ext("MISSING"),
            json!({ "delta": 1 }),
        ), // 5 unresolved
        ev(&now, "stock.delta", 1, ext("DUP"), json!({ "delta": 1 })),    // 6 ambiguous
        ev(&now, "stock.delta", 1, ext("LOC"), json!({ "delta": 1 })),    // 7 wrong subject type
        "{not json".to_string(),                                          // 8 malformed
        json!({ "type": "stock.delta", "extra": 1 }).to_string(),         // 9 structural
    ];
    let body = lines.join("\n");
    let rows = parse_rows(body.as_bytes(), Format::NdJson, "events").unwrap();

    let first = ingest_events(
        &pool,
        ctx(org, "k1"),
        rows.clone(),
        &IngestOptions::default(),
    )
    .await
    .unwrap();

    let ok: Vec<u32> = first
        .accepted
        .as_ref()
        .unwrap()
        .iter()
        .map(|a| a.index)
        .collect();
    assert_eq!(ok, vec![0, 1]);
    assert_eq!(first.accepted_count, 2);

    let codes: Vec<(u32, RejectionCode)> =
        first.rejected.iter().map(|r| (r.index, r.code)).collect();
    assert_eq!(
        codes,
        vec![
            (2, RejectionCode::SchemaInvalid),
            (3, RejectionCode::UnknownEventType),
            (4, RejectionCode::UnknownTypeVersion),
            (5, RejectionCode::UnresolvedSubject),
            (6, RejectionCode::AmbiguousSubject),
            (7, RejectionCode::SchemaInvalid),
            (8, RejectionCode::MalformedLine),
            (9, RejectionCode::SchemaInvalid),
        ]
    );
    assert_eq!(first.rejected[0].field.as_deref(), Some("/payload/delta"));
    assert_eq!(first.rejected[5].field.as_deref(), Some("/subject"));

    assert_eq!(count(&admin, "events", org).await, 2);
    assert_eq!(count(&admin, "ingest_quarantine", org).await, 8);

    let counts: (i32, i32) =
        sqlx::query_as("SELECT accepted_count, rejected_count FROM ingest_batches WHERE id = $1")
            .bind(first.batch_id)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(counts, (2, 8));

    // The row as received is kept, even when it is not JSON.
    let raw: String = sqlx::query_scalar(
        "SELECT raw #>> '{}' FROM ingest_quarantine WHERE organisation_id = $1 AND row_index = 8",
    )
    .bind(org)
    .fetch_one(&admin)
    .await
    .unwrap();
    assert_eq!(raw, "{not json");

    // A retry with the same key returns the same ids and writes nothing new.
    let again = ingest_events(&pool, ctx(org, "k1"), rows, &IngestOptions::default())
        .await
        .unwrap();
    assert_eq!(again, first);
    assert_eq!(count(&admin, "events", org).await, 2);
    assert_eq!(count(&admin, "ingest_quarantine", org).await, 8);
}

#[sqlx::test(migrations = "../../migrations")]
async fn chunking_commits_every_chunk(admin: PgPool) {
    let org = seed_org(&admin, "chunks").await;
    seed_type(&admin, org).await;
    let item = seed_object(&admin, org, "item", json!({ "erp": "A1" })).await;
    let pool = app_pool(&admin).await;

    let now = chrono::Utc::now().to_rfc3339();
    let body: Vec<String> = (0..5)
        .map(|i| {
            ev(
                &now,
                "stock.delta",
                1,
                json!({ "id": item }),
                json!({ "delta": i }),
            )
        })
        .collect();
    let rows = parse_rows(body.join("\n").as_bytes(), Format::NdJson, "events").unwrap();

    let r = ingest_events(&pool, ctx(org, "k"), rows, &IngestOptions { chunk_size: 2 })
        .await
        .unwrap();
    assert_eq!(r.accepted_count, 5);
    assert!(r.rejected.is_empty());
    assert_eq!(count(&admin, "events", org).await, 5);
}

#[sqlx::test(migrations = "../../migrations")]
async fn another_tenants_event_types_do_not_exist(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    seed_type(&admin, b).await; // registered for B only
    let item = seed_object(&admin, a, "item", json!({ "erp": "A1" })).await;
    let pool = app_pool(&admin).await;

    let now = chrono::Utc::now().to_rfc3339();
    let body = ev(
        &now,
        "stock.delta",
        1,
        json!({ "id": item }),
        json!({ "delta": 1 }),
    );
    let rows = parse_rows(
        format!(r#"{{"events":[{body}]}}"#).as_bytes(),
        Format::Json,
        "events",
    )
    .unwrap();

    let r = ingest_events(&pool, ctx(a, "k"), rows, &IngestOptions::default())
        .await
        .unwrap();
    assert_eq!(r.accepted_count, 0);
    assert_eq!(r.rejected[0].code, RejectionCode::UnknownEventType);
}
