use futures::FutureExt;
use kuki_store::ingest::*;
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_types::ids;
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

async fn app_pool(admin: &PgPool) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
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

fn session(org: Uuid) -> OrgSession {
    OrgSession {
        org_id: org,
        scope_path: None,
    }
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

async fn seed_unit(admin: &PgPool, org: Uuid, path: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO org_units (id, organisation_id, path, name) VALUES ($1, $2, $3::ltree, $3)",
    )
    .bind(id)
    .bind(org)
    .bind(path)
    .execute(admin)
    .await
    .unwrap();
    id
}

async fn seed_object(admin: &PgPool, org: Uuid, external: Value, unit: Option<Uuid>) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO objects (id, organisation_id, org_unit_id, type, type_version, external_ids, attrs, valid_from)
         VALUES ($1, $2, $3, 'item', 1, $4, '{}', now())",
    )
    .bind(id)
    .bind(org)
    .bind(unit)
    .bind(external)
    .execute(admin)
    .await
    .unwrap();
    id
}

async fn begin(pool: &PgPool, org: Uuid, key: &str, hash: &[u8]) -> Idempotency {
    let (key, hash) = (key.to_owned(), hash.to_vec());
    with_org_tx(pool, &session(org), |c| {
        async move { idempotency_begin(c, org, &key, &hash).await }.boxed()
    })
    .await
    .unwrap()
}

async fn complete(pool: &PgPool, org: Uuid, key: &str, hash: &[u8], status: i32, response: Value) {
    let (key, hash) = (key.to_owned(), hash.to_vec());
    with_org_tx(pool, &session(org), |c| {
        async move { idempotency_complete(c, org, &key, &hash, status, &response).await }.boxed()
    })
    .await
    .unwrap()
}

async fn resolve(
    pool: &PgPool,
    sess: OrgSession,
    org: Uuid,
    subject: (Option<Uuid>, Option<(String, String)>),
) -> Resolution {
    with_org_tx(pool, &sess, |c| {
        async move {
            match &subject {
                (Some(id), _) => subject_resolve(c, org, &Subject::Id(*id)).await,
                (_, Some((s, i))) => {
                    subject_resolve(c, org, &Subject::External { system: s, id: i }).await
                }
                _ => unreachable!(),
            }
        }
        .boxed()
    })
    .await
    .unwrap()
}

fn event(org: Uuid, key: &str, index: u32, subject: Uuid) -> NewEvent {
    NewEvent {
        id: ids::event_id(org, key, index),
        event_type: "stock.delta".into(),
        type_version: 1,
        subject_id: subject,
        occurred_at: chrono::Utc::now(),
        payload: json!({ "delta": 1 }),
        supersedes: None,
    }
}

async fn insert(
    pool: &PgPool,
    session_org: Uuid,
    org: Uuid,
    key: &str,
    events: Vec<NewEvent>,
) -> sqlx::Result<u64> {
    let key = key.to_owned();
    with_org_tx(pool, &session(session_org), |c| {
        async move { events_insert(c, org, "api_key:test", &key, None, &events).await }.boxed()
    })
    .await
}

#[sqlx::test(migrations = "../../migrations")]
async fn idempotency_lifecycle(admin: PgPool) {
    let org = seed_org(&admin, "idem").await;
    let pool = app_pool(&admin).await;

    assert_eq!(
        begin(&pool, org, "k1", &[1u8; 32]).await,
        Idempotency::Proceed
    );
    // Unfinished: a crash or a concurrent duplicate. Safe to resume.
    assert_eq!(
        begin(&pool, org, "k1", &[1u8; 32]).await,
        Idempotency::Resume
    );
    assert_eq!(
        begin(&pool, org, "k1", &[2u8; 32]).await,
        Idempotency::Mismatch
    );

    complete(
        &pool,
        org,
        "k1",
        &[1u8; 32],
        202,
        json!({ "acceptedCount": 3 }),
    )
    .await;
    assert_eq!(
        begin(&pool, org, "k1", &[1u8; 32]).await,
        Idempotency::Replay {
            status: 202,
            response: json!({ "acceptedCount": 3 })
        }
    );
    assert_eq!(
        begin(&pool, org, "k1", &[2u8; 32]).await,
        Idempotency::Mismatch
    );

    // An expired key is reclaimed, even for a different body.
    sqlx::query("UPDATE idempotency_keys SET expires_at = now() - interval '1 minute' WHERE organisation_id = $1")
        .bind(org)
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(
        begin(&pool, org, "k1", &[2u8; 32]).await,
        Idempotency::Proceed
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn idempotency_is_per_organisation(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let pool = app_pool(&admin).await;
    assert_eq!(
        begin(&pool, a, "same", &[1u8; 32]).await,
        Idempotency::Proceed
    );
    assert_eq!(
        begin(&pool, b, "same", &[9u8; 32]).await,
        Idempotency::Proceed
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn event_inserts_are_idempotent(admin: PgPool) {
    let org = seed_org(&admin, "ev").await;
    let subject = seed_object(&admin, org, json!({ "erp": "A1" }), None).await;
    let pool = app_pool(&admin).await;

    let evs = || {
        (0..3)
            .map(|i| event(org, "k", i, subject))
            .collect::<Vec<_>>()
    };
    let first = evs();
    // A retry rebuilds the same rows, so keep the original timestamps.
    let again: Vec<NewEvent> = first
        .iter()
        .map(|e| NewEvent {
            id: e.id,
            event_type: e.event_type.clone(),
            type_version: e.type_version,
            subject_id: e.subject_id,
            occurred_at: e.occurred_at,
            payload: e.payload.clone(),
            supersedes: None,
        })
        .collect();

    assert_eq!(insert(&pool, org, org, "k", first).await.unwrap(), 3);
    assert_eq!(insert(&pool, org, org, "k", again).await.unwrap(), 0);

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE organisation_id = $1")
        .bind(org)
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(n, 3);
}

#[sqlx::test(migrations = "../../migrations")]
async fn events_cannot_be_written_for_another_organisation(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let pool = app_pool(&admin).await;

    // Session is tenant A; the rows claim tenant B. RLS must refuse.
    let r = insert(&pool, a, b, "k", vec![event(b, "k", 0, Uuid::new_v4())]).await;
    assert!(r.is_err(), "cross-tenant insert must fail");
}

#[sqlx::test(migrations = "../../migrations")]
async fn quarantine_rows_are_deterministic(admin: PgPool) {
    let org = seed_org(&admin, "q").await;
    let pool = app_pool(&admin).await;
    let batch = ids::batch_id(org, "k");
    let qid = ids::quarantine_id(org, "k", 3);

    for _ in 0..2 {
        with_org_tx(&pool, &session(org), |c| {
            async move {
                batch_ensure(
                    c,
                    &NewBatch {
                        id: batch,
                        org,
                        kind: "events",
                        idempotency_key: "k",
                        backfill_id: None,
                        source: "api_key:test",
                    },
                )
                .await?;
                quarantine_insert(
                    c,
                    org,
                    &Rejection {
                        id: qid,
                        batch_id: batch,
                        index: 3,
                        code: "SCHEMA_INVALID",
                        message: "delta must be an integer",
                        field: Some("/payload/delta"),
                        raw: &json!({ "payload": { "delta": "x" } }),
                    },
                )
                .await?;
                batch_finish(c, batch, 2, 1).await?;
                Ok::<_, sqlx::Error>(())
            }
            .boxed()
        })
        .await
        .unwrap();
    }

    let n: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_quarantine WHERE organisation_id = $1")
            .bind(org)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(n, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn subjects_resolve_by_id_and_by_external_id(admin: PgPool) {
    let org = seed_org(&admin, "r").await;
    let id = seed_object(&admin, org, json!({ "erp": "A1" }), None).await;
    let pool = app_pool(&admin).await;

    let by_id = resolve(&pool, session(org), org, (Some(id), None)).await;
    assert!(matches!(by_id, Resolution::Found(ref s) if s.id == id && s.object_type == "item"));

    let by_ext = resolve(
        &pool,
        session(org),
        org,
        (None, Some(("erp".into(), "A1".into()))),
    )
    .await;
    assert!(matches!(by_ext, Resolution::Found(ref s) if s.id == id));

    let missing = resolve(
        &pool,
        session(org),
        org,
        (None, Some(("erp".into(), "nope".into()))),
    )
    .await;
    assert_eq!(missing, Resolution::NotFound);
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_external_id_shared_by_two_objects_is_ambiguous(admin: PgPool) {
    let org = seed_org(&admin, "amb").await;
    seed_object(&admin, org, json!({ "erp": "DUP" }), None).await;
    seed_object(&admin, org, json!({ "erp": "DUP" }), None).await;
    let pool = app_pool(&admin).await;

    let r = resolve(
        &pool,
        session(org),
        org,
        (None, Some(("erp".into(), "DUP".into()))),
    )
    .await;
    assert_eq!(r, Resolution::Ambiguous);
}

#[sqlx::test(migrations = "../../migrations")]
async fn other_tenants_and_other_units_are_invisible(admin: PgPool) {
    let a = seed_org(&admin, "a").await;
    let b = seed_org(&admin, "b").await;
    let in_b = seed_object(&admin, b, json!({ "erp": "B1" }), None).await;
    let ab = seed_unit(&admin, a, "a.b").await;
    let ac = seed_unit(&admin, a, "a.c").await;
    let in_ac = seed_object(&admin, a, json!({ "erp": "C1" }), Some(ac)).await;
    let _ = ab;
    let pool = app_pool(&admin).await;

    // Another tenant's object does not exist, even by exact id.
    assert_eq!(
        resolve(&pool, session(a), a, (Some(in_b), None)).await,
        Resolution::NotFound
    );

    // Unscoped key sees the unit's object; a key scoped to a.b does not.
    assert!(matches!(
        resolve(&pool, session(a), a, (Some(in_ac), None)).await,
        Resolution::Found(_)
    ));
    let scoped = OrgSession {
        org_id: a,
        scope_path: Some("a.b".into()),
    };
    assert_eq!(
        resolve(&pool, scoped, a, (Some(in_ac), None)).await,
        Resolution::NotFound
    );
}
