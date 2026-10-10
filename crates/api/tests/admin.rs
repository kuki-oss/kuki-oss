mod common;

use axum::http::StatusCode;
use common::*;
use kuki_api::{
    admin::{
        AdminError, CreateKey, OrgRef, Revoked, create_key, create_org, list_keys, revoke_key,
    },
    auth::{KeyHasher, Scope},
};
use sqlx::PgPool;
use uuid::Uuid;

fn req(org: OrgRef) -> CreateKey {
    CreateKey {
        org,
        name: "ci".into(),
        scopes: vec![Scope::IngestWrite, Scope::IngestRead],
        unit: None,
        expires_at: None,
    }
}

async fn mint(admin: &PgPool, r: CreateKey) -> Result<kuki_api::admin::CreatedKey, AdminError> {
    create_key(admin, &TestHasher, "test", r).await
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_created_key_authenticates_until_it_is_revoked(admin: PgPool) {
    let org = seed_org(&admin, "adm-flow").await;
    let app = app(app_pool(&admin).await);
    let k = mint(&admin, req(OrgRef::Slug("adm-flow".into())))
        .await
        .unwrap();
    assert_eq!(k.organisation_id, org);

    let (s, _) = common::viewer(&app, &k.raw).await;
    assert_eq!(s, StatusCode::OK);

    // Only the HMAC is stored, never the secret.
    let secret = k.raw.rsplit('_').next().unwrap();
    let stored: Vec<u8> = sqlx::query_scalar("SELECT secret_hash FROM api_keys WHERE key_id = $1")
        .bind(&k.key_id)
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(stored, TestHasher.hash(secret).await.unwrap().to_vec());
    assert_ne!(stored, secret.as_bytes());

    assert_eq!(revoke_key(&admin, &k.key_id).await.unwrap(), Revoked::Now);
    let (s, _) = common::viewer(&app, &k.raw).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(
        revoke_key(&admin, &k.key_id).await.unwrap(),
        Revoked::Already
    );
    assert_eq!(
        revoke_key(&admin, "abcdefghij12").await.unwrap(),
        Revoked::NotFound
    );
    assert!(matches!(
        revoke_key(&admin, "NOT-A-KEY-ID").await,
        Err(AdminError::Invalid(_))
    ));
}

#[sqlx::test(migrations = "../../migrations")]
async fn bad_requests_are_refused_and_write_nothing(admin: PgPool) {
    let org = seed_org(&admin, "adm-bad").await;
    let id = || OrgRef::Id(org);

    let mut r = req(id());
    r.scopes.clear();
    assert!(matches!(mint(&admin, r).await, Err(AdminError::Invalid(_))));
    let mut r = req(id());
    r.name = "  ".into();
    assert!(matches!(mint(&admin, r).await, Err(AdminError::Invalid(_))));
    let mut r = req(id());
    r.expires_at = Some(chrono::Utc::now() - chrono::Duration::hours(1));
    assert!(matches!(mint(&admin, r).await, Err(AdminError::Invalid(_))));
    let mut r = req(id());
    r.unit = Some("a..b".into());
    assert!(matches!(mint(&admin, r).await, Err(AdminError::Invalid(_))));
    let mut r = req(id());
    r.unit = Some("no.such.unit".into());
    assert!(matches!(
        mint(&admin, r).await,
        Err(AdminError::NotFound(_))
    ));
    assert!(matches!(
        mint(&admin, req(OrgRef::Slug("nope".into()))).await,
        Err(AdminError::NotFound(_))
    ));
    assert!(matches!(
        mint(&admin, req(OrgRef::Id(Uuid::new_v4()))).await,
        Err(AdminError::NotFound(_))
    ));
    assert!(matches!(
        create_key(&admin, &TestHasher, "te_st", req(id())).await,
        Err(AdminError::Invalid(_))
    ));

    assert_eq!(count(&admin, "api_keys", org).await, 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_key_can_be_limited_to_an_existing_unit(admin: PgPool) {
    let org = seed_org(&admin, "adm-unit").await;
    sqlx::query("INSERT INTO org_units (id, organisation_id, path, name) VALUES (gen_random_uuid(), $1, $2::text::ltree, 'ab')")
        .bind(org)
        .bind("a.b")
        .execute(&admin)
        .await
        .unwrap();

    let mut r = req(OrgRef::Id(org));
    r.unit = Some("a.b".into());
    let k = mint(&admin, r).await.unwrap();
    let unit: Option<String> =
        sqlx::query_scalar("SELECT org_unit_scope_path::text FROM api_keys WHERE key_id = $1")
            .bind(&k.key_id)
            .fetch_one(&admin)
            .await
            .unwrap();
    assert_eq!(unit.as_deref(), Some("a.b"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn listing_filters_by_organisation_and_shows_revocation(admin: PgPool) {
    let a = seed_org(&admin, "adm-a").await;
    let b = seed_org(&admin, "adm-b").await;
    let k1 = mint(&admin, req(OrgRef::Id(a))).await.unwrap();
    mint(&admin, req(OrgRef::Id(a))).await.unwrap();
    mint(&admin, req(OrgRef::Id(b))).await.unwrap();
    revoke_key(&admin, &k1.key_id).await.unwrap();

    assert_eq!(list_keys(&admin, None).await.unwrap().len(), 3);
    let only_a = list_keys(&admin, Some(OrgRef::Slug("adm-a".into())))
        .await
        .unwrap();
    assert_eq!(only_a.len(), 2);
    assert!(
        only_a
            .iter()
            .all(|k| k.org_slug.as_deref() == Some("adm-a"))
    );
    assert_eq!(only_a.iter().filter(|k| k.revoked_at.is_some()).count(), 1);
    assert!(
        only_a
            .iter()
            .all(|k| k.scopes == ["ingest:read", "ingest:write"])
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn creating_an_organisation_is_idempotent_by_slug(admin: PgPool) {
    assert!(matches!(
        create_org(&admin, "Bad Slug", "x").await,
        Err(AdminError::Invalid(_))
    ));
    let (id, created) = create_org(&admin, "acme", "Acme").await.unwrap();
    assert!(created);
    assert_eq!(
        create_org(&admin, "acme", "Acme").await.unwrap(),
        (id, false)
    );
}
