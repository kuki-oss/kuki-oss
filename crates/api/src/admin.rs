//! Operator operations on organisations and API keys. Callers pass a pool connected as the
//! owner role. The secret is hashed by the secrets backend before anything is written, and
//! the raw key is returned to the caller exactly once.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use kuki_store::admin as db;
pub use kuki_store::admin::{ApiKeyRow, Revoked};
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use crate::auth::{KeyHasher, Scope, generate_key};

#[derive(Debug, Clone, PartialEq)]
pub enum OrgRef {
    Id(Uuid),
    Slug(String),
}

impl OrgRef {
    pub fn parse(s: &str) -> Self {
        Uuid::parse_str(s)
            .map(Self::Id)
            .unwrap_or_else(|_| Self::Slug(s.to_owned()))
    }
}

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    NotFound(String),
    #[error("secrets backend: {0}")]
    Hasher(String),
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
}

fn invalid(m: impl Into<String>) -> AdminError {
    AdminError::Invalid(m.into())
}

fn is_fk_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.code().as_deref() == Some("23503"))
}

fn valid_ltree(p: &str) -> bool {
    !p.is_empty()
        && p.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 256
                && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
}

fn valid_key_id(s: &str) -> bool {
    s.len() == 12
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

async fn resolve_org(pool: &PgPool, org: &OrgRef) -> Result<Uuid, AdminError> {
    match org {
        OrgRef::Id(id) => Ok(*id),
        OrgRef::Slug(slug) => {
            let mut c = pool.acquire().await?;
            db::org_by_slug(&mut c, slug).await?.ok_or_else(|| {
                AdminError::NotFound(format!(
                    "no organisation `{slug}` is visible to this database role \
                     (if the role does not bypass row-level security, pass the organisation id)"
                ))
            })
        }
    }
}

pub async fn create_org(pool: &PgPool, slug: &str, name: &str) -> Result<(Uuid, bool), AdminError> {
    let ok = !slug.is_empty()
        && slug.len() <= 63
        && !slug.starts_with('-')
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !ok {
        return Err(invalid(
            "slug must be 1-63 characters of a-z, 0-9 and '-', not starting with '-'",
        ));
    }
    if name.trim().is_empty() || name.len() > 200 {
        return Err(invalid("name must be 1-200 characters"));
    }
    Ok(db::org_create(pool, slug, name.trim()).await?)
}

pub struct CreateKey {
    pub org: OrgRef,
    pub name: String,
    pub scopes: Vec<Scope>,
    /// Org-unit subtree (ltree path) the key is limited to. Must exist.
    pub unit: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

pub struct CreatedKey {
    /// Shown once; only its HMAC is stored.
    pub raw: String,
    pub key_id: String,
    pub id: Uuid,
    pub organisation_id: Uuid,
}

impl std::fmt::Debug for CreatedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

pub async fn create_key(
    pool: &PgPool,
    hasher: &dyn KeyHasher,
    key_env: &str,
    req: CreateKey,
) -> Result<CreatedKey, AdminError> {
    let name = req.name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(invalid("name must be 1-128 characters"));
    }
    if req.scopes.is_empty() {
        return Err(invalid("at least one scope is required"));
    }
    if key_env.is_empty()
        || !key_env
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(invalid(
            "the key environment must be lowercase letters and digits",
        ));
    }
    if req.unit.as_deref().is_some_and(|u| !valid_ltree(u)) {
        return Err(invalid(
            "unit must be dot-separated labels of letters, digits and _",
        ));
    }
    if req.expires_at.is_some_and(|t| t <= Utc::now()) {
        return Err(invalid("expiry must be in the future"));
    }

    let org = resolve_org(pool, &req.org).await?;
    let key = generate_key(key_env);
    // Hash first: if the secrets backend is down, nothing is written.
    let hash = hasher
        .hash(&key.secret)
        .await
        .map_err(|e| AdminError::Hasher(format!("{e:?}")))?;
    let scopes: Vec<String> = req
        .scopes
        .iter()
        .map(|s| s.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect();

    let created = db::api_key_create(
        pool,
        &db::NewApiKey {
            org,
            key_id: &key.key_id,
            name,
            secret_hash: &hash[..],
            scopes: &scopes,
            unit: req.unit.as_deref(),
            expires_at: req.expires_at,
        },
    )
    .await;
    match created {
        Ok(Some(id)) => Ok(CreatedKey {
            raw: key.raw,
            key_id: key.key_id,
            id,
            organisation_id: org,
        }),
        Ok(None) => Err(AdminError::NotFound(
            "no such org unit in that organisation".into(),
        )),
        Err(e) if is_fk_violation(&e) => Err(AdminError::NotFound("no such organisation".into())),
        Err(e) => Err(e.into()),
    }
}

pub async fn list_keys(pool: &PgPool, org: Option<OrgRef>) -> Result<Vec<ApiKeyRow>, AdminError> {
    let org = match org {
        Some(o) => Some(resolve_org(pool, &o).await?),
        None => None,
    };
    Ok(db::api_key_list(pool, org).await?)
}

/// By public key id (the 12 characters shown by `list`); never by raw key.
pub async fn revoke_key(pool: &PgPool, key_id: &str) -> Result<Revoked, AdminError> {
    if !valid_key_id(key_id) {
        return Err(invalid(
            "a key id is 12 lowercase letters or digits (see `kuki keys list`)",
        ));
    }
    Ok(db::api_key_revoke(pool, key_id).await?)
}
