//! API-key authentication (spec 3.4, 13.1). Keys are stored as HMAC-SHA256(pepper, key), shown
//! once, and looked up by that hash. The store is a trait so this crate stays free of SQL.
use std::{collections::BTreeSet, sync::Arc, time::Instant};

use async_trait::async_trait;
use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts},
};
use chrono::{DateTime, Utc};
use kuki_store::session::OrgSession;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::ratelimit::{RateLimiter, RateSlot};
use crate::{error::ApiError, state::AppState};

const KEY_PREFIX: &str = "kuki";
const KEY_ID_LEN: usize = 12;
const SECRET_LEN: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Scope {
    IngestWrite,
    IngestRead,
    DecisionsRead,
    DecisionsResolve,
}

impl Scope {
    pub const ALL: [Scope; 4] = [
        Scope::IngestWrite,
        Scope::IngestRead,
        Scope::DecisionsRead,
        Scope::DecisionsResolve,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::IngestWrite => "ingest:write",
            Scope::IngestRead => "ingest:read",
            Scope::DecisionsRead => "decisions:read",
            Scope::DecisionsResolve => "decisions:resolve",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ingest:write" => Some(Scope::IngestWrite),
            "ingest:read" => Some(Scope::IngestRead),
            "decisions:read" => Some(Scope::DecisionsRead),
            "decisions:resolve" => Some(Scope::DecisionsResolve),
            _ => None,
        }
    }
}

/// Database's returns this for a key handle (mirrors `auth_lookup_api_key`).
#[derive(Clone, Debug)]
pub struct KeyRecord {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub secret_hash: Vec<u8>,
    pub scopes: Vec<String>,
    pub org_unit_scope_path: Option<String>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Look up by the public key id. `Ok(None)` = no such key.
    /// `Err` = infrastructure failure (becomes 5xx not 401).
    async fn find(&self, key_id: &str) -> Result<Option<KeyRecord>, ApiError>;

    /// Best-effort bookkeeping after a successful authentication. Must not block
    /// the request or fail it; the default does nothing.
    fn touch(&self, _key: Uuid) {}
}

#[derive(Clone, Debug)]
pub struct Principal {
    /// Audit identity, e.g. `api_key:<uuid>`.
    pub actor: String,
    pub key_id: Uuid,
    /// Feeds `with_org_tx` so RLS applies to everything this request does.
    pub session: OrgSession,
    pub scopes: BTreeSet<Scope>,
}

impl Principal {
    pub fn has(&self, scope: Scope) -> bool {
        self.scopes.contains(&scope)
    }

    /// 403 error naming the missing scope.
    pub fn require(&self, scope: Scope) -> Result<(), ApiError> {
        if self.has(scope) {
            Ok(())
        } else {
            Err(ApiError::forbidden(scope.as_str()))
        }
    }
}

pub struct GeneratedKey {
    /// `kuki_<env>_<key_id>_<secret>`. Shown once, never stored.
    pub raw: String,
    pub key_id: String,
    pub secret: String,
}

impl std::fmt::Debug for GeneratedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedKey")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// 12 lowercase-hex id characters and a 64-hex-character secret from the OS RNG (UUIDv4).
/// `env` must contain `_` (checked by the caller).
pub fn generate_key(env: &str) -> GeneratedKey {
    let key_id = Uuid::new_v4().simple().to_string()[..KEY_ID_LEN].to_owned();
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    GeneratedKey {
        raw: format!("{KEY_PREFIX}_{env}_{key_id}_{secret}"),
        key_id,
        secret,
    }
}

pub struct ParsedKey<'a> {
    pub key_id: &'a str,
    pub secret: &'a str,
}

/// Strict shape check. Returns None for anything malformed or for a key
/// minted for another environment (a `test` key never works on `live`).
pub fn parse_key<'a>(raw: &'a str, expected_env: &str) -> Option<ParsedKey<'a>> {
    let mut parts = raw.splitn(4, '_');
    let prefix = parts.next()?;
    let env = parts.next()?;
    let key_id = parts.next()?;
    let secret = parts.next()?;

    if prefix != KEY_PREFIX || env != expected_env {
        return None;
    }
    if key_id.len() != KEY_ID_LEN
        || !key_id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return None;
    }
    if secret.len() != SECRET_LEN || !secret.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(ParsedKey { key_id, secret })
}

fn bearer(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        let token = token.trim();
        (!token.is_empty()).then_some(token)
    } else {
        None
    }
}

#[async_trait]
pub trait KeyHasher: Send + Sync {
    /// HMAC of the key secret under a key that never leaves the secrets backend.
    async fn hash(&self, secret: &str) -> Result<[u8; 32], ApiError>;
}

pub async fn authenticate(
    store: &dyn KeyStore,
    hasher: &dyn KeyHasher,
    key_env: &str,
    authorization: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Principal, ApiError> {
    // Malformed/unknown keys are rejected locally: no OpenBao call,
    // so an invalid-key flood cannot be amplified onto the secrets backend.
    let Some(parsed) = authorization
        .and_then(bearer)
        .and_then(|t| parse_key(t, key_env))
    else {
        return Err(ApiError::unauthenticated());
    };
    let Some(record) = store.find(parsed.key_id).await? else {
        return Err(ApiError::unauthenticated());
    };

    let candidate = hasher.hash(parsed.secret).await?;
    if !bool::from(candidate.as_slice().ct_eq(record.secret_hash.as_slice())) {
        return Err(ApiError::unauthenticated());
    }

    if record.revoked_at.is_some() || record.expires_at.is_some_and(|t| t <= now) {
        return Err(ApiError::unauthenticated());
    }

    let scopes = record
        .scopes
        .iter()
        .filter_map(|s| Scope::parse(s))
        .collect();

    Ok(Principal {
        actor: format!("api_key:{}", record.id),
        key_id: record.id,
        session: OrgSession {
            org_id: record.organisation_id,
            scope_path: record.org_unit_scope_path,
        },
        scopes,
    })
}

impl FromRequestParts<AppState> for Principal {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());

        let principal = authenticate(
            &*state.keys,
            &*state.hasher,
            state.config.key_env.as_str(),
            header,
            Utc::now(),
        )
        .await?;

        // Only authenticated requests spend tokens, so buckets are keyed by real key ids.
        if let Some(limiter) = parts.extensions.get::<Arc<RateLimiter>>() {
            let verdict = limiter.check(principal.key_id, Instant::now());
            if let Some(slot) = parts.extensions.get::<RateSlot>() {
                slot.set(verdict);
            }
            if !verdict.allowed {
                return Err(ApiError::rate_limited(verdict.retry_after_secs));
            }
        }

        state.keys.touch(principal.key_id);
        Ok(principal)
    }
}

#[cfg(test)]
pub mod testing {
    use std::collections::HashMap;

    use super::*;

    pub struct TestHasher(pub Vec<u8>);

    impl TestHasher {
        pub fn standard() -> Self {
            Self(b"test-pepper-test-pepper-test-pepper-32+".to_vec())
        }
    }

    #[async_trait]
    impl KeyHasher for TestHasher {
        async fn hash(&self, secret: &str) -> Result<[u8; 32], ApiError> {
            use hmac::{Hmac, Mac};
            let mut m = Hmac::<sha2::Sha256>::new_from_slice(&self.0).unwrap();
            m.update(secret.as_bytes());
            Ok(m.finalize().into_bytes().into())
        }
    }

    pub async fn issue_key(env: &str, hasher: &TestHasher) -> (String, String, [u8; 32]) {
        let key_id = Uuid::new_v4().simple().to_string()[..KEY_ID_LEN].to_string();
        let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let raw = format!("{KEY_PREFIX}_{env}_{key_id}_{secret}");
        let hash = hasher.hash(&secret).await.unwrap();
        (raw, key_id, hash)
    }

    pub fn record(hash: [u8; 32], scopes: &[&str]) -> KeyRecord {
        KeyRecord {
            id: Uuid::new_v4(),
            organisation_id: Uuid::new_v4(),
            secret_hash: hash.to_vec(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            org_unit_scope_path: None,
            revoked_at: None,
            expires_at: None,
        }
    }

    #[derive(Default)]
    pub struct MemoryKeys(pub HashMap<String, KeyRecord>);

    impl MemoryKeys {
        pub fn with(mut self, key_id: &str, rec: KeyRecord) -> Self {
            self.0.insert(key_id.to_string(), rec);
            self
        }
    }

    #[async_trait]
    impl KeyStore for MemoryKeys {
        async fn find(&self, key_id: &str) -> Result<Option<KeyRecord>, ApiError> {
            Ok(self.0.get(key_id).cloned())
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::testing::*;
    use super::*;
    use crate::error::ErrorCode;

    fn is_401<T>(r: &Result<T, ApiError>) -> bool {
        matches!(r, Err(e) if e.code() == ErrorCode::Unauthenticated)
    }

    async fn run(store: &MemoryKeys, header: Option<&str>) -> Result<Principal, ApiError> {
        authenticate(store, &TestHasher::standard(), "test", header, Utc::now()).await
    }

    struct CountingHasher(std::sync::atomic::AtomicUsize);

    #[async_trait]
    impl KeyHasher for CountingHasher {
        async fn hash(&self, _: &str) -> Result<[u8; 32], ApiError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok([0; 32])
        }
    }

    #[tokio::test]
    async fn malformed_and_unknown_keys_never_call_the_hasher() {
        let h = CountingHasher(Default::default());
        let (raw, _, _) = issue_key("test", &TestHasher::standard()).await;
        let store = MemoryKeys::default();
        let _ = authenticate(&store, &h, "test", Some("Bearer nope"), Utc::now()).await;
        let _ = authenticate(
            &store,
            &h,
            "test",
            Some(&format!("Bearer {raw}")),
            Utc::now(),
        )
        .await;
        assert_eq!(h.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_key_yields_principal_with_session() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let rec = record(hash, &["ingest:write", "ingest:read", "bogus:scope"]);
        let org = rec.organisation_id;
        let store = MemoryKeys::default().with(&id, rec);

        let p = run(&store, Some(&format!("Bearer {raw}"))).await.unwrap();
        assert_eq!(p.session.org_id, org);
        assert!(p.has(Scope::IngestWrite) && p.has(Scope::IngestRead));
        assert_eq!(p.scopes.len(), 2, "unknown scopes are dropped");
        assert!(p.require(Scope::DecisionsResolve).is_err());
    }

    #[tokio::test]
    async fn scheme_is_case_insensitive() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let store = MemoryKeys::default().with(&id, record(hash, &[]));
        assert!(run(&store, Some(&format!("bearer {raw}"))).await.is_ok());
    }

    #[tokio::test]
    async fn missing_or_malformed_header_is_401() {
        let store = MemoryKeys::default();
        assert!(is_401(&run(&store, None).await));
        assert!(is_401(&run(&store, Some("Basic abc")).await));
        assert!(is_401(&run(&store, Some("Bearer ")).await));
        assert!(is_401(&run(&store, Some("Bearer kuki_test_short_x")).await));
    }

    #[tokio::test]
    async fn unknown_key_id_is_401() {
        let (raw, _id, _hash) = issue_key("test", &TestHasher::standard()).await;
        let store = MemoryKeys::default();
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));
    }

    #[tokio::test]
    async fn wrong_secret_is_401() {
        let (raw, id, _hash) = issue_key("test", &TestHasher::standard()).await;
        let (_, _, other_hash) = issue_key("test", &TestHasher::standard()).await;
        let store = MemoryKeys::default().with(&id, record(other_hash, &[]));
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));
    }

    #[tokio::test]
    async fn wrong_pepper_is_401() {
        let other = TestHasher(b"another-pepper-another-pepper-32+!".to_vec());
        let (raw, id, hash) = issue_key("test", &other).await;
        let store = MemoryKeys::default().with(&id, record(hash, &[]));
        // `run` verifies with TestHasher::standard(), so the hashes differ.
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));
    }

    #[tokio::test]
    async fn key_from_other_environment_is_401() {
        let (raw, id, hash) = issue_key("live", &TestHasher::standard()).await;
        let store = MemoryKeys::default().with(&id, record(hash, &[]));
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));
    }

    #[tokio::test]
    async fn revoked_key_is_401() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let mut rec = record(hash, &[]);
        rec.revoked_at = Some(Utc::now() - Duration::hours(1));
        let store = MemoryKeys::default().with(&id, rec);
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));
    }

    #[tokio::test]
    async fn expired_key_is_401_and_future_expiry_is_ok() {
        let (raw, id, hash) = issue_key("test", &TestHasher::standard()).await;
        let mut rec = record(hash, &[]);
        rec.expires_at = Some(Utc::now() - Duration::seconds(1));
        let store = MemoryKeys::default().with(&id, rec.clone());
        assert!(is_401(&run(&store, Some(&format!("Bearer {raw}"))).await));

        rec.expires_at = Some(Utc::now() + Duration::hours(1));
        let store = MemoryKeys::default().with(&id, rec);
        assert!(run(&store, Some(&format!("Bearer {raw}"))).await.is_ok());
    }

    #[test]
    fn parse_key_rejects_bad_shapes() {
        let id = "abcdefghij12";
        let secret = "a".repeat(64);
        assert!(parse_key(&format!("kuki_test_{id}_{secret}"), "test").is_some());
        assert!(parse_key(&format!("kuki_test_{id}_{secret}"), "live").is_none());
        assert!(parse_key(&format!("kuk_test_{id}_{secret}"), "test").is_none());
        assert!(parse_key(&format!("kuki_test_ABCDEFGHIJ12_{secret}"), "test").is_none());
        assert!(parse_key(&format!("kuki_test_{id}_{}", "g".repeat(64)), "test").is_none());
        assert!(parse_key(&format!("kuki_test_{id}_{secret}_extra"), "test").is_none());
    }

    #[test]
    fn scope_names_round_trip() {
        for s in Scope::ALL {
            assert_eq!(Scope::parse(s.as_str()), Some(s));
        }
        assert_eq!(Scope::parse("decisions:write"), None);
    }

    #[test]
    fn generated_keys_parse_and_never_debug_print_the_secret() {
        let k = generate_key("test");
        let p = parse_key(&k.raw, "test").expect("generated keys must satisfy parse_key");
        assert_eq!((p.key_id, p.secret), (k.key_id.as_str(), k.secret.as_str()));
        assert_ne!(k.raw, generate_key("test").raw);
        assert!(!format!("{k:?}").contains(&k.secret));
    }
}
