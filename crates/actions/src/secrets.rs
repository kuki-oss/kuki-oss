//! Where webhook signing secrets come from.
//!
//! The database stores only a *reference* (an OpenBao KV path). The value is
//! read at delivery time through [`SecretSource`], held in memory briefly, and
//! never printed: [`SecretBytes`] redacts itself in `Debug`.

use std::{
    collections::HashMap,
    fmt,
    sync::Mutex,
    time::{Duration, Instant},
};

use futures::{FutureExt, future::BoxFuture};

/// Secret bytes that never print.
#[derive(Clone)]
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBytes(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretError {
    /// The secret cannot be used as configured (bad reference, no such secret,
    /// field missing). Retrying will not help until someone fixes the setup.
    Missing(String),
    /// The secrets backend could not be reached or refused us. Worth retrying.
    Unavailable,
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(why) => write!(f, "signing secret unusable: {why}"),
            Self::Unavailable => f.write_str("secrets backend unavailable"),
        }
    }
}

impl std::error::Error for SecretError {}

pub trait SecretSource: Send + Sync {
    fn read<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>>;
}

/// Same shape the database enforces for `webhook_endpoints.secret_ref`:
/// slash-separated segments of `[A-Za-z0-9_-]`, no empty segments, no dots.
pub fn valid_reference(reference: &str) -> bool {
    !reference.is_empty()
        && reference.len() <= 256
        && reference.split('/').all(|s| {
            !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
}

/// Fixed in-memory secrets, for tests and local dev.
#[derive(Default)]
pub struct StaticSecrets(HashMap<String, Vec<u8>>);

impl StaticSecrets {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with(mut self, reference: &str, value: &[u8]) -> Self {
        self.0.insert(reference.to_owned(), value.to_vec());
        self
    }
}

impl SecretSource for StaticSecrets {
    fn read<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>> {
        let found = self.0.get(reference).cloned();
        async move {
            found
                .map(SecretBytes::new)
                .ok_or_else(|| SecretError::Missing(format!("no secret at {reference}")))
        }
        .boxed()
    }
}

/// Keeps successful reads for `ttl` so a burst of deliveries is not a burst of
/// backend calls. Failures are never cached. Rotation takes effect within `ttl`.
pub struct CachedSecrets<S> {
    inner: S,
    ttl: Duration,
    cache: Mutex<HashMap<String, (Instant, SecretBytes)>>,
}

impl<S: SecretSource> CachedSecrets<S> {
    pub fn new(inner: S, ttl: Duration) -> Self {
        Self { inner, ttl, cache: Mutex::new(HashMap::new()) }
    }

    fn hit(&self, reference: &str) -> Option<SecretBytes> {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .get(reference)
            .filter(|(at, _)| at.elapsed() < self.ttl)
            .map(|(_, v)| v.clone())
    }

    fn keep(&self, reference: &str, value: SecretBytes) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache.retain(|_, (at, _)| at.elapsed() < self.ttl); // bounded by live entries
        cache.insert(reference.to_owned(), (Instant::now(), value));
    }
}

impl<S: SecretSource> SecretSource for CachedSecrets<S> {
    fn read<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>> {
        async move {
            if let Some(v) = self.hit(reference) {
                return Ok(v);
            }
            let v = self.inner.read(reference).await?;
            self.keep(reference, v.clone());
            Ok(v)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[test]
    fn reference_shapes_match_the_database_rule() {
        for ok in ["a", "kuki/webhooks/acme/draft_po", "a-b/c_d/E9"] {
            assert!(valid_reference(ok), "{ok}");
        }
        for bad in ["", "/a", "a/", "a//b", "../x", "a/../b", "a b", "a.b", "a\\b", &"a".repeat(257)] {
            assert!(!valid_reference(bad), "{bad:?}");
        }
    }

    #[test]
    fn secrets_never_print() {
        let s = SecretBytes::new(b"hunter2".to_vec());
        assert!(!format!("{s:?}").contains("hunter2"));
        assert_eq!(s.expose(), b"hunter2");
    }

    #[tokio::test]
    async fn the_static_source_finds_or_reports_missing() {
        let src = StaticSecrets::new().with("a/b", b"v");
        assert_eq!(src.read("a/b").await.unwrap().expose(), b"v");
        assert!(matches!(src.read("a/c").await, Err(SecretError::Missing(_))));
    }

    struct Counting(Arc<AtomicUsize>);

    impl SecretSource for Counting {
        fn read<'a>(&'a self, reference: &'a str) -> BoxFuture<'a, Result<SecretBytes, SecretError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let out = if reference == "bad" {
                Err(SecretError::Unavailable)
            } else {
                Ok(SecretBytes::new(reference.as_bytes().to_vec()))
            };
            async move { out }.boxed()
        }
    }

    #[tokio::test]
    async fn repeat_reads_within_the_ttl_hit_the_backend_once() {
        let n = Arc::new(AtomicUsize::new(0));
        let src = CachedSecrets::new(Counting(n.clone()), Duration::from_secs(3600));
        for _ in 0..3 {
            assert_eq!(src.read("a").await.unwrap().expose(), b"a");
        }
        assert_eq!(n.load(Ordering::SeqCst), 1);
        src.read("b").await.unwrap();
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn expired_entries_are_refetched_and_failures_are_never_cached() {
        let n = Arc::new(AtomicUsize::new(0));
        let expired = CachedSecrets::new(Counting(n.clone()), Duration::ZERO);
        expired.read("a").await.unwrap();
        expired.read("a").await.unwrap();
        assert_eq!(n.load(Ordering::SeqCst), 2);

        let n = Arc::new(AtomicUsize::new(0));
        let src = CachedSecrets::new(Counting(n.clone()), Duration::from_secs(3600));
        assert_eq!(src.read("bad").await.unwrap_err(), SecretError::Unavailable);
        assert_eq!(src.read("bad").await.unwrap_err(), SecretError::Unavailable);
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }
}
