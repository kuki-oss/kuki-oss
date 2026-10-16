//! `kuki actions run`: the action executor service.
//!
//! Claims queued action executions, routes each to the organisation's registered
//! webhook endpoint, and settles the execution and its recommendation.
//!
//! Settings (each also accepts `NAME_FILE`):
//!   KUKI_DATABASE_URL, KUKI_BAO_ADDR, KUKI_BAO_TOKEN   as for the API
//!   KUKI_BAO_KV_MOUNT              KV v2 mount holding signing secrets      (default "secret")
//!   KUKI_ACTIONS_SECRET_TTL_SECS   how long a secret is cached, 0..=3600    (default 60)
//!   KUKI_WEBHOOK_TIMEOUT_SECS      per-request timeout, 1..=25              (default 20)
//!   KUKI_WEBHOOK_ALLOW_HTTP        "true" permits http:// endpoints (dev)   (default false)

use std::{collections::HashMap, sync::Arc, time::Duration};

use kuki_actions::{
    CachedSecrets, Config as ExecutorConfig, Executor, RegistryRouter, SecretSource, WebhookConfig,
    valid_reference,
};
use tokio::sync::watch;

use crate::bao::{BaoClient, BaoKv, BaoReadiness};
use crate::config::{ApiConfig, ConfigError};
use crate::serve::{ServeError, connect_app_pool, shutdown_signal};
use crate::state::Readiness;

#[derive(Debug, Clone)]
pub struct ActionsConfig {
    pub kv_mount: String,
    pub secret_ttl: Duration,
    pub webhook: WebhookConfig,
}

impl ActionsConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut resolved = HashMap::new();
        for name in [
            "KUKI_BAO_KV_MOUNT",
            "KUKI_ACTIONS_SECRET_TTL_SECS",
            "KUKI_WEBHOOK_TIMEOUT_SECS",
            "KUKI_WEBHOOK_ALLOW_HTTP",
        ] {
            if let Some(v) = ApiConfig::read_setting(name)? {
                resolved.insert(name, v);
            }
        }
        Self::from_lookup(|k| resolved.get(k).cloned())
    }

    /// Takes a lookup function so tests never touch process environment.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let kv_mount = get("KUKI_BAO_KV_MOUNT").unwrap_or_else(|| "secret".into());
        if !valid_reference(&kv_mount) {
            return Err(ConfigError::Invalid {
                name: "KUKI_BAO_KV_MOUNT",
                reason: "must be a plain path such as `secret`".into(),
            });
        }
        let secret_ttl = seconds(&get, "KUKI_ACTIONS_SECRET_TTL_SECS", 60, 0, 3600)?;
        // Must stay below the executor's per-attempt timeout (30 s).
        let timeout = seconds(&get, "KUKI_WEBHOOK_TIMEOUT_SECS", 20, 1, 25)?;
        let allow_insecure_http = match get("KUKI_WEBHOOK_ALLOW_HTTP").as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => {
                return Err(ConfigError::Invalid {
                    name: "KUKI_WEBHOOK_ALLOW_HTTP",
                    reason: format!("expected true or false, got {other:?}"),
                });
            }
        };
        Ok(Self {
            kv_mount,
            secret_ttl,
            webhook: WebhookConfig { timeout, allow_insecure_http, ..WebhookConfig::default() },
        })
    }
}

fn seconds(
    get: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<Duration, ConfigError> {
    let n = match get(name) {
        None => default,
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|e| ConfigError::Invalid { name, reason: e.to_string() })?,
    };
    if !(min..=max).contains(&n) {
        return Err(ConfigError::Invalid { name, reason: format!("must be between {min} and {max}") });
    }
    Ok(Duration::from_secs(n))
}

pub async fn run_actions(api: ApiConfig) -> Result<(), ServeError> {
    let cfg = ActionsConfig::from_env().map_err(|e| ServeError::Config(e.to_string()))?;
    let pool = connect_app_pool(api.database_url.expose_str(), 8).await?;

    let bao = Arc::new(BaoClient::new(api.bao_addr.clone(), api.bao_token.clone())?);
    // Fail fast: without the secrets backend no webhook can be signed.
    BaoReadiness(bao.clone())
        .check()
        .await
        .map_err(|_| ServeError::Config("secrets backend is not ready".into()))?;

    let kv = BaoKv::new(bao, cfg.kv_mount.clone()).map_err(ServeError::Config)?;
    let secrets: Arc<dyn SecretSource> = Arc::new(CachedSecrets::new(kv, cfg.secret_ttl));
    let router = RegistryRouter::new(pool.clone(), secrets, &cfg.webhook).map_err(ServeError::Config)?;
    let executor =
        Executor::new(pool, Arc::new(router), ExecutorConfig::default()).map_err(ServeError::Config)?;

    if cfg.webhook.allow_insecure_http {
        tracing::warn!("http:// webhook endpoints are allowed (KUKI_WEBHOOK_ALLOW_HTTP=true)");
    }
    tracing::info!(
        kv_mount = %cfg.kv_mount,
        secret_ttl_secs = cfg.secret_ttl.as_secs(),
        webhook_timeout_secs = cfg.webhook.timeout.as_secs(),
        "kuki action executor running"
    );

    let (stop, stopped) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop.send(true);
    });
    executor.run(stopped).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    }

    #[test]
    fn defaults() {
        let c = ActionsConfig::from_lookup(env(&[])).unwrap();
        assert_eq!(c.kv_mount, "secret");
        assert_eq!(c.secret_ttl, Duration::from_secs(60));
        assert_eq!(c.webhook.timeout, Duration::from_secs(20));
        assert!(!c.webhook.allow_insecure_http);
    }

    #[test]
    fn overrides() {
        let c = ActionsConfig::from_lookup(env(&[
            ("KUKI_BAO_KV_MOUNT", "kuki-kv"),
            ("KUKI_ACTIONS_SECRET_TTL_SECS", "0"),
            ("KUKI_WEBHOOK_TIMEOUT_SECS", "25"),
            ("KUKI_WEBHOOK_ALLOW_HTTP", "true"),
        ]))
        .unwrap();
        assert_eq!(c.kv_mount, "kuki-kv");
        assert_eq!(c.secret_ttl, Duration::ZERO);
        assert_eq!(c.webhook.timeout, Duration::from_secs(25));
        assert!(c.webhook.allow_insecure_http);
    }

    #[test]
    fn nonsense_is_refused_and_names_the_setting() {
        for (name, value) in [
            ("KUKI_BAO_KV_MOUNT", "../sys"),
            ("KUKI_ACTIONS_SECRET_TTL_SECS", "abc"),
            ("KUKI_ACTIONS_SECRET_TTL_SECS", "3601"),
            ("KUKI_WEBHOOK_TIMEOUT_SECS", "0"),
            ("KUKI_WEBHOOK_TIMEOUT_SECS", "26"),
            ("KUKI_WEBHOOK_ALLOW_HTTP", "yes"),
        ] {
            let err = ActionsConfig::from_lookup(env(&[(name, value)])).unwrap_err();
            assert!(
                matches!(&err, ConfigError::Invalid { name: n, .. } if *n == name),
                "{name}={value}: {err:?}"
            );
        }
    }
}
