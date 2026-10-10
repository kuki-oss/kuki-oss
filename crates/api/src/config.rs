use std::{fmt, net::SocketAddr};

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing required setting {0}")]
    Missing(&'static str),
    #[error("invalid {name}: {reason}")]
    Invalid { name: &'static str, reason: String },
}

/// Secret bytes that never print. Secrets(database URL, OpenBao token) must not appear in logs.
#[derive(Clone)]
pub struct Secret(Vec<u8>);

impl Secret {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
    pub fn expose_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or_default()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub database_url: Secret,
    /// OpenBao base URL, e.g. <https://bao.internal:8200>
    pub bao_addr: String,
    /// Bootstrap credential for OpenBao. From env or KUKI_BAO_TOKEN_FILE; never a repo file.
    pub bao_token: Secret,
    /// Transit key used to HMAC API-key secrets.
    pub bao_transit_key: String,
    /// "live" or "test": the second segment of API keys.
    pub key_env: String,
}

impl ApiConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut resolved = std::collections::HashMap::new();
        for name in [
            "KUKI_BIND",
            "KUKI_DATABASE_URL",
            "KUKI_BAO_ADDR",
            "KUKI_BAO_TOKEN",
            "KUKI_BAO_TRANSIT_KEY",
            "KUKI_KEY_ENV",
        ] {
            if let Some(v) = Self::read_setting(name)? {
                resolved.insert(name, v);
            }
        }
        Self::from_lookup(|k| resolved.get(k).cloned())
    }

    pub fn read_setting(name: &'static str) -> Result<Option<String>, ConfigError> {
        if let Ok(path) = std::env::var(format!("{name}_FILE")) {
            let raw = std::fs::read_to_string(&path).map_err(|e| ConfigError::Invalid {
                name,
                reason: format!("cannot read {path}: {e}"),
            })?;
            return Ok(Some(raw.trim_end_matches(['\n', '\r']).to_owned()));
        }
        Ok(std::env::var(name).ok())
    }

    /// Takes a lookup function so tests never touch process environment.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let bind = get("KUKI_BIND").unwrap_or_else(|| "0.0.0.0:8080".into());
        let bind = bind
            .parse()
            .map_err(|e: std::net::AddrParseError| ConfigError::Invalid {
                name: "KUKI_BIND",
                reason: e.to_string(),
            })?;
        let database_url =
            get("KUKI_DATABASE_URL").ok_or(ConfigError::Missing("KUKI_DATABASE_URL"))?;
        let bao_addr = get("KUKI_BAO_ADDR").ok_or(ConfigError::Missing("KUKI_BAO_ADDR"))?;
        if !(bao_addr.starts_with("http://") || bao_addr.starts_with("https://")) {
            return Err(ConfigError::Invalid {
                name: "KUKI_BAO_ADDR",
                reason: "must start with http:// or https://".into(),
            });
        }
        let bao_token = get("KUKI_BAO_TOKEN").ok_or(ConfigError::Missing("KUKI_BAO_TOKEN"))?;
        if bao_token.trim().is_empty() {
            return Err(ConfigError::Invalid {
                name: "KUKI_BAO_TOKEN",
                reason: "must not be empty".into(),
            });
        }
        let bao_transit_key = get("KUKI_BAO_TRANSIT_KEY").unwrap_or_else(|| "kuki-api-keys".into());
        let key_env = get("KUKI_KEY_ENV").unwrap_or_else(|| "live".into());
        if !matches!(key_env.as_str(), "live" | "test") {
            return Err(ConfigError::Invalid {
                name: "KUKI_KEY_ENV",
                reason: "must be live or test".into(),
            });
        }
        Ok(Self {
            bind,
            database_url: Secret::new(database_url.into_bytes()),
            bao_addr,
            bao_token: Secret::new(bao_token.into_bytes()),
            bao_transit_key,
            key_env,
        })
    }
}

#[cfg(test)]
impl ApiConfig {
    pub fn for_tests() -> Self {
        Self::from_lookup(|k| match k {
            "KUKI_DATABASE_URL" => Some("postgres://unused".into()),
            "KUKI_BAO_ADDR" => Some("http://127.0.0.1:8200".into()),
            "KUKI_BAO_TOKEN" => Some("test-token".into()),
            "KUKI_KEY_ENV" => Some("test".into()),
            _ => None,
        })
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    }

    const TOKEN: &str = "s.test-token-value";

    fn base() -> [(&'static str, &'static str); 3] {
        [
            ("KUKI_DATABASE_URL", "postgres://x"),
            ("KUKI_BAO_ADDR", "http://127.0.0.1:8200"),
            ("KUKI_BAO_TOKEN", TOKEN),
        ]
    }

    #[test]
    fn defaults_and_required_settings() {
        let c = ApiConfig::from_lookup(env(&base())).unwrap();
        assert_eq!(c.bind.port(), 8080);
        assert_eq!(c.bao_transit_key, "kuki-api-keys");
        assert_eq!(
            ApiConfig::from_lookup(env(&[
                ("KUKI_BAO_ADDR", "http://x"),
                ("KUKI_BAO_TOKEN", TOKEN)
            ]))
            .unwrap_err(),
            ConfigError::Missing("KUKI_DATABASE_URL")
        );
        assert_eq!(
            ApiConfig::from_lookup(env(&[
                ("KUKI_DATABASE_URL", "x"),
                ("KUKI_BAO_TOKEN", TOKEN)
            ]))
            .unwrap_err(),
            ConfigError::Missing("KUKI_BAO_ADDR")
        );
    }

    #[test]
    fn bad_bao_addr_is_rejected_and_secrets_never_print() {
        let err = ApiConfig::from_lookup(env(&[
            ("KUKI_DATABASE_URL", "x"),
            ("KUKI_BAO_ADDR", "bao:8200"),
            ("KUKI_BAO_TOKEN", TOKEN),
        ]))
        .unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                name: "KUKI_BAO_ADDR",
                ..
            }
        ));
        let c = ApiConfig::from_lookup(env(&base())).unwrap();
        assert!(!format!("{c:?}").contains(TOKEN));
        assert!(!format!("{c:?}").contains("postgres://"));
    }
}
