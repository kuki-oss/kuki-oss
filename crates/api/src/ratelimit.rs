//! Per-API-key rate limiting: an in-process token bucket.
//!
//! LIMITS (also documented in api/openapi.yaml):
//! - Buckets live in this process's memory. One instance enforces the limit exactly; with
//!   several replicas each enforces its own, so the effective limit is multiplied.
//! - A restart refills every bucket.
//! - Only authenticated requests are counted. Requests with bad credentials are not (they never
//!   reach the limiter); use a reverse proxy for per-IP limits.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

use crate::config::ApiConfig;

const LIMIT: HeaderName = HeaderName::from_static("ratelimit-limit");
const REMAINING: HeaderName = HeaderName::from_static("ratelimit-remaining");
/// Idle, full buckets are equivalent to absent ones and are pruned past this size.
const PRUNE_AT: usize = 10_000;

pub const DEFAULT_BURST: u32 = 200;
pub const DEFAULT_REFILL_PER_SECOND: f64 = 100.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Verdict {
    pub allowed: bool,
    /// Bucket size.
    pub limit: u32,
    /// Whole requests left after this one.
    pub remaining: u32,
    /// When denied: seconds until a request would be allowed (at least 1).
    pub retry_after_secs: u64,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct RateLimiter {
    burst: f64,
    refill_per_sec: f64,
    buckets: Mutex<HashMap<Uuid, Bucket>>,
}

impl RateLimiter {
    /// `None` when `burst` is 0 (disabled) or the refill rate is not a positive finite number.
    pub fn new(burst: u32, refill_per_sec: f64) -> Option<Self> {
        if burst == 0 || !refill_per_sec.is_finite() || refill_per_sec <= 0.0 {
            return None;
        }
        Some(Self {
            burst: f64::from(burst),
            refill_per_sec,
            buckets: Mutex::default(),
        })
    }

    /// Takes one token for `key` if there is one. `now` is a parameter so tests control time.
    pub fn check(&self, key: Uuid, now: Instant) -> Verdict {
        let mut map = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= PRUNE_AT {
            let (burst, rate) = (self.burst, self.refill_per_sec);
            map.retain(|_, b| {
                let idle = now.saturating_duration_since(b.last).as_secs_f64();
                b.tokens + idle * rate < burst
            });
        }
        let b = map.entry(key).or_insert(Bucket {
            tokens: self.burst,
            last: now,
        });
        let idle = now.saturating_duration_since(b.last).as_secs_f64();
        b.tokens = (b.tokens + idle * self.refill_per_sec).min(self.burst);
        b.last = now;

        let limit = self.burst as u32;
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Verdict {
                allowed: true,
                limit,
                remaining: b.tokens.floor() as u32,
                retry_after_secs: 0,
            }
        } else {
            let wait = (1.0 - b.tokens) / self.refill_per_sec;
            Verdict {
                allowed: false,
                limit,
                remaining: 0,
                retry_after_secs: wait.ceil().max(1.0) as u64,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateLimitConfig {
    /// 0 disables rate limiting
    pub burst: u32,
    pub refill_per_sec: f64,
}

impl RateLimitConfig {
    pub fn parse(burst: Option<&str>, refill: Option<&str>) -> Result<Self, String> {
        let burst = match burst {
            None => DEFAULT_BURST,
            Some(s) => s
                .trim()
                .parse::<u32>()
                .ok()
                .filter(|n| *n <= 1_000_000)
                .ok_or("KUKI_RATE_LIMIT_BURST must be an integer from 0 to 1000000 (0 disables rate limiting)")?,
        };
        let refill_per_sec = match refill {
            None => DEFAULT_REFILL_PER_SECOND,
            Some(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite() && *n > 0.0 && *n <= 1_000_000.0)
                .ok_or("KUKI_RATE_LIMIT_REFILL_PER_SECOND must be a number greater than 0 and at most 1000000")?,
        };
        Ok(Self {
            burst,
            refill_per_sec,
        })
    }

    pub fn from_env() -> Result<Self, String> {
        let burst = ApiConfig::read_setting("KUKI_RATE_LIMIT_BURST").map_err(|e| e.to_string())?;
        let refill = ApiConfig::read_setting("KUKI_RATE_LIMIT_REFILL_PER_SECOND")
            .map_err(|e| e.to_string())?;
        Self::parse(burst.as_deref(), refill.as_deref())
    }

    pub fn limiter(&self) -> Option<RateLimiter> {
        RateLimiter::new(self.burst, self.refill_per_sec)
    }
}

/// Filled by the `Principal` extractor, read by [`report`] once the response exists.
#[derive(Clone, Default)]
pub struct RateSlot(Arc<Mutex<Option<Verdict>>>);

impl RateSlot {
    pub(crate) fn set(&self, v: Verdict) {
        *self.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(v);
    }
    fn take(&self) -> Option<Verdict> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

pub async fn report(mut req: Request, next: Next) -> Response {
    let slot = RateSlot::default();
    req.extensions_mut().insert(slot.clone());
    let mut res = next.run(req).await;
    if let Some(v) = slot.take() {
        let h = res.headers_mut();
        h.insert(LIMIT, HeaderValue::from(v.limit));
        h.insert(REMAINING, HeaderValue::from(v.remaining));
    }
    res
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn limiter(burst: u32, refill: f64) -> RateLimiter {
        RateLimiter::new(burst, refill).unwrap()
    }

    #[test]
    fn the_bucket_empties_then_refills() {
        let (l, k, t0) = (limiter(3, 1.0), Uuid::new_v4(), Instant::now());
        let remaining: Vec<u32> = (0..3)
            .map(|_| {
                let v = l.check(k, t0);
                assert!(v.allowed);
                v.remaining
            })
            .collect();
        assert_eq!(remaining, [2, 1, 0]);

        let denied = l.check(k, t0);
        assert_eq!(
            (
                denied.allowed,
                denied.remaining,
                denied.retry_after_secs,
                denied.limit
            ),
            (false, 0, 1, 3)
        );
        assert!(!l.check(k, t0 + Duration::from_millis(500)).allowed);
        assert!(l.check(k, t0 + Duration::from_millis(1000)).allowed);
    }

    #[test]
    fn idle_time_never_banks_more_than_the_burst() {
        let (l, k, t0) = (limiter(3, 1.0), Uuid::new_v4(), Instant::now());
        assert!(l.check(k, t0).allowed);
        let v = l.check(k, t0 + Duration::from_secs(3600));
        assert!(v.allowed);
        assert_eq!(v.remaining, 2);
    }

    #[test]
    fn keys_have_separate_buckets() {
        let (l, t0) = (limiter(1, 0.001), Instant::now());
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(l.check(a, t0).allowed);
        assert!(!l.check(a, t0).allowed);
        assert!(l.check(b, t0).allowed);
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        let (l, k, t0) = (limiter(1, 0.5), Uuid::new_v4(), Instant::now());
        assert!(l.check(k, t0).allowed);
        assert_eq!(l.check(k, t0).retry_after_secs, 2);
        let fast = limiter(1, 1000.0);
        assert!(fast.check(k, t0).allowed);
        assert_eq!(fast.check(k, t0).retry_after_secs, 1, "never advertises 0");
    }

    #[test]
    fn configuration_defaults_disables_and_rejects_nonsense() {
        let d = RateLimitConfig::parse(None, None).unwrap();
        assert_eq!(
            (d.burst, d.refill_per_sec),
            (DEFAULT_BURST, DEFAULT_REFILL_PER_SECOND)
        );
        assert!(d.limiter().is_some());
        assert!(
            RateLimitConfig::parse(Some("0"), None)
                .unwrap()
                .limiter()
                .is_none()
        );
        assert_eq!(
            RateLimitConfig::parse(Some(" 5 "), Some("0.5"))
                .unwrap()
                .burst,
            5
        );
        for (b, r) in [
            ("x", "1"),
            ("-1", "1"),
            ("2000000", "1"),
            ("5", "0"),
            ("5", "nan"),
            ("5", "-2"),
            ("5", "inf"),
        ] {
            assert!(RateLimitConfig::parse(Some(b), Some(r)).is_err(), "{b} {r}");
        }
    }
}
