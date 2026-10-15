//! Webhook signing: `Kuki-Signature: t=<unix>,v1=<hex>`.
//!
//! `v1 = HMAC-SHA256(secret, "<t>.<body>")`. Receivers must reject signatures
//! whose timestamp is more than [`REPLAY_WINDOW_SECS`] from their clock.
//! Several `v1=` entries may be present so a secret can be rotated without
//! downtime. The same signer is used for status-event fan-out and for the
//! actions webhook adapter.

use hmac::{Hmac, Mac};
use sha2::Sha256;

pub const HEADER: &str = "Kuki-Signature";
pub const REPLAY_WINDOW_SECS: i64 = 300;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyError {
    /// Header missing `t=` or any well-formed `v1=`.
    Malformed,
    /// No `v1` matched.
    Mismatch,
    /// Signature is valid but the timestamp is outside the replay window.
    Outside,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Malformed => "malformed signature header",
            Self::Mismatch => "signature mismatch",
            Self::Outside => "timestamp outside the replay window",
        })
    }
}

impl std::error::Error for VerifyError {}

fn mac(secret: &[u8], timestamp: i64, body: &[u8]) -> HmacSha256 {
    // HMAC accepts keys of any length, so this cannot fail.
    let mut m = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    m.update(timestamp.to_string().as_bytes());
    m.update(b".");
    m.update(body);
    m
}

/// Build the header value for `body`, signed at `timestamp` (unix seconds).
pub fn sign(secret: &[u8], timestamp: i64, body: &[u8]) -> String {
    let tag = mac(secret, timestamp, body).finalize().into_bytes();
    format!("t={timestamp},v1={}", hex::encode(tag))
}

/// Verify a header against `body` at `now` (unix seconds).
/// The tag comparison is constant-time.
pub fn verify(secret: &[u8], header: &str, body: &[u8], now: i64) -> Result<(), VerifyError> {
    let mut timestamp = None;
    let mut tags: Vec<Vec<u8>> = Vec::new();
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => timestamp = v.parse::<i64>().ok(),
            Some(("v1", v)) => {
                if let Ok(b) = hex::decode(v) {
                    tags.push(b);
                }
            }
            _ => {} // unknown keys are ignored for forward compatibility
        }
    }
    let timestamp = timestamp.ok_or(VerifyError::Malformed)?;
    if tags.is_empty() {
        return Err(VerifyError::Malformed);
    }

    let expected = mac(secret, timestamp, body);
    let matched = tags.iter().fold(false, |acc, t| {
        acc | expected.clone().verify_slice(t).is_ok()
    });
    if !matched {
        return Err(VerifyError::Mismatch);
    }
    if now.abs_diff(timestamp) > REPLAY_WINDOW_SECS as u64 {
        return Err(VerifyError::Outside);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"whsec_test";
    const BODY: &[u8] = br#"{"a":1}"#;
    const T: i64 = 1_700_000_000;
    // Independent reference: python hmac.new(b"whsec_test", b'1700000000.{"a":1}', sha256)
    const VECTOR: &str = "38877139021993b830af32feea6e18a8da83eb2f6e49ee50bd9e4cf4ca4d3789";

    #[test]
    fn matches_an_independently_computed_vector() {
        assert_eq!(sign(SECRET, T, BODY), format!("t={T},v1={VECTOR}"));
    }

    #[test]
    fn round_trips() {
        let h = sign(SECRET, T, BODY);
        assert_eq!(verify(SECRET, &h, BODY, T), Ok(()));
    }

    #[test]
    fn rejects_a_tampered_body_and_a_wrong_secret() {
        let h = sign(SECRET, T, BODY);
        assert_eq!(
            verify(SECRET, &h, br#"{"a":2}"#, T),
            Err(VerifyError::Mismatch)
        );
        assert_eq!(verify(b"other", &h, BODY, T), Err(VerifyError::Mismatch));
    }

    #[test]
    fn rejects_a_tampered_timestamp() {
        let h = sign(SECRET, T, BODY).replace(&format!("t={T}"), &format!("t={}", T + 1));
        assert_eq!(verify(SECRET, &h, BODY, T), Err(VerifyError::Mismatch));
    }

    #[test]
    fn replay_window_is_300_seconds_either_side() {
        let h = sign(SECRET, T, BODY);
        assert_eq!(verify(SECRET, &h, BODY, T + 300), Ok(()));
        assert_eq!(verify(SECRET, &h, BODY, T - 300), Ok(()));
    }

    #[test]
    fn rejects_stale_signatures() {
        let h = sign(SECRET, T, BODY);
        assert_eq!(verify(SECRET, &h, BODY, T + 301), Err(VerifyError::Outside));
    }

    #[test]
    fn rejects_signatures_from_the_future() {
        let h = sign(SECRET, T, BODY);
        assert_eq!(verify(SECRET, &h, BODY, T - 301), Err(VerifyError::Outside));
    }

    #[test]
    fn malformed_headers_are_rejected() {
        for h in ["", "garbage", "t=abc,v1=00", "v1=00", "t=1", "t=1,v1=zz"] {
            assert_eq!(
                verify(SECRET, h, BODY, T),
                Err(VerifyError::Malformed),
                "{h}"
            );
        }
    }

    #[test]
    fn any_one_of_several_v1_tags_may_match_for_rotation() {
        let new = sign(b"new-secret", T, BODY);
        let old_tag = sign(SECRET, T, BODY)
            .split("v1=")
            .nth(1)
            .unwrap()
            .to_string();
        let both = format!("{new},v1={old_tag}");
        assert_eq!(verify(SECRET, &both, BODY, T), Ok(()));
        assert_eq!(verify(b"new-secret", &both, BODY, T), Ok(()));
    }

    #[test]
    fn unknown_keys_are_ignored() {
        let h = format!("{},v2=ff,extra=1", sign(SECRET, T, BODY));
        assert_eq!(verify(SECRET, &h, BODY, T), Ok(()));
    }
}
