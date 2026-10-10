//! Shared relay pieces: `Cursor`, `PageInfo`, page-size and ltree validation.

use async_graphql::{
    Error, ErrorExtensions, ID, InputValueError, InputValueResult, Result, Scalar, ScalarType,
    SimpleObject, Value,
};
use uuid::Uuid;

pub const DEFAULT_PAGE: i64 = 25;
pub const MAX_PAGE: i64 = 100;

/// Opaque pagination cursor. Never parse it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor(pub String);

#[Scalar]
impl ScalarType for Cursor {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(Cursor(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

#[derive(SimpleObject)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<Cursor>,
    pub end_cursor: Option<Cursor>,
}

pub fn validation(msg: &str, reason: Option<&'static str>) -> Error {
    Error::new(msg).extend_with(|_, ext| {
        ext.set("code", "VALIDATION_FAILED");
        if let Some(r) = reason {
            ext.set("reason", r);
        }
    })
}

pub fn conflict(msg: &str) -> Error {
    Error::new(msg).extend_with(|_, ext| {
        ext.set("code", "CONFLICT");
    })
}

pub fn parse_uuid(id: &ID, what: &str) -> Result<Uuid> {
    Uuid::parse_str(&id.0).map_err(|_| validation(&format!("{what} is not a valid UUID"), None))
}

const ID_PREFIX: &str = "id1.";

pub fn encode_id_cursor(id: Uuid) -> Cursor {
    Cursor(format!("{ID_PREFIX}{}", id.simple()))
}

pub fn decode_id_cursor(c: &Cursor) -> Result<Uuid> {
    let bad = || validation("invalid cursor", None);
    let s = c.0.strip_prefix(ID_PREFIX).ok_or_else(bad)?;
    if s.len() != 32 {
        return Err(bad());
    }
    Uuid::parse_str(s).map_err(|_| bad())
}

const NAME_PREFIX: &str = "nm1.";
const NAME_VERSION_PREFIX: &str = "nv1.";

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<String> {
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

pub fn encode_name_cursor(name: &str) -> Cursor {
    Cursor(format!("{NAME_PREFIX}{}", hex(name)))
}

pub fn decode_name_cursor(c: &Cursor) -> Result<String> {
    c.0.strip_prefix(NAME_PREFIX)
        .and_then(unhex)
        .filter(|n| !n.is_empty())
        .ok_or_else(|| validation("invalid cursor", None))
}

pub fn encode_name_version_cursor(name: &str, version: i32) -> Cursor {
    Cursor(format!("{NAME_VERSION_PREFIX}{}.{version}", hex(name)))
}

pub fn decode_name_version_cursor(c: &Cursor) -> Result<(String, i32)> {
    let parsed = c.0.strip_prefix(NAME_VERSION_PREFIX).and_then(|s| {
        let (n, v) = s.rsplit_once('.')?;
        let name = unhex(n).filter(|n| !n.is_empty())?;
        Some((name, v.parse::<i32>().ok()?))
    });
    parsed.ok_or_else(|| validation("invalid cursor", None))
}

const EVENT_PREFIX: &str = "ev1.";

/// Postgres timestamps have microsecond precision, so this is lossless.
pub fn encode_event_cursor(at: chrono::DateTime<chrono::Utc>, id: Uuid) -> Cursor {
    Cursor(format!(
        "{EVENT_PREFIX}{}.{}",
        at.timestamp_micros(),
        id.simple()
    ))
}

pub fn decode_event_cursor(c: &Cursor) -> Result<(chrono::DateTime<chrono::Utc>, Uuid)> {
    let bad = || validation("invalid cursor", None);
    let s = c.0.strip_prefix(EVENT_PREFIX).ok_or_else(bad)?;
    let (micros, id) = s.split_once('.').ok_or_else(bad)?;
    if id.len() != 32 {
        return Err(bad());
    }
    let micros: i64 = micros.parse().map_err(|_| bad())?;
    let at = chrono::DateTime::from_timestamp_micros(micros).ok_or_else(bad)?;
    let id = Uuid::parse_str(id).map_err(|_| bad())?;
    Ok((at, id))
}

/// `first` default 25, max 100. Larger values fail with PAGE_SIZE_EXCEEDED.
pub fn page_size(first: Option<i32>) -> Result<i64> {
    match first {
        None => Ok(DEFAULT_PAGE),
        Some(n) if n < 1 => Err(validation("first must be at least 1", None)),
        Some(n) if i64::from(n) > MAX_PAGE => Err(validation(
            "first must be at most 100",
            Some("PAGE_SIZE_EXCEEDED"),
        )),
        Some(n) => Ok(i64::from(n)),
    }
}

/// ltree labels: alphanumerics, `_` and `-`, dot-separated.
pub fn valid_ltree(p: &str) -> bool {
    !p.is_empty()
        && p.len() <= 2000
        && p.split('.').all(|l| {
            !l.is_empty()
                && l.len() <= 1000
                && l.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
}

pub fn check_ltree(p: &str, what: &str) -> Result<()> {
    if valid_ltree(p) {
        Ok(())
    } else {
        Err(validation(
            &format!("{what} is not a valid ltree path"),
            None,
        ))
    }
}

const PATH_PREFIX: &str = "ou1.";

pub fn encode_path_cursor(path: &str) -> Cursor {
    let hex: String = path.bytes().map(|b| format!("{b:02x}")).collect();
    Cursor(format!("{PATH_PREFIX}{hex}"))
}

pub fn decode_path_cursor(c: &Cursor) -> Result<String> {
    let bad = || validation("invalid cursor", None);
    let hex = c.0.strip_prefix(PATH_PREFIX).ok_or_else(bad)?;
    if hex.is_empty() || hex.len() % 2 != 0 || !hex.is_ascii() {
        return Err(bad());
    }
    let bytes: Result<Vec<u8>, _> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect();
    let path = String::from_utf8(bytes.map_err(|_| bad())?).map_err(|_| bad())?;
    if valid_ltree(&path) {
        Ok(path)
    } else {
        Err(bad())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_size_rules() {
        assert_eq!(page_size(None).unwrap(), 25);
        assert_eq!(page_size(Some(100)).unwrap(), 100);
        assert!(page_size(Some(0)).is_err());
        let e = page_size(Some(101)).unwrap_err();
        let ext = e.extensions.unwrap();
        assert_eq!(ext.get("reason"), Some(&Value::from("PAGE_SIZE_EXCEEDED")));
    }

    #[test]
    fn cursor_round_trips_and_rejects_garbage() {
        let c = encode_path_cursor("hq.east.nairobi");
        assert_eq!(decode_path_cursor(&c).unwrap(), "hq.east.nairobi");
        for bad in ["", "ou1.", "ou1.zz", "ou1.abc", "x.6871", "ou1.2e2e"] {
            assert!(decode_path_cursor(&Cursor(bad.into())).is_err(), "{bad}");
        }
    }

    #[test]
    fn id_cursor_round_trips_and_rejects_garbage() {
        let id = Uuid::from_u128(42);
        assert_eq!(decode_id_cursor(&encode_id_cursor(id)).unwrap(), id);
        let hyphenated = format!("id1.{id}");
        for bad in ["", "id1.", "id1.2a", "x.00", "id1.zz", hyphenated.as_str()] {
            assert!(decode_id_cursor(&Cursor(bad.into())).is_err(), "{bad}");
        }
    }

    #[test]
    fn name_cursors_round_trip_and_reject_garbage() {
        let c = encode_name_cursor("pricing.v1/ünï");
        assert_eq!(decode_name_cursor(&c).unwrap(), "pricing.v1/ünï");

        let c = encode_name_version_cursor("a.b", 7);
        assert_eq!(
            decode_name_version_cursor(&c).unwrap(),
            ("a.b".to_owned(), 7)
        );

        for bad in ["", "nm1.", "nm1.zz", "nm1.6", "nm1.+f", "id1.00"] {
            assert!(decode_name_cursor(&Cursor(bad.into())).is_err(), "{bad}");
        }
        for bad in ["", "nv1.", "nv1.61", "nv1.61.x", "nv1.zz.1", "nm1.61.1"] {
            assert!(
                decode_name_version_cursor(&Cursor(bad.into())).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn event_cursor_round_trips_and_rejects_garbage() {
        let at = chrono::DateTime::from_timestamp_micros(1_790_000_000_123_456).unwrap();
        let id = Uuid::from_u128(7);
        assert_eq!(
            decode_event_cursor(&encode_event_cursor(at, id)).unwrap(),
            (at, id)
        );

        let hyphenated = format!("ev1.5.{id}");
        for bad in [
            "",
            "ev1.",
            "ev1.5",
            "ev1.x.00",
            "ev1.5.zz",
            "id1.5.00",
            hyphenated.as_str(),
        ] {
            assert!(decode_event_cursor(&Cursor(bad.into())).is_err(), "{bad}");
        }
    }
}
