//! Kuki canonical JSON, version 1. The byte format behind `input_digest`, this
//! must never change silently. A change needs a new version string in `digest.rs`.
//!
//! Rules: no whitespace; object keys sorted by UTF-8 byte order at every depth (not
//! RFC 8785 UTF-16 order); strings as serde_json writes them (raw UTF-8, control
//! characters escaped); integers as digits; floats in Rust's shortest round-trip form.
//! Prefer integers and strings (decimals as strings) in anything that feeds a digest.
use serde_json::Value;

pub fn canonical_json(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(value, &mut out);
    out
}

fn write_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            out.push(b'{');
            for (i, (key, val)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_string(key, out);
                out.push(b':');
                write_value(val, out);
            }
            out.push(b'}');
        }
    }
}

fn write_string(s: &str, out: &mut Vec<u8>) {
    serde_json::to_writer(&mut *out, s).expect("writing a string to a Vec cannot fail");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn text(v: &Value) -> String {
        String::from_utf8(canonical_json(v)).unwrap()
    }

    #[test]
    fn compact_and_sorted_at_every_depth() {
        let v = json!({"b": [{"y": 1, "x": 2}], "a": null});
        assert_eq!(text(&v), r#"{"a":null,"b":[{"x":2,"y":1}]}"#);
    }

    #[test]
    fn key_order_in_the_source_does_not_matter() {
        let a: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn keys_sort_by_utf8_bytes_not_utf16() {
        // U+FF5E is EF BD 9E in UTF-8; U+10000 is F0 90 80 80. UTF-16 order would reverse them.
        let v = json!({"\u{10000}": 1, "\u{FF5E}": 2});
        assert_eq!(text(&v), "{\"\u{FF5E}\":2,\"\u{10000}\":1}");
    }

    #[test]
    fn non_ascii_is_raw_utf8_and_controls_are_escaped() {
        assert_eq!(canonical_json(&json!("é")), "\"é\"".as_bytes());
        assert_eq!(canonical_json(&json!("a\nb")), br#""a\nb""#);
    }

    #[test]
    fn numbers_and_scalars() {
        assert_eq!(
            text(&json!({"n": 1.5, "k": 10, "t": true, "f": false})),
            r#"{"f":false,"k":10,"n":1.5,"t":true}"#
        );
    }
}
