//! Body parsing. Request-level problems are errors; row-level problems become rows.

use serde_json::Value;

use crate::pipeline::IngestError;

pub const MAX_JSON_ROWS: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    NdJson,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RowContent {
    Json(Value),
    Malformed { text: String, message: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RawRow {
    /// Array position (JSON) or zero-based line number (NDJSON, blank lines counted).
    pub index: u32,
    pub content: RowContent,
}

/// `array_key` is "events" here; "objects" and "links" reuse this later.
pub fn parse_rows(
    body: &[u8],
    format: Format,
    array_key: &str,
) -> Result<Vec<RawRow>, IngestError> {
    match format {
        Format::Json => parse_json(body, array_key),
        Format::NdJson => parse_ndjson(body),
    }
}

fn parse_json(body: &[u8], key: &str) -> Result<Vec<RawRow>, IngestError> {
    let pointer = Some(format!("/{key}"));
    let doc: Value = serde_json::from_slice(body)
        .map_err(|e| IngestError::invalid(None, format!("body is not valid JSON: {e}")))?;

    let items = match doc {
        Value::Object(mut m) => match m.remove(key) {
            Some(Value::Array(a)) => a,
            Some(_) => return Err(IngestError::invalid(pointer, "must be an array")),
            None => return Err(IngestError::invalid(pointer, "is required")),
        },
        _ => {
            return Err(IngestError::invalid(
                Some(String::new()),
                "body must be a JSON object",
            ));
        }
    };
    if items.is_empty() {
        return Err(IngestError::invalid(
            pointer,
            "must contain at least one item",
        ));
    }
    if items.len() > MAX_JSON_ROWS {
        return Err(IngestError::invalid(
            pointer,
            format!(
                "at most {MAX_JSON_ROWS} items per request; use application/x-ndjson for larger loads"
            ),
        ));
    }
    Ok(items
        .into_iter()
        .enumerate()
        .map(|(i, v)| RawRow {
            index: i as u32,
            content: RowContent::Json(v),
        })
        .collect())
}

fn parse_ndjson(body: &[u8]) -> Result<Vec<RawRow>, IngestError> {
    let mut rows = Vec::new();
    for (i, line) in body.split(|b| *b == b'\n').enumerate() {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let index = u32::try_from(i).map_err(|_| IngestError::invalid(None, "too many lines"))?;
        let content = match serde_json::from_slice::<Value>(line) {
            Ok(v) => RowContent::Json(v),
            Err(e) => RowContent::Malformed {
                text: String::from_utf8_lossy(line).into_owned(),
                message: format!("line is not valid JSON: {e}"),
            },
        };
        rows.push(RawRow { index, content });
    }
    if rows.is_empty() {
        return Err(IngestError::invalid(None, "body contains no rows"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn pointer_of(r: Result<Vec<RawRow>, IngestError>) -> Option<String> {
        match r {
            Err(IngestError::Invalid { pointer, .. }) => pointer,
            other => panic!("expected an Invalid error, got {other:?}"),
        }
    }

    #[test]
    fn ndjson_indexes_are_line_numbers_and_blank_lines_count() {
        let rows = parse_rows(b"{\"a\":1}\n\n{\"a\":2}\r\n", Format::NdJson, "events").unwrap();
        let idx: Vec<u32> = rows.iter().map(|r| r.index).collect();
        assert_eq!(idx, vec![0, 2]);
        assert_eq!(rows[1].content, RowContent::Json(json!({ "a": 2 })));
    }

    #[test]
    fn a_bad_ndjson_line_is_a_row_not_a_request_failure() {
        let rows = parse_rows(b"{\"a\":1}\n{not json\n", Format::NdJson, "events").unwrap();
        assert!(
            matches!(&rows[1].content, RowContent::Malformed { text, .. } if text == "{not json")
        );
    }

    #[test]
    fn an_empty_ndjson_body_is_invalid() {
        assert!(parse_rows(b"\n \n", Format::NdJson, "events").is_err());
    }

    #[test]
    fn json_bodies_must_carry_a_bounded_array() {
        assert!(parse_rows(br#"{"events":[{"a":1},{"a":2}]}"#, Format::Json, "events").is_ok());
        assert_eq!(
            pointer_of(parse_rows(b"{}", Format::Json, "events")),
            Some("/events".into())
        );
        assert_eq!(
            pointer_of(parse_rows(br#"{"events":{}}"#, Format::Json, "events")),
            Some("/events".into())
        );
        assert_eq!(
            pointer_of(parse_rows(br#"{"events":[]}"#, Format::Json, "events")),
            Some("/events".into())
        );
        let big = format!(
            r#"{{"events":[{}]}}"#,
            vec!["{}"; MAX_JSON_ROWS + 1].join(",")
        );
        assert_eq!(
            pointer_of(parse_rows(big.as_bytes(), Format::Json, "events")),
            Some("/events".into())
        );
        assert!(parse_rows(b"[1]", Format::Json, "events").is_err());
        assert!(parse_rows(b"nope", Format::Json, "events").is_err());
    }
}
