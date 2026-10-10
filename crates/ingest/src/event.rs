//! wire shape of one event (openapi EventInput) and its structural validation.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventInput {
    #[serde(rename = "type")]
    pub event_type: String,
    pub type_version: i64,
    pub subject: SubjectRef,
    /// RFC 3339; any offset is accepted and normalised to UTC.
    pub occurred_at: DateTime<Utc>,
    pub payload: Map<String, Value>,
    #[serde(default)]
    pub supersedes: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum SubjectRef {
    Id(ById),
    External(ByExternal),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ById {
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ByExternal {
    pub external_id: ExternalId,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalId {
    pub system: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldError {
    /// JSON pointer into the row, e.g. "/subject/externalId".
    pub field: Option<String>,
    pub message: String,
}

fn pointer(path: &str) -> Option<String> {
    if path == "." || path.is_empty() {
        None
    } else {
        Some(format!("/{}", path.replace('.', "/")))
    }
}

pub fn parse_event(value: &Value) -> Result<EventInput, FieldError> {
    let input: EventInput = serde_path_to_error::deserialize(value).map_err(|e| {
        let mut field = pointer(&e.path().to_string());
        let mut message = e.inner().to_string();

        // serde reports unknown fields against their parent; point at the field itself.
        if field.is_none()
            && let Some(rest) = message.strip_prefix("unknown field `")
            && let Some(name) = rest.split('`').next()
        {
            field = Some(format!("/{name}"));
        }
        if field.as_deref() == Some("/subject") && message.contains("untagged") {
            message =
                r#"must be {"id": <uuid>} or {"externalId": {"system": ..., "id": ...}}"#.into();
        }
        FieldError { field, message }
    })?;

    if input.type_version < 1 || input.type_version > i64::from(i32::MAX) {
        return Err(FieldError {
            field: Some("/typeVersion".into()),
            message: "must be an integer >= 1".into(),
        });
    }
    if input.event_type.trim().is_empty() {
        return Err(FieldError {
            field: Some("/type".into()),
            message: "must not be empty".into(),
        });
    }
    if let SubjectRef::External(e) = &input.subject
        && (e.external_id.system.is_empty() || e.external_id.id.is_empty())
    {
        return Err(FieldError {
            field: Some("/subject/externalId".into()),
            message: "system and id must not be empty".into(),
        });
    }
    Ok(input)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn good() -> Value {
        json!({
            "type": "stock.delta",
            "typeVersion": 1,
            "subject": { "id": "7a3c2f0e-5b4d-4c1e-9a55-1c9d2f6a8b10" },
            "occurredAt": "2026-10-07T10:00:00Z",
            "payload": { "delta": 1 }
        })
    }

    fn with(key: &str, v: Value) -> Value {
        let mut g = good();
        g[key] = v;
        g
    }

    fn field(v: Value) -> Option<String> {
        parse_event(&v).unwrap_err().field
    }

    #[test]
    fn a_well_formed_event_parses() {
        let e = parse_event(&good()).unwrap();
        assert_eq!(e.event_type, "stock.delta");
        assert!(matches!(e.subject, SubjectRef::Id(_)));
    }

    #[test]
    fn external_subjects_parse() {
        let e = parse_event(&with(
            "subject",
            json!({ "externalId": { "system": "erp", "id": "A1" } }),
        ))
        .unwrap();
        assert!(matches!(e.subject, SubjectRef::External(ref x) if x.external_id.system == "erp"));
    }

    #[test]
    fn offsets_are_normalised_to_utc() {
        let e = parse_event(&with("occurredAt", json!("2026-10-07T12:00:00+02:00"))).unwrap();
        assert_eq!(e.occurred_at.to_rfc3339(), "2026-10-07T10:00:00+00:00");
    }

    #[test]
    fn problems_carry_a_json_pointer() {
        assert_eq!(field(with("extra", json!(1))), Some("/extra".into()));
        assert_eq!(
            field(with("typeVersion", json!(0))),
            Some("/typeVersion".into())
        );
        assert_eq!(
            field(with("typeVersion", json!(1.5))),
            Some("/typeVersion".into())
        );
        assert_eq!(
            field(with("occurredAt", json!("yesterday"))),
            Some("/occurredAt".into())
        );
        assert_eq!(field(with("payload", json!([1]))), Some("/payload".into()));
        assert_eq!(
            field(with("supersedes", json!("nope"))),
            Some("/supersedes".into())
        );
        assert_eq!(
            field(with("subject", json!({ "id": "not-a-uuid" }))),
            Some("/subject".into())
        );
        assert_eq!(field(with("subject", json!({}))), Some("/subject".into()));
        assert_eq!(
            field(with(
                "subject",
                json!({ "externalId": { "system": "", "id": "A1" } })
            )),
            Some("/subject/externalId".into())
        );
    }

    #[test]
    fn a_missing_required_field_is_reported() {
        let mut g = good();
        g.as_object_mut().unwrap().remove("payload");
        let err = parse_event(&g).unwrap_err();
        assert!(err.message.contains("payload"), "{}", err.message);
    }

    #[test]
    fn a_row_that_is_not_an_object_is_rejected() {
        assert!(parse_event(&json!([1, 2])).is_err());
        assert!(parse_event(&json!("x")).is_err());
    }
}
