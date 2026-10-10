use serde::{Serialize, Serializer};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionCode {
    SchemaInvalid,
    UnknownEventType,
    UnknownTypeVersion,
    UnresolvedSubject,
    AmbiguousSubject,
    OutOfScope,
    MalformedLine,
    UnknownObjectType,
}

impl RejectionCode {
    pub const ALL: [RejectionCode; 8] = [
        RejectionCode::SchemaInvalid,
        RejectionCode::UnknownEventType,
        RejectionCode::UnknownTypeVersion,
        RejectionCode::UnresolvedSubject,
        RejectionCode::AmbiguousSubject,
        RejectionCode::OutOfScope,
        RejectionCode::MalformedLine,
        RejectionCode::UnknownObjectType,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RejectionCode::SchemaInvalid => "SCHEMA_INVALID",
            RejectionCode::UnknownEventType => "UNKNOWN_EVENT_TYPE",
            RejectionCode::UnknownTypeVersion => "UNKNOWN_TYPE_VERSION",
            RejectionCode::UnresolvedSubject => "UNRESOLVED_SUBJECT",
            RejectionCode::AmbiguousSubject => "AMBIGUOUS_SUBJECT",
            RejectionCode::OutOfScope => "OUT_OF_SCOPE",
            RejectionCode::MalformedLine => "MALFORMED_LINE",
            RejectionCode::UnknownObjectType => "UNKNOWN_OBJECT_TYPE",
        }
    }
}

impl Serialize for RejectionCode {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertAccepted {
    pub index: u32,
    pub id: Uuid,
    pub outcome: &'static str, // created | updated | unchanged
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertResult {
    pub batch_id: Uuid,
    pub accepted_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted: Option<Vec<UpsertAccepted>>,
    pub rejected: Vec<Rejected>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Accepted {
    pub index: u32,
    pub event_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejected {
    pub index: u32,
    pub code: RejectionCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub quarantine_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestResult {
    pub batch_id: Uuid,
    pub accepted_count: u32,
    /// Omitted when `return=summary`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accepted: Option<Vec<Accepted>>,
    pub rejected: Vec<Rejected>,
}

impl IngestResult {
    pub fn summarised(mut self) -> Self {
        self.accepted = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_codes_match_the_contract() {
        let spec = include_str!("../../../api/schema/openapi.yaml");
        let start = spec.find("IngestRejectionCode:").expect("schema present");
        let rest = &spec[start..];
        let open = rest.find("enum: [").expect("enum present") + "enum: [".len();
        let list = &rest[open..open + rest[open..].find(']').unwrap()];

        let mut from_spec: Vec<&str> = list.split(',').map(str::trim).collect();
        let mut ours: Vec<&str> = RejectionCode::ALL.iter().map(|c| c.as_str()).collect();
        from_spec.sort();
        ours.sort();
        assert_eq!(from_spec, ours);
    }

    #[test]
    fn the_result_serialises_like_the_contract() {
        let r = IngestResult {
            batch_id: Uuid::nil(),
            accepted_count: 1,
            accepted: Some(vec![Accepted {
                index: 0,
                event_id: Uuid::nil(),
            }]),
            rejected: vec![Rejected {
                index: 1,
                code: RejectionCode::SchemaInvalid,
                message: "bad".into(),
                field: None,
                quarantine_id: Uuid::nil(),
            }],
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v["acceptedCount"], 1);
        assert_eq!(v["accepted"][0]["eventId"], Uuid::nil().to_string());
        assert_eq!(v["rejected"][0]["code"], "SCHEMA_INVALID");
        assert!(v["rejected"][0].get("field").is_none());
        assert!(
            serde_json::to_value(r.summarised())
                .unwrap()
                .get("accepted")
                .is_none()
        );
    }
}
