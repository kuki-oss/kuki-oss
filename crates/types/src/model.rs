//! Recommendation, candidate and rationale types (spec 7.1, 9.1).
//! Stored form is snake_case JSON.
use std::fmt;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    T0,
    T1,
    T2,
    T3,
    T4,
    T5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceKind {
    DataQuality,
    PredictiveInterval,
    CalibratedProbability,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Event,
    Object,
    Link,
    Metric,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactorDirection {
    Raises,
    Lowers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationStatus {
    Pending,
    Accepted,
    Rejected,
    Expired,
    Superseded,
    Executing,
    Executed,
    Failed,
    AutoExecuted,
}

impl RecommendationStatus {
    /// The value stored in `recommendations.status`
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Superseded => "superseded",
            Self::Executing => "executing",
            Self::Executed => "executed",
            Self::Failed => "failed",
            Self::AutoExecuted => "auto_executed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "accepted" => Self::Accepted,
            "rejected" => Self::Rejected,
            "expired" => Self::Expired,
            "superseded" => Self::Superseded,
            "executing" => Self::Executing,
            "executed" => Self::Executed,
            "failed" => Self::Failed,
            "auto_executed" => Self::AutoExecuted,
            _ => return None,
        })
    }

    /// The state machine (spec 7.5).
    pub fn can_transition_to(self, next: Self) -> bool {
        use RecommendationStatus::*;
        matches!(
            (self, next),
            (
                Pending,
                Accepted | Rejected | Expired | Superseded | AutoExecuted
            ) | (Accepted, Executing)
                | (Executing, Executed | Failed)
                | (Failed, Executing)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRef {
    pub kind: EvidenceKind,
    pub id: String,
    pub as_of: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FactorValue {
    Number(serde_json::Number),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Factor {
    pub label: String,
    pub value: FactorValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// \[0,1\]. Only for model-derived factors (normalised SHAP)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<FactorDirection>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Rationale {
    pub summary: String,
    pub factors: Vec<Factor>,
    pub confidence: f64,
    pub confidence_kind: ConfidenceKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counterfactual: Option<String>,
    /// Optional LLM paragraph. Never replaces `summary`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub narrative: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Impact {
    pub value: Decimal,
    pub unit: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    pub generator_id: String,
    pub generator_version: String,
    pub tier: Tier,
    pub pack: String,
    pub ontology_version: i32,
    pub config_hash: String,
    pub as_of: DateTime<Utc>,
    pub input_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_artefact: Option<String>,
}

/// Canonical dedupe key: `<org>/<decision_type>/<entity ids sorted, joined by +>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DedupKey(String);

impl DedupKey {
    /// Entity ids are sorted so that the key does not depend on the order a
    /// generator found them in.
    pub fn new(org: Uuid, decision_type: &str, entity_ids: &[&str]) -> Self {
        let mut ids = entity_ids.to_vec();
        ids.sort_unstable();
        Self(format!("{org}/{decision_type}/{}", ids.join("+")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a generator returns. The orchestrator assigns ids, timestamps and provenance.
/// (Differs from spec 7.1: confidence lives only in `rationale`, and `org_unit_id` is added so
/// the orchestrator can place the recommendation without guessing.)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub decision_type: String,
    pub dedup_key: DedupKey,
    pub org_unit_id: Option<Uuid>,
    pub payload: Value,
    pub rationale: Rationale,
    pub urgency: f64,
    pub impact: Option<Impact>,
    pub evidence: Vec<EvidenceRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    pub field: &'static str,
    pub message: String,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ValidationError {}

fn unit_interval(field: &'static str, v: f64) -> Result<(), ValidationError> {
    if v.is_finite() && (0.0..=1.0).contains(&v) {
        Ok(())
    } else {
        Err(ValidationError {
            field,
            message: format!("{v} is not in [0,1]"),
        })
    }
}

impl Candidate {
    /// Structural checks every candidate must pass before the orchestrator looks
    /// at its payload.
    pub fn validate(&self) -> Result<(), ValidationError> {
        let bad = |field, message: &str| {
            Err(ValidationError {
                field,
                message: message.to_owned(),
            })
        };
        if self.decision_type.trim().is_empty() {
            return bad("decision_type", "must not be empty");
        }
        if self.dedup_key.as_str().is_empty() {
            return bad("dedup_key", "must not be empty");
        }
        unit_interval("urgency", self.urgency)?;
        unit_interval("rationale.confidence", self.rationale.confidence)?;
        if self.rationale.summary.trim().is_empty() {
            return bad("rationale.summary", "must not be empty");
        }
        if self.rationale.factors.is_empty() {
            return bad("rationale.factors", "at least one factor is required");
        }
        for f in &self.rationale.factors {
            if let Some(w) = f.weight {
                unit_interval("rationale.factors.weight", w)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Action {
    pub proposed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_type: Option<String>,
    pub payload: Value,
    pub payload_schema_ref: String,
}

/// The stored `recommendations.body` (spec 9.1)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Recommendation {
    pub id: Uuid,
    pub organisation_id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub decision_type: String,
    pub dedup_key: DedupKey,
    pub action: Action,
    pub rationale: Rationale,
    pub urgency: f64,
    pub impact: Option<Impact>,
    pub provenance: Provenance,
    pub status: RecommendationStatus,
    pub resolved_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_confirmed_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    type Mutation = Box<dyn Fn(&mut Candidate)>;

    fn candidate() -> Candidate {
        let org = Uuid::nil();
        Candidate {
            decision_type: "inventory.replenish".into(),
            dedup_key: DedupKey::new(org, "inventory.replenish", &["item-1"]),
            org_unit_id: None,
            payload: json!({"quantity": "39.000"}),
            rationale: Rationale {
                summary: "Stock will run out in 6 days.".into(),
                factors: vec![Factor {
                    label: "days of cover".into(),
                    value: FactorValue::Number(6.into()),
                    unit: Some("days".into()),
                    weight: None,
                    direction: None,
                    evidence: vec![],
                }],
                confidence: 0.8,
                confidence_kind: ConfidenceKind::DataQuality,
                counterfactual: None,
                narrative: None,
            },
            urgency: 0.7,
            impact: None,
            evidence: vec![],
        }
    }

    #[test]
    fn a_well_formed_candidate_validates() {
        assert!(candidate().validate().is_ok());
    }

    #[test]
    fn invalid_candidates_are_rejected_with_the_offending_field() {
        let cases: Vec<(&str, Mutation)> = vec![
            ("urgency", Box::new(|c| c.urgency = 1.5)),
            ("urgency", Box::new(|c| c.urgency = f64::NAN)),
            (
                "rationale.confidence",
                Box::new(|c| c.rationale.confidence = -0.1),
            ),
            (
                "rationale.summary",
                Box::new(|c| c.rationale.summary = "  ".into()),
            ),
            (
                "rationale.factors",
                Box::new(|c| c.rationale.factors.clear()),
            ),
            (
                "rationale.factors.weight",
                Box::new(|c| c.rationale.factors[0].weight = Some(2.0)),
            ),
            ("decision_type", Box::new(|c| c.decision_type.clear())),
        ];
        for (field, mutate) in cases {
            let mut c = candidate();
            mutate(&mut c);
            assert_eq!(c.validate().unwrap_err().field, field);
        }
    }

    #[test]
    fn dedup_key_ignores_entity_order() {
        let org = Uuid::nil();
        assert_eq!(
            DedupKey::new(org, "scheduling.conflict", &["b", "a"]),
            DedupKey::new(org, "scheduling.conflict", &["a", "b"])
        );
        assert_ne!(
            DedupKey::new(org, "scheduling.conflict", &["a", "b"]),
            DedupKey::new(org, "scheduling.conflict", &["a", "c"])
        );
    }

    #[test]
    fn status_strings_round_trip() {
        use RecommendationStatus::*;
        for s in [
            Pending,
            Accepted,
            Rejected,
            Expired,
            Superseded,
            Executing,
            Executed,
            Failed,
            AutoExecuted,
        ] {
            assert_eq!(RecommendationStatus::parse(s.as_str()), Some(s));
            assert_eq!(serde_json::to_value(s).unwrap(), json!(s.as_str()));
        }
        assert_eq!(RecommendationStatus::parse("suppressed_holdout"), None);
    }

    #[test]
    fn state_machine_matches_spec_7_5() {
        use RecommendationStatus::*;
        let all = [
            Pending,
            Accepted,
            Rejected,
            Expired,
            Superseded,
            Executing,
            Executed,
            Failed,
            AutoExecuted,
        ];
        let allowed = [
            (Pending, Accepted),
            (Pending, Rejected),
            (Pending, Expired),
            (Pending, Superseded),
            (Pending, AutoExecuted),
            (Accepted, Executing),
            (Executing, Executed),
            (Executing, Failed),
            (Failed, Executing),
        ];
        for from in all {
            for to in all {
                assert_eq!(
                    from.can_transition_to(to),
                    allowed.contains(&(from, to)),
                    "{from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn impact_value_serialises_as_a_decimal_string() {
        let v = serde_json::to_value(Impact {
            value: "39.000".parse().unwrap(),
            unit: "KES".into(),
        })
        .unwrap();
        assert_eq!(v, json!({"value": "39.000", "unit": "KES"}));
    }
}
