//! Who may resolve a recommendation. The state machine (what transitions are legal)
//! lives in kuki-types; this decides basic authority, Kuki Core handles advanced
//! authority, i.e, two-person ruling, guarded auto-execution, etc.
use async_trait::async_trait;
use kuki_types::Recommendation;

use crate::error::ExtensionError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PolicyAction {
    Accept,
    Reject,
    /// The system proposes to execute without a human. Only Core ever answers yes.
    AutoExecute,
}

pub struct PolicyRequest<'a> {
    pub recommendation: &'a Recommendation,
    /// `key:<uuid>`, `user:<sub>` or `policy:<id>`
    pub actor: &'a str,
    pub action: PolicyAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    /// A person must act. This is the only answer to `AutoExecute` in Kuki OSS.
    RequiresHuman,
    /// Kuki Core's two-person rule.
    RequiresAdditionalApproval {
        have: u32,
        need: u32,
    },
    Deny {
        reason: String,
    },
}

#[async_trait]
pub trait PolicyEvaluator: Send + Sync {
    async fn evaluate(&self, request: &PolicyRequest<'_>)
    -> Result<PolicyDecision, ExtensionError>;
}

/// Kuki OSS default: one approver, no auto-execution. Scope checks already happen at the API.
pub struct SingleApproverPolicy;

#[async_trait]
impl PolicyEvaluator for SingleApproverPolicy {
    async fn evaluate(
        &self,
        request: &PolicyRequest<'_>,
    ) -> Result<PolicyDecision, ExtensionError> {
        Ok(match request.action {
            PolicyAction::Accept | PolicyAction::Reject => PolicyDecision::Allow,
            PolicyAction::AutoExecute => PolicyDecision::RequiresHuman,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use kuki_types::*;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn recommendation() -> Recommendation {
        let now = Utc::now();
        Recommendation {
            id: Uuid::nil(),
            organisation_id: Uuid::nil(),
            org_unit_id: None,
            decision_type: "inventory.replenish".into(),
            dedup_key: DedupKey::new(Uuid::nil(), "inventory.replenish", &["item-1"]),
            action: Action {
                proposed: true,
                action_type: None,
                payload: json!({}),
                payload_schema_ref: "inventory.replenish@1".into(),
            },
            rationale: Rationale {
                summary: "s".into(),
                factors: vec![],
                confidence: 0.9,
                confidence_kind: ConfidenceKind::DataQuality,
                counterfactual: None,
                narrative: None,
            },
            urgency: 0.5,
            impact: None,
            provenance: Provenance {
                generator_id: "g".into(),
                generator_version: "1.0.0".into(),
                tier: Tier::T0,
                pack: "inventory@1.0.0".into(),
                ontology_version: 1,
                config_hash: "c".into(),
                as_of: now,
                input_digest: "d".into(),
                model_artefact: None,
            },
            status: RecommendationStatus::Pending,
            resolved_by: None,
            created_at: now,
            last_confirmed_at: now,
            resolved_at: None,
            expires_at: None,
        }
    }

    #[tokio::test]
    async fn oss_policy_lets_a_person_resolve_and_never_auto_executes() {
        let rec = recommendation();
        let policy = SingleApproverPolicy;
        for (action, expected) in [
            (PolicyAction::Accept, PolicyDecision::Allow),
            (PolicyAction::Reject, PolicyDecision::Allow),
            (PolicyAction::AutoExecute, PolicyDecision::RequiresHuman),
        ] {
            let req = PolicyRequest {
                recommendation: &rec,
                actor: "key:1",
                action,
            };
            assert_eq!(policy.evaluate(&req).await.unwrap(), expected);
        }
    }
}
