//! acceptRecommendation and rejectRecommendation (spec 7.5, 8.3): decisions:resolve.
//!
//! read the row, answer idempotent retries, check ifVersion and the state machine,
//! ask the PolicyEvaluator, validate any payload override, then write the transition,
//! the audit row and the outbox event in ONE transaction. The AuditSink runs after
//! commit.
//!
//! A row outside the caller's unit scope is NOT_FOUND, like the reads: RLS makes it
//! indistinguishable from a missing one (contract text says FORBIDDEN).

use std::sync::Arc;

use async_graphql::{
    Context, Enum, Error, ErrorExtensions, ID, InputObject, Json, Object, Result, SimpleObject,
};
use chrono::Utc;
use futures::FutureExt;
use kuki_extension::{AuditEntry, Extensions, PolicyAction, PolicyDecision, PolicyRequest};
use kuki_store::audit;
use kuki_store::executions;
use kuki_store::recommendation_reads as reads;
use kuki_store::recommendations::{
    self as store, RecommendationRow, Transition, TransitionError, Transitioned,
};
use kuki_types as kt;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use super::connection::{parse_uuid, validation};
use super::guard::RequiresScope;
use super::recommendations::{ActionExecution, KukiRecommendation};
use crate::auth::Principal;
use crate::error::ApiError;

const MAX_TEXT: usize = 2000;

// ── contract types ──────────────────────────────────────────────────────────

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum RejectionReasonCode {
    WrongRecommendation,
    AlreadyHandled,
    NotNow,
    NotApplicable,
    Other,
}

impl RejectionReasonCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::WrongRecommendation => "WRONG_RECOMMENDATION",
            Self::AlreadyHandled => "ALREADY_HANDLED",
            Self::NotNow => "NOT_NOW",
            Self::NotApplicable => "NOT_APPLICABLE",
            Self::Other => "OTHER",
        }
    }
}

#[derive(InputObject)]
pub struct AcceptRecommendationInput {
    recommendation_id: ID,
    note: Option<String>,
    /// Validated against the payload schema. Stored alongside the original.
    payload_override: Option<Json<Value>>,
    /// Compare-and-set: fails with CONFLICT/STALE_VERSION if the row moved on.
    if_version: Option<i32>,
    client_mutation_id: String,
}

#[derive(InputObject)]
pub struct RejectRecommendationInput {
    recommendation_id: ID,
    reason_code: RejectionReasonCode,
    reason_text: Option<String>,
    client_mutation_id: String,
    if_version: Option<i32>,
}

#[derive(SimpleObject)]
pub struct AcceptRecommendationPayload {
    recommendation: KukiRecommendation,
    /// Present when an action execution was created in the same transaction.
    action_execution: Option<ActionExecution>,
    client_mutation_id: String,
}

#[derive(SimpleObject)]
pub struct RejectRecommendationPayload {
    recommendation: KukiRecommendation,
    client_mutation_id: String,
}

// ── errors ──────────────────────────────────────────────────────────────────

fn problem(code: &'static str, reason: Option<&'static str>, msg: impl Into<String>) -> Error {
    Error::new(msg).extend_with(|_, ext| {
        ext.set("code", code);
        if let Some(r) = reason {
            ext.set("reason", r);
        }
    })
}

fn not_found() -> Error {
    problem("NOT_FOUND", None, "recommendation not found")
}

fn internal_error() -> Error {
    problem("INTERNAL", None, "internal error")
}

fn stale(current: i32) -> Error {
    Error::new(format!("recommendation is at version {current}")).extend_with(|_, ext| {
        ext.set("code", "CONFLICT");
        ext.set("reason", "STALE_VERSION");
        ext.set("currentVersion", current);
    })
}

fn invalid(from: kt::RecommendationStatus, to: kt::RecommendationStatus) -> Error {
    problem(
        "CONFLICT",
        Some("INVALID_STATE_TRANSITION"),
        format!("cannot move from {} to {}", from.as_str(), to.as_str()),
    )
}

fn map_failure(e: TransitionError) -> Error {
    match e {
        TransitionError::NotFound => not_found(),
        TransitionError::VersionConflict { current } => stale(current),
        TransitionError::Illegal { from, to } => invalid(from, to),
        TransitionError::Corrupt(_) | TransitionError::Db(_) => internal_error(),
    }
}

/// Only `Allow` resolves. Core's two-person rule answers `RequiresAdditionalApproval` and
/// records approvals itself; the kernel just refuses to resolve until the policy allows it.
fn policy_gate(d: &PolicyDecision) -> Result<()> {
    match d {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::RequiresHuman => Err(problem(
            "FORBIDDEN",
            None,
            "policy requires a human approver",
        )),
        PolicyDecision::RequiresAdditionalApproval { have, need } => Err(problem(
            "FORBIDDEN",
            None,
            format!("{have} of {need} approvals recorded; more approvals are required"),
        )),
        PolicyDecision::Deny { reason } => Err(problem("FORBIDDEN", None, reason.clone())),
    }
}

// ── validation ──────────────────────────────────────────────────────────────

fn check_text(v: Option<&str>, what: &str) -> Result<()> {
    if v.is_some_and(|s| s.chars().count() > MAX_TEXT) {
        return Err(validation(
            &format!("{what} must be at most {MAX_TEXT} characters"),
            None,
        ));
    }
    Ok(())
}

fn check_mutation_id(s: &str) -> Result<()> {
    if s.is_empty() || s.len() > 255 {
        return Err(validation(
            "clientMutationId must be 1 to 255 characters",
            None,
        ));
    }
    Ok(())
}

fn check_version(v: Option<i32>) -> Result<()> {
    if v.is_some_and(|n| n < 1) {
        return Err(validation("ifVersion must be at least 1", None));
    }
    Ok(())
}

fn check_payload(schema: &Value, payload: &Value) -> Result<()> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|_| problem("INTERNAL", None, "the registered payload schema is invalid"))?;
    if let Some(e) = validator.iter_errors(payload).next() {
        return Err(validation(
            &format!("payloadOverride does not match the payload schema: {e}"),
            None,
        ));
    }
    Ok(())
}

// ── resolutions ─────────────────────────────────────────────────────────────

enum Resolution {
    Accept {
        note: Option<String>,
        payload_override: Option<Value>,
    },
    Reject {
        code: RejectionReasonCode,
        text: Option<String>,
    },
}

fn accept_resolution(note: Option<&str>, original: &Value, over: Option<&Value>) -> Value {
    let overridden = over.is_some_and(|o| o != original);
    let mut r = json!({ "payload_overridden": overridden });
    if let Some(n) = note {
        r["note"] = json!(n);
    }
    if let (true, Some(o)) = (overridden, over) {
        r["payload_override"] = o.clone();
    }
    r
}

fn reject_resolution(code: RejectionReasonCode, text: Option<&str>) -> Value {
    let mut r = json!({ "reason_code": code.as_str(), "payload_overridden": false });
    if let Some(t) = text {
        r["note"] = json!(t);
    }
    r
}

/// The domain view of a stored row, for the policy evaluator.
fn domain(row: &RecommendationRow) -> Result<kt::Recommendation> {
    let part = |k: &str| row.body.get(k).cloned().unwrap_or(Value::Null);
    serde_json::from_value(json!({
        "id": row.id,
        "organisation_id": row.organisation_id,
        "org_unit_id": row.org_unit_id,
        "decision_type": row.decision_type,
        "dedup_key": row.dedup_key,
        "action": part("action"),
        "rationale": part("rationale"),
        "urgency": row.urgency,
        "impact": part("impact"),
        "provenance": row.provenance,
        "status": row.status,
        "resolved_by": row.resolved_by,
        "created_at": row.created_at,
        "last_confirmed_at": row.last_confirmed_at,
        "resolved_at": row.resolved_at,
        "expires_at": row.expires_at,
    }))
    .map_err(|_| internal_error())
}

// ── the work ────────────────────────────────────────────────────────────────

struct Done {
    row: Option<RecommendationRow>,
    changed: bool,
    execution: Option<reads::ExecutionRow>,
}

enum Outcome {
    Done(Box<Done>),
    Failed(TransitionError),
}

/// The oldest execution of a recommendation: a retried accept returns the one it queued.
async fn first_execution(
    pool: &PgPool,
    p: &Principal,
    id: Uuid,
) -> Result<Option<reads::ExecutionRow>> {
    let org = p.session.org_id;
    let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
        async move { reads::executions_for(c, org, id).await }.boxed()
    })
    .await
    .map_err(ApiError::internal)?;
    Ok(if rows.is_empty() {
        None
    } else {
        Some(rows.swap_remove(0))
    })
}

async fn resolve(
    ctx: &Context<'_>,
    id: Uuid,
    if_version: Option<i32>,
    client_mutation_id: &str,
    res: Resolution,
) -> Result<(RecommendationRow, Option<reads::ExecutionRow>)> {
    let p = ctx.data::<Principal>()?;
    let pool = ctx.data::<PgPool>()?;
    let ext = ctx.data::<Arc<Extensions>>()?;
    let org = p.session.org_id;
    let actor = p.actor.clone();

    let accepting = matches!(res, Resolution::Accept { .. });
    let target = if accepting {
        kt::RecommendationStatus::Accepted
    } else {
        kt::RecommendationStatus::Rejected
    };
    let verb = if accepting { "accepted" } else { "rejected" };
    let wants_schema = matches!(
        &res,
        Resolution::Accept {
            payload_override: Some(_),
            ..
        }
    );

    // 1. Read the row (RLS decides visibility) and, when overriding, the payload schema.
    let (row, schema) =
        kuki_store::session::with_org_tx::<_, sqlx::Error, _>(pool, &p.session, |c| {
            async move {
                let Some(row) = store::get(c, org, id).await? else {
                    return Ok((None, None));
                };
                let schema = if wants_schema {
                    reads::payload_schema(c, org, &row.decision_type).await?
                } else {
                    None
                };
                Ok((Some(row), schema))
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;
    let row = row.ok_or_else(not_found)?;
    let status: kt::RecommendationStatus =
        serde_json::from_value(Value::String(row.status.clone())).map_err(|_| internal_error())?;

    // 2. Retries by the same actor succeed without writing; anyone else's resolution conflicts.
    let already = if accepting {
        matches!(
            status,
            kt::RecommendationStatus::Accepted
                | kt::RecommendationStatus::Executing
                | kt::RecommendationStatus::Executed
                | kt::RecommendationStatus::Failed
        )
    } else {
        status == kt::RecommendationStatus::Rejected
    };
    if already {
        if row.resolved_by.as_deref() == Some(actor.as_str()) {
            let execution = if accepting {
                first_execution(pool, p, id).await?
            } else {
                None
            };
            return Ok((row, execution));
        }
        return Err(problem(
            "CONFLICT",
            Some("INVALID_STATE_TRANSITION"),
            format!("recommendation is already {}", status.as_str()),
        ));
    }

    // 3. Compare-and-set, then the state machine.
    if let Some(v) = if_version
        && v != row.version
    {
        return Err(stale(row.version));
    }
    if !status.can_transition_to(target) {
        return Err(invalid(status, target));
    }

    // 4. Authority.
    let rec = domain(&row)?;
    let action = if accepting {
        PolicyAction::Accept
    } else {
        PolicyAction::Reject
    };
    let decision = ext
        .policy
        .evaluate(&PolicyRequest {
            recommendation: &rec,
            actor: &actor,
            action,
        })
        .await
        .map_err(|_| Error::from(ApiError::unavailable("policy evaluation is unavailable")))?;
    policy_gate(&decision)?;

    // 5. The resolution to store, with any override validated first.
    let resolution = match &res {
        Resolution::Accept {
            note,
            payload_override,
        } => {
            if let Some(o) = payload_override {
                let schema = schema.as_ref().ok_or_else(|| {
                    validation("payloadOverride cannot be validated: the decision type has no registered schema", None)
                })?;
                check_payload(schema, o)?;
            }
            accept_resolution(
                note.as_deref(),
                &rec.action.payload,
                payload_override.as_ref(),
            )
        }
        Resolution::Reject { code, text } => reject_resolution(*code, text.as_deref()),
    };

    // 5b. An accepted, proposed action with a type queues its execution with
    // the decision. It carries the payload the operator actually approved:
    // the override when it differs.
    let queued: Option<(String, Value)> = if accepting && rec.action.proposed {
        rec.action.action_type.clone().map(|action_type| {
            let payload = resolution
                .get("payload_override")
                .unwrap_or(&rec.action.payload)
                .clone();
            let input = json!({
                "recommendationId": id.to_string(),
                "decisionType": row.decision_type,
                "actionType": action_type,
                "payload": payload,
                "overridden": resolution["payload_overridden"],
            });
            (action_type, input)
        })
    } else {
        None
    };

    // 6. One transaction: the transition (with its outbox event), the audit row, the re-read.
    let now = Utc::now();
    let audit_id = Uuid::new_v4();
    let version = row.version;
    let details = json!({
        "recommendationId": id.to_string(),
        "decisionType": row.decision_type,
        "fromVersion": version,
        "clientMutationId": client_mutation_id,
        "resolution": resolution,
    });
    let outcome = kuki_store::session::with_org_tx(pool, &p.session, |c| {
        let (actor, resolution, details, queued) = (
            actor.clone(),
            resolution.clone(),
            details.clone(),
            queued.clone(),
        );
        async move {
            let t = Transition {
                id,
                to: target,
                expected_version: Some(version),
                actor: &actor,
                resolution: Some(&resolution),
            };
            let changed = match store::transition(c, org, &t, now).await {
                Ok(Transitioned::Changed { .. }) => true,
                // An identical resolution landed between the read and now: nothing to write or audit.
                Ok(Transitioned::Unchanged) => false,
                Err(TransitionError::Db(e)) => return Err(e),
                Err(e) => return Ok(Outcome::Failed(e)),
            };
            let mut execution = None;
            if changed {
                audit::append(
                    c,
                    org,
                    &audit::AuditRow {
                        id: audit_id,
                        occurred_at: now,
                        actor: &actor,
                        action: &format!("recommendation.{verb}"),
                        subject: &format!("recommendation:{id}"),
                        details: &details,
                    },
                )
                .await?;
                if let Some((action_type, input)) = &queued {
                    execution = executions::enqueue(
                        c,
                        org,
                        &executions::NewExecution {
                            id: Uuid::new_v4(),
                            recommendation_id: id,
                            action_type,
                            input,
                            idempotency_key: &format!("recommendation:{id}:accept"),
                            created_by: &actor,
                            now,
                        },
                    )
                    .await?;
                }
            }
            let row = store::get(c, org, id).await?;
            Ok(Outcome::Done(Box::new(Done {
                row,
                changed,
                execution,
            })))
        }
        .boxed()
    })
    .await
    .map_err(ApiError::internal)?;

    match outcome {
        Outcome::Failed(e) => Err(map_failure(e)),
        Outcome::Done(done) => {
            let Done {
                row,
                changed,
                execution,
            } = *done;
            let row = row.ok_or_else(not_found)?;
            if changed {
                // After commit, in commit order per organisation. A sink failure never undoes the
                // decision: the append-only table above is the audit log of record.
                let entry = AuditEntry {
                    id: audit_id,
                    organisation_id: org,
                    occurred_at: now,
                    actor: actor.clone(),
                    action: format!("recommendation.{verb}"),
                    subject: format!("recommendation:{id}"),
                    details,
                };
                if let Err(e) = ext.audit.record(&entry).await {
                    tracing::warn!(error = %e, audit_id = %audit_id, "audit sink failed after commit");
                }
            }
            Ok((row, execution))
        }
    }
}

// ── mutation root ───────────────────────────────────────────────────────────

#[derive(Default)]
pub struct MutationRoot;

#[Object(name = "Mutation")]
impl MutationRoot {
    #[graphql(guard = "RequiresScope::new(\"decisions:resolve\")")]
    async fn accept_recommendation(
        &self,
        ctx: &Context<'_>,
        input: AcceptRecommendationInput,
    ) -> Result<AcceptRecommendationPayload> {
        let id = parse_uuid(&input.recommendation_id, "recommendationId")?;
        check_mutation_id(&input.client_mutation_id)?;
        check_text(input.note.as_deref(), "note")?;
        check_version(input.if_version)?;

        let res = Resolution::Accept {
            note: input.note,
            payload_override: input.payload_override.map(|j| j.0),
        };
        let (row, execution) =
            resolve(ctx, id, input.if_version, &input.client_mutation_id, res).await?;
        Ok(AcceptRecommendationPayload {
            recommendation: KukiRecommendation::new(row)?,
            action_execution: execution.map(ActionExecution::from_row).transpose()?,
            client_mutation_id: input.client_mutation_id,
        })
    }

    #[graphql(guard = "RequiresScope::new(\"decisions:resolve\")")]
    async fn reject_recommendation(
        &self,
        ctx: &Context<'_>,
        input: RejectRecommendationInput,
    ) -> Result<RejectRecommendationPayload> {
        let id = parse_uuid(&input.recommendation_id, "recommendationId")?;
        check_mutation_id(&input.client_mutation_id)?;
        check_text(input.reason_text.as_deref(), "reasonText")?;
        check_version(input.if_version)?;

        let res = Resolution::Reject {
            code: input.reason_code,
            text: input.reason_text,
        };
        let (row, _) = resolve(ctx, id, input.if_version, &input.client_mutation_id, res).await?;
        Ok(RejectRecommendationPayload {
            recommendation: KukiRecommendation::new(row)?,
            client_mutation_id: input.client_mutation_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use async_graphql::Value as GqlValue;

    use super::*;

    #[test]
    fn only_allow_passes_the_policy_gate() {
        assert!(policy_gate(&PolicyDecision::Allow).is_ok());
        for d in [
            PolicyDecision::RequiresHuman,
            PolicyDecision::RequiresAdditionalApproval { have: 1, need: 2 },
            PolicyDecision::Deny {
                reason: "not you".into(),
            },
        ] {
            let e = policy_gate(&d).unwrap_err();
            assert_eq!(
                e.extensions.unwrap().get("code"),
                Some(&GqlValue::from("FORBIDDEN")),
                "{d:?}"
            );
        }
    }

    #[test]
    fn resolutions_record_what_the_operator_did() {
        let original = json!({ "object_id": "o-1" });

        let r = accept_resolution(Some("go"), &original, None);
        assert_eq!(r, json!({ "payload_overridden": false, "note": "go" }));

        // An override identical to the original is not an override.
        let r = accept_resolution(None, &original, Some(&original));
        assert_eq!(r, json!({ "payload_overridden": false }));

        let changed = json!({ "object_id": "o-2" });
        let r = accept_resolution(None, &original, Some(&changed));
        assert_eq!(
            r,
            json!({ "payload_overridden": true, "payload_override": changed })
        );

        let r = reject_resolution(RejectionReasonCode::NotNow, Some("after quarter end"));
        assert_eq!(
            r,
            json!({ "reason_code": "NOT_NOW", "payload_overridden": false, "note": "after quarter end" })
        );
    }
}
