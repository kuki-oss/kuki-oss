//! The orchestrator: runs the enabled generators for one organisation and writes what they
//! propose to the recommendation store.
//!
//! One run, per generator:
//!     1. load the decision type (payload schema) and the pending rows of that type;
//!     2. evaluate against a `PgOntologyView` pinned to the run's clock (no database write is open);
//!     3. check every proposal and set aside the ones that fail, without failing the run;
//!     4. in shadow mode stop here; otherwise, in ONE transaction, upsert the proposals (most
//!      urgent first, within the review budget) and, after a full-scope run, expire this
//!      generator's pending rows that it no longer proposes.
//!
//! A generator that errors writes nothing and clears nothing: a bug must not wipe the inbox.
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::FutureExt;
use kuki_extension::{
    DecisionContext, DecisionGenerator, GenError, PendingRef, Proposal, ScopeSpec, Trigger,
    config::{GeneratorMode, OrgDecisionConfig},
};
use kuki_store::{
    engine_reads::{DecisionTypeInfo, created_counts_since, decision_type_info, pending_for_type},
    recommendations::{
        NewRecommendation, Transition, TransitionError, Transitioned, Upserted, expire_due,
        transition, upsert_pending,
    },
    session::{OrgSession, with_org_tx},
};
use kuki_types::{Action, Provenance, RecommendationStatus};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    config::{ConfigError, load_config},
    registry::Registry,
    view::PgOntologyView,
};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
    #[error("generator failed: {0}")]
    Generator(GenError),
    #[error("decision type `{0}` is not registered for this organisation")]
    UnregisteredDecisionType(String),
    #[error("decision type `{0}` has an unusable payload schema: {1}")]
    BadSchema(String, String),
    #[error("recommendation {0} has an unknown stored status {1:?}")]
    Corrupt(Uuid, String),
}

#[derive(Clone, Debug)]
pub struct EvaluatorOptions {
    /// How long a recommendation lives without being re-confirmed. Every run that still proposes
    /// it pushes the expiry out again, so a live condition never expires; if the evaluator stops
    /// running, the inbox drains instead of going stale.
    pub ttl: Duration,
    /// Recorded in provenance.
    pub ontology_version: i32,
    /// At most this many due recommendations are expired at the start of a run.
    pub expire_batch: i64,
}

impl Default for EvaluatorOptions {
    fn default() -> Self {
        Self {
            ttl: Duration::days(7),
            ontology_version: 1,
            expire_batch: 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rejection {
    pub dedup_key: String,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct GeneratorReport {
    pub generator_id: String,
    /// Shadow runs evaluate and validate but never write.
    pub shadow: bool,
    pub proposed: usize,
    pub created: usize,
    pub revised: usize,
    pub unchanged: usize,
    /// Pending recommendations expired because the generator no longer proposes them.
    pub cleared: usize,
    pub deferred_by_budget: usize,
    pub below_confidence: usize,
    pub rejected: Vec<Rejection>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunReport {
    pub organisation_id: Uuid,
    pub as_of: DateTime<Utc>,
    /// Recommendations whose `expires_at` had passed.
    pub expired: usize,
    pub generators: Vec<GeneratorReport>,
}

pub struct Evaluator {
    pool: PgPool,
    registry: Arc<Registry>,
    opts: EvaluatorOptions,
}

/// Midnight UTC of the day containing `now`: the review budget's window.
pub(crate) fn day_start(now: DateTime<Utc>) -> DateTime<Utc> {
    Utc.from_utc_datetime(
        &now.date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("midnight exists"),
    )
}

/// Whether a pending row's dedup key (`<org>/<decision_type>/<id>+<id>…`) names one of the scoped
/// entities. A full scope covers everything.
pub(crate) fn key_in_scope(prefix: &str, key: &str, scope: &ScopeSpec) -> bool {
    match scope {
        ScopeSpec::Entities(ids) => key.strip_prefix(prefix).is_some_and(|rest| {
            rest.split('+')
                .any(|seg| ids.iter().any(|id| id.to_string() == seg))
        }),
        _ => true,
    }
}

/// Everything about a proposal that the generator could get wrong.
pub(crate) fn check(
    org: Uuid,
    decision_type: &str,
    p: &Proposal,
    schema: &jsonschema::Validator,
) -> Result<(), String> {
    let c = &p.candidate;
    if c.decision_type != decision_type {
        return Err(format!(
            "decision_type `{}` is not the generator's `{decision_type}`",
            c.decision_type
        ));
    }
    c.validate().map_err(|e| e.to_string())?;
    let prefix = format!("{org}/{decision_type}/");
    if !c.dedup_key.as_str().starts_with(&prefix) {
        return Err("dedup_key is not scoped to this organisation and decision type".into());
    }
    if p.input_digest.len() != 64 || !p.input_digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("input_digest must be 64 hex characters".into());
    }
    if let Some(e) = schema.iter_errors(&c.payload).next() {
        return Err(format!("payload: {e}"));
    }
    Ok(())
}

/// Builds the store row. The body holds only what is stable between runs; ids,
/// clocks and the provenance live in their own columns, so an unchanged candidate
/// is a silent re-confirmation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble(
    g: &dyn DecisionGenerator,
    p: &Proposal,
    info: &DecisionTypeInfo,
    cfg: &OrgDecisionConfig,
    ontology_version: i32,
    now: DateTime<Utc>,
    ttl: Duration,
    action_type: Option<&str>,
) -> NewRecommendation {
    let c = &p.candidate;
    let action = Action {
        proposed: true,
        action_type: action_type.map(str::to_owned),
        payload: c.payload.clone(),
        payload_schema_ref: info.payload_schema_ref.clone(),
    };
    let body = json!({
        "decision_type": c.decision_type,
        "dedup_key": c.dedup_key,
        "org_unit_id": c.org_unit_id,
        "action": action,
        "rationale": c.rationale,
        "urgency": c.urgency,
        "impact": c.impact,
        "evidence": c.evidence,
    });
    let provenance = Provenance {
        generator_id: g.id().to_owned(),
        generator_version: g.version().to_string(),
        tier: g.tier(),
        pack: info.pack.clone(),
        ontology_version,
        config_hash: cfg.config_hash(),
        as_of: now,
        input_digest: p.input_digest.clone(),
        model_artefact: None,
    };
    NewRecommendation {
        id: Uuid::new_v4(),
        org_unit_id: c.org_unit_id,
        decision_type: c.decision_type.clone(),
        dedup_key: c.dedup_key.as_str().to_owned(),
        urgency: c.urgency,
        confidence: c.rationale.confidence,
        rationale_summary: c.rationale.summary.clone(),
        body,
        provenance: serde_json::to_value(provenance).expect("provenance serialises"),
        expires_at: Some(now + ttl),
    }
}

#[derive(Default)]
struct Tally {
    created: usize,
    revised: usize,
    unchanged: usize,
    cleared: usize,
    deferred: usize,
}

impl Evaluator {
    pub fn new(pool: PgPool, registry: Arc<Registry>, opts: EvaluatorOptions) -> Self {
        Self {
            pool,
            registry,
            opts,
        }
    }

    /// Runs every generator the organisation has enabled, at the clock `now`.
    pub async fn run(
        &self,
        trigger: &Trigger,
        now: DateTime<Utc>,
    ) -> Result<RunReport, EngineError> {
        let org = trigger.organisation_id;
        let session = OrgSession {
            org_id: org,
            scope_path: None,
        };

        let cfg = Arc::new(
            with_org_tx::<_, ConfigError, _>(&self.pool, &session, move |c| {
                async move { load_config(c, org).await }.boxed()
            })
            .await?,
        );

        let batch = self.opts.expire_batch;
        let expired = with_org_tx::<_, EngineError, _>(&self.pool, &session, move |c| {
            async move { Ok(expire_due(c, org, now, batch).await?.len()) }.boxed()
        })
        .await?;

        let since = day_start(now);
        let counts = with_org_tx::<_, EngineError, _>(&self.pool, &session, move |c| {
            async move { Ok(created_counts_since(c, org, since).await?) }.boxed()
        })
        .await?;
        let mut used: HashMap<Option<Uuid>, i64> = counts.into_iter().collect();

        let mut generators = Vec::new();
        for g in self.registry.enabled(&cfg) {
            let mut rep = GeneratorReport {
                generator_id: g.id().to_owned(),
                shadow: cfg.mode(g.id()) == GeneratorMode::Shadow,
                ..Default::default()
            };
            if let Err(e) = self
                .run_generator(g, &cfg, trigger, now, &session, &mut used, &mut rep)
                .await
            {
                rep.error = Some(e.to_string());
            }
            generators.push(rep);
        }
        Ok(RunReport {
            organisation_id: org,
            as_of: now,
            expired,
            generators,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_generator(
        &self,
        g: &Arc<dyn DecisionGenerator>,
        cfg: &Arc<OrgDecisionConfig>,
        trigger: &Trigger,
        now: DateTime<Utc>,
        session: &OrgSession,
        used: &mut HashMap<Option<Uuid>, i64>,
        rep: &mut GeneratorReport,
    ) -> Result<(), EngineError> {
        let org = session.org_id;
        let name = g.decision_type().to_owned();

        let (info, pending) = with_org_tx::<_, EngineError, _>(&self.pool, session, {
            let name = name.clone();
            move |c| {
                async move {
                    let info = decision_type_info(c, org, &name).await?;
                    let pending = pending_for_type(c, org, &name).await?;
                    Ok((info, pending))
                }
                .boxed()
            }
        })
        .await?;
        let info = info.ok_or_else(|| EngineError::UnregisteredDecisionType(name.clone()))?;
        let schema = jsonschema::validator_for(&info.payload_schema)
            .map_err(|e| EngineError::BadSchema(name.clone(), e.to_string()))?;

        let refs: Vec<PendingRef> = pending
            .iter()
            .filter(|p| p.generator_id.as_deref() == Some(g.id()))
            .filter_map(|p| {
                Some(PendingRef {
                    recommendation_id: p.id,
                    dedup_key: serde_json::from_value(Value::String(p.dedup_key.clone())).ok()?,
                    version: p.version,
                })
            })
            .collect();

        let view = Arc::new(PgOntologyView::new(self.pool.clone(), org, now));
        let ctx = DecisionContext::new(org, now, g.id(), cfg.clone(), view).with_pending(refs);
        let scope = g.scope(trigger, &ctx);
        let proposals = g
            .evaluate(&ctx, &scope)
            .await
            .map_err(EngineError::Generator)?;
        rep.proposed = proposals.len();

        // Check every proposal. `mentioned` catches duplicates; `keep` is every key the generator
        // still stands behind (accepted or merely malformed), which must not be cleared below.
        let floor = cfg.min_confidence_for_display;
        let mut mentioned = HashSet::new();
        let mut keep = HashSet::new();
        let mut accepted: Vec<NewRecommendation> = Vec::new();
        for p in &proposals {
            let key = p.candidate.dedup_key.as_str().to_owned();
            if let Err(reason) = check(org, g.decision_type(), p, &schema) {
                keep.insert(key.clone());
                rep.rejected.push(Rejection {
                    dedup_key: key,
                    reason,
                });
                continue;
            }
            if !mentioned.insert(key.clone()) {
                rep.rejected.push(Rejection {
                    dedup_key: key,
                    reason: "duplicate dedup_key within one run".into(),
                });
                continue;
            }
            if p.candidate.rationale.confidence < floor {
                rep.below_confidence += 1;
                continue;
            }
            keep.insert(key);
            accepted.push(assemble(
                g.as_ref(),
                p,
                &info,
                cfg,
                self.opts.ontology_version,
                now,
                self.opts.ttl,
                info.default_action_type.as_deref(),
            ));
        }

        if rep.shadow {
            return Ok(());
        }

        // Most urgent first, so the review budget is spent where it matters most.
        accepted.sort_by(|a, b| {
            b.urgency
                .partial_cmp(&a.urgency)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.dedup_key.cmp(&b.dedup_key))
        });

        // A run can only conclude that something is no longer proposed for what it covered:
        // everything for a full scope, otherwise the rows whose dedup key names a scoped entity.
        let key_prefix = format!("{org}/{name}/");
        let clear: Vec<Uuid> = pending
            .iter()
            .filter(|p| {
                p.generator_id.as_deref() == Some(g.id())
                    && !keep.contains(&p.dedup_key)
                    && key_in_scope(&key_prefix, &p.dedup_key, &scope)
            })
            .map(|p| p.id)
            .collect();

        let pending_keys: HashSet<String> = pending.iter().map(|p| p.dedup_key.clone()).collect();
        let budget = i64::from(cfg.review_budget.per_org_unit_per_day);
        let mut used_now = used.clone();
        let generator_id = g.id().to_owned();

        let (tally, used_after) = with_org_tx::<_, EngineError, _>(&self.pool, session, move |c| {
            async move {
                let mut t = Tally::default();
                for new in accepted {
                    // Revising or confirming something already pending never costs budget.
                    if !pending_keys.contains(&new.dedup_key)
                        && *used_now.entry(new.org_unit_id).or_insert(0) >= budget
                    {
                        t.deferred += 1;
                        continue;
                    }
                    match upsert_pending(c, org, &new, now).await? {
                        Upserted::Created { .. } => {
                            t.created += 1;
                            *used_now.entry(new.org_unit_id).or_insert(0) += 1;
                        }
                        Upserted::Revised { .. } => t.revised += 1,
                        Upserted::Unchanged { .. } => t.unchanged += 1,
                    }
                }
                let resolution =
                    json!({ "reason": "condition_cleared", "generator": generator_id });
                for id in clear {
                    let tr = Transition {
                        id,
                        to: RecommendationStatus::Expired,
                        expected_version: None,
                        actor: "system:evaluator",
                        resolution: Some(&resolution),
                    };
                    match transition(c, org, &tr, now).await {
                        Ok(Transitioned::Changed { .. }) => t.cleared += 1,
                        // Resolved by someone else in the meantime: nothing to do.
                        Ok(Transitioned::Unchanged)
                        | Err(TransitionError::NotFound)
                        | Err(TransitionError::Illegal { .. })
                        | Err(TransitionError::VersionConflict { .. }) => {}
                        Err(TransitionError::Corrupt(s)) => {
                            return Err(EngineError::Corrupt(id, s));
                        }
                        Err(TransitionError::Db(e)) => return Err(e.into()),
                    }
                }
                Ok((t, used_now))
            }
            .boxed()
        })
        .await?;

        *used = used_after;
        rep.created = tally.created;
        rep.revised = tally.revised;
        rep.unchanged = tally.unchanged;
        rep.cleared = tally.cleared;
        rep.deferred_by_budget = tally.deferred;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use kuki_types::{Candidate, ConfidenceKind, DedupKey, Factor, FactorValue, Rationale};

    use super::*;
    use crate::generators::deadline::DeadlineAtRisk;

    fn org() -> Uuid {
        Uuid::from_u128(7)
    }

    fn proposal(mut f: impl FnMut(&mut Candidate, &mut String)) -> Proposal {
        let mut c = Candidate {
            decision_type: "t.d".into(),
            dedup_key: DedupKey::new(org(), "t.d", &["x"]),
            org_unit_id: None,
            payload: json!({ "n": 1 }),
            rationale: Rationale {
                summary: "s".into(),
                factors: vec![Factor {
                    label: "f".into(),
                    value: FactorValue::Text("v".into()),
                    unit: None,
                    weight: None,
                    direction: None,
                    evidence: vec![],
                }],
                confidence: 0.9,
                confidence_kind: ConfidenceKind::DataQuality,
                counterfactual: None,
                narrative: None,
            },
            urgency: 0.5,
            impact: None,
            evidence: vec![],
        };
        let mut digest = "a".repeat(64);
        f(&mut c, &mut digest);
        Proposal {
            candidate: c,
            input_digest: digest,
        }
    }

    fn schema() -> jsonschema::Validator {
        jsonschema::validator_for(&json!({ "type": "object", "required": ["n"] })).unwrap()
    }

    fn rejected(p: &Proposal) -> String {
        check(org(), "t.d", p, &schema()).unwrap_err()
    }

    #[test]
    fn a_well_formed_proposal_passes() {
        assert!(check(org(), "t.d", &proposal(|_, _| {}), &schema()).is_ok());
    }

    #[test]
    fn the_wrong_decision_type_is_rejected() {
        assert!(
            rejected(&proposal(|c, _| c.decision_type = "other".into()))
                .contains("not the generator's")
        );
    }

    #[test]
    fn a_key_for_another_organisation_is_rejected() {
        let p = proposal(|c, _| c.dedup_key = DedupKey::new(Uuid::from_u128(8), "t.d", &["x"]));
        assert!(rejected(&p).contains("scoped"));
    }

    #[test]
    fn an_out_of_range_urgency_is_rejected() {
        assert!(rejected(&proposal(|c, _| c.urgency = 2.0)).contains("urgency"));
    }

    #[test]
    fn a_malformed_digest_is_rejected() {
        assert!(rejected(&proposal(|_, d| *d = "nope".into())).contains("input_digest"));
    }

    #[test]
    fn a_payload_that_breaks_the_schema_is_rejected() {
        assert!(rejected(&proposal(|c, _| c.payload = json!({}))).contains("payload"));
    }

    #[test]
    fn an_unchanged_candidate_assembles_to_the_same_body() {
        let info = DecisionTypeInfo {
            pack: "p".into(),
            payload_schema_ref: "p/t.d.json".into(),
            payload_schema: json!({}),
            default_action_type: None,
        };
        let cfg = OrgDecisionConfig::default();
        let p = proposal(|_, _| {});
        let t0 = Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap();
        let a = assemble(
            &DeadlineAtRisk,
            &p,
            &info,
            &cfg,
            1,
            t0,
            Duration::days(7),
            None,
        );
        let b = assemble(
            &DeadlineAtRisk,
            &p,
            &info,
            &cfg,
            1,
            t0 + Duration::minutes(5),
            Duration::days(7),
            None,
        );
        assert_eq!(a.body, b.body, "the store would revise on every run");
        assert_eq!(
            (a.urgency, a.confidence, &a.rationale_summary),
            (b.urgency, b.confidence, &b.rationale_summary)
        );
        assert_ne!(a.expires_at, b.expires_at);
        assert_ne!(a.provenance, b.provenance, "provenance records the clock");
    }

    #[test]
    fn the_decision_types_action_is_carried_into_the_body() {
        let info = DecisionTypeInfo {
            pack: "p".into(),
            payload_schema_ref: "p/t.d.json".into(),
            payload_schema: json!({}),
            default_action_type: Some("builtin:flag_review".into()),
        };
        let cfg = OrgDecisionConfig::default();
        let p = proposal(|_, _| {});
        let t0 = Utc.with_ymd_and_hms(2026, 10, 11, 12, 0, 0).unwrap();
        let with = assemble(
            &DeadlineAtRisk,
            &p,
            &info,
            &cfg,
            1,
            t0,
            Duration::days(7),
            Some("builtin:flag_review"),
        );
        assert_eq!(with.body["action"]["action_type"], "builtin:flag_review");
        assert_eq!(with.body["action"]["proposed"], true);
        let without = assemble(
            &DeadlineAtRisk,
            &p,
            &info,
            &cfg,
            1,
            t0,
            Duration::days(7),
            None,
        );
        assert!(without.body["action"]["action_type"].is_null());
    }

    #[test]
    fn the_budget_window_starts_at_midnight_utc() {
        let now = Utc.with_ymd_and_hms(2026, 10, 11, 23, 59, 59).unwrap();
        assert_eq!(
            day_start(now),
            Utc.with_ymd_and_hms(2026, 10, 11, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn scoped_keys_match_on_entity_ids() {
        let a = Uuid::from_u128(1);
        let prefix = "o/t.d/";
        let some = ScopeSpec::Entities(vec![a]);
        assert!(key_in_scope(prefix, &format!("o/t.d/{a}"), &some));
        assert!(key_in_scope(prefix, &format!("o/t.d/{a}+other"), &some));
        assert!(!key_in_scope(prefix, "o/t.d/other", &some));
        assert!(!key_in_scope(prefix, &format!("elsewhere/{a}"), &some));
        assert!(key_in_scope(prefix, "o/t.d/other", &ScopeSpec::All));
    }
}
