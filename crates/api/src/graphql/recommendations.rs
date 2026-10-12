//! Recommendation reads (spec 9.1): recommendation, recommendations, recommendationSummary and
//! recommendationChanges. Stored bodies are parsed back into the kuki-types model, so a row that
//! does not parse is an INTERNAL error, never a partial answer.
//!
//! Not served yet: `EvidenceRef.target`, and the pack-typed `ActionPayload` members.

use std::collections::BTreeSet;

use async_graphql::{
    Context, Enum, Error, ID, InputObject, InputValueError, InputValueResult, Json, Object, Result,
    Scalar, ScalarType, SimpleObject, Union, Value,
};
use chrono::{DateTime, Duration, Utc};
use futures::FutureExt;
use kuki_store::recommendation_reads as reads;
use kuki_store::recommendations::RecommendationRow;
use kuki_types as kt;
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use uuid::Uuid;

use super::connection::{Cursor, PageInfo, check_ltree, page_size, parse_uuid, validation};
use super::guard::RequiresScope;
use crate::auth::Principal;
use crate::error::ApiError;

/// Decimal as a string, never a JSON float.
pub struct Decimal(String);

fn is_decimal(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    let mut parts = s.splitn(2, '.');
    let int = parts.next().unwrap_or("");
    let frac = parts.next();
    !int.is_empty()
        && int.bytes().all(|b| b.is_ascii_digit())
        && frac.is_none_or(|f| !f.is_empty() && f.bytes().all(|b| b.is_ascii_digit()))
}

#[Scalar]
impl ScalarType for Decimal {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) if is_decimal(&s) => Ok(Decimal(s)),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

/// A string or a number (spec 9.1 KukiFactor.value).
pub struct FactorValue(kt::FactorValue);

#[Scalar]
impl ScalarType for FactorValue {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => Ok(FactorValue(kt::FactorValue::Text(s))),
            Value::Number(n) => Ok(FactorValue(kt::FactorValue::Number(n))),
            other => Err(InputValueError::expected_type(other)),
        }
    }
    fn to_value(&self) -> Value {
        match &self.0 {
            kt::FactorValue::Text(s) => Value::String(s.clone()),
            kt::FactorValue::Number(n) => Value::Number(n.clone()),
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum KukiRecommendationStatus {
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

impl From<kt::RecommendationStatus> for KukiRecommendationStatus {
    fn from(s: kt::RecommendationStatus) -> Self {
        use kt::RecommendationStatus as S;
        match s {
            S::Pending => Self::Pending,
            S::Accepted => Self::Accepted,
            S::Rejected => Self::Rejected,
            S::Expired => Self::Expired,
            S::Superseded => Self::Superseded,
            S::Executing => Self::Executing,
            S::Executed => Self::Executed,
            S::Failed => Self::Failed,
            S::AutoExecuted => Self::AutoExecuted,
        }
    }
}

impl From<KukiRecommendationStatus> for kt::RecommendationStatus {
    fn from(s: KukiRecommendationStatus) -> Self {
        use KukiRecommendationStatus as S;
        match s {
            S::Pending => Self::Pending,
            S::Accepted => Self::Accepted,
            S::Rejected => Self::Rejected,
            S::Expired => Self::Expired,
            S::Superseded => Self::Superseded,
            S::Executing => Self::Executing,
            S::Executed => Self::Executed,
            S::Failed => Self::Failed,
            S::AutoExecuted => Self::AutoExecuted,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum Tier {
    #[graphql(name = "T0")]
    T0,
    #[graphql(name = "T1")]
    T1,
    #[graphql(name = "T2")]
    T2,
    #[graphql(name = "T3")]
    T3,
    #[graphql(name = "T4")]
    T4,
    #[graphql(name = "T5")]
    T5,
}

impl From<kt::Tier> for Tier {
    fn from(t: kt::Tier) -> Self {
        match t {
            kt::Tier::T0 => Self::T0,
            kt::Tier::T1 => Self::T1,
            kt::Tier::T2 => Self::T2,
            kt::Tier::T3 => Self::T3,
            kt::Tier::T4 => Self::T4,
            kt::Tier::T5 => Self::T5,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum ConfidenceKind {
    DataQuality,
    PredictiveInterval,
    CalibratedProbability,
}

impl From<kt::ConfidenceKind> for ConfidenceKind {
    fn from(k: kt::ConfidenceKind) -> Self {
        match k {
            kt::ConfidenceKind::DataQuality => Self::DataQuality,
            kt::ConfidenceKind::PredictiveInterval => Self::PredictiveInterval,
            kt::ConfidenceKind::CalibratedProbability => Self::CalibratedProbability,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum EvidenceKind {
    Event,
    Object,
    Link,
    Metric,
}

impl From<kt::EvidenceKind> for EvidenceKind {
    fn from(k: kt::EvidenceKind) -> Self {
        match k {
            kt::EvidenceKind::Event => Self::Event,
            kt::EvidenceKind::Object => Self::Object,
            kt::EvidenceKind::Link => Self::Link,
            kt::EvidenceKind::Metric => Self::Metric,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum FactorDirection {
    Raises,
    Lowers,
}

impl From<kt::FactorDirection> for FactorDirection {
    fn from(d: kt::FactorDirection) -> Self {
        match d {
            kt::FactorDirection::Raises => Self::Raises,
            kt::FactorDirection::Lowers => Self::Lowers,
        }
    }
}

/// Also the stored form (`action_executions.status`).
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionExecutionStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Compensated,
    Cancelled,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum RecommendationOrder {
    /// urgency desc, created desc. Matches the inbox index.
    Inbox,
    CreatedDesc,
}

#[derive(SimpleObject)]
pub struct Impact {
    value: Decimal,
    unit: String,
}

impl From<kt::Impact> for Impact {
    fn from(i: kt::Impact) -> Self {
        Impact {
            value: Decimal(i.value.to_string()),
            unit: i.unit,
        }
    }
}

#[derive(SimpleObject)]
pub struct EvidenceRef {
    kind: EvidenceKind,
    id: String,
    as_of: DateTime<Utc>,
    note: Option<String>,
}

impl From<kt::EvidenceRef> for EvidenceRef {
    fn from(e: kt::EvidenceRef) -> Self {
        EvidenceRef {
            kind: e.kind.into(),
            id: e.id,
            as_of: e.as_of,
            note: e.note,
        }
    }
}

#[derive(SimpleObject)]
pub struct KukiFactor {
    label: String,
    value: FactorValue,
    unit: Option<String>,
    /// [0,1]. Present only for model-derived factors.
    weight: Option<f64>,
    direction: Option<FactorDirection>,
    evidence: Vec<EvidenceRef>,
}

impl From<kt::Factor> for KukiFactor {
    fn from(f: kt::Factor) -> Self {
        KukiFactor {
            label: f.label,
            value: FactorValue(f.value),
            unit: f.unit,
            weight: f.weight,
            direction: f.direction.map(Into::into),
            evidence: f.evidence.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(SimpleObject)]
pub struct KukiRationale {
    summary: String,
    factors: Vec<KukiFactor>,
    confidence: f64,
    confidence_kind: ConfidenceKind,
    counterfactual: Option<String>,
    narrative: Option<String>,
}

impl From<kt::Rationale> for KukiRationale {
    fn from(r: kt::Rationale) -> Self {
        KukiRationale {
            summary: r.summary,
            factors: r.factors.into_iter().map(Into::into).collect(),
            confidence: r.confidence,
            confidence_kind: r.confidence_kind.into(),
            counterfactual: r.counterfactual,
            narrative: r.narrative,
        }
    }
}

#[derive(SimpleObject)]
pub struct Provenance {
    generator_id: String,
    generator_version: String,
    tier: Tier,
    pack: String,
    ontology_version: i32,
    config_hash: String,
    as_of: DateTime<Utc>,
    input_digest: String,
    model_artefact: Option<String>,
}

impl From<kt::Provenance> for Provenance {
    fn from(p: kt::Provenance) -> Self {
        Provenance {
            generator_id: p.generator_id,
            generator_version: p.generator_version,
            tier: p.tier.into(),
            pack: p.pack,
            ontology_version: p.ontology_version,
            config_hash: p.config_hash,
            as_of: p.as_of,
            input_digest: p.input_digest,
            model_artefact: p.model_artefact,
        }
    }
}

#[derive(SimpleObject)]
pub struct GenericPayload {
    payload: Json<JsonValue>,
}

/// Pack-typed members (replenish, scheduling, anomaly) arrive with their packs.
#[derive(Union)]
pub enum ActionPayload {
    Generic(GenericPayload),
}

#[derive(SimpleObject)]
pub struct KukiAction {
    proposed: bool,
    action_type: Option<String>,
    payload_schema_ref: String,
    payload: ActionPayload,
}

impl From<kt::Action> for KukiAction {
    fn from(a: kt::Action) -> Self {
        KukiAction {
            proposed: a.proposed,
            action_type: a.action_type,
            payload_schema_ref: a.payload_schema_ref,
            payload: ActionPayload::Generic(GenericPayload {
                payload: Json(a.payload),
            }),
        }
    }
}

#[derive(SimpleObject)]
pub struct RecommendationRevision {
    revised_at: DateTime<Utc>,
    snapshot: Json<JsonValue>,
}

fn revisions(row: &RecommendationRow) -> Vec<RecommendationRevision> {
    row.revision_history
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| RecommendationRevision {
                    revised_at: e
                        .get("replacedAt")
                        .and_then(JsonValue::as_str)
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|d| d.with_timezone(&Utc))
                        .unwrap_or(row.updated_at),
                    snapshot: Json(e.clone()),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Why a recommendation left `pending`. Reads both writers' shapes: the evaluator's
/// `{"reason": ...}` and the resolve mutations' `{"reason_code", "note", "payload_overriden"}`.
#[derive(SimpleObject)]
pub struct RecommendationResolution {
    reason_code: Option<String>,
    note: Option<String>,
    payload_overridden: bool,
}

fn resolution(v: &JsonValue) -> RecommendationResolution {
    let s = |k: &str| v.get(k).and_then(JsonValue::as_str).map(str::to_owned);
    RecommendationResolution {
        reason_code: s("reason_code").or_else(|| s("reason")),
        note: s("note"),
        payload_overridden: v
            .get("payload_overridden")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    }
}

#[derive(SimpleObject)]
pub struct ActionExecution {
    id: ID,
    action_type: String,
    status: ActionExecutionStatus,
    adapter: String,
    attempts: i32,
    created_by: String,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    result: Option<Json<JsonValue>>,
}

fn internal(e: serde_json::Error) -> Error {
    Error::from(ApiError::internal(e))
}

fn stored<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_value(JsonValue::String(s.to_owned())).map_err(internal)
}

fn part<T: serde::de::DeserializeOwned>(v: &JsonValue, key: &str) -> Result<T> {
    serde_json::from_value(v.get(key).cloned().unwrap_or(JsonValue::Null)).map_err(internal)
}

impl ActionExecution {
    fn from_row(r: reads::ExecutionRow) -> Result<Self> {
        Ok(ActionExecution {
            id: ID(r.id.to_string()),
            action_type: r.action_type,
            status: stored(&r.status)?,
            adapter: r.adapter,
            attempts: r.attempts,
            created_by: r.created_by,
            created_at: r.created_at,
            finished_at: r.finished_at,
            result: r.result.map(Json),
        })
    }
}

pub struct KukiRecommendation {
    row: RecommendationRow,
    status: KukiRecommendationStatus,
    action: kt::Action,
    rationale: kt::Rationale,
    impact: Option<kt::Impact>,
    provenance: kt::Provenance,
}

impl KukiRecommendation {
    fn new(row: RecommendationRow) -> Result<Self> {
        let status: kt::RecommendationStatus = stored(&row.status)?;
        let action: kt::Action = part(&row.body, "action")?;
        let rationale: kt::Rationale = part(&row.body, "rationale")?;
        let impact: Option<kt::Impact> = part(&row.body, "impact")?;
        let provenance: kt::Provenance =
            serde_json::from_value(row.provenance.clone()).map_err(internal)?;
        Ok(KukiRecommendation {
            status: status.into(),
            row,
            action,
            rationale,
            impact,
            provenance,
        })
    }
}

#[Object]
impl KukiRecommendation {
    async fn id(&self) -> ID {
        ID(self.row.id.to_string())
    }
    async fn organisation_id(&self) -> ID {
        ID(self.row.organisation_id.to_string())
    }
    async fn org_unit_id(&self) -> Option<ID> {
        self.row.org_unit_id.map(|u| ID(u.to_string()))
    }
    /// Namespaced: inventory.replenish, scheduling.conflict, ...
    async fn decision_type(&self) -> &str {
        &self.row.decision_type
    }
    async fn dedup_key(&self) -> &str {
        &self.row.dedup_key
    }
    async fn action(&self) -> KukiAction {
        self.action.clone().into()
    }
    async fn rationale(&self) -> KukiRationale {
        self.rationale.clone().into()
    }
    /// [0,1].
    async fn urgency(&self) -> f64 {
        self.row.urgency
    }
    async fn impact(&self) -> Option<Impact> {
        self.impact.clone().map(Into::into)
    }
    async fn provenance(&self) -> Provenance {
        self.provenance.clone().into()
    }
    async fn status(&self) -> KukiRecommendationStatus {
        self.status
    }
    /// User id, or policy:<id>.
    async fn resolved_by(&self) -> Option<&str> {
        self.row.resolved_by.as_deref()
    }
    async fn resolution(&self) -> Option<RecommendationResolution> {
        self.row.resolution.as_ref().map(resolution)
    }
    async fn created_at(&self) -> DateTime<Utc> {
        self.row.created_at
    }
    async fn last_confirmed_at(&self) -> DateTime<Utc> {
        self.row.last_confirmed_at
    }
    async fn resolved_at(&self) -> Option<DateTime<Utc>> {
        self.row.resolved_at
    }
    async fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.row.expires_at
    }
    /// Bounded, not paginated.
    async fn revisions(&self) -> Vec<RecommendationRevision> {
        revisions(&self.row)
    }
    /// Bounded, not paginated.
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "10 + child_complexity"
    )]
    async fn executions(&self, ctx: &Context<'_>) -> Result<Vec<ActionExecution>> {
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let (org, id) = (p.session.org_id, self.row.id);
        let rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            async move { reads::executions_for(c, org, id).await }.boxed()
        })
        .await
        .map_err(ApiError::internal)?;
        rows.into_iter().map(ActionExecution::from_row).collect()
    }
    /// Increments on every row change. Dedupe key for clients and webhooks.
    async fn version(&self) -> i32 {
        self.row.version
    }
    async fn updated_at(&self) -> DateTime<Utc> {
        self.row.updated_at
    }
}

#[derive(SimpleObject)]
pub struct KukiRecommendationEdge {
    node: KukiRecommendation,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct KukiRecommendationConnection {
    edges: Vec<KukiRecommendationEdge>,
    page_info: PageInfo,
}

fn connection(
    edges: Vec<KukiRecommendationEdge>,
    has_next_page: bool,
) -> KukiRecommendationConnection {
    KukiRecommendationConnection {
        page_info: PageInfo {
            has_next_page,
            has_previous_page: false, // forward-only pagination (contract)
            start_cursor: edges.first().map(|e| e.cursor.clone()),
            end_cursor: edges.last().map(|e| e.cursor.clone()),
        },
        edges,
    }
}

#[derive(SimpleObject)]
pub struct DecisionTypeCount {
    decision_type: String,
    count: i32,
}

#[derive(SimpleObject)]
pub struct RecommendationSummary {
    pending_total: i32,
    pending_by_decision_type: Vec<DecisionTypeCount>,
    #[graphql(name = "acceptedLast30d")]
    accepted_last_30d: i32,
    #[graphql(name = "rejectedLast30d")]
    rejected_last_30d: i32,
    as_of: DateTime<Utc>,
}

fn int(n: i64) -> i32 {
    i32::try_from(n).unwrap_or(i32::MAX)
}

#[derive(InputObject, Default)]
pub struct RecommendationFilter {
    /// Default: [PENDING].
    statuses: Option<Vec<KukiRecommendationStatus>>,
    decision_types: Option<Vec<String>>,
    /// ltree descendant match (path <@ value). Intersected with the caller's scope.
    org_unit_path: Option<String>,
    min_urgency: Option<f64>,
    /// Server floor min_confidence_for_display still applies (default 0.30).
    min_confidence: Option<f64>,
}

#[derive(Clone)]
struct Prepared {
    statuses: Vec<String>,
    decision_types: Option<Vec<String>>,
    under: Option<String>,
    min_urgency: Option<f64>,
    min_confidence: f64,
}

fn unit_interval(v: Option<f64>, what: &str) -> Result<Option<f64>> {
    match v {
        Some(x) if !(x.is_finite() && (0.0..=1.0).contains(&x)) => {
            Err(validation(&format!("{what} must be between 0 and 1"), None))
        }
        other => Ok(other),
    }
}

fn prepare(f: RecommendationFilter) -> Result<Prepared> {
    let statuses: Vec<String> = match f.statuses {
        None => vec!["pending".to_owned()],
        Some(s) if s.is_empty() => return Err(validation("statuses must not be empty", None)),
        Some(s) => s
            .into_iter()
            .map(|s| kt::RecommendationStatus::from(s).as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect(),
    };
    let decision_types = match f.decision_types {
        None => None,
        Some(t) if t.is_empty() => return Err(validation("decisionTypes must not be empty", None)),
        Some(t) if t.iter().any(|x| x.is_empty() || x.len() > 200) => {
            return Err(validation(
                "decisionTypes entries must be 1 to 200 characters",
                None,
            ));
        }
        Some(t) => Some(t),
    };
    if let Some(u) = &f.org_unit_path {
        check_ltree(u, "orgUnitPath")?;
    }
    Ok(Prepared {
        statuses,
        decision_types,
        under: f.org_unit_path,
        min_urgency: unit_interval(f.min_urgency, "minUrgency")?,
        min_confidence: unit_interval(f.min_confidence, "minConfidence")?.unwrap_or(0.0),
    })
}

// cursors
//
// ib1.<urgency>_<micros>_<id>   inbox
// cd1.<micros>_<id>             created desc
// ch1.<micros>_<id>             changes
// Timestamps are microseconds, the precision Postgres stores, so cursors are lossless.

fn bad_cursor() -> Error {
    validation("invalid cursor", None)
}

fn from_micros(s: &str) -> Option<DateTime<Utc>> {
    s.parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_micros)
}

fn simple_uuid(s: &str) -> Option<Uuid> {
    if s.len() == 32 {
        Uuid::parse_str(s).ok()
    } else {
        None
    }
}

fn urgency_ok(s: &str) -> bool {
    s.len() <= 24 && !s.starts_with('-') && is_decimal(s)
}

fn encode_inbox(urgency: &str, at: DateTime<Utc>, id: Uuid) -> Cursor {
    Cursor(format!(
        "ib1.{urgency}_{}_{}",
        at.timestamp_micros(),
        id.simple()
    ))
}

fn encode_created(at: DateTime<Utc>, id: Uuid) -> Cursor {
    Cursor(format!("cd1.{}_{}", at.timestamp_micros(), id.simple()))
}

fn encode_changes(at: DateTime<Utc>, id: Uuid) -> Cursor {
    Cursor(format!("ch1.{}_{}", at.timestamp_micros(), id.simple()))
}

fn decode_pair(c: &Cursor, prefix: &str) -> Result<(DateTime<Utc>, Uuid)> {
    let rest = c.0.strip_prefix(prefix).ok_or_else(bad_cursor)?;
    let (t, i) = rest.split_once('_').ok_or_else(bad_cursor)?;
    Ok((
        from_micros(t).ok_or_else(bad_cursor)?,
        simple_uuid(i).ok_or_else(bad_cursor)?,
    ))
}

fn decode_list(c: &Cursor, order: RecommendationOrder) -> Result<reads::After> {
    match order {
        RecommendationOrder::Inbox => {
            let rest = c.0.strip_prefix("ib1.").ok_or_else(bad_cursor)?;
            let mut it = rest.splitn(3, '_');
            match (it.next(), it.next(), it.next()) {
                (Some(u), Some(t), Some(i)) if urgency_ok(u) => Ok(reads::After::Inbox {
                    urgency: u.to_owned(),
                    created_at: from_micros(t).ok_or_else(bad_cursor)?,
                    id: simple_uuid(i).ok_or_else(bad_cursor)?,
                }),
                _ => Err(bad_cursor()),
            }
        }
        RecommendationOrder::CreatedDesc => {
            let (created_at, id) = decode_pair(c, "cd1.")?;
            Ok(reads::After::Created { created_at, id })
        }
    }
}

#[derive(Default)]
pub struct RecommendationQuery;

#[Object]
impl RecommendationQuery {
    /// Null when it does not exist or is outside the caller's scope. Not subject to the
    /// display floor: a direct lookup is not an inbox.
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn recommendation(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> Result<Option<KukiRecommendation>> {
        let id = parse_uuid(&id, "id")?;
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let row = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            async move { kuki_store::recommendations::get(c, org, id).await }.boxed()
        })
        .await
        .map_err(ApiError::internal)?;
        row.map(KukiRecommendation::new).transpose()
    }

    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn recommendations(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
        filter: Option<RecommendationFilter>,
        #[graphql(default_with = "Some(RecommendationOrder::Inbox)")] order_by: Option<
            RecommendationOrder,
        >,
    ) -> Result<KukiRecommendationConnection> {
        let limit = page_size(first)?;
        let order = order_by.unwrap_or(RecommendationOrder::Inbox);
        let after = after.as_ref().map(|c| decode_list(c, order)).transpose()?;
        let f = prepare(filter.unwrap_or_default())?;
        let store_order = match order {
            RecommendationOrder::Inbox => reads::Order::Inbox,
            RecommendationOrder::CreatedDesc => reads::Order::Created,
        };

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let (f, after) = (f.clone(), after.clone());
            async move {
                let floor = reads::display_floor(c, org).await?;
                let lf = reads::ListFilter {
                    statuses: &f.statuses,
                    decision_types: f.decision_types.as_deref(),
                    min_urgency: f.min_urgency,
                    visible: reads::Visible {
                        under: f.under.as_deref(),
                        min_confidence: floor.max(f.min_confidence),
                    },
                };
                reads::list(c, org, &lf, store_order, after.as_ref(), limit + 1).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let mut edges = Vec::with_capacity(rows.len());
        for l in rows {
            let cursor = match order {
                RecommendationOrder::Inbox => {
                    encode_inbox(&l.urgency_key, l.row.created_at, l.row.id)
                }
                RecommendationOrder::CreatedDesc => encode_created(l.row.created_at, l.row.id),
            };
            edges.push(KukiRecommendationEdge {
                cursor,
                node: KukiRecommendation::new(l.row)?,
            });
        }
        Ok(connection(edges, has_next))
    }

    /// Counts over the same visible population as `recommendations`, so `pendingTotal` equals
    /// the inbox size. `orgUnitPath` excludes org-wide recommendations.
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn recommendation_summary(
        &self,
        ctx: &Context<'_>,
        org_unit_path: Option<String>,
    ) -> Result<RecommendationSummary> {
        if let Some(u) = &org_unit_path {
            check_ltree(u, "orgUnitPath")?;
        }
        let now = Utc::now();
        let since = now - Duration::days(30);

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let counts = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let under = org_unit_path.clone();
            async move {
                let floor = reads::display_floor(c, org).await?;
                let v = reads::Visible {
                    under: under.as_deref(),
                    min_confidence: floor,
                };
                reads::summary(c, org, &v, since).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        Ok(RecommendationSummary {
            pending_total: int(counts.iter().map(|c| c.pending).sum()),
            pending_by_decision_type: counts
                .iter()
                .filter(|c| c.pending > 0)
                .map(|c| DecisionTypeCount {
                    decision_type: c.decision_type.clone(),
                    count: int(c.pending),
                })
                .collect(),
            accepted_last_30d: int(counts.iter().map(|c| c.accepted).sum()),
            rejected_last_30d: int(counts.iter().map(|c| c.rejected).sum()),
            as_of: now,
        })
    }

    /// Catch-up feed for reconnecting clients: `updatedAt` ascending, `updatedAfter` exclusive.
    /// Re-read with an overlap and dedupe on (id, version).
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn recommendation_changes(
        &self,
        ctx: &Context<'_>,
        updated_after: DateTime<Utc>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
    ) -> Result<KukiRecommendationConnection> {
        let limit = page_size(first)?;
        let after = after.as_ref().map(|c| decode_pair(c, "ch1.")).transpose()?;

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            async move {
                let floor = reads::display_floor(c, org).await?;
                let v = reads::Visible {
                    under: None,
                    min_confidence: floor,
                };
                reads::changes(c, org, updated_after, after, &v, limit + 1).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let mut edges = Vec::with_capacity(rows.len());
        for r in rows {
            let cursor = encode_changes(r.updated_at, r.id);
            edges.push(KukiRecommendationEdge {
                cursor,
                node: KukiRecommendation::new(r)?,
            });
        }
        Ok(connection(edges, has_next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip_and_reject_garbage() {
        let at = DateTime::<Utc>::from_timestamp_micros(1_760_000_000_123_456).unwrap();
        let id = Uuid::from_u128(42);

        let c = encode_inbox("0.850", at, id);
        match decode_list(&c, RecommendationOrder::Inbox).unwrap() {
            reads::After::Inbox {
                urgency,
                created_at,
                id: got,
            } => {
                assert_eq!((urgency.as_str(), created_at, got), ("0.850", at, id));
            }
            other => panic!("{other:?}"),
        }
        let c = encode_created(at, id);
        assert!(matches!(
            decode_list(&c, RecommendationOrder::CreatedDesc).unwrap(),
            reads::After::Created { created_at, id: got } if created_at == at && got == id
        ));
        assert_eq!(
            decode_pair(&encode_changes(at, id), "ch1.").unwrap(),
            (at, id)
        );

        // A cursor belongs to one order.
        assert!(decode_list(&encode_created(at, id), RecommendationOrder::Inbox).is_err());
        assert!(
            decode_list(
                &encode_inbox("0.5", at, id),
                RecommendationOrder::CreatedDesc
            )
            .is_err()
        );

        let hyphenated = format!("ch1.1_{id}");
        for bad in [
            "",
            "ch1.",
            "ch1.1",
            "ch1.x_00",
            hyphenated.as_str(),
            "ib1.-1_1_00",
            "ib1.1._1_00",
        ] {
            assert!(decode_pair(&Cursor(bad.into()), "ch1.").is_err(), "{bad}");
            assert!(
                decode_list(&Cursor(bad.into()), RecommendationOrder::Inbox).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn decimals_are_plain_strings() {
        for ok in ["0", "39.000", "-1.5", "100"] {
            assert!(is_decimal(ok), "{ok}");
        }
        for bad in ["", "-", ".5", "1.", "1e5", "1,5", "0x1", "1.2.3", " 1"] {
            assert!(!is_decimal(bad), "{bad}");
        }
    }

    #[test]
    fn every_stored_status_maps_to_the_graphql_enum_and_back() {
        for s in [
            "pending",
            "accepted",
            "rejected",
            "expired",
            "superseded",
            "executing",
            "executed",
            "failed",
            "auto_executed",
        ] {
            let k: kt::RecommendationStatus = stored(s).unwrap();
            assert_eq!(k.as_str(), s);
            let g = KukiRecommendationStatus::from(k);
            assert_eq!(kt::RecommendationStatus::from(g), k);
        }
        assert!(stored::<kt::RecommendationStatus>("nonsense").is_err());
        assert_eq!(
            stored::<ActionExecutionStatus>("compensated").unwrap(),
            ActionExecutionStatus::Compensated
        );
    }

    #[test]
    fn resolution_reads_both_writers_shapes() {
        let r = resolution(&serde_json::json!({ "reason": "condition_cleared", "generator": "g" }));
        assert_eq!(r.reason_code.as_deref(), Some("condition_cleared"));
        assert!(r.note.is_none() && !r.payload_overridden);

        let r = resolution(&serde_json::json!({
            "reason_code": "ALREADY_HANDLED", "note": "done elsewhere", "payload_overridden": true
        }));
        assert_eq!(r.reason_code.as_deref(), Some("ALREADY_HANDLED"));
        assert_eq!(r.note.as_deref(), Some("done elsewhere"));
        assert!(r.payload_overridden);
    }
}
