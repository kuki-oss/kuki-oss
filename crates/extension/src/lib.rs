//! kuki-extenstion: the bridge between the OSS kernel and anything that extends it.
//! Traits, the data they exchange, and inert defaults. No I/O, no database access.
//! Adding a method to a trait here is a breaking change for Core.

pub mod audit;
pub mod config;
pub mod context;
pub mod error;
pub mod extensions;
pub mod generator;
pub mod inference;
pub mod llm;
pub mod policy;
pub mod priors;
pub mod view;

pub use audit::{AuditEntry, AuditSink, NoopAuditSink};
pub use context::{DecisionContext, PendingRef, ScopeSpec, Trigger, derive_seed};
pub use error::{ExtensionError, GenError};
pub use extensions::{Capabilities, Extensions};
pub use generator::{Capability, DecisionGenerator, Proposal};
pub use inference::{InferenceClient, InferenceRequest, InferenceResponse};
pub use llm::{DataClass, DisabledLlm, LlmProvider, LlmRequest, LlmResponse, LlmTask};
pub use policy::{
    PolicyAction, PolicyDecision, PolicyEvaluator, PolicyRequest, SingleApproverPolicy,
};
pub use priors::{NoPriors, Prior, PriorProvider, PriorRequest};
pub use view::{EmptyView, EventQuery, EventRecord, LinkRecord, ObjectRecord, OntologyView};
