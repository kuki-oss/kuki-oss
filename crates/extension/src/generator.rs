use async_trait::async_trait;
use kuki_types::{Candidate, Tier};

use crate::{
    context::{DecisionContext, ScopeSpec, Trigger},
    error::GenError,
};

/// What the host must provide for a generator to run. `build_registry` skips generators whose
/// requirements are not met, so the same engine runs OSS and Core with no `if core` branching
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Capability {
    Inference,
    NetworkPriors,
    Llm,
}

/// A candidate plus the digest of the inputs it was computed from.
#[derive(Clone, Debug, PartialEq)]
pub struct Proposal {
    pub candidate: Candidate,
    /// Built with `kuki_types::InputDigestBuilder` over exactly what the generator read for this candidate.
    pub input_digest: String,
}

/// A pure function of the ontology state. Nevers writes to a database.
#[async_trait]
pub trait DecisionGenerator: Send + Sync {
    /// For example `inventory.replenish.t0`.
    fn id(&self) -> &'static str;
    /// Bump on any logic change.
    fn version(&self) -> semver::Version;
    fn decision_type(&self) -> &'static str;
    fn tier(&self) -> Tier;
    fn requires(&self) -> &'static [Capability];
    fn min_freshness(&self) -> Option<chrono::Duration> {
        None
    }

    fn scope(&self, trigger: &Trigger, ctx: &DecisionContext) -> ScopeSpec;

    async fn evaluate(
        &self,
        ctx: &DecisionContext,
        scope: &ScopeSpec,
    ) -> Result<Vec<Proposal>, GenError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{config::OrgDecisionConfig, view::EmptyView};

    struct Noop;

    #[async_trait]
    impl DecisionGenerator for Noop {
        fn id(&self) -> &'static str {
            "test.noop"
        }
        fn version(&self) -> semver::Version {
            semver::Version::new(1, 0, 0)
        }
        fn decision_type(&self) -> &'static str {
            "test.decision"
        }
        fn tier(&self) -> Tier {
            Tier::T0
        }
        fn requires(&self) -> &'static [Capability] {
            &[Capability::Inference]
        }
        fn scope(&self, _t: &Trigger, _c: &DecisionContext) -> ScopeSpec {
            ScopeSpec::All
        }
        async fn evaluate(
            &self,
            _c: &DecisionContext,
            _s: &ScopeSpec,
        ) -> Result<Vec<Proposal>, GenError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn generators_work_as_trait_objects() {
        let g: Arc<dyn DecisionGenerator> = Arc::new(Noop);
        let ctx = DecisionContext::new(
            Uuid::nil(),
            Utc::now(),
            g.id(),
            Arc::new(OrgDecisionConfig::default()),
            Arc::new(EmptyView),
        );
        assert!(g.evaluate(&ctx, &ScopeSpec::All).await.unwrap().is_empty());
        assert_eq!(g.requires(), &[Capability::Inference]);
        assert!(g.min_freshness().is_none());
    }
}
