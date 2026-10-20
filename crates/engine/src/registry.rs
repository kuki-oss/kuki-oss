//! The set of generators this build can run. A generator whose capabilities are not available
//! (inference, network priors, an LLM) is left out and recorded, so one engine serves OSS and
//! Core with no edition branches. Whether an organisation has switched a generator on is a
//! separate, per-run question: see `Registry::enabled`.
//!
//! A registry is built from packs (`from_packs`): each pack is validated, then its decision
//! types and generators join the set. Two packs may not share a name, a decision type or a
//! generator id.

use std::{collections::BTreeSet, sync::Arc};

use kuki_extension::{Capability, DecisionGenerator, config::OrgDecisionConfig};
use sqlx::PgConnection;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    builtin::base_pack,
    pack::{self, DecisionTypeDef, Pack, PackError},
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("two generators share the id `{0}`")]
    Duplicate(&'static str),
    #[error("two packs are both named `{0}`")]
    DuplicatePack(&'static str),
    #[error("decision type `{0}` is declared by more than one pack")]
    DuplicateDecisionType(&'static str),
    #[error(transparent)]
    Pack(#[from] PackError),
}

/// A generator left out because capabilities it requires are not available.
#[derive(Debug, PartialEq, Eq)]
pub struct Skipped {
    pub id: &'static str,
    pub missing: Vec<Capability>,
}

pub struct Registry {
    generators: Vec<Arc<dyn DecisionGenerator>>,
    skipped: Vec<Skipped>,
    decision_types: Vec<DecisionTypeDef>,
    packs: Vec<(&'static str, semver::Version)>,
}

impl Registry {
    /// Keeps the generators whose `requires()` is a subset of `available`. Ordered by id, so the
    /// order of a run does not depend on registration order.
    pub fn build(
        candidates: Vec<Arc<dyn DecisionGenerator>>,
        available: &BTreeSet<Capability>,
    ) -> Result<Self, RegistryError> {
        let mut seen = BTreeSet::new();
        let mut generators = Vec::new();
        let mut skipped = Vec::new();
        for g in candidates {
            if !seen.insert(g.id()) {
                return Err(RegistryError::Duplicate(g.id()));
            }
            let missing: Vec<Capability> = g
                .requires()
                .iter()
                .copied()
                .filter(|c| !available.contains(c))
                .collect();
            if missing.is_empty() {
                generators.push(g);
            } else {
                skipped.push(Skipped {
                    id: g.id(),
                    missing,
                });
            }
        }
        generators.sort_by_key(|g| g.id());
        skipped.sort_by_key(|s| s.id);
        Ok(Self {
            generators,
            skipped,
            decision_types: Vec::new(),
            packs: Vec::new(),
        })
    }

    /// Validates each pack, then registers its decision types and generators. Every declared
    /// decision type is kept even when its generator is skipped for missing capabilities, so
    /// installing is the same on every edition.
    pub fn from_packs(
        packs: Vec<Pack>,
        available: &BTreeSet<Capability>,
    ) -> Result<Self, RegistryError> {
        let mut pack_names = BTreeSet::new();
        let mut type_names = BTreeSet::new();
        let mut listed = Vec::new();
        let mut decision_types = Vec::new();
        let mut candidates = Vec::new();
        for p in packs {
            p.validate()?;
            if !pack_names.insert(p.name) {
                return Err(RegistryError::DuplicatePack(p.name));
            }
            for d in &p.decision_types {
                if !type_names.insert(d.name) {
                    return Err(RegistryError::DuplicateDecisionType(d.name));
                }
            }
            listed.push((p.name, p.version));
            decision_types.extend(p.decision_types);
            candidates.extend(p.generators);
        }
        let mut registry = Self::build(candidates, available)?;
        decision_types.sort_by_key(|d| d.name);
        listed.sort_by_key(|p| p.0);
        registry.decision_types = decision_types;
        registry.packs = listed;
        Ok(registry)
    }

    /// The pack kuki-oss ships. Core adds its own through `from_packs`.
    pub fn builtin(available: &BTreeSet<Capability>) -> Result<Self, RegistryError> {
        Self::from_packs(vec![base_pack()], available)
    }

    pub fn generators(&self) -> &[Arc<dyn DecisionGenerator>] {
        &self.generators
    }

    pub fn skipped(&self) -> &[Skipped] {
        &self.skipped
    }

    pub fn get(&self, id: &str) -> Option<&Arc<dyn DecisionGenerator>> {
        self.generators.iter().find(|g| g.id() == id)
    }

    /// Every decision type the registered packs declare, ordered by name.
    pub fn decision_types(&self) -> &[DecisionTypeDef] {
        &self.decision_types
    }

    /// The registered packs, ordered by name.
    pub fn packs(&self) -> &[(&'static str, semver::Version)] {
        &self.packs
    }

    /// Registers every pack's decision types for one organisation (privileged connection).
    pub async fn install_decision_types(
        &self,
        c: &mut PgConnection,
        org: Uuid,
    ) -> Result<(), sqlx::Error> {
        pack::install(c, org, &self.decision_types).await
    }

    /// The generators this organisation has switched on.
    pub fn enabled<'a>(
        &'a self,
        cfg: &'a OrgDecisionConfig,
    ) -> impl Iterator<Item = &'a Arc<dyn DecisionGenerator>> + 'a {
        self.generators.iter().filter(|g| cfg.enabled(g.id()))
    }
}

#[cfg(test)]
mod tests {
    use crate::pack::fakes;
    use async_trait::async_trait;
    use kuki_extension::{DecisionContext, GenError, Proposal, ScopeSpec, Trigger};
    use kuki_types::Tier;

    use super::*;

    struct Fake {
        id: &'static str,
        requires: &'static [Capability],
    }

    #[async_trait]
    impl DecisionGenerator for Fake {
        fn id(&self) -> &'static str {
            self.id
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
            self.requires
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

    fn fake(id: &'static str, requires: &'static [Capability]) -> Arc<dyn DecisionGenerator> {
        Arc::new(Fake { id, requires })
    }

    #[test]
    fn a_generator_with_unmet_capabilities_is_skipped_and_recorded() {
        let none = BTreeSet::new();
        let r = Registry::build(
            vec![fake("a.t0", &[]), fake("b.t3", &[Capability::Inference])],
            &none,
        )
        .unwrap();
        assert_eq!(r.generators().len(), 1);
        assert_eq!(
            r.skipped(),
            &[Skipped {
                id: "b.t3",
                missing: vec![Capability::Inference]
            }]
        );
        assert!(r.get("b.t3").is_none());
    }

    #[test]
    fn available_capabilities_let_the_generator_in() {
        let have = BTreeSet::from([Capability::Inference]);
        let r = Registry::build(
            vec![fake("b.t3", &[Capability::Inference, Capability::Llm])],
            &have,
        )
        .unwrap();
        assert!(r.generators().is_empty());
        assert_eq!(r.skipped()[0].missing, vec![Capability::Llm]);
        let have = BTreeSet::from([Capability::Inference, Capability::Llm]);
        let r = Registry::build(
            vec![fake("b.t3", &[Capability::Inference, Capability::Llm])],
            &have,
        )
        .unwrap();
        assert_eq!(r.generators().len(), 1);
    }

    #[test]
    fn duplicate_ids_are_refused() {
        let e = Registry::build(vec![fake("a", &[]), fake("a", &[])], &BTreeSet::new())
            .err()
            .unwrap();
        assert_eq!(e, RegistryError::Duplicate("a"));
    }

    #[test]
    fn order_is_by_id_and_enabled_follows_the_config() {
        let r =
            Registry::build(vec![fake("z.t0", &[]), fake("a.t0", &[])], &BTreeSet::new()).unwrap();
        let ids: Vec<_> = r.generators().iter().map(|g| g.id()).collect();
        assert_eq!(ids, ["a.t0", "z.t0"]);

        let mut cfg = OrgDecisionConfig::default();
        assert_eq!(r.enabled(&cfg).count(), 0, "no entry means not enabled");
        cfg.generators.insert("z.t0".into(), Default::default());
        let on: Vec<_> = r.enabled(&cfg).map(|g| g.id()).collect();
        assert_eq!(on, ["z.t0"]);
    }

    #[test]
    fn the_builtin_registry_has_the_deadline_generator_in_oss() {
        let r = Registry::builtin(&BTreeSet::new()).unwrap();
        assert!(r.get("deadline.at_risk.t0").is_some());
        assert!(r.skipped().is_empty());
    }

    #[test]
    fn packs_contribute_their_generators_and_decision_types() {
        let r = Registry::from_packs(
            vec![
                fakes::pack(
                    "b",
                    vec![fakes::decision_type("b.t", "b", "b/t.json")],
                    vec![fakes::generator("b.g", "b.t")],
                ),
                fakes::pack(
                    "a",
                    vec![fakes::decision_type("a.t", "a", "a/t.json")],
                    vec![fakes::generator("a.g", "a.t")],
                ),
            ],
            &BTreeSet::new(),
        )
        .unwrap();
        let gens: Vec<_> = r.generators().iter().map(|g| g.id()).collect();
        let types: Vec<_> = r.decision_types().iter().map(|d| d.name).collect();
        let packs: Vec<_> = r.packs().iter().map(|p| p.0).collect();
        assert_eq!(
            (gens, types, packs),
            (vec!["a.g", "b.g"], vec!["a.t", "b.t"], vec!["a", "b"])
        );
    }

    #[test]
    fn two_packs_with_one_name_are_refused() {
        let e = Registry::from_packs(
            vec![
                fakes::pack(
                    "a",
                    vec![fakes::decision_type("a.t", "a", "a/t.json")],
                    vec![],
                ),
                fakes::pack(
                    "a",
                    vec![fakes::decision_type("a.u", "a", "a/u.json")],
                    vec![],
                ),
            ],
            &BTreeSet::new(),
        )
        .err()
        .unwrap();
        assert_eq!(e, RegistryError::DuplicatePack("a"));
    }

    #[test]
    fn a_decision_type_declared_by_two_packs_is_refused() {
        let e = Registry::from_packs(
            vec![
                fakes::pack(
                    "a",
                    vec![fakes::decision_type("shared.t", "a", "a/t.json")],
                    vec![],
                ),
                fakes::pack(
                    "b",
                    vec![fakes::decision_type("shared.t", "b", "b/t.json")],
                    vec![],
                ),
            ],
            &BTreeSet::new(),
        )
        .err()
        .unwrap();
        assert_eq!(e, RegistryError::DuplicateDecisionType("shared.t"));
    }

    #[test]
    fn generator_ids_must_be_unique_across_packs() {
        let e = Registry::from_packs(
            vec![
                fakes::pack(
                    "a",
                    vec![fakes::decision_type("a.t", "a", "a/t.json")],
                    vec![fakes::generator("dup.g", "a.t")],
                ),
                fakes::pack(
                    "b",
                    vec![fakes::decision_type("b.t", "b", "b/t.json")],
                    vec![fakes::generator("dup.g", "b.t")],
                ),
            ],
            &BTreeSet::new(),
        )
        .err()
        .unwrap();
        assert_eq!(e, RegistryError::Duplicate("dup.g"));
    }

    #[test]
    fn the_builtin_registry_lists_the_base_pack_and_its_decision_types() {
        let r = Registry::builtin(&BTreeSet::new()).unwrap();
        let packs: Vec<_> = r.packs().iter().map(|p| p.0).collect();
        assert_eq!(packs, ["kuki.base"]);
        assert!(r.decision_types().iter().any(|d| d.name == "work.escalate"));
    }
}
