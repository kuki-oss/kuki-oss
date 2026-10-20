//! A pack: the unit a build registers. It bundles hte decision types (name,
//! payload schema, default action) and the generators that produce them. Packs
//! are compile-time values, not loaded plugins.
//!
//! A pack is checked as a whole before its parts are used, so a mistake is a
//! startup error naming the pack, not a generator that fails on its first run.

use std::{collections::BTreeSet, sync::Arc};

use kuki_extension::DecisionGenerator;
use kuki_store::engine_reads::register_decision_type;
use serde_json::Value;
use sqlx::PgConnection;
use thiserror::Error;
use uuid::Uuid;

pub struct DecisionTypeDef {
    pub name: &'static str,
    pub pack: &'static str,
    pub schema_ref: &'static str,
    /// What accepting a decision of this type queues, if anything. Constant per type, so it
    /// cannot make a candidate drift between runs. `builtin:` names are served by
    /// `kuki_actions::BuiltinRouter`; any other name needs a registered webhook endpoint.
    pub default_action: Option<&'static str>,
    pub schema: Value,
}

pub struct Pack {
    pub name: &'static str,
    pub version: semver::Version,
    pub decision_types: Vec<DecisionTypeDef>,
    pub generators: Vec<Arc<dyn DecisionGenerator>>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PackError {
    #[error(
        "decision type `{decision_type}` says it belongs to pack `{declared}` but is declared in `{pack}`"
    )]
    WrongPack {
        pack: String,
        decision_type: String,
        declared: String,
    },
    #[error(
        "decision type `{decision_type}` has schema ref `{schema_ref}`, which must start with `{pack}/`"
    )]
    BadSchemaRef {
        pack: String,
        decision_type: String,
        schema_ref: String,
    },
    #[error("pack `{pack}` declares decision type `{decision_type}` twice")]
    DuplicateDecisionType { pack: String, decision_type: String },
    #[error(
        "generator `{generator}` in pack `{pack}` emits `{decision_type}`, which the pack does not declare"
    )]
    UndeclaredDecisionType {
        pack: String,
        generator: String,
        decision_type: String,
    },
}

impl Pack {
    /// Everything that can be checked without a database. A generator may only emit decision
    /// types its own pack declares, so a pack is installable on its own.
    pub fn validate(&self) -> Result<(), PackError> {
        let mut declared = BTreeSet::new();
        for d in &self.decision_types {
            if d.pack != self.name {
                return Err(PackError::WrongPack {
                    pack: self.name.into(),
                    decision_type: d.name.into(),
                    declared: d.pack.into(),
                });
            }
            if !d.schema_ref.starts_with(&format!("{}/", self.name)) {
                return Err(PackError::BadSchemaRef {
                    pack: self.name.into(),
                    decision_type: d.name.into(),
                    schema_ref: d.schema_ref.into(),
                });
            }
            if !declared.insert(d.name) {
                return Err(PackError::DuplicateDecisionType {
                    pack: self.name.into(),
                    decision_type: d.name.into(),
                });
            }
        }
        for g in &self.generators {
            if !declared.contains(g.decision_type()) {
                return Err(PackError::UndeclaredDecisionType {
                    pack: self.name.into(),
                    generator: g.id().into(),
                    decision_type: g.decision_type().into(),
                });
            }
        }
        Ok(())
    }
}

/// Registers decision types for one organisation. Needs a privileged connection: `kuki_app`
/// only reads the registry. Re-running replaces what is stored, so the pack stays the source.
pub async fn install(
    c: &mut PgConnection,
    org: Uuid,
    types: &[DecisionTypeDef],
) -> Result<(), sqlx::Error> {
    for d in types {
        register_decision_type(
            c,
            org,
            d.name,
            d.pack,
            d.schema_ref,
            &d.schema,
            d.default_action,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod fakes {
    use std::sync::Arc;

    use async_trait::async_trait;
    use kuki_extension::{
        Capability, DecisionContext, DecisionGenerator, GenError, Proposal, ScopeSpec, Trigger,
    };
    use kuki_types::Tier;
    use serde_json::json;

    use super::{DecisionTypeDef, Pack};

    struct Gen {
        id: &'static str,
        decision_type: &'static str,
    }

    #[async_trait]
    impl DecisionGenerator for Gen {
        fn id(&self) -> &'static str {
            self.id
        }
        fn version(&self) -> semver::Version {
            semver::Version::new(1, 0, 0)
        }
        fn decision_type(&self) -> &'static str {
            self.decision_type
        }
        fn tier(&self) -> Tier {
            Tier::T0
        }
        fn requires(&self) -> &'static [Capability] {
            &[]
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

    pub fn generator(id: &'static str, decision_type: &'static str) -> Arc<dyn DecisionGenerator> {
        Arc::new(Gen { id, decision_type })
    }

    pub fn decision_type(
        name: &'static str,
        pack: &'static str,
        schema_ref: &'static str,
    ) -> DecisionTypeDef {
        DecisionTypeDef {
            name,
            pack,
            schema_ref,
            default_action: None,
            schema: json!({ "type": "object" }),
        }
    }

    pub fn pack(
        name: &'static str,
        decision_types: Vec<DecisionTypeDef>,
        generators: Vec<Arc<dyn DecisionGenerator>>,
    ) -> Pack {
        Pack {
            name,
            version: semver::Version::new(0, 1, 0),
            decision_types,
            generators,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::{decision_type, generator, pack};
    use super::*;

    fn consistent() -> Pack {
        pack(
            "acme",
            vec![decision_type("acme.t", "acme", "acme/t.json")],
            vec![generator("acme.g.t0", "acme.t")],
        )
    }

    #[test]
    fn a_consistent_pack_passes() {
        assert_eq!(consistent().validate(), Ok(()));
    }

    #[test]
    fn a_decision_type_declared_for_another_pack_is_refused() {
        let p = pack(
            "acme",
            vec![decision_type("acme.t", "other", "acme/t.json")],
            vec![],
        );
        assert_eq!(
            p.validate(),
            Err(PackError::WrongPack {
                pack: "acme".into(),
                decision_type: "acme.t".into(),
                declared: "other".into()
            })
        );
    }

    #[test]
    fn a_schema_ref_outside_the_packs_namespace_is_refused() {
        let p = pack(
            "acme",
            vec![decision_type("acme.t", "acme", "other/t.json")],
            vec![],
        );
        assert_eq!(
            p.validate(),
            Err(PackError::BadSchemaRef {
                pack: "acme".into(),
                decision_type: "acme.t".into(),
                schema_ref: "other/t.json".into()
            })
        );
    }

    #[test]
    fn a_decision_type_declared_twice_is_refused() {
        let p = pack(
            "acme",
            vec![
                decision_type("acme.t", "acme", "acme/t.json"),
                decision_type("acme.t", "acme", "acme/t2.json"),
            ],
            vec![],
        );
        assert_eq!(
            p.validate(),
            Err(PackError::DuplicateDecisionType {
                pack: "acme".into(),
                decision_type: "acme.t".into()
            })
        );
    }

    #[test]
    fn a_generator_for_an_undeclared_type_is_refused() {
        let p = pack(
            "acme",
            vec![decision_type("acme.t", "acme", "acme/t.json")],
            vec![generator("acme.g.t0", "acme.missing")],
        );
        assert_eq!(
            p.validate(),
            Err(PackError::UndeclaredDecisionType {
                pack: "acme".into(),
                generator: "acme.g.t0".into(),
                decision_type: "acme.missing".into()
            })
        );
    }
}
