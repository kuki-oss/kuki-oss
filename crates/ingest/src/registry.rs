//! Event types for one request, loaded once and compiled once.

use std::collections::{BTreeSet, HashMap};

use jsonschema::Validator;
use kuki_store::ingest::{EventType, event_type_get, event_type_known};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::outcome::RejectionCode;
use crate::pipeline::IngestError;

pub struct Loaded {
    pub name: String,
    pub version: i32,
    pub found: Option<EventType>,
    pub name_known: bool,
}

pub enum TypeEntry {
    Ready {
        subject_type: String,
        validator: Validator,
    },
    Missing(RejectionCode),
}

#[derive(Default)]
pub struct Registry {
    entries: HashMap<String, HashMap<i32, TypeEntry>>,
}

impl Registry {
    pub fn get(&self, name: &str, version: i32) -> Option<&TypeEntry> {
        self.entries.get(name)?.get(&version)
    }

    /// Runs inside a tenant transaction.
    pub async fn fetch(
        c: &mut PgConnection,
        org: Uuid,
        wanted: BTreeSet<(String, i32)>,
    ) -> sqlx::Result<Vec<Loaded>> {
        let mut out = Vec::with_capacity(wanted.len());
        for (name, version) in wanted {
            let found = event_type_get(c, org, &name, version).await?;
            let name_known = match &found {
                Some(_) => true,
                None => event_type_known(c, org, &name).await?,
            };
            out.push(Loaded {
                name,
                version,
                found,
                name_known,
            });
        }
        Ok(out)
    }

    /// Compiles outside any transaction. A broken registered schema is a server problem, not a bad row.
    pub fn compile(loaded: Vec<Loaded>) -> Result<Self, IngestError> {
        let mut reg = Registry::default();
        for l in loaded {
            let entry = match l.found {
                Some(t) => TypeEntry::Ready {
                    validator: compile_schema(&t.json_schema).map_err(|e| {
                        IngestError::internal(format!(
                            "event type {}@{} has an invalid schema: {e}",
                            l.name, l.version
                        ))
                    })?,
                    subject_type: t.subject_type,
                },
                None if l.name_known => TypeEntry::Missing(RejectionCode::UnknownTypeVersion),
                None => TypeEntry::Missing(RejectionCode::UnknownEventType),
            };
            reg.entries
                .entry(l.name)
                .or_default()
                .insert(l.version, entry);
        }
        Ok(reg)
    }
}

fn compile_schema(schema: &Value) -> Result<Validator, String> {
    jsonschema::validator_for(schema).map_err(|e| e.to_string())
}

/// First schema violation as (JSON pointer into the row, message).
pub(crate) fn first_violation(v: &Validator, payload: &Value) -> Option<(String, String)> {
    v.iter_errors(payload)
        .next()
        .map(|e| (format!("/payload{}", e.instance_path()), e.to_string()))
}
