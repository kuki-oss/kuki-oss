//! The mapping file: which columns become what.
//!
//! ```json
//! { "kind": "objects", "type": "case", "typeVersion": 1,
//!   "externalIds": { "erp": "Case No" },
//!   "orgUnitPath": "Unit",
//!   "attrs": { "status": { "column": "Status" },
//!              "due_at": { "column": "Due", "as": "datetime", "format": "%d/%m/%Y" },
//!              "load":   { "column": "Hours", "as": "number", "required": true } } }
//! ```
//!
//! `kind` is `objects`, `events` or `links`. A column's `as` is `string` (the default), `number`,
//! `integer`, `boolean` or `datetime`; `format` is a chrono strftime pattern for dates in text.
//! An empty cell leaves the attribute out unless the column is `required`.

use std::collections::BTreeMap;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct MappingError(pub String);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CellKind {
    #[default]
    String,
    Number,
    Integer,
    Boolean,
    Datetime,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub column: String,
    #[serde(rename = "as", default)]
    pub kind: CellKind,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub required: bool,
}

impl Column {
    /// A plain text column, as used for ids and paths.
    pub fn text(name: &str, required: bool) -> Self {
        Self {
            column: name.to_owned(),
            kind: CellKind::String,
            format: None,
            required,
        }
    }
}

/// A date column: always a datetime, so it has no `as`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DateColumn {
    pub column: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub required: bool,
}

impl DateColumn {
    pub fn as_column(&self) -> Column {
        Column {
            column: self.column.clone(),
            kind: CellKind::Datetime,
            format: self.format.clone(),
            required: self.required,
        }
    }
}

/// An object named by its id in an external system: `{system, column}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ref {
    pub system: String,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObjectsMapping {
    #[serde(rename = "type")]
    pub object_type: String,
    pub type_version: i64,
    /// External system -> column holding the id there. Always required per row.
    pub external_ids: BTreeMap<String, String>,
    #[serde(default)]
    pub org_unit_path: Option<String>,
    #[serde(default)]
    pub valid_from: Option<DateColumn>,
    #[serde(default)]
    pub attrs: BTreeMap<String, Column>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventsMapping {
    #[serde(rename = "type")]
    pub event_type: String,
    pub type_version: i64,
    pub subject: Ref,
    pub occurred_at: DateColumn,
    #[serde(default)]
    pub payload: BTreeMap<String, Column>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinksMapping {
    #[serde(rename = "type")]
    pub link_type: String,
    pub src: Ref,
    pub dst: Ref,
    #[serde(default)]
    pub valid_from: Option<DateColumn>,
    #[serde(default)]
    pub attrs: BTreeMap<String, Column>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Mapping {
    Objects(ObjectsMapping),
    Events(EventsMapping),
    Links(LinksMapping),
}

fn invalid(message: impl AsRef<str>) -> MappingError {
    MappingError(format!("the mapping is not valid: {}", message.as_ref()))
}

fn check_type(name: &str, version: Option<i64>) -> Result<(), MappingError> {
    if name.trim().is_empty() {
        return Err(invalid("`type` must not be empty"));
    }
    if let Some(v) = version
        && !(1..=i64::from(i32::MAX)).contains(&v)
    {
        return Err(invalid("`typeVersion` must be an integer >= 1"));
    }
    Ok(())
}

fn check_columns(cols: &BTreeMap<String, Column>, what: &str) -> Result<(), MappingError> {
    for (name, c) in cols {
        if name.is_empty() || c.column.is_empty() {
            return Err(invalid(format!(
                "`{what}` names and columns must not be empty"
            )));
        }
        match &c.format {
            Some(f) if f.is_empty() => {
                return Err(invalid(format!(
                    "`{what}.{name}`: `format` must not be empty"
                )));
            }
            Some(_) if c.kind != CellKind::Datetime => {
                return Err(invalid(format!(
                    "`{what}.{name}`: `format` only applies to `\"as\": \"datetime\"`"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_ref(r: &Ref, what: &str) -> Result<(), MappingError> {
    if r.system.is_empty() || r.column.is_empty() {
        return Err(invalid(format!("`{what}` needs a system and a column")));
    }
    Ok(())
}

fn check_date(c: &Option<DateColumn>, what: &str) -> Result<(), MappingError> {
    match c {
        Some(c) if c.column.is_empty() || c.format.as_deref() == Some("") => Err(invalid(format!(
            "`{what}` needs a column and a non-empty format"
        ))),
        _ => Ok(()),
    }
}

impl Mapping {
    pub fn parse(bytes: &[u8]) -> Result<Self, MappingError> {
        let mapping: Mapping = serde_json::from_slice(bytes)
            .map_err(|e| MappingError(format!("the mapping is not valid: {e}")))?;
        mapping.validate()?;
        Ok(mapping)
    }

    fn validate(&self) -> Result<(), MappingError> {
        match self {
            Mapping::Objects(o) => {
                check_type(&o.object_type, Some(o.type_version))?;
                if o.external_ids.is_empty() {
                    return Err(invalid(
                        "`externalIds` needs at least one system and column",
                    ));
                }
                if o.external_ids
                    .iter()
                    .any(|(s, c)| s.is_empty() || c.is_empty())
                {
                    return Err(invalid("`externalIds` names and columns must not be empty"));
                }
                if o.org_unit_path.as_deref() == Some("") {
                    return Err(invalid("`orgUnitPath` must name a column"));
                }
                check_date(&o.valid_from, "validFrom")?;
                check_columns(&o.attrs, "attrs")
            }
            Mapping::Events(e) => {
                check_type(&e.event_type, Some(e.type_version))?;
                check_ref(&e.subject, "subject")?;
                check_date(&Some(e.occurred_at.clone()), "occurredAt")?;
                check_columns(&e.payload, "payload")
            }
            Mapping::Links(l) => {
                check_type(&l.link_type, None)?;
                check_ref(&l.src, "src")?;
                check_ref(&l.dst, "dst")?;
                check_date(&l.valid_from, "validFrom")?;
                check_columns(&l.attrs, "attrs")
            }
        }
    }

    /// Every column name the mapping refers to.
    pub fn columns(&self) -> Vec<&str> {
        let mut v: Vec<&str> = Vec::new();
        match self {
            Mapping::Objects(o) => {
                v.extend(o.external_ids.values().map(String::as_str));
                v.extend(o.org_unit_path.as_deref());
                v.extend(o.valid_from.iter().map(|c| c.column.as_str()));
                v.extend(o.attrs.values().map(|c| c.column.as_str()));
            }
            Mapping::Events(e) => {
                v.push(e.subject.column.as_str());
                v.push(e.occurred_at.column.as_str());
                v.extend(e.payload.values().map(|c| c.column.as_str()));
            }
            Mapping::Links(l) => {
                v.push(l.src.column.as_str());
                v.push(l.dst.column.as_str());
                v.extend(l.valid_from.iter().map(|c| c.column.as_str()));
                v.extend(l.attrs.values().map(|c| c.column.as_str()));
            }
        }
        v
    }

    /// Fails, naming every column the file lacks, before any row is read.
    pub fn check_header(&self, header: &[String]) -> Result<(), MappingError> {
        let mut missing: Vec<&str> = self
            .columns()
            .into_iter()
            .filter(|c| !header.iter().any(|h| h.as_str() == *c))
            .collect();
        missing.sort_unstable();
        missing.dedup();
        if missing.is_empty() {
            return Ok(());
        }
        let found: Vec<&str> = header
            .iter()
            .map(String::as_str)
            .filter(|h| !h.is_empty())
            .collect();
        Err(MappingError(format!(
            "column(s) not found in the file: {}; the file has: {}",
            missing
                .iter()
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", "),
            found.join(", ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<Mapping, MappingError> {
        Mapping::parse(json.as_bytes())
    }

    #[test]
    fn all_three_kinds_parse() {
        let objects = parse(
            r#"{ "kind": "objects", "type": "case", "typeVersion": 1,
                 "externalIds": { "erp": "Case No" }, "orgUnitPath": "Unit",
                 "attrs": { "due_at": { "column": "Due", "as": "datetime", "format": "%d/%m/%Y" },
                            "load": { "column": "Hours", "as": "number", "required": true } } }"#,
        )
        .unwrap();
        assert!(matches!(&objects, Mapping::Objects(o)
            if o.attrs["load"].required && o.attrs["due_at"].kind == CellKind::Datetime));
        assert_eq!(objects.columns().len(), 4);

        let events = parse(
            r#"{ "kind": "events", "type": "reading", "typeVersion": 1,
                 "subject": { "system": "erp", "column": "Sensor" },
                 "occurredAt": { "column": "At" },
                 "payload": { "value": { "column": "Value", "as": "number" } } }"#,
        )
        .unwrap();
        assert!(matches!(events, Mapping::Events(_)));

        let links = parse(
            r#"{ "kind": "links", "type": "assigned_to",
                 "src": { "system": "erp", "column": "Booking" },
                 "dst": { "system": "erp", "column": "Room" } }"#,
        )
        .unwrap();
        assert!(matches!(links, Mapping::Links(_)));
    }

    #[test]
    fn mistakes_are_refused_with_a_reason() {
        let objects = |body: &str| {
            format!(r#"{{ "kind": "objects", "type": "case", "typeVersion": 1, {body} }}"#)
        };
        let ext = r#""externalIds": { "erp": "Id" }"#;
        let bad = [
            "{}".to_owned(),
            r#"{ "kind": "widgets" }"#.to_owned(),
            objects(r#""externalIds": {}"#),
            objects(&format!(r#"{ext}, "surprise": 1"#)),
            objects(&format!(r#"{ext}, "attrs": {{ "a": {{ "column": "" }} }}"#)),
            objects(&format!(
                r#"{ext}, "attrs": {{ "a": {{ "column": "A", "format": "%d" }} }}"#
            )),
            objects(&format!(
                r#"{ext}, "attrs": {{ "a": {{ "column": "A", "as": "blob" }} }}"#
            )),
            r#"{ "kind": "objects", "type": "", "typeVersion": 1, "externalIds": { "e": "I" } }"#
                .to_owned(),
            r#"{ "kind": "objects", "type": "c", "typeVersion": 0, "externalIds": { "e": "I" } }"#
                .to_owned(),
        ];
        for text in &bad {
            assert!(parse(text).is_err(), "should be refused: {text}");
        }
    }

    #[test]
    fn missing_columns_are_all_named() {
        let m = parse(
            r#"{ "kind": "objects", "type": "case", "typeVersion": 1,
                 "externalIds": { "erp": "Case No" },
                 "attrs": { "a": { "column": "Due" }, "b": { "column": "Hours" } } }"#,
        )
        .unwrap();
        let header = vec!["Case No".to_owned(), "Status".to_owned()];
        let e = m.check_header(&header).unwrap_err().0;
        assert!(
            e.contains("`Due`") && e.contains("`Hours`") && e.contains("Status"),
            "{e}"
        );
        assert!(
            m.check_header(&["Case No".into(), "Due".into(), "Hours".into()])
                .is_ok()
        );
    }
}
