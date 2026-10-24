//! Table rows to ingest rows, following a mapping.
//!
//! Each data row becomes the JSON the pipeline already parses (`parse_object`, `parse_event`,
//! `parse_link`). A row that cannot be converted becomes a `Malformed` row carrying the reason and
//! the raw cells, so the pipeline quarantines it and the rest of the file still loads. Dates
//! without an offset are read as UTC.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use serde_json::{Map, Number, Value, json};

use super::{
    mapping::{CellKind, Column, Mapping, MappingError},
    table::{Cell, Table, number_text},
};
use crate::body::{RawRow, RowContent};

static EMPTY: Cell = Cell::Empty;

/// Converts every row. Fails only when the mapping names a column the file lacks.
pub fn convert(mapping: &Mapping, table: &Table) -> Result<Vec<RawRow>, MappingError> {
    mapping.check_header(&table.header)?;
    let cols: BTreeMap<&str, usize> = table
        .header
        .iter()
        .enumerate()
        .filter(|(_, name)| !name.is_empty())
        .map(|(i, name)| (name.as_str(), i))
        .collect();

    Ok(table
        .rows
        .iter()
        .map(|row| {
            let content = match &row.cells {
                Err(message) => RowContent::Malformed {
                    text: String::new(),
                    message: message.clone(),
                },
                Ok(cells) => match build(mapping, &Row { cols: &cols, cells }) {
                    Ok(value) => RowContent::Json(value),
                    Err(message) => RowContent::Malformed {
                        text: raw_text(cells),
                        message,
                    },
                },
            };
            RawRow {
                index: row.index,
                content,
            }
        })
        .collect())
}

struct Row<'a> {
    cols: &'a BTreeMap<&'a str, usize>,
    cells: &'a [Cell],
}

impl Row<'_> {
    fn cell(&self, name: &str) -> &Cell {
        self.cols
            .get(name)
            .and_then(|i| self.cells.get(*i))
            .unwrap_or(&EMPTY)
    }

    /// The column's value, or `None` for an empty cell that is not required.
    fn value(&self, col: &Column) -> Result<Option<Value>, String> {
        match convert_cell(self.cell(&col.column), col)
            .map_err(|m| format!("column `{}`: {m}", col.column))?
        {
            Some(v) => Ok(Some(v)),
            None if col.required => Err(format!("column `{}` is empty but required", col.column)),
            None => Ok(None),
        }
    }

    fn must(&self, col: &Column) -> Result<Value, String> {
        self.value(col)?
            .ok_or_else(|| format!("column `{}` is empty but required", col.column))
    }

    fn map(&self, cols: &BTreeMap<String, Column>) -> Result<Map<String, Value>, String> {
        let mut out = Map::new();
        for (name, col) in cols {
            if let Some(v) = self.value(col)? {
                out.insert(name.clone(), v);
            }
        }
        Ok(out)
    }

    fn external(&self, system: &str, column: &str) -> Result<Value, String> {
        let id = self.must(&Column::text(column, true))?;
        Ok(json!({ "externalId": { "system": system, "id": id } }))
    }
}

fn build(mapping: &Mapping, row: &Row) -> Result<Value, String> {
    match mapping {
        Mapping::Objects(o) => {
            let mut external = Map::new();
            for (system, column) in &o.external_ids {
                external.insert(system.clone(), row.must(&Column::text(column, true))?);
            }
            let mut out = json!({
                "type": o.object_type,
                "typeVersion": o.type_version,
                "externalIds": external,
                "attrs": row.map(&o.attrs)?,
            });
            if let Some(col) = &o.org_unit_path
                && let Some(v) = row.value(&Column::text(col, false))?
            {
                out["orgUnitPath"] = v;
            }
            if let Some(col) = &o.valid_from
                && let Some(v) = row.value(&col.as_column())?
            {
                out["validFrom"] = v;
            }
            Ok(out)
        }
        Mapping::Events(e) => {
            let mut occurred = e.occurred_at.as_column();
            occurred.required = true;
            Ok(json!({
                "type": e.event_type,
                "typeVersion": e.type_version,
                "subject": row.external(&e.subject.system, &e.subject.column)?,
                "occurredAt": row.must(&occurred)?,
                "payload": row.map(&e.payload)?,
            }))
        }
        Mapping::Links(l) => {
            let mut out = json!({
                "type": l.link_type,
                "src": row.external(&l.src.system, &l.src.column)?,
                "dst": row.external(&l.dst.system, &l.dst.column)?,
                "attrs": row.map(&l.attrs)?,
            });
            if let Some(col) = &l.valid_from
                && let Some(v) = row.value(&col.as_column())?
            {
                out["validFrom"] = v;
            }
            Ok(out)
        }
    }
}

fn describe(cell: &Cell) -> String {
    match cell {
        Cell::Empty => "an empty cell".to_owned(),
        Cell::Text(s) => format!("{s:?}"),
        Cell::Number(f) => number_text(*f),
        Cell::Bool(b) => b.to_string(),
        Cell::DateTime(t) => rfc3339(t.and_utc()),
    }
}

fn rfc3339(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn raw_text(cells: &[Cell]) -> String {
    let texts: Vec<String> = cells
        .iter()
        .map(|c| match c {
            Cell::Empty => String::new(),
            Cell::Text(s) => s.clone(),
            Cell::Number(f) => number_text(*f),
            Cell::Bool(b) => b.to_string(),
            Cell::DateTime(t) => rfc3339(t.and_utc()),
        })
        .collect();
    serde_json::to_string(&texts).unwrap_or_default()
}

fn whole(f: f64) -> Result<i64, String> {
    if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 9.0e15 {
        Ok(f.round() as i64)
    } else {
        Err(format!("not a whole number: {f}"))
    }
}

fn midnight(d: NaiveDate) -> Option<DateTime<Utc>> {
    d.and_hms_opt(0, 0, 0).map(|t| t.and_utc())
}

fn parse_date(s: &str, format: Option<&str>) -> Result<DateTime<Utc>, String> {
    let parsed = match format {
        Some(f) => NaiveDateTime::parse_from_str(s, f)
            .map(|t| t.and_utc())
            .ok()
            .or_else(|| NaiveDate::parse_from_str(s, f).ok().and_then(midnight)),
        None => DateTime::parse_from_rfc3339(s)
            .map(|d| d.with_timezone(&Utc))
            .ok()
            .or_else(|| {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(midnight)
            }),
    };
    parsed.ok_or_else(|| match format {
        Some(f) => format!("not a date in the format `{f}`: {s:?}"),
        None => format!("not a date (RFC 3339 or YYYY-MM-DD; give a `format` for others): {s:?}"),
    })
}

/// `None` for an empty cell; otherwise the cell as the column's type.
fn convert_cell(cell: &Cell, col: &Column) -> Result<Option<Value>, String> {
    if *cell == Cell::Empty {
        return Ok(None);
    }
    let value = match col.kind {
        CellKind::String => Value::String(match cell {
            Cell::Text(s) => s.trim().to_owned(),
            Cell::Number(f) => number_text(*f),
            Cell::Bool(b) => b.to_string(),
            Cell::DateTime(t) => rfc3339(t.and_utc()),
            Cell::Empty => return Ok(None),
        }),
        CellKind::Number => {
            let f = match cell {
                Cell::Number(f) => *f,
                Cell::Text(s) => s
                    .trim()
                    .parse::<f64>()
                    .map_err(|_| format!("not a number: {s:?}"))?,
                other => return Err(format!("not a number: {}", describe(other))),
            };
            Number::from_f64(f)
                .map(Value::Number)
                .ok_or_else(|| "not a finite number".to_owned())?
        }
        CellKind::Integer => {
            let n = match cell {
                Cell::Number(f) => whole(*f)?,
                Cell::Text(s) => s
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| format!("not a whole number: {s:?}"))?,
                other => return Err(format!("not a whole number: {}", describe(other))),
            };
            Value::from(n)
        }
        CellKind::Boolean => Value::Bool(match cell {
            Cell::Bool(b) => *b,
            Cell::Number(f) if (*f - 1.0).abs() < 1e-9 => true,
            Cell::Number(f) if f.abs() < 1e-9 => false,
            Cell::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "yes" | "y" | "1" => true,
                "false" | "no" | "n" | "0" => false,
                _ => return Err(format!("not a yes/no value: {s:?}")),
            },
            other => return Err(format!("not a yes/no value: {}", describe(other))),
        }),
        CellKind::Datetime => Value::String(rfc3339(match cell {
            Cell::DateTime(t) => t.and_utc(),
            Cell::Text(s) => parse_date(s.trim(), col.format.as_deref())?,
            other => return Err(format!("not a date: {}", describe(other))),
        })),
    };
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::files::table::{FileFormat, TableRow, read_table};

    fn table(csv: &str) -> Table {
        read_table(csv.as_bytes(), FileFormat::Csv, None).unwrap()
    }

    fn run(csv: &str, mapping: &str) -> Vec<RawRow> {
        convert(&Mapping::parse(mapping.as_bytes()).unwrap(), &table(csv)).unwrap()
    }

    fn json_of(row: &RawRow) -> &Value {
        match &row.content {
            RowContent::Json(v) => v,
            other => panic!("expected a converted row, got {other:?}"),
        }
    }

    fn problem_of(row: &RawRow) -> (&str, &str) {
        match &row.content {
            RowContent::Malformed { text, message } => (text, message),
            other => panic!("expected a problem, got {other:?}"),
        }
    }

    /// An objects mapping with one datetime attribute `w` read from column `When`.
    fn dated(format: Option<&str>) -> String {
        let format = format.map_or(String::new(), |f| format!(r#", "format": "{f}""#));
        format!(
            r#"{{ "kind": "objects", "type": "t", "typeVersion": 1,
                  "externalIds": {{ "erp": "Id" }},
                  "attrs": {{ "w": {{ "column": "When", "as": "datetime"{format} }} }} }}"#
        )
    }

    #[test]
    fn an_object_row_is_converted_with_typed_attributes() {
        let rows = run(
            "Case No,Status,Due,Hours,Unit,Urgent\nA-1,open,15/03/2023,2.5,emea.uk,yes\n",
            r#"{ "kind": "objects", "type": "case", "typeVersion": 1,
                 "externalIds": { "erp": "Case No" }, "orgUnitPath": "Unit",
                 "attrs": { "status": { "column": "Status" },
                            "due_at": { "column": "Due", "as": "datetime", "format": "%d/%m/%Y" },
                            "load": { "column": "Hours", "as": "number" },
                            "urgent": { "column": "Urgent", "as": "boolean" } } }"#,
        );
        assert_eq!(
            json_of(&rows[0]),
            &json!({
                "type": "case", "typeVersion": 1,
                "externalIds": { "erp": "A-1" },
                "orgUnitPath": "emea.uk",
                "attrs": { "status": "open", "due_at": "2023-03-15T00:00:00Z",
                           "load": 2.5, "urgent": true }
            })
        );
    }

    #[test]
    fn empty_cells_are_left_out_and_required_ones_reject_the_row() {
        let rows = run(
            "Case No,Status,Owner,Notes\nA-1,open,,\nA-2,open,bob,hello\nA-3,open,carol,\n",
            r#"{ "kind": "objects", "type": "case", "typeVersion": 1,
                 "externalIds": { "erp": "Case No" },
                 "attrs": { "status": { "column": "Status" },
                            "owner": { "column": "Owner", "required": true },
                            "notes": { "column": "Notes" } } }"#,
        );
        let (_, message) = problem_of(&rows[0]);
        assert!(
            message.contains("`Owner`") && message.contains("required"),
            "{message}"
        );
        assert_eq!(json_of(&rows[1])["attrs"]["notes"], "hello");
        assert!(json_of(&rows[2])["attrs"].get("notes").is_none());
    }

    #[test]
    fn a_cell_that_cannot_be_converted_spoils_only_its_row() {
        let rows = run(
            "Case No,Hours\nA-1,2.5\nA-2,n/a\nA-3,4\n",
            r#"{ "kind": "objects", "type": "case", "typeVersion": 1,
                 "externalIds": { "erp": "Case No" },
                 "attrs": { "hours": { "column": "Hours", "as": "number" } } }"#,
        );
        assert_eq!(rows.iter().map(|r| r.index).collect::<Vec<_>>(), [0, 1, 2]);
        assert!(json_of(&rows[0]).is_object() && json_of(&rows[2]).is_object());
        let (text, message) = problem_of(&rows[1]);
        assert!(
            message.contains("not a number") && message.contains("`Hours`"),
            "{message}"
        );
        assert!(text.contains("n/a") && text.contains("A-2"), "{text}");
    }

    #[test]
    fn dates_are_normalised_to_utc_and_unreadable_ones_are_problems() {
        let attr = |csv: &str, format: Option<&str>| {
            let rows = run(csv, &dated(format));
            rows.iter()
                .map(|r| match &r.content {
                    RowContent::Json(v) => v["attrs"]["w"].as_str().unwrap().to_owned(),
                    RowContent::Malformed { .. } => "problem".to_owned(),
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            attr(
                "Id,When\na,2023-03-15T10:00:00+02:00\nb,2023-03-15\nc,yesterday\nd,45000\n",
                None
            ),
            [
                "2023-03-15T08:00:00Z",
                "2023-03-15T00:00:00Z",
                "problem",
                "problem"
            ]
        );
        assert_eq!(
            attr(
                "Id,When\na,15/03/2023 14:30\nb,15/03/2023\nc,2023-03-15\n",
                Some("%d/%m/%Y %H:%M")
            ),
            ["2023-03-15T14:30:00Z", "problem", "problem"],
            "a date-only value does not fit a date-and-time format"
        );
    }

    #[test]
    fn booleans_and_whole_numbers_are_strict() {
        let rows = run(
            "Id,Flag,Count\na,yes,3\nb,No,3.5\nc,maybe,4\nd,1,7\n",
            r#"{ "kind": "objects", "type": "t", "typeVersion": 1,
                 "externalIds": { "erp": "Id" },
                 "attrs": { "flag": { "column": "Flag", "as": "boolean" },
                            "count": { "column": "Count", "as": "integer" } } }"#,
        );
        assert_eq!(
            json_of(&rows[0])["attrs"],
            json!({ "flag": true, "count": 3 })
        );
        assert!(problem_of(&rows[1]).1.contains("whole number"));
        assert!(problem_of(&rows[2]).1.contains("yes/no"));
        assert_eq!(
            json_of(&rows[3])["attrs"],
            json!({ "flag": true, "count": 7 })
        );
    }

    #[test]
    fn an_event_row_names_its_subject_and_time() {
        let rows = run(
            "Sensor,At,Value\ns1,2026-10-11T10:00:00Z,10.5\n",
            r#"{ "kind": "events", "type": "reading", "typeVersion": 1,
                 "subject": { "system": "erp", "column": "Sensor" },
                 "occurredAt": { "column": "At" },
                 "payload": { "value": { "column": "Value", "as": "number" } } }"#,
        );
        assert_eq!(
            json_of(&rows[0]),
            &json!({
                "type": "reading", "typeVersion": 1,
                "subject": { "externalId": { "system": "erp", "id": "s1" } },
                "occurredAt": "2026-10-11T10:00:00Z",
                "payload": { "value": 10.5 }
            })
        );
    }

    #[test]
    fn a_link_row_names_both_ends() {
        let rows = run(
            "Booking,Room,Hours\nb1,r1,2\n",
            r#"{ "kind": "links", "type": "assigned_to",
                 "src": { "system": "erp", "column": "Booking" },
                 "dst": { "system": "erp", "column": "Room" },
                 "attrs": { "hours": { "column": "Hours", "as": "number" } } }"#,
        );
        assert_eq!(
            json_of(&rows[0]),
            &json!({
                "type": "assigned_to",
                "src": { "externalId": { "system": "erp", "id": "b1" } },
                "dst": { "externalId": { "system": "erp", "id": "r1" } },
                "attrs": { "hours": 2.0 }
            })
        );
    }

    #[test]
    fn a_column_the_file_lacks_fails_before_any_row() {
        let mapping = Mapping::parse(
            dated(None).as_bytes(), // needs `Id` and `When`
        )
        .unwrap();
        let e = convert(&mapping, &table("Id,Other\na,b\n")).unwrap_err().0;
        assert!(e.contains("`When`"), "{e}");
    }

    #[test]
    fn number_cells_become_clean_text_for_text_columns() {
        let t = Table {
            header: vec!["Id".into()],
            rows: vec![
                TableRow {
                    index: 0,
                    cells: Ok(vec![Cell::Number(1234.0)]),
                },
                TableRow {
                    index: 1,
                    cells: Ok(vec![Cell::Number(2.5)]),
                },
            ],
        };
        let mapping = Mapping::parse(
            br#"{ "kind": "objects", "type": "t", "typeVersion": 1,
                 "externalIds": { "erp": "Id" } }"#,
        )
        .unwrap();
        let rows = convert(&mapping, &t).unwrap();
        assert_eq!(json_of(&rows[0])["externalIds"]["erp"], "1234");
        assert_eq!(json_of(&rows[1])["externalIds"]["erp"], "2.5");
    }
}
