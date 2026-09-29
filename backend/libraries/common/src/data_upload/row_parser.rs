use std::collections::HashMap;
use chrono::NaiveDate;
use serde::{Serialize, Deserialize};

use crate::data_upload::canonical_fields::FieldType;
use crate::data_upload::mapping::MappingResult;

/// A single parsed cell value, typed according to its canonical field's
/// `FieldType`. Dates are stored as validated ISO-8601 (`YYYY-MM-DD`)
/// strings, not a calendar type — see the module-level dependency note
/// on why `chrono` isn't in use yet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CanonicalValue {
    Text(String),
    Number(f64),
    Date(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowStatus {
    /// Every mapped field (required and optional) parsed cleanly.
    Clean,
    /// All required fields parsed cleanly; at least one optional field had
    /// a bad-but-recoverable value and was dropped with a reason. The row
    /// still gets written on confirm.
    Flagged,
    /// A required field was blank or failed to parse for this specific
    /// row. The row is not written on confirm.
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowIssue {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParsedRow {
    /// 1-indexed position among the data rows (excluding the header row),
    /// for showing the visitor exactly which row a flag/skip refers to.
    pub row_number: usize,
    pub status: RowStatus,
    /// Only fields that parsed successfully are present here. A row can
    /// be `Flagged` and still be missing an optional field's entry.
    pub values: HashMap<String, CanonicalValue>,
    /// Empty for `Clean` rows. Exactly one entry (the fatal field) is the
    /// common case for `Skipped`, though multiple required fields can all
    /// fail in the same row. Any number of entries for `Flagged`.
    pub issues: Vec<RowIssue>,
}

/// Parses raw CSV data rows against an already-resolved `MappingResult`.
///
/// Each entry in `rows` must have the same length and column order as
/// `mapping.headers` — i.e. `rows[i][j]` is the raw cell under the header
/// at `mapping.headers[j]`. This mirrors how a CSV reader naturally
/// produces rows once the header row itself has been consumed.
///
/// Assumes `mapping.is_complete()` (no missing required fields, no
/// duplicate field mappings) has already been checked by the caller —
/// typically right after `resolve_mapping`, before ever reaching row
/// parsing. This function does not re-validate that; duplicate-field
/// resolution here (last successfully-parsed value wins) exists only as
/// a defensive fallback, not the primary handling of that case.
pub fn parse_csv_rows(mapping: &MappingResult, rows: &[Vec<String>]) -> Vec<ParsedRow> {
    let required_fields: Vec<&'static str> = mapping.kind.required_fields().map(|f| f.name).collect();

    rows.iter()
        .enumerate()
        .map(|(idx, row)| parse_one_row(mapping, &required_fields, idx + 1, row))
        .collect()
}

fn parse_one_row(
    mapping: &MappingResult,
    required_fields: &[&'static str],
    row_number: usize,
    row: &[String],
) -> ParsedRow {
    if row.len() != mapping.headers.len() {
        return ParsedRow {
            row_number,
            status: RowStatus::Skipped,
            values: HashMap::new(),
            issues: vec![RowIssue {
                field: "__row__".to_string(),
                reason: format!(
                    "row has {} cell(s), expected {} to match the header row",
                    row.len(),
                    mapping.headers.len()
                ),
            }],
        };
    }

    let mut values: HashMap<String, CanonicalValue> = HashMap::new();
    let mut issues: Vec<RowIssue> = Vec::new();
    let mut required_failures: Vec<RowIssue> = Vec::new();

    for (header_match, raw_cell) in mapping.headers.iter().zip(row.iter()) {
        let Some(field_name) = header_match.canonical_field else {
            // Unmatched column, visitor's mapping already decided this
            // header carries no meaning; ignore it during row parsing.
            continue;
        };

        let field = mapping
            .kind
            .canonical_fields()
            .iter()
            .find(|f| f.name == field_name)
            .expect("mapping.headers should only reference fields of mapping.kind");

        let trimmed = raw_cell.trim();
        let is_required = required_fields.contains(&field_name);

        if trimmed.is_empty() {
            if is_required {
                required_failures.push(RowIssue {
                    field: field_name.to_string(),
                    reason: format!("required field '{field_name}' is blank"),
                });
            }
            // Blank optional field: not an issue, just absent from `values`.
            continue;
        }

        match parse_typed(trimmed, field.field_type) {
            Ok(value) => {
                // Duplicate headers mapping to the same canonical field:
                // last non-blank, successfully-parsed value wins.
                values.insert(field_name.to_string(), value);
            }
            Err(reason) => {
                if is_required {
                    required_failures.push(RowIssue {
                        field: field_name.to_string(),
                        reason,
                    });
                } else {
                    issues.push(RowIssue {
                        field: field_name.to_string(),
                        reason,
                    });
                }
            }
        }
    }

    if !required_failures.is_empty() {
        // Required failures win outright: the row is unwritable regardless
        // of how many optional fields also had issues, so we report only
        // the required failures as the reason it was skipped.
        return ParsedRow {
            row_number,
            status: RowStatus::Skipped,
            values: HashMap::new(),
            issues: required_failures,
        };
    }

    let status = if issues.is_empty() {
        RowStatus::Clean
    } else {
        RowStatus::Flagged
    };

    ParsedRow {
        row_number,
        status,
        values,
        issues,
    }
}

fn parse_typed(trimmed: &str, field_type: FieldType) -> Result<CanonicalValue, String> {
    match field_type {
        FieldType::String => Ok(CanonicalValue::Text(trimmed.to_string())),
        FieldType::Number => parse_number(trimmed)
            .map(CanonicalValue::Number)
            .ok_or_else(|| format!("'{trimmed}' is not a valid number")),
        FieldType::Date => parse_multi_format_date(trimmed)
            .map(CanonicalValue::Date)
            .ok_or_else(|| {
                format!("'{trimmed}' is not a recognized date format")
            }),
    }
}

/// Strips common thousands-separator commas before parsing, since
/// spreadsheet exports frequently write e.g. "1,234" for a plain number.
fn parse_number(trimmed: &str) -> Option<f64> {
    let cleaned: String = trimmed.chars().filter(|c| *c != ',').collect();
    cleaned.parse::<f64>().ok()
}

/// Formats tried in order; first successful parse wins. Day-first formats
/// are tried before month-first, per regional convention (day-first is
/// standard across most of Africa/UK-influenced locales) — but note this
/// is a genuine ambiguity for dates where both day and month are <= 12
/// (e.g. "03/04/2026"), not a solved problem. See module docs.
const DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d %b %Y",
    "%d %B %Y",
];

fn parse_multi_format_date(trimmed: &str) -> Option<String> {
    for fmt in DATE_FORMATS {
        if let Ok(date) = NaiveDate::parse_from_str(trimmed, fmt) {
            return Some(date.format("%Y-%m-%d").to_string());
        }
    }
    None
}
