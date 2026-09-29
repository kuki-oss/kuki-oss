use std::collections::HashMap;

use crate::data_upload::canonical_fields::UploadKind;
use crate::data_upload::synonyms::{lookup_synonym, normalize_header};

/// How a single raw header ended up resolved to a canonical field
/// (or didn't). Order here mirrors the agreed matching order:
/// override > exact > synonym > unmatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchKind {
    /// Visitor explicitly supplied `raw_header -> canonical_field` via
    /// `mapping_overrides`. Always wins regardless of what exact/synonym
    /// matching would have produced.
    Override,
    /// Normalized raw header exactly equals a normalized canonical field
    /// name (e.g. "Supplier Name" -> supplier_name).
    Exact,
    /// Resolved via the synonym table (e.g. "vendor" -> supplier_name).
    Synonym,
    /// No override, exact, or synonym match found.
    Unmatched,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderMatch {
    pub raw_header: String,
    pub canonical_field: Option<&'static str>,
    pub match_kind: MatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingResult {
    pub kind: UploadKind,
    pub headers: Vec<HeaderMatch>,
    /// Required canonical fields for this kind that no header (override,
    /// exact, or synonym) resolved to. Empty means the upload can proceed
    /// to row parsing.
    pub missing_required: Vec<&'static str>,
    /// Canonical fields that more than one header resolved to (via any
    /// combination of override/exact/synonym). Non-empty blocks the
    /// upload the same way missing_required does -- ambiguous mapping,
    /// not a row-level data problem, so it's caught here rather than
    /// silently resolved during row parsing.
    pub duplicate_fields: Vec<&'static str>,
}

impl MappingResult {
    pub fn is_complete(&self) -> bool {
        self.missing_required.is_empty() && self.duplicate_fields.is_empty()
    }
}

/// Resolves raw uploaded headers against a kind's canonical fields.
///
/// `overrides` is visitor-supplied `raw_header -> canonical_field`
/// mapping (from `merchant_uploads.mapping_overrides`), keyed exactly as
/// the visitor typed the raw header — normalization is applied internally
/// for lookup, so callers don't need to pre-normalize override keys.
pub fn resolve_mapping(
    kind: UploadKind,
    raw_headers: &[String],
    overrides: &HashMap<String, String>,
) -> MappingResult {
    // Normalize override keys once, so lookups below are normalization-aware
    // the same way exact/synonym matching is.
    let normalized_overrides: HashMap<String, String> = overrides
        .iter()
        .map(|(k, v)| (normalize_header(k), v.clone()))
        .collect();

    // Precompute normalized canonical field names for exact-match checking.
    let canonical_fields = kind.canonical_fields();
    let normalized_canonical: Vec<(String, &'static str)> = canonical_fields
        .iter()
        .map(|f| (normalize_header(f.name), f.name))
        .collect();

    let mut headers = Vec::with_capacity(raw_headers.len());
    let mut matched_fields: Vec<&'static str> = Vec::new();

    for raw in raw_headers {
        let normalized = normalize_header(raw);

        // 1. Override
        if let Some(target) = normalized_overrides.get(&normalized) {
            // Only accept the override if it names a real canonical field
            // for this kind — an override pointing at a bogus/other-kind
            // field is treated as unmatched rather than silently trusted.
            if let Some(field) = canonical_fields.iter().find(|f| f.name == target) {
                headers.push(HeaderMatch {
                    raw_header: raw.clone(),
                    canonical_field: Some(field.name),
                    match_kind: MatchKind::Override,
                });
                matched_fields.push(field.name);
                continue;
            }
        }

        // 2. Exact match
        if let Some((_, canonical)) = normalized_canonical
            .iter()
            .find(|(norm, _)| *norm == normalized)
        {
            headers.push(HeaderMatch {
                raw_header: raw.clone(),
                canonical_field: Some(*canonical),
                match_kind: MatchKind::Exact,
            });
            matched_fields.push(*canonical);
            continue;
        }

        // 3. Synonym table
        if let Some(canonical) = lookup_synonym(raw) {
            // Guard: a synonym might point at a field that belongs to the
            // OTHER kind (the table is shared across both kinds). Only
            // accept it if it's actually a field of the requested kind.
            if canonical_fields.iter().any(|f| f.name == canonical) {
                headers.push(HeaderMatch {
                    raw_header: raw.clone(),
                    canonical_field: Some(canonical),
                    match_kind: MatchKind::Synonym,
                });
                matched_fields.push(canonical);
                continue;
            }
        }

        // 4. Unmatched
        headers.push(HeaderMatch {
            raw_header: raw.clone(),
            canonical_field: None,
            match_kind: MatchKind::Unmatched,
        });
    }

    let missing_required: Vec<&'static str> = kind
        .required_fields()
        .map(|f| f.name)
        .filter(|name| !matched_fields.contains(name))
        .collect();

    let mut duplicate_fields: Vec<&'static str> = Vec::new();
    {
        let mut seen: Vec<&'static str> = Vec::new();
        for field in &matched_fields {
            if seen.contains(field) {
                if !duplicate_fields.contains(field) {
                    duplicate_fields.push(field);
                }
            } else {
                seen.push(field);
            }
        }
    }

    MappingResult {
        kind,
        headers,
        missing_required,
        duplicate_fields,
    }
}
