/// Normalizes a raw uploaded header for matching: lowercase, trim, and
/// collapse whitespace/underscore variation so "Supplier Name",
/// "supplier_name", and "supplier   name" all compare equal.
pub fn normalize_header(raw: &str) -> String {
    raw.trim()
        .to_lowercase()
        .split(|c: char| c == '_' || c.is_whitespace())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// (normalized synonym phrase, canonical field name) pairs. Deliberately a
/// flat list rather than a match statement, since Dennis's framing is that
/// this table is expected to grow as real uploads surface new header
/// variants — appending a tuple should be the whole diff.
///
/// Note: normalized canonical field names themselves (e.g. "supplier name"
/// for "supplier_name") don't need an entry here — that's handled by the
/// exact-match step in mapping.rs, which normalizes the canonical field
/// name the same way before comparing.
const SYNONYMS: &[(&str, &str)] = &[
    // supplier_name
    ("supplier", "supplier_name"),
    ("vendor", "supplier_name"),
    ("vendor name", "supplier_name"),
    ("supplier name", "supplier_name"),
    // lead_time_days
    ("lead time", "lead_time_days"),
    ("delivery time", "lead_time_days"),
    ("lead time days", "lead_time_days"),
    // moq
    ("minimum order quantity", "moq"),
    ("min order qty", "moq"),
    // reliability
    ("reliability score", "reliability"),
    ("supplier reliability", "reliability"),
    // product_name
    ("product", "product_name"),
    ("item", "product_name"),
    ("item name", "product_name"),
    ("sku name", "product_name"),
    // product_category
    ("category", "product_category"),
    ("product cat", "product_category"),
    // current_stock
    ("stock", "current_stock"),
    ("inventory", "current_stock"),
    ("qty on hand", "current_stock"),
    ("quantity on hand", "current_stock"),
    // reorder_point
    ("reorder level", "reorder_point"),
    ("reorder threshold", "reorder_point"),
    // event_date
    ("date", "event_date"),
    ("transaction date", "event_date"),
    ("txn date", "event_date"),
    // quantity_delta
    ("qty", "quantity_delta"),
    ("amount", "quantity_delta"),
    ("units", "quantity_delta"),
    ("quantity", "quantity_delta"),
    // reason
    ("notes", "reason"),
    ("comment", "reason"),
];

/// Looks up a canonical field name for a raw header via the synonym
/// table. Caller is responsible for having already checked for an exact
/// match against the canonical field list first — this function only
/// covers the second step of the matching order.
pub fn lookup_synonym(raw_header: &str) -> Option<&'static str> {
    let normalized = normalize_header(raw_header);
    SYNONYMS
        .iter()
        .find(|(synonym, _)| *synonym == normalized)
        .map(|(_, canonical)| *canonical)
}
