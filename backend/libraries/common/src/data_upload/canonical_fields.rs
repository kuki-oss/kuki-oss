use serde::{Deserialize, Serialize};

/// Discriminates the data upload kinds, currently only two. Serde
/// repr MUST match the literal strings used in the `merchant_upload
/// s.kind` CHECK constraint ('supplier' | 'demand') exactly, since
/// this enum round-trips through `parsed_review`/DB columns as plain
/// lowercase strings
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UploadKind {
    Supplier,
    Demand,
}

impl UploadKind {
    /// Explicit accessor for the DB-literal string, so call sites
    /// building raw sql (rather than going through serde) don't
    /// have to trust `#[serde(rename_all = "lowercase")]` working
    pub fn as_db_str(self) -> &'static str {
        match self {
            UploadKind::Supplier => "supplier",
            UploadKind::Demand => "demand",
        }
    }

    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "supplier" => Some(UploadKind::Supplier),
            "demand" => Some(UploadKind::Demand),
            _ => None,
        }
    }

    pub fn canonical_fields(self) -> &'static [CanonicalField] {
        match self {
            UploadKind::Supplier => SUPPLIER_FIELDS,
            UploadKind::Demand => DEMAND_FIELDS,
        }
    }

    pub fn required_fields(self) -> impl Iterator<Item = &'static CanonicalField> {
        self.canonical_fields().iter().filter(|f| f.required)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    String,
    Number,
    Date,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalField {
    pub name: &'static str,
    pub required: bool,
    pub field_type: FieldType,
}

impl CanonicalField {
    const fn new(name: &'static str, required: bool, field_type: FieldType) -> Self {
        CanonicalField { name, required, field_type }
    }
}

pub const SUPPLIER_FIELDS: &[CanonicalField] = &[
    CanonicalField::new("supplier_name", true, FieldType::String),
    CanonicalField::new("lead_time_days", true, FieldType::Number),
    CanonicalField::new("moq", false, FieldType::Number),
    CanonicalField::new("reliability", false, FieldType::Number),
    CanonicalField::new("product_name", true, FieldType::String),
    CanonicalField::new("product_category", false, FieldType::String),
    CanonicalField::new("current_stock", true, FieldType::Number),
    CanonicalField::new("reorder_point", false, FieldType::Number),
];

pub const DEMAND_FIELDS: &[CanonicalField] = &[
    CanonicalField::new("product_name", true, FieldType::String),
    CanonicalField::new("product_category", false, FieldType::String),
    CanonicalField::new("event_date", true, FieldType::Date),
    CanonicalField::new("quantity_delta", true, FieldType::Number),
    CanonicalField::new("reason", false, FieldType::String),
];

/// Look up a canonical field by name within a given kind. Used by
/// mapping.rs to validate override targets against the real field
/// list rather than trusting caller-supplied strings blindly
pub fn find_field(kind: UploadKind, name: &str) -> Option<&'static CanonicalField> {
    kind.canonical_fields().iter().find(|f| f.name == name)
}
