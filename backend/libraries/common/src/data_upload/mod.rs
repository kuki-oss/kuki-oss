//! Shared types and parsing logic for the data upload pipeline
//! (S3 -> Ingestion Worker -> Preview -> Confirm), used by both
//! api-service and the Ingestion Worker so they can never disagree
//! on canonical field shapes, mapping rules, or slug generation.

pub mod canonical_fields;
pub mod mapping;
pub mod row_parser;
pub mod synonyms;
pub mod slugify;

pub use canonical_fields::{CanonicalField, FieldType, UploadKind};
pub use mapping::{resolve_mapping, HeaderMatch, MappingResult, MatchKind};
pub use row_parser::{parse_csv_rows, CanonicalValue, ParsedRow, RowIssue, RowStatus};
pub use synonyms::{lookup_synonym, normalize_header};
pub use slugify::slugify;
