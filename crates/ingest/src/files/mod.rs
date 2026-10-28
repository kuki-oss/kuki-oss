//! Spreadsheet files as ingest input.
//!
//! A CSV or XLSX file is read into a table of cells (`table`), and a mapping file (`mapping`)
//! says how each row becomes the JSON the ingest pipeline already accepts (`rows`): an object,
//! an event or a link. Rows that cannot be converted become `Malformed` rows, so the pipeline
//! quarantines them with the reason and the rest of the file still loads. `load` sends the rows
//! through the pipeline, and `key` derives the idempotency key from the file's contents.
//!
//! Behind the `files` feature so the API server does not carry the readers.

pub mod key;
pub mod load;
pub mod mapping;
pub mod rows;
pub mod table;

pub use key::idempotency_key;
pub use load::{LoadSummary, load_rows};
pub use mapping::{Mapping, MappingError};
pub use rows::convert;
pub use table::{Cell, FileFormat, MAX_FILE_BYTES, Table, TableError, TableRow, read_table};
