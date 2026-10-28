//! `kuki import ...`: spreadsheets (CSV, XLSX) as ingest input.
//!
//! `check` shows what a mapping makes of a file and never touches the database. `run` loads the
//! file through the ingest pipeline: rows that cannot be converted or are refused by the pipeline
//! are quarantined with the reason, and the rest are written.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use clap::Subcommand;
use kuki_ingest::{
    IngestContext, IngestOptions,
    body::RowContent,
    files::{
        FileFormat, MAX_FILE_BYTES, Mapping, Table, convert, idempotency_key, load_rows, read_table,
    },
};
use serde_json::json;

use crate::webhooks::{connect, session};

type Res = Result<(), Box<dyn Error>>;

const MAX_MAPPING_BYTES: u64 = 1024 * 1024;
const SHOWN_PROBLEMS: usize = 20;

#[derive(Subcommand)]
pub enum ImportCmd {
    /// Read a file and show what the mapping makes of it. Nothing is written. Exits with an
    /// error if any row cannot be converted.
    Check {
        /// A .csv or .xlsx file.
        file: PathBuf,
        /// The mapping file (JSON).
        #[arg(long)]
        mapping: PathBuf,
        /// Worksheet name (.xlsx only; default: the first sheet).
        #[arg(long)]
        sheet: Option<String>,
        /// How many converted rows to print.
        #[arg(long, default_value_t = 5)]
        limit: usize,
    },
    /// Load a file through the ingest pipeline. Importing the same file with the same mapping
    /// again changes nothing.
    Run {
        /// A .csv or .xlsx file.
        file: PathBuf,
        /// Organisation slug or id.
        #[arg(long)]
        org: String,
        /// The mapping file (JSON).
        #[arg(long)]
        mapping: PathBuf,
        /// Worksheet name (.xlsx only; default: the first sheet).
        #[arg(long)]
        sheet: Option<String>,
        /// Exit with an error if any row was rejected (the other rows are still written).
        #[arg(long)]
        strict: bool,
        /// Audit source recorded on the batch.
        #[arg(long, default_value = "cli:import")]
        source: String,
    },
}

pub async fn run(cmd: ImportCmd) -> Res {
    match cmd {
        ImportCmd::Check {
            file,
            mapping,
            sheet,
            limit,
        } => check(&file, &mapping, sheet.as_deref(), limit),
        ImportCmd::Run {
            file,
            org,
            mapping,
            sheet,
            strict,
            source,
        } => load(&file, &org, &mapping, sheet.as_deref(), strict, &source).await,
    }
}

struct Input {
    mapping: Mapping,
    mapping_bytes: Vec<u8>,
    file_bytes: Vec<u8>,
    table: Table,
}

fn read_limited(path: &Path, max: u64, what: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let len = fs::metadata(path)
        .map_err(|e| format!("cannot read the {what} {}: {e}", path.display()))?
        .len();
    if len > max {
        return Err(format!("the {what} {} is larger than {max} bytes", path.display()).into());
    }
    Ok(fs::read(path).map_err(|e| format!("cannot read the {what} {}: {e}", path.display()))?)
}

fn read_input(
    file: &Path,
    mapping_path: &Path,
    sheet: Option<&str>,
) -> Result<Input, Box<dyn Error>> {
    let mapping_bytes = read_limited(mapping_path, MAX_MAPPING_BYTES, "mapping")?;
    let mapping = Mapping::parse(&mapping_bytes)?;
    let ext = file.extension().and_then(|e| e.to_str()).unwrap_or("");
    let format = FileFormat::from_extension(ext)
        .ok_or_else(|| format!("unsupported file type `.{ext}`; use .csv or .xlsx"))?;
    let file_bytes = read_limited(file, MAX_FILE_BYTES as u64, "file")?;
    let table = read_table(&file_bytes, format, sheet)?;
    Ok(Input {
        mapping,
        mapping_bytes,
        file_bytes,
        table,
    })
}

fn check(file: &Path, mapping_path: &Path, sheet: Option<&str>, limit: usize) -> Res {
    let input = read_input(file, mapping_path, sheet)?;
    let rows = convert(&input.mapping, &input.table)?;

    let mut converted = Vec::new();
    let mut problems = Vec::new();
    for row in &rows {
        match &row.content {
            RowContent::Json(v) => converted.push(v),
            RowContent::Malformed { message, .. } => problems.push((row.index, message)),
        }
    }

    let columns: Vec<&str> = input
        .table
        .header
        .iter()
        .map(String::as_str)
        .filter(|h| !h.is_empty())
        .collect();
    println!("columns: {}", columns.join(", "));
    println!(
        "rows: {} read, {} converted, {} with problems",
        rows.len(),
        converted.len(),
        problems.len()
    );
    for v in converted.iter().take(limit) {
        println!("{v}");
    }
    for (index, message) in problems.iter().take(SHOWN_PROBLEMS) {
        println!("row {}: {message}", index + 1);
    }
    if problems.len() > SHOWN_PROBLEMS {
        println!("... and {} more", problems.len() - SHOWN_PROBLEMS);
    }
    if !problems.is_empty() {
        return Err(format!(
            "{} of {} rows could not be converted",
            problems.len(),
            rows.len()
        )
        .into());
    }
    Ok(())
}

async fn load(
    file: &Path,
    org: &str,
    mapping_path: &Path,
    sheet: Option<&str>,
    strict: bool,
    source: &str,
) -> Res {
    let input = read_input(file, mapping_path, sheet)?;
    let rows = convert(&input.mapping, &input.table)?;
    let total = rows.len();
    let key = idempotency_key(&input.file_bytes, &input.mapping_bytes, sheet);

    let (org_id, pool) = connect(org).await?;
    let ctx = IngestContext {
        org: org_id,
        session: session(org_id),
        source: source.to_owned(),
        idempotency_key: key,
        backfill_id: None,
    };
    let summary = load_rows(&pool, ctx, &input.mapping, rows, &IngestOptions::default()).await?;

    let rejected: Vec<_> = summary
        .rejected
        .iter()
        .map(|r| {
            json!({
                "row": r.index + 1,
                "code": r.code,
                "message": r.message,
                "field": r.field,
                "quarantineId": r.quarantine_id,
            })
        })
        .collect();
    let out = json!({
        "kind": summary.kind,
        "batchId": summary.batch_id,
        "rows": total,
        "accepted": summary.accepted,
        "created": summary.created,
        "updated": summary.updated,
        "unchanged": summary.unchanged,
        "rejected": rejected,
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    eprintln!(
        "imported {} of {total} rows; {} rejected",
        summary.accepted,
        summary.rejected.len()
    );
    if strict && !summary.rejected.is_empty() {
        return Err(format!(
            "{} rows were rejected (the other {} were written)",
            summary.rejected.len(),
            summary.accepted
        )
        .into());
    }
    Ok(())
}
