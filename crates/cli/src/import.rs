//! `kuki import ...`: spreadsheets (CSV, XLSX) as ingest input.
//!
//! `check` reads a file and shows what a mapping makes of it. It never touches the database.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use clap::Subcommand;
use kuki_ingest::{
    body::RowContent,
    files::{FileFormat, MAX_FILE_BYTES, Mapping, convert, read_table},
};

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
}

pub fn run(cmd: ImportCmd) -> Res {
    match cmd {
        ImportCmd::Check {
            file,
            mapping,
            sheet,
            limit,
        } => check(&file, &mapping, sheet.as_deref(), limit),
    }
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

fn check(file: &Path, mapping_path: &Path, sheet: Option<&str>, limit: usize) -> Res {
    let mapping = Mapping::parse(&read_limited(mapping_path, MAX_MAPPING_BYTES, "mapping")?)?;
    let ext = file.extension().and_then(|e| e.to_str()).unwrap_or("");
    let format = FileFormat::from_extension(ext)
        .ok_or_else(|| format!("unsupported file type `.{ext}`; use .csv or .xlsx"))?;
    let bytes = read_limited(file, MAX_FILE_BYTES as u64, "file")?;
    let table = read_table(&bytes, format, sheet)?;
    let rows = convert(&mapping, &table)?;

    let mut converted = Vec::new();
    let mut problems = Vec::new();
    for row in &rows {
        match &row.content {
            RowContent::Json(v) => converted.push(v),
            RowContent::Malformed { message, .. } => problems.push((row.index, message)),
        }
    }

    let columns: Vec<&str> = table
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
