//! A spreadsheet file read into a table of cells.
//!
//! Row indexes count data from: index 0 is the first row below the header. A row
//! that cannot be read (bad UTF-8, a spreadsheet error value) is kept with its
//! reason, so it is reported and quarantined rather than failing the file.
//!
//! Dates in XLSX are serial numbers; they are read in the 1900 date system and
//! carry no timezone, so they are treated as UTC every other date without an offset.

use std::{collections::BTreeSet, io::Cursor};

use calamine::{Data, Reader, Xlsx, open_workbook_from_rs};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use thiserror::Error;

pub const MAX_FILE_BYTES: usize = 50 * 1024 * 1024;
pub const MAX_ROWS: usize = 500_000;
pub const MAX_COLUMNS: usize = 512;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct TableError(pub String);

fn fail<T>(message: impl Into<String>) -> Result<T, TableError> {
    Err(TableError(message.into()))
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Empty,
    Text(String),
    Number(f64),
    Bool(bool),
    /// A spreadsheet date or time. No time zone: treated as UTC.
    DateTime(NaiveDateTime),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRow {
    /// Zero-based data row: 0 is the first row below the header.
    pub index: u32,
    /// The cells, or why the row could not be read.
    pub cells: Result<Vec<Cell>, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    /// Trimmed column names. Unnamed columns are empty strings and cannot be mapped.
    pub header: Vec<String>,
    pub rows: Vec<TableRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Xlsx,
}

impl FileFormat {
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "xlsx" => Some(Self::Xlsx),
            _ => None,
        }
    }
}

/// Reads a whole file. `sheet` picks a worksheet and applies to XLSX only.
pub fn read_table(
    bytes: &[u8],
    format: FileFormat,
    sheet: Option<&str>,
) -> Result<Table, TableError> {
    if bytes.len() > MAX_FILE_BYTES {
        return fail(format!(
            "the file is larger than {} MiB",
            MAX_FILE_BYTES / (1024 * 1024)
        ));
    }
    match format {
        FileFormat::Csv => {
            if sheet.is_some() {
                return fail("a sheet can only be chosen for .xlsx files");
            }
            read_csv(bytes)
        }
        FileFormat::Xlsx => read_xlsx(bytes, sheet),
    }
}

fn clean_header(raw: Vec<String>) -> Result<Vec<String>, TableError> {
    if raw.len() > MAX_COLUMNS {
        return fail(format!("the file has more than {MAX_COLUMNS} columns"));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(raw.len());
    for name in raw {
        let name = name.trim_start_matches('\u{feff}').trim().to_owned();
        if !name.is_empty() && !seen.insert(name.clone()) {
            return fail(format!("duplicate column name `{name}`"));
        }
        out.push(name);
    }
    Ok(out)
}

fn text_cell(s: &str) -> Cell {
    if s.trim().is_empty() {
        Cell::Empty
    } else {
        Cell::Text(s.to_owned())
    }
}

fn too_many_rows<T>() -> Result<T, TableError> {
    fail(format!("the file has more than {MAX_ROWS} data rows"))
}

fn read_csv(bytes: &[u8]) -> Result<Table, TableError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(bytes);
    let header = reader
        .byte_headers()
        .map_err(|e| TableError(format!("cannot read the header row: {e}")))?
        .iter()
        .map(|f| std::str::from_utf8(f).map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| TableError("the header row is not valid UTF-8".into()))?;
    let header = clean_header(header)?;

    let mut rows = Vec::new();
    for (i, record) in reader.byte_records().enumerate() {
        if rows.len() >= MAX_ROWS {
            return too_many_rows();
        }
        let cells = match record {
            Err(e) => Err(format!("unreadable row: {e}")),
            Ok(r) => r
                .iter()
                .enumerate()
                .map(|(n, f)| {
                    std::str::from_utf8(f)
                        .map(text_cell)
                        .map_err(|_| format!("field {} is not valid UTF-8", n + 1))
                })
                .collect(),
        };
        rows.push(TableRow {
            index: i as u32,
            cells,
        });
    }
    if rows.is_empty() {
        return fail("the file has no data rows");
    }
    Ok(Table { header, rows })
}

fn read_xlsx(bytes: &[u8], sheet: Option<&str>) -> Result<Table, TableError> {
    let mut workbook: Xlsx<Cursor<&[u8]>> = open_workbook_from_rs(Cursor::new(bytes))
        .map_err(|e| TableError(format!("cannot open the workbook: {e}")))?;
    let names = workbook.sheet_names();
    let name = match sheet {
        Some(s) if names.iter().any(|n| n == s) => s.to_owned(),
        Some(s) => {
            return fail(format!(
                "no sheet named `{s}`; the workbook has: {}",
                names.join(", ")
            ));
        }
        None => match names.first() {
            Some(first) => first.clone(),
            None => return fail("the workbook has no sheets"),
        },
    };
    let range = workbook
        .worksheet_range(&name)
        .map_err(|e| TableError(format!("cannot read sheet `{name}`: {e}")))?;

    let mut lines = range.rows();
    let Some(head) = lines.next() else {
        return fail(format!("sheet `{name}` is empty"));
    };
    let header = clean_header(head.iter().map(header_text).collect())?;

    let mut rows = Vec::new();
    for (i, line) in lines.enumerate() {
        let cells: Result<Vec<Cell>, String> = line
            .iter()
            .enumerate()
            .map(|(c, d)| xlsx_cell(d).map_err(|m| format!("column {}: {m}", c + 1)))
            .collect();
        // Blank rows between or after the data are not rows.
        if matches!(&cells, Ok(v) if v.iter().all(|c| *c == Cell::Empty)) {
            continue;
        }
        if rows.len() >= MAX_ROWS {
            return too_many_rows();
        }
        rows.push(TableRow {
            index: i as u32,
            cells,
        });
    }
    if rows.is_empty() {
        return fail(format!("sheet `{name}` has no data rows"));
    }
    Ok(Table { header, rows })
}

fn header_text(d: &Data) -> String {
    match d {
        Data::String(s) => s.clone(),
        Data::Float(f) => number_text(*f),
        Data::Int(i) => i.to_string(),
        _ => String::new(),
    }
}

fn xlsx_cell(d: &Data) -> Result<Cell, String> {
    Ok(match d {
        Data::Empty => Cell::Empty,
        Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => text_cell(s),
        Data::Float(f) => Cell::Number(*f),
        Data::Int(i) => Cell::Number(*i as f64),
        Data::Bool(b) => Cell::Bool(*b),
        Data::DateTime(dt) => match from_serial(dt.as_f64()) {
            Some(t) => Cell::DateTime(t),
            None => return Err(format!("not a valid date: {}", dt.as_f64())),
        },
        Data::Error(e) => return Err(format!("the cell holds a spreadsheet error ({e:?})")),
    })
}

/// An Excel serial date (1900 system) as a date and time, to the second.
pub(crate) fn from_serial(serial: f64) -> Option<NaiveDateTime> {
    if !serial.is_finite() || !(0.0..2_958_466.0).contains(&serial) {
        return None;
    }
    let base = NaiveDate::from_ymd_opt(1899, 12, 30)?.and_hms_opt(0, 0, 0)?;
    base.checked_add_signed(Duration::seconds((serial * 86_400.0).round() as i64))
}

/// A number as a cell's text: whole numbers without a trailing `.0`.
pub(crate) fn number_text(f: f64) -> String {
    if f.is_finite() && (f - f.round()).abs() < 1e-9 && f.abs() < 1e15 {
        format!("{}", f.round() as i64)
    } else {
        format!("{f}")
    }
}

#[cfg(test)]
mod tests {
    use rust_xlsxwriter::{Format, Workbook};

    use super::*;

    fn csv(text: &[u8]) -> Result<Table, TableError> {
        read_table(text, FileFormat::Csv, None)
    }

    fn cells(t: &Table, row: usize) -> Vec<Cell> {
        t.rows[row].cells.clone().unwrap()
    }

    #[test]
    fn csv_reads_the_header_quotes_blanks_short_rows_and_a_bom() {
        let t = csv("\u{feff}Name,Qty\nBolt,2\n\"Nut, hex\",\nShort\n".as_bytes()).unwrap();
        assert_eq!(t.header, ["Name", "Qty"]);
        assert_eq!(t.rows.len(), 3);
        assert_eq!(
            cells(&t, 0),
            [Cell::Text("Bolt".into()), Cell::Text("2".into())]
        );
        assert_eq!(cells(&t, 1), [Cell::Text("Nut, hex".into()), Cell::Empty]);
        assert_eq!(cells(&t, 2), [Cell::Text("Short".into())]);
        assert_eq!(t.rows[2].index, 2);
    }

    #[test]
    fn a_csv_row_that_is_not_utf8_is_unreadable_but_the_file_loads() {
        let t = csv(b"Name,Qty\nBolt,2\n\xff\xfe,3\nNut,4\n").unwrap();
        assert_eq!(t.rows.len(), 3);
        assert!(t.rows[0].cells.is_ok() && t.rows[2].cells.is_ok());
        let e = t.rows[1].cells.clone().unwrap_err();
        assert!(e.contains("UTF-8"), "{e}");
    }

    #[test]
    fn a_csv_without_data_or_with_duplicate_columns_is_refused() {
        let e = csv(b"Name,Qty\n").unwrap_err().0;
        assert!(e.contains("no data rows"), "{e}");
        let e = csv(b"A,A\n1,2\n").unwrap_err().0;
        assert!(e.contains("duplicate"), "{e}");
        let e = read_table(b"A\n1\n", FileFormat::Csv, Some("Sheet1"))
            .unwrap_err()
            .0;
        assert!(e.contains("xlsx"), "{e}");
    }

    #[test]
    fn serial_dates_convert_to_the_second() {
        let t = from_serial(45000.5).unwrap();
        assert_eq!(
            t,
            NaiveDate::from_ymd_opt(2023, 3, 15)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap()
        );
        assert!(from_serial(f64::NAN).is_none());
        assert!(from_serial(-1.0).is_none());
        assert_eq!(number_text(1234.0), "1234");
        assert_eq!(number_text(2.5), "2.5");
    }

    fn workbook() -> Vec<u8> {
        let mut wb = Workbook::new();
        let open = wb.add_worksheet();
        open.set_name("Open").unwrap();
        open.write_string(0, 0, "Case No").unwrap();
        open.write_string(0, 1, "Hours").unwrap();
        open.write_string(0, 2, "Due").unwrap();
        open.write_string(1, 0, "A-1").unwrap();
        open.write_number(1, 1, 2.5).unwrap();
        open.write_number_with_format(
            1,
            2,
            45000.5,
            &Format::new().set_num_format("yyyy-mm-dd hh:mm"),
        )
        .unwrap();
        let closed = wb.add_worksheet();
        closed.set_name("Closed").unwrap();
        closed.write_string(0, 0, "Case No").unwrap();
        closed.write_string(1, 0, "B-9").unwrap();
        wb.save_to_buffer().unwrap()
    }

    #[test]
    fn xlsx_reads_text_numbers_and_date_cells_from_the_first_sheet() {
        let t = read_table(&workbook(), FileFormat::Xlsx, None).unwrap();
        assert_eq!(t.header, ["Case No", "Hours", "Due"]);
        assert_eq!(
            cells(&t, 0),
            [
                Cell::Text("A-1".into()),
                Cell::Number(2.5),
                Cell::DateTime(
                    NaiveDate::from_ymd_opt(2023, 3, 15)
                        .unwrap()
                        .and_hms_opt(12, 0, 0)
                        .unwrap()
                ),
            ]
        );
    }

    #[test]
    fn xlsx_sheets_are_chosen_by_name_and_a_missing_one_lists_the_others() {
        let t = read_table(&workbook(), FileFormat::Xlsx, Some("Closed")).unwrap();
        assert_eq!(t.header, ["Case No"]);
        assert_eq!(cells(&t, 0), [Cell::Text("B-9".into())]);
        let e = read_table(&workbook(), FileFormat::Xlsx, Some("Nope"))
            .unwrap_err()
            .0;
        assert!(e.contains("Open") && e.contains("Closed"), "{e}");
    }
}
