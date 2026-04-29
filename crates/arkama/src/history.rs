use arkama_data::{Db, DownloadRecord};
use clap::Args;
use eyre::Result;
use std::fmt::Write as _;

use crate::util::history_progress_text;

#[derive(Args, Debug)]
pub struct HistoryArgs {
    #[arg(
        short = 'n',
        long = "limit",
        default_value_t = 20,
        help = "Number of records to show."
    )]
    pub limit: usize,

    #[arg(short = 'q', long = "query", help = "Filter by URL, path, or status.")]
    pub query: Option<String>,
}

pub fn run(args: HistoryArgs) -> Result<()> {
    let db = Db::open()?;
    let records = if let Some(query) = &args.query {
        db.load_downloads(query)?
    } else {
        db.load_downloads_limit(args.limit)?
    };

    if records.is_empty() {
        println!("no downloads found");
        return Ok(());
    }

    println!("{}", render_table(&records));

    Ok(())
}

fn render_table(records: &[DownloadRecord]) -> String {
    let headers = ["ID", "STATUS", "PROGRESS", "OUTPUT", "URL"];
    let mut widths = headers.map(str::len);
    let mut rows = Vec::new();

    for record in records {
        let row = [
            record.id.to_string(),
            record.status.clone(),
            history_progress_text(record.downloaded_bytes, record.total_bytes),
            record.output_path.clone(),
            record.url.clone(),
        ];
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.chars().count());
        }
        rows.push(row);
    }

    let mut table = String::new();
    write_row(&mut table, &headers, &widths);
    write_separator(&mut table, &widths);
    for row in rows {
        write_row(&mut table, &row, &widths);
    }

    table
}

fn write_row<const N: usize>(table: &mut String, row: &[impl AsRef<str>; N], widths: &[usize; N]) {
    for (index, value) in row.iter().enumerate() {
        let value = value.as_ref();
        let _ = write!(table, "{value:<width$}", width = widths[index]);
        if index + 1 < N {
            table.push_str("  ");
        }
    }
    table.push('\n');
}

fn write_separator<const N: usize>(table: &mut String, widths: &[usize; N]) {
    for (index, width) in widths.iter().enumerate() {
        for _ in 0..*width {
            table.push('-');
        }
        if index + 1 < N {
            table.push_str("  ");
        }
    }
    table.push('\n');
}
