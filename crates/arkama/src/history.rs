use arkama_data::{Db, DownloadRecord};
use clap::Args;
use comfy_table::{
    Attribute, Cell, CellAlignment, Color, ContentArrangement, Table, presets::UTF8_FULL,
};
use eyre::Result;

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
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header([
            header_cell("ID"),
            header_cell("STATUS"),
            header_cell("PROGRESS"),
            header_cell("OUTPUT"),
            header_cell("URL"),
        ]);

    for record in records {
        table.add_row([
            Cell::new(record.id).set_alignment(CellAlignment::Right),
            status_cell(&record.status),
            Cell::new(history_progress_text(
                record.downloaded_bytes,
                record.total_bytes,
            ))
            .set_alignment(CellAlignment::Right),
            Cell::new(&record.output_path),
            Cell::new(&record.url),
        ]);
    }

    table.to_string()
}

fn header_cell(label: &str) -> Cell {
    Cell::new(label)
        .fg(Color::Cyan)
        .add_attribute(Attribute::Bold)
}

fn status_cell(status: &str) -> Cell {
    let color = if status.starts_with("failed") {
        Color::Red
    } else if status == "finished" {
        Color::Green
    } else if status == "queued" {
        Color::Cyan
    } else if status == "starting" || status == "running" {
        Color::Blue
    } else if status == "paused" {
        Color::Yellow
    } else if status == "cancelled" {
        Color::DarkYellow
    } else {
        Color::White
    };

    Cell::new(status).fg(color)
}
