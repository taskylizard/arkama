use arkama_data::{Db, DownloadRecord};
use clap::Args;
use eyre::Result;

use crate::util::format_bytes;

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

    for record in records {
        print_record(&record);
    }

    Ok(())
}

fn print_record(record: &DownloadRecord) {
    let progress = match record.total_bytes {
        Some(total) => format!(
            "{}/{}",
            format_bytes(record.downloaded_bytes),
            format_bytes(total)
        ),
        None => format_bytes(record.downloaded_bytes),
    };

    println!(
        "[{}] {} -> {} ({})",
        record.status, record.url, record.output_path, progress
    );
}
