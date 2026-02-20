mod cli;
mod completions;
mod config;
mod download;
mod gui;
mod history;
mod progress;
mod util;

use clap::{Parser, Subcommand};
use eyre::Result;
use owo_colors::OwoColorize;
use tokio::task;

#[derive(Parser)]
#[command(name = "arkama", version, about = "High-performance download manager")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    #[arg(long = "reset-db", help = "Reset the application database.")]
    reset_db: bool,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Download a file from a URL.")]
    Download(cli::DownloadArgs),

    #[command(about = "Launch the graphical user interface.")]
    Gui,

    #[command(about = "Show download history.")]
    History(history::HistoryArgs),

    #[command(about = "View or modify configuration.")]
    Config(config::ConfigArgs),

    #[command(about = "Generate shell completions.")]
    Completions(completions::CompletionsArgs),
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{} {err:#}", "error:".red().bold());
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    match args.command {
        Some(Command::Download(download_args)) => download::run(download_args).await,
        Some(Command::Gui) => run_gui().await,
        Some(Command::History(history_args)) => history::run(history_args),
        Some(Command::Config(config_args)) => config::run(config_args),
        Some(Command::Completions(completions_args)) => completions::run(completions_args),
        None => run_gui().await,
    }
}

async fn run_gui() -> Result<()> {
    task::spawn_blocking(gui::run)
        .await
        .map_err(|err| eyre::eyre!(err))?
}
