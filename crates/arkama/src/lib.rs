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
use tokio::task;

#[derive(Parser)]
#[command(name = "arkama", version, about = "High-performance download manager")]
struct CombinedArgs {
    #[command(subcommand)]
    command: Option<CombinedCommand>,

    #[arg(long = "reset-db", help = "Reset the application database.")]
    reset_db: bool,
}

#[derive(Subcommand)]
enum CombinedCommand {
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

#[derive(Parser)]
#[command(
    name = "arkama-cli",
    version,
    about = "High-performance download manager CLI",
    arg_required_else_help = true
)]
struct CliArgs {
    #[command(subcommand)]
    command: CliCommand,

    #[arg(long = "reset-db", help = "Reset the application database.")]
    reset_db: bool,
}

#[derive(Subcommand)]
enum CliCommand {
    #[command(about = "Download a file from a URL.")]
    Download(cli::DownloadArgs),

    #[command(about = "Show download history.")]
    History(history::HistoryArgs),

    #[command(about = "View or modify configuration.")]
    Config(config::ConfigArgs),

    #[command(about = "Generate shell completions.")]
    Completions(completions::CompletionsArgs),
}

#[derive(Parser)]
#[command(
    name = "arkama-gui",
    version,
    about = "High-performance download manager GUI"
)]
struct GuiArgs {
    #[arg(long = "reset-db", help = "Reset the application database.")]
    reset_db: bool,
}

pub async fn run_combined() -> Result<()> {
    let args = CombinedArgs::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    match args.command {
        Some(CombinedCommand::Download(download_args)) => download::run(download_args).await,
        Some(CombinedCommand::Gui) => run_gui().await,
        Some(CombinedCommand::History(history_args)) => history::run(history_args),
        Some(CombinedCommand::Config(config_args)) => config::run(config_args),
        Some(CombinedCommand::Completions(completions_args)) => {
            completions::run::<CombinedArgs>(completions_args, "arkama")
        }
        None => run_gui().await,
    }
}

pub async fn run_cli() -> Result<()> {
    let args = CliArgs::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    match args.command {
        CliCommand::Download(download_args) => download::run(download_args).await,
        CliCommand::History(history_args) => history::run(history_args),
        CliCommand::Config(config_args) => config::run(config_args),
        CliCommand::Completions(completions_args) => {
            completions::run::<CliArgs>(completions_args, "arkama-cli")
        }
    }
}

pub async fn run_gui_app() -> Result<()> {
    let args = GuiArgs::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    run_gui().await
}

async fn run_gui() -> Result<()> {
    task::spawn_blocking(gui::run)
        .await
        .map_err(|err| eyre::eyre!(err))?
}
