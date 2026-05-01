mod cli;
mod completions;
mod config;
mod daemon;
mod download;
mod history;
mod progress;
mod util;

use clap::{Args, Parser, Subcommand};
use eyre::Result;

#[derive(Parser)]
#[command(
    name = "arkama",
    version,
    about = "High-performance download manager",
    arg_required_else_help = true
)]
struct CombinedArgs {
    #[command(subcommand)]
    command: CombinedCommand,

    #[arg(long = "reset-db", help = "Reset the application database.")]
    reset_db: bool,
}

#[derive(Subcommand)]
enum CombinedCommand {
    #[command(about = "Download a file from a URL.")]
    Download(cli::DownloadArgs),

    #[command(about = "Run or manage the background daemon.")]
    Daemon(DaemonCliArgs),

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

    #[command(about = "Run or manage the background daemon.")]
    Daemon(DaemonCliArgs),

    #[command(about = "Show download history.")]
    History(history::HistoryArgs),

    #[command(about = "View or modify configuration.")]
    Config(config::ConfigArgs),

    #[command(about = "Generate shell completions.")]
    Completions(completions::CompletionsArgs),
}

#[derive(Args, Debug)]
struct DaemonCliArgs {
    #[command(subcommand)]
    command: daemon::DaemonCommand,
}

pub async fn run_combined() -> Result<()> {
    let args = CombinedArgs::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    match args.command {
        CombinedCommand::Download(download_args) => download::run(download_args).await,
        CombinedCommand::Daemon(daemon_args) => daemon::run(daemon_args.command).await,
        CombinedCommand::History(history_args) => history::run(history_args),
        CombinedCommand::Config(config_args) => config::run(config_args),
        CombinedCommand::Completions(completions_args) => {
            completions::run::<CombinedArgs>(completions_args, "arkama")
        }
    }
}

pub async fn run_cli() -> Result<()> {
    let args = CliArgs::parse();

    if args.reset_db {
        arkama_data::reset_db()?;
    }

    match args.command {
        CliCommand::Download(download_args) => download::run(download_args).await,
        CliCommand::Daemon(daemon_args) => daemon::run(daemon_args.command).await,
        CliCommand::History(history_args) => history::run(history_args),
        CliCommand::Config(config_args) => config::run(config_args),
        CliCommand::Completions(completions_args) => {
            completions::run::<CliArgs>(completions_args, "arkama-cli")
        }
    }
}
