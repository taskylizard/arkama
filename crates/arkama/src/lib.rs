mod cli;
mod completions;
mod config;
mod daemon;
mod download;
mod history;
mod job;
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
    #[command(about = "Add a download job to the daemon.")]
    Add(cli::DownloadArgs),

    #[command(about = "Download a file from a URL.")]
    Download(cli::DownloadArgs),

    #[command(about = "List daemon jobs.")]
    List(job::JobListArgs),

    #[command(about = "Show a daemon job.")]
    Show(job::JobIdArgs),

    #[command(about = "Pause a daemon job.")]
    Pause(job::JobIdArgs),

    #[command(about = "Resume a daemon job.")]
    Resume(job::JobIdArgs),

    #[command(about = "Cancel a daemon job.")]
    Cancel(job::JobIdArgs),

    #[command(about = "Retry a failed or cancelled daemon job.")]
    Retry(job::JobIdArgs),

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
    #[command(about = "Add a download job to the daemon.")]
    Add(cli::DownloadArgs),

    #[command(about = "Download a file from a URL.")]
    Download(cli::DownloadArgs),

    #[command(about = "List daemon jobs.")]
    List(job::JobListArgs),

    #[command(about = "Show a daemon job.")]
    Show(job::JobIdArgs),

    #[command(about = "Pause a daemon job.")]
    Pause(job::JobIdArgs),

    #[command(about = "Resume a daemon job.")]
    Resume(job::JobIdArgs),

    #[command(about = "Cancel a daemon job.")]
    Cancel(job::JobIdArgs),

    #[command(about = "Retry a failed or cancelled daemon job.")]
    Retry(job::JobIdArgs),

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
        CombinedCommand::Add(mut download_args) => {
            download_args.daemon = true;
            download::run(download_args).await
        }
        CombinedCommand::Download(download_args) => download::run(download_args).await,
        CombinedCommand::List(job_args) => job::list(job_args).await,
        CombinedCommand::Show(job_args) => job::show(job_args).await,
        CombinedCommand::Pause(job_args) => job::pause(job_args).await,
        CombinedCommand::Resume(job_args) => job::resume(job_args).await,
        CombinedCommand::Cancel(job_args) => job::cancel(job_args).await,
        CombinedCommand::Retry(job_args) => job::retry(job_args).await,
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
        CliCommand::Add(mut download_args) => {
            download_args.daemon = true;
            download::run(download_args).await
        }
        CliCommand::Download(download_args) => download::run(download_args).await,
        CliCommand::List(job_args) => job::list(job_args).await,
        CliCommand::Show(job_args) => job::show(job_args).await,
        CliCommand::Pause(job_args) => job::pause(job_args).await,
        CliCommand::Resume(job_args) => job::resume(job_args).await,
        CliCommand::Cancel(job_args) => job::cancel(job_args).await,
        CliCommand::Retry(job_args) => job::retry(job_args).await,
        CliCommand::Daemon(daemon_args) => daemon::run(daemon_args.command).await,
        CliCommand::History(history_args) => history::run(history_args),
        CliCommand::Config(config_args) => config::run(config_args),
        CliCommand::Completions(completions_args) => {
            completions::run::<CliArgs>(completions_args, "arkama-cli")
        }
    }
}
