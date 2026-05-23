use arkama_data::DownloadRecord;
use clap::Args;
use eyre::Result;

use crate::daemon;
use crate::history;
use crate::util::history_progress_text;

#[derive(Args, Debug)]
pub struct JobListArgs {
    #[arg(
        short = 'n',
        long = "limit",
        default_value_t = 20,
        help = "Number of jobs to show."
    )]
    pub limit: usize,

    #[arg(
        short = 'q',
        long = "query",
        help = "Filter by URL, path, status, or error."
    )]
    pub query: Option<String>,
}

#[derive(Args, Debug)]
pub struct JobIdArgs {
    #[arg(help = "Job ID.")]
    pub id: i64,
}

pub async fn list(args: JobListArgs) -> Result<()> {
    let jobs = daemon::list_jobs(args.limit, args.query).await?;
    if jobs.is_empty() {
        println!("no jobs found");
        return Ok(());
    }
    println!("{}", history::render_table(&jobs));
    Ok(())
}

pub async fn show(args: JobIdArgs) -> Result<()> {
    let job = daemon::show_job(args.id).await?;
    print_job(&job);
    Ok(())
}

pub async fn pause(args: JobIdArgs) -> Result<()> {
    println!("{}", daemon::pause_job(args.id).await?);
    Ok(())
}

pub async fn resume(args: JobIdArgs) -> Result<()> {
    println!("{}", daemon::resume_job(args.id).await?);
    Ok(())
}

pub async fn cancel(args: JobIdArgs) -> Result<()> {
    println!("{}", daemon::cancel_job(args.id).await?);
    Ok(())
}

pub async fn retry(args: JobIdArgs) -> Result<()> {
    println!("{}", daemon::retry_job(args.id).await?);
    Ok(())
}

fn print_job(job: &DownloadRecord) {
    println!("id = {}", job.id);
    println!("status = {}", job.status);
    if let Some(message) = &job.error_message
        && !message.is_empty()
    {
        println!("error = {message}");
    }
    println!("queue = {}", job.queue_name);
    println!("priority = {}", job.priority);
    println!(
        "progress = {}",
        history_progress_text(job.downloaded_bytes, job.total_bytes)
    );
    println!("downloaded_bytes = {}", job.downloaded_bytes);
    match job.total_bytes {
        Some(total_bytes) => println!("total_bytes = {total_bytes}"),
        None => println!("total_bytes = (unknown)"),
    }
    println!("output = {}", empty_as_unset(&job.output_path));
    println!("url = {}", job.url);
    println!("created_at = {}", job.created_at);
    println!("updated_at = {}", job.updated_at);
    println!("started_at = {}", job.started_at);
    match job.finished_at {
        Some(finished_at) => println!("finished_at = {finished_at}"),
        None => println!("finished_at = (not finished)"),
    }
}

fn empty_as_unset(value: &str) -> &str {
    if value.is_empty() {
        "(not resolved yet)"
    } else {
        value
    }
}
