//! Client requests to the existing local daemon.

use arkama_core::DownloadRequest;
use arkama_data::DownloadRecord;
use eyre::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use super::protocol::{DaemonRequest, DaemonResponse, DaemonStatus};
use super::read_daemon_addr;

pub(crate) async fn enqueue_download(request: DownloadRequest) -> Result<i64> {
    let response = send_request(DaemonRequest::Enqueue(request.into())).await?;
    match response {
        DaemonResponse::Enqueued { id } if id >= 0 => Ok(id),
        DaemonResponse::Enqueued { .. } => Err(eyre::eyre!("daemon is stopping")),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

pub(crate) async fn list_jobs(limit: usize, query: Option<String>) -> Result<Vec<DownloadRecord>> {
    match send_request(DaemonRequest::List { limit, query }).await? {
        DaemonResponse::Jobs(jobs) => Ok(jobs),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

pub(crate) async fn show_job(id: i64) -> Result<DownloadRecord> {
    match send_request(DaemonRequest::Show { id }).await? {
        DaemonResponse::Job(job) => Ok(job),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

pub(crate) async fn pause_job(id: i64) -> Result<String> {
    job_command(DaemonRequest::Pause { id }).await
}

pub(crate) async fn resume_job(id: i64) -> Result<String> {
    job_command(DaemonRequest::Resume { id }).await
}

pub(crate) async fn cancel_job(id: i64) -> Result<String> {
    job_command(DaemonRequest::Cancel { id }).await
}

pub(crate) async fn retry_job(id: i64) -> Result<String> {
    job_command(DaemonRequest::Retry { id }).await
}

async fn job_command(request: DaemonRequest) -> Result<String> {
    match send_request(request).await? {
        DaemonResponse::Ok { message } => Ok(message),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

pub(super) async fn status() -> Result<DaemonStatus> {
    let response = send_request(DaemonRequest::Ping).await?;
    match response {
        DaemonResponse::Pong(status) => Ok(status),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        DaemonResponse::ShuttingDown => Err(eyre::eyre!("daemon is shutting down")),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

pub(super) async fn send_request(request: DaemonRequest) -> Result<DaemonResponse> {
    let daemon_addr = read_daemon_addr()?;
    let stream = TcpStream::connect(daemon_addr)
        .await
        .with_context(|| format!("failed to connect to daemon at {daemon_addr}"))?;
    let (read_half, mut write_half) = stream.into_split();
    let request = serde_json::to_vec(&request).context("failed to serialize daemon request")?;
    write_half
        .write_all(&request)
        .await
        .context("failed to write daemon request")?;
    write_half
        .write_all(b"\n")
        .await
        .context("failed to finish daemon request")?;
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .context("failed to read daemon response")?;
    serde_json::from_str(&line).context("invalid daemon response")
}
