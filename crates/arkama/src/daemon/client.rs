//! Client requests to the existing local daemon.

use arkama_core::DownloadRequest;
use arkama_data::DownloadRecord;
use eyre::{Context, Result};

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

pub(crate) async fn manager_command(
    command: arkama_manager::Command,
) -> Result<arkama_manager::Response> {
    match send_request(DaemonRequest::Manage { command }).await? {
        DaemonResponse::Managed(response) => Ok(response),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected manager response")),
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
    tokio::time::timeout(std::time::Duration::from_secs(30), send_inner(request))
        .await
        .context("daemon request timed out; fetch the job state before retrying")?
}

async fn send_inner(request: DaemonRequest) -> Result<DaemonResponse> {
    let daemon_addr = read_daemon_addr()?;
    if !daemon_addr.ip().is_loopback() {
        return Err(eyre::eyre!(
            "local CLI discovery must refer to a loopback HTTP API"
        ));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut request = client
        .post(format!("http://{daemon_addr}/v1/legacy"))
        .json(&request);
    if let Ok(key) = super::auth::read_key() {
        request = request.bearer_auth(key);
    }
    let mut response = request.send().await.context("daemon HTTP request failed")?;
    if !response.status().is_success() {
        return Err(eyre::eyre!(
            "daemon HTTP request refused ({})",
            response.status()
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err(eyre::eyre!("daemon reply exceeds limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).context("invalid daemon response")
}
