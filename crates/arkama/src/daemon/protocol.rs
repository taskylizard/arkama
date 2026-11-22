//! The existing local daemon's line-delimited JSON request and response types.

use std::path::PathBuf;

use arkama_core::DownloadRequest;
use arkama_data::DownloadRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum DaemonRequest {
    Ping,
    Shutdown,
    Enqueue(QueuedDownloadRequest),
    List { limit: usize, query: Option<String> },
    Show { id: i64 },
    Pause { id: i64 },
    Resume { id: i64 },
    Cancel { id: i64 },
    Retry { id: i64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum DaemonResponse {
    Pong(DaemonStatus),
    Enqueued { id: i64 },
    Jobs(Vec<DownloadRecord>),
    Job(DownloadRecord),
    Ok { message: String },
    ShuttingDown,
    Error { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct QueuedDownloadRequest {
    url: String,
    output: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    connections: usize,
    user_agent: Option<String>,
    limit: Option<u64>,
    experimental_entropy: bool,
    #[serde(default)]
    recovery: arkama_core::RecoveryPolicy,
}

impl From<DownloadRequest> for QueuedDownloadRequest {
    fn from(request: DownloadRequest) -> Self {
        let DownloadRequest {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
            recovery,
        } = request;
        Self {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
            recovery,
        }
    }
}

impl From<QueuedDownloadRequest> for DownloadRequest {
    fn from(request: QueuedDownloadRequest) -> Self {
        let QueuedDownloadRequest {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
            recovery,
        } = request;
        Self {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
            recovery,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct DaemonStatus {
    pub pid: u32,
    pub active: usize,
    pub queued: usize,
}
