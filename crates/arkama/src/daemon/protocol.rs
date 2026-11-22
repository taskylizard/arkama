//! The existing local daemon's line-delimited JSON request and response types.

use arkama_core::DownloadRequest;
use arkama_data::DownloadRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum DaemonRequest {
    Ping,
    Shutdown,
    Enqueue(DownloadRequest),
    List { limit: usize, query: Option<String> },
    Show { id: i64 },
    Pause { id: i64 },
    Resume { id: i64 },
    Cancel { id: i64 },
    Retry { id: i64 },
    Manage { command: arkama_manager::Command },
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum DaemonResponse {
    Pong(DaemonStatus),
    Enqueued { id: i64 },
    Jobs(Vec<DownloadRecord>),
    Job(DownloadRecord),
    Ok { message: String },
    ShuttingDown,
    Managed(arkama_manager::Response),
    Error { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct DaemonStatus {
    pub pid: u32,
    pub active: usize,
    pub queued: usize,
}
