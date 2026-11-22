use arkama_core::{DownloadRequest, ObservationSnapshot};
use arkama_data::DownloadRecord;
use serde::{Deserialize, Serialize};

/// Shared local/server operations. Page sizes are 1–100, offsets 0–1,000,000.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Status,
    Enqueue {
        request: DownloadRequest,
    },
    List {
        limit: usize,
        offset: usize,
        query: String,
    },
    Show {
        id: i64,
    },
    Diagnostics {
        id: i64,
    },
    Pause {
        id: i64,
    },
    Resume {
        id: i64,
    },
    Cancel {
        id: i64,
    },
    Retry {
        id: i64,
    },
    GetSetting {
        key: String,
    },
    SetSetting {
        key: String,
        value: String,
    },
}

impl Command {
    pub(crate) fn size_valid(&self) -> bool {
        match self {
            Self::Enqueue { request } => {
                request.url.len() <= 8192
                    && request
                        .output
                        .as_ref()
                        .is_none_or(|path| path.as_os_str().len() <= 4096)
                    && request
                        .output_dir
                        .as_ref()
                        .is_none_or(|path| path.as_os_str().len() <= 4096)
                    && request
                        .user_agent
                        .as_ref()
                        .is_none_or(|value| value.len() <= 1024)
            }
            Self::List { query, .. } => query.len() <= 1024,
            Self::GetSetting { key } => key.len() <= 128,
            Self::SetSetting { key, value } => key.len() <= 128 && value.len() <= 4096,
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Status {
    pub pid: u32,
    pub active: usize,
    pub queued: usize,
    pub revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub record: DownloadRecord,
    pub observation: Option<ObservationSnapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Response {
    Status(Status),
    Accepted {
        id: i64,
    },
    Jobs {
        jobs: Vec<JobSnapshot>,
        offset: usize,
        has_more: bool,
        revision: u64,
    },
    Job(Box<JobSnapshot>),
    Diagnostics {
        id: i64,
        snapshot: Option<Box<arkama_core::DiagnosticsSnapshot>>,
    },
    Setting {
        key: String,
        value: Option<String>,
    },
    Ok,
}
