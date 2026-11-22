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
    Submit {
        submission: crate::Submission,
    },
    Batch {
        submissions: Vec<crate::Submission>,
    },
    Queues,
    PutQueue {
        queue: crate::Queue,
    },
    DeleteQueue {
        name: String,
    },
    MoveJob {
        id: i64,
        queue: String,
        priority: i64,
        position: i64,
    },
    Rules,
    SetRules {
        rules: Vec<crate::FolderRule>,
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
    ForgetRequest {
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
            Self::Submit { submission } => submission.size_valid(),
            Self::Batch { submissions } => {
                !submissions.is_empty()
                    && submissions.len() <= 32
                    && submissions.iter().all(crate::Submission::size_valid)
            }
            Self::PutQueue { queue } => {
                queue.name.len() <= 64
                    && queue.window.as_ref().is_none_or(|window| {
                        window.timezone.len() <= 128 && window.weekdays.len() <= 7
                    })
            }
            Self::DeleteQueue { name } | Self::MoveJob { queue: name, .. } => name.len() <= 64,
            Self::SetRules { rules } => {
                rules.len() <= 64
                    && rules.iter().all(|rule| {
                        rule.category.as_ref().is_none_or(|c| c.len() <= 64)
                            && rule
                                .source_host
                                .as_ref()
                                .is_none_or(|host| host.len() <= 253)
                            && rule.directory.as_os_str().len() <= 4096
                    })
            }
            Self::Enqueue { request } => request_size_valid(request),
            Self::List { query, .. } => query.len() <= 1024,
            Self::GetSetting { key } => key.len() <= 128,
            Self::SetSetting { key, value } => key.len() <= 128 && value.len() <= 4096,
            _ => true,
        }
    }
}

pub(crate) fn request_size_valid(request: &DownloadRequest) -> bool {
    request.url.len() <= 8192
        && request.context.headers.len() <= 16
        && request
            .context
            .headers
            .iter()
            .all(|(k, v)| k.len() <= 128 && v.len() <= 8192)
        && request
            .context
            .cookies
            .as_ref()
            .is_none_or(|v| v.len() <= 4096)
        && request
            .context
            .referrer
            .as_ref()
            .is_none_or(|v| v.len() <= 8192)
        && request
            .context
            .source
            .as_ref()
            .is_none_or(|v| v.len() <= 8192)
        && request
            .context
            .filename
            .as_ref()
            .is_none_or(|v| v.len() <= 240)
        && request.context.method.len() <= 16
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
    Queues {
        queues: Vec<crate::Queue>,
    },
    Rules {
        rules: Vec<crate::FolderRule>,
    },
    Batch {
        results: Vec<crate::BatchResult>,
    },
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
