use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DownloadRecord {
    pub id: i64,
    pub url: String,
    pub output_path: String,
    pub status: String,
    pub error_message: Option<String>,
    pub queue_name: String,
    pub priority: i64,
    pub total_bytes: Option<i64>,
    pub downloaded_bytes: i64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}
