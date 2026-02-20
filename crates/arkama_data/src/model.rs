#[derive(Clone)]
pub struct DownloadRecord {
    pub id: i64,
    pub url: String,
    pub output_path: String,
    pub status: String,
    pub total_bytes: Option<i64>,
    pub downloaded_bytes: i64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}
