//! SQLite access for settings, download records, and the existing daemon queue.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use arkama_core::{DownloadRequest, DownloadSummary};
use eyre::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};

use crate::model::DownloadRecord;
use crate::paths::db_path;

mod schema;

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open() -> Result<Self> {
        let path = db_path()?;
        Self::open_path(&path)
    }

    /// Opens an explicit store. Synchronous; call away from transfer and UI threads.
    pub fn open_path(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create data dir {parent:?}"))?;
        }
        let conn = Connection::open(path).context("failed to open sqlite db")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM settings WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let value: String = row.get(0)?;
        Ok(Some(value))
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn insert_download(&self, url: &str, output_path: &Path) -> Result<i64> {
        self.insert_download_with_status(url, output_path, "running")
    }

    pub fn insert_download_with_status(
        &self,
        url: &str,
        output_path: &Path,
        status: &str,
    ) -> Result<i64> {
        let now = now_ts();
        self.conn.execute(
            "INSERT INTO downloads (
                url, output_path, status, total_bytes, downloaded_bytes, started_at,
                queue_name, priority, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'default', 0, ?6, ?6)",
            params![
                url,
                output_path.display().to_string(),
                status,
                Option::<i64>::None,
                0i64,
                now,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn insert_daemon_download(&self, request: &DownloadRequest) -> Result<i64> {
        let now = now_ts();
        let output_path = queued_output_path(request);
        let url = request.url.clone();
        let request =
            serde_json::to_string(request).context("failed to serialize daemon request")?;
        self.conn.execute(
            "INSERT INTO downloads (
                url,
                output_path,
                status,
                total_bytes,
                downloaded_bytes,
                started_at,
                queue_name,
                priority,
                created_at,
                updated_at,
                daemon_request,
                state_since_ms,
                output_key
             ) VALUES (?1, ?2, 'queued', ?3, ?4, ?5, 'default', 0, ?5, ?5, ?6, ?7, ?8)",
            params![
                url,
                output_path.display().to_string(),
                Option::<i64>::None,
                0i64,
                now,
                request,
                now_ms(),
                output_key(output_path),
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn update_daemon_request(&self, id: i64, request: &DownloadRequest) -> Result<()> {
        let output = queued_output_path(request);
        let request =
            serde_json::to_string(request).context("failed to serialize daemon request")?;
        self.conn.execute(
            "UPDATE downloads SET daemon_request = ?1, updated_at = ?2, output_path = ?4, output_key = ?5 WHERE id = ?3",
            params![request, now_ts(), id, output.to_string_lossy(), output_key(output)],
        )?;
        Ok(())
    }

    pub fn update_download_started(
        &self,
        id: i64,
        output_path: &Path,
        total_bytes: Option<u64>,
        downloaded_bytes: u64,
    ) -> Result<()> {
        let total_bytes = total_bytes.map(|value| value as i64);
        self.conn.execute(
            "UPDATE downloads
             SET output_path = ?1, status = 'running', error_message = NULL,
                 total_bytes = ?2, downloaded_bytes = ?3, finished_at = NULL, updated_at = ?4,
                 output_key = ?6
             WHERE id = ?5",
            params![
                output_path.display().to_string(),
                total_bytes,
                downloaded_bytes as i64,
                now_ts(),
                id,
                output_key(output_path),
            ],
        )?;
        Ok(())
    }

    pub fn update_download_metadata(
        &self,
        id: i64,
        etag: Option<&str>,
        last_modified: Option<&str>,
        mime_type: Option<&str>,
        final_url: &str,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads
             SET etag = ?1, last_modified = ?2, mime_type = ?3, final_url = ?4, updated_at = ?5
             WHERE id = ?6",
            params![etag, last_modified, mime_type, final_url, now_ts(), id],
        )?;
        Ok(())
    }

    pub fn update_download_progress(
        &self,
        id: i64,
        total_bytes: Option<u64>,
        downloaded_bytes: u64,
    ) -> Result<()> {
        let total_bytes = total_bytes.map(|value| value as i64);
        self.conn.execute(
            "UPDATE downloads SET total_bytes = ?1, downloaded_bytes = ?2, updated_at = ?3 WHERE id = ?4",
            params![total_bytes, downloaded_bytes as i64, now_ts(), id],
        )?;
        Ok(())
    }

    pub fn update_download_finished(&self, id: i64, summary: &DownloadSummary) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads
             SET status = ?1, error_message = NULL, total_bytes = ?2, downloaded_bytes = ?3,
                 finished_at = ?4, updated_at = ?4
             WHERE id = ?5",
            params![
                "finished",
                summary.total_bytes as i64,
                summary.downloaded_bytes as i64,
                finished_at,
                id
            ],
        )?;
        Ok(())
    }

    pub fn update_download_failed(&self, id: i64, message: &str) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads SET status = 'failed', error_message = ?1, finished_at = ?2, updated_at = ?2 WHERE id = ?3",
            params![message, finished_at, id],
        )?;
        Ok(())
    }

    pub fn update_download_paused(&self, id: i64, downloaded_bytes: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused', downloaded_bytes = ?1, updated_at = ?2 WHERE id = ?3",
            params![downloaded_bytes as i64, now_ts(), id],
        )?;
        Ok(())
    }

    pub fn update_download_cancelled(&self, id: i64) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads SET status = 'cancelled', downloaded_bytes = 0, finished_at = ?1, updated_at = ?1 WHERE id = ?2",
            params![finished_at, id],
        )?;
        Ok(())
    }

    pub fn requeue_download(&self, id: i64, downloaded_bytes: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads
             SET status = 'queued', error_message = NULL, downloaded_bytes = ?1,
                 finished_at = NULL, updated_at = ?2
             WHERE id = ?3",
            params![downloaded_bytes as i64, now_ts(), id],
        )?;
        Ok(())
    }

    pub fn load_download(&self, id: i64) -> Result<Option<DownloadRecord>> {
        let sql = DOWNLOAD_RECORD_SELECT.to_string() + " WHERE id = ?1";
        self.conn
            .query_row(&sql, params![id], map_download_record)
            .optional()
            .context("failed to load download")
    }

    pub fn download_status(&self, id: i64) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT status FROM downloads WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .context("failed to load download status")
    }

    pub fn mark_queued_paused(&self, id: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE downloads SET status = 'paused', updated_at = ?1 WHERE id = ?2 AND status = 'queued'",
            params![now_ts(), id],
        )?;
        Ok(changed > 0)
    }

    pub fn resume_download(&self, id: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE downloads
             SET status = 'queued', error_message = NULL, finished_at = NULL, updated_at = ?1
             WHERE id = ?2 AND status = 'paused' AND daemon_request IS NOT NULL",
            params![now_ts(), id],
        )?;
        Ok(changed > 0)
    }

    pub fn retry_download(&self, id: i64) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE downloads
             SET status = 'queued', error_message = NULL, finished_at = NULL, updated_at = ?1
             WHERE id = ?2 AND status IN ('failed', 'cancelled') AND daemon_request IS NOT NULL",
            params![now_ts(), id],
        )?;
        Ok(changed > 0)
    }

    pub fn delete_download(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM downloads WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn normalize_running_to_paused(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused', updated_at = ?1 WHERE status = 'running' AND daemon_request IS NULL",
            params![now_ts()],
        )?;
        Ok(())
    }

    pub fn normalize_queued_to_paused(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused', updated_at = ?1 WHERE status = 'queued' AND daemon_request IS NULL",
            params![now_ts()],
        )?;
        Ok(())
    }

    pub fn recover_daemon_downloads(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads
             SET status = CASE WHEN status IN ('pausing', 'cancelling') THEN 'paused' ELSE 'queued' END,
                 finished_at = NULL, updated_at = ?1, state_since_ms = ?2
             WHERE daemon_request IS NOT NULL AND status IN ('starting', 'running', 'pausing', 'cancelling')",
            params![now_ts(), now_ms()],
        )?;
        Ok(())
    }

    pub fn claim_next_daemon_download(&self) -> Result<Option<(i64, DownloadRequest)>> {
        loop {
            let mut stmt = self.conn.prepare(
                "SELECT id, daemon_request
                 FROM downloads
                 WHERE daemon_request IS NOT NULL AND status = 'queued'
                 ORDER BY id ASC
                 LIMIT 1",
            )?;
            let mut rows = stmt.query([])?;
            let Some(row) = rows.next()? else {
                return Ok(None);
            };
            let id: i64 = row.get(0)?;
            let request: String = row.get(1)?;

            let claimed = self.conn.execute(
                "UPDATE downloads SET status = 'starting', finished_at = NULL, updated_at = ?1,
                    queue_wait_ms = queue_wait_ms + MAX(0, ?3 - state_since_ms), state_since_ms = ?3,
                    attempt_started = 1
                 WHERE id = ?2 AND status = 'queued'",
                params![now_ts(), id, now_ms()],
            )?;
            if claimed == 0 {
                continue;
            }

            let request = serde_json::from_str(&request).with_context(|| {
                format!("failed to deserialize daemon request for download {id}")
            })?;
            return Ok(Some((id, request)));
        }
    }

    pub fn count_queued_daemon_downloads(&self) -> Result<usize> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM downloads WHERE daemon_request IS NOT NULL AND status = 'queued'",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn load_downloads(&self, query: &str) -> Result<Vec<DownloadRecord>> {
        let mut records = Vec::new();
        let like = format!("%{query}%");
        let mut stmt = if query.is_empty() {
            self.conn
                .prepare(&(DOWNLOAD_RECORD_SELECT.to_string() + " ORDER BY id DESC"))?
        } else {
            self.conn.prepare(
                &(DOWNLOAD_RECORD_SELECT.to_string()
                    + " WHERE url LIKE ?1 OR output_path LIKE ?1 OR status LIKE ?1 OR error_message LIKE ?1 ORDER BY id DESC"),
            )?
        };

        let mut rows = if query.is_empty() {
            stmt.query([])?
        } else {
            stmt.query(params![like])?
        };

        while let Some(row) = rows.next()? {
            let record = map_download_record(row)?;
            records.push(record);
        }

        Ok(records)
    }

    pub fn load_downloads_limit(&self, limit: usize) -> Result<Vec<DownloadRecord>> {
        let mut records = Vec::new();
        let mut stmt = self
            .conn
            .prepare(&(DOWNLOAD_RECORD_SELECT.to_string() + " ORDER BY id DESC LIMIT ?1"))?;
        let mut rows = stmt.query(params![limit as i64])?;

        while let Some(row) = rows.next()? {
            let record = map_download_record(row)?;
            records.push(record);
        }

        Ok(records)
    }

    /// Paged history and search. Callers validate page sizes and offsets.
    pub fn load_downloads_page(
        &self,
        limit: usize,
        offset: usize,
        query: &str,
    ) -> Result<Vec<DownloadRecord>> {
        let sql = DOWNLOAD_RECORD_SELECT.to_string()
            + " WHERE url LIKE ?1 OR output_path LIKE ?1 OR status LIKE ?1 OR error_message LIKE ?1 ORDER BY id DESC LIMIT ?2 OFFSET ?3";
        let mut statement = self.conn.prepare(&sql)?;
        let records = statement.query_map(
            params![format!("%{query}%"), limit as i64, offset as i64],
            map_download_record,
        )?;
        records
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Accounts for queued/paused time and changes status atomically for manager-owned jobs.
    pub fn transition_download(&self, id: i64, status: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET
                queue_wait_ms = queue_wait_ms + CASE WHEN status = 'queued' THEN MAX(0, ?1 - state_since_ms) ELSE 0 END,
                paused_ms = paused_ms + CASE WHEN status = 'paused' THEN MAX(0, ?1 - state_since_ms) ELSE 0 END,
                state_since_ms = ?1, status = ?2, updated_at = ?3
             WHERE id = ?4 AND status != ?2",
            params![now_ms(), status, now_ts(), id],
        )?;
        Ok(())
    }

    pub fn load_daemon_request(&self, id: i64) -> Result<Option<DownloadRequest>> {
        let serialized: Option<String> = self
            .conn
            .query_row(
                "SELECT daemon_request FROM downloads WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        serialized
            .map(|request| serde_json::from_str(&request).map_err(Into::into))
            .transpose()
    }

    pub fn output_owner(&self, output: &Path, except_id: i64) -> Result<Option<i64>> {
        self.conn.query_row(
            "SELECT id FROM downloads WHERE daemon_request IS NOT NULL AND output_key = ?1 AND id != ?2 AND status != 'cancelled' LIMIT 1",
            params![output_key(output), except_id], |row| row.get(0),
        ).optional().map_err(Into::into)
    }

    /// Refreshes filesystem identities during manager startup, including legacy or moved stores.
    pub fn refresh_output_keys(&self) -> Result<()> {
        let mut statement = self
            .conn
            .prepare("SELECT id, output_path FROM downloads WHERE daemon_request IS NOT NULL")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let output: String = row.get(1)?;
            self.conn.execute(
                "UPDATE downloads SET output_key = ?1 WHERE id = ?2",
                params![output_key(Path::new(&output)), id],
            )?;
        }
        Ok(())
    }

    /// Whether a managed attempt could have created partial artifacts, independent of diagnostics.
    pub fn attempt_started(&self, id: i64) -> Result<bool> {
        self.conn
            .query_row(
                "SELECT attempt_started FROM downloads WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn save_observation(
        &self,
        id: i64,
        observation: &arkama_core::ObservationSnapshot,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET observation_json = ?1 WHERE id = ?2",
            params![serde_json::to_string(observation)?, id],
        )?;
        Ok(())
    }

    pub fn load_observation(&self, id: i64) -> Result<Option<arkama_core::ObservationSnapshot>> {
        let serialized: Option<String> = self
            .conn
            .query_row(
                "SELECT observation_json FROM downloads WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        Ok(serialized.and_then(|snapshot| serde_json::from_str(&snapshot).ok()))
    }
}

const DOWNLOAD_RECORD_SELECT: &str = "SELECT id, url, output_path, status, error_message, queue_name, priority, total_bytes, downloaded_bytes, started_at, finished_at, created_at, updated_at, queue_wait_ms, paused_ms, state_since_ms FROM downloads";

fn map_download_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<DownloadRecord> {
    Ok(DownloadRecord {
        id: row.get(0)?,
        url: row.get(1)?,
        output_path: row.get(2)?,
        status: row.get(3)?,
        error_message: row.get(4)?,
        queue_name: row.get(5)?,
        priority: row.get(6)?,
        total_bytes: row.get(7)?,
        downloaded_bytes: row.get(8)?,
        started_at: row.get(9)?,
        finished_at: row.get(10)?,
        created_at: row.get(11)?,
        updated_at: row.get(12)?,
        queue_wait_ms: row.get(13)?,
        paused_ms: row.get(14)?,
        state_since_ms: row.get(15)?,
    })
}

fn queued_output_path(request: &DownloadRequest) -> &Path {
    let Some(output) = request.output.as_deref() else {
        return Path::new("");
    };
    output
}

fn output_key(output: &Path) -> String {
    let mut lock_name = output.file_name().unwrap_or_default().to_os_string();
    lock_name.push(".arkama.lock");
    let lock = output.with_file_name(lock_name);
    #[cfg(unix)]
    if let Ok(metadata) = std::fs::metadata(&lock) {
        use std::os::unix::fs::MetadataExt;
        return format!("inode:{}:{}", metadata.dev(), metadata.ino());
    }
    lock.canonicalize()
        .unwrap_or(lock)
        .to_string_lossy()
        .into_owned()
}

fn now_ts() -> i64 {
    let now = SystemTime::now();
    let Ok(duration) = now.duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_secs() as i64
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::Db;
    use arkama_core::DownloadRequest;
    use rusqlite::params;
    use tempfile::TempDir;

    fn open_test_db() -> (TempDir, Db) {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("arkama.sqlite");
        let db = Db::open_path(&path).unwrap();
        (temp_dir, db)
    }

    fn request(url: &str) -> DownloadRequest {
        DownloadRequest {
            url: url.to_string(),
            output: None,
            output_dir: Some(PathBuf::from("/tmp")),
            connections: 4,
            user_agent: None,
            limit: None,
            experimental_entropy: false,
            recovery: Default::default(),
        }
    }

    fn status_for(db: &Db, id: i64) -> String {
        let records = db.load_downloads("").unwrap();
        for record in records {
            if record.id == id {
                return record.status;
            }
        }
        panic!("missing record {id}");
    }

    #[test]
    fn test_claim_next_daemon_download_returns_oldest_queued_job() {
        let (_temp_dir, db) = open_test_db();
        let first_id = db
            .insert_daemon_download(&request("https://example.com/first"))
            .unwrap();
        let second_id = db
            .insert_daemon_download(&request("https://example.com/second"))
            .unwrap();

        let claimed = db.claim_next_daemon_download().unwrap();
        let Some((claimed_id, claimed_request)) = claimed else {
            panic!("expected queued daemon download");
        };

        assert_eq!(claimed_id, first_id);
        assert_eq!(claimed_request.url, "https://example.com/first");
        assert_eq!(status_for(&db, first_id), "starting");
        assert_eq!(status_for(&db, second_id), "queued");
    }

    #[test]
    fn test_normalize_running_to_paused_skips_daemon_downloads() {
        let (_temp_dir, db) = open_test_db();
        let local_id = db
            .insert_download("https://example.com/local", Path::new(""))
            .unwrap();
        let daemon_id = db
            .insert_daemon_download(&request("https://example.com/daemon"))
            .unwrap();
        let claimed = db.claim_next_daemon_download().unwrap();
        let Some((claimed_id, daemon_request)) = claimed else {
            panic!("expected daemon download");
        };
        assert_eq!(claimed_id, daemon_id);
        db.update_daemon_request(daemon_id, &daemon_request)
            .unwrap();
        db.update_download_started(daemon_id, Path::new("/tmp/file.bin"), Some(100), 20)
            .unwrap();

        db.normalize_running_to_paused().unwrap();

        assert_eq!(status_for(&db, local_id), "paused");
        assert_eq!(status_for(&db, daemon_id), "running");
    }

    #[test]
    fn test_recover_daemon_downloads_requeues_in_progress_jobs() {
        let (_temp_dir, db) = open_test_db();
        let queued_id = db
            .insert_daemon_download(&request("https://example.com/queued"))
            .unwrap();
        let running_id = db
            .insert_daemon_download(&request("https://example.com/running"))
            .unwrap();
        let starting_id = db
            .insert_daemon_download(&request("https://example.com/starting"))
            .unwrap();
        let paused_id = db
            .insert_daemon_download(&request("https://example.com/paused"))
            .unwrap();

        let claimed = db.claim_next_daemon_download().unwrap();
        let Some((claimed_id, _queued_request)) = claimed else {
            panic!("expected running daemon download");
        };
        assert_eq!(claimed_id, queued_id);

        let claimed = db.claim_next_daemon_download().unwrap();
        let Some((claimed_id, running_request)) = claimed else {
            panic!("expected running daemon download");
        };
        assert_eq!(claimed_id, running_id);
        db.update_daemon_request(running_id, &running_request)
            .unwrap();
        db.update_download_started(running_id, Path::new("/tmp/running.bin"), Some(100), 20)
            .unwrap();

        let claimed = db.claim_next_daemon_download().unwrap();
        let Some((claimed_id, _starting_request)) = claimed else {
            panic!("expected starting daemon download");
        };
        assert_eq!(claimed_id, starting_id);

        db.conn
            .execute(
                "UPDATE downloads SET status = 'paused' WHERE id = ?1",
                params![paused_id],
            )
            .unwrap();

        db.recover_daemon_downloads().unwrap();

        assert_eq!(status_for(&db, queued_id), "queued");
        assert_eq!(status_for(&db, running_id), "queued");
        assert_eq!(status_for(&db, starting_id), "queued");
        assert_eq!(status_for(&db, paused_id), "paused");
    }
}
