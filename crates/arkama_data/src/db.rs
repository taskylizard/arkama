use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use eyre::{Context, Result};
use rusqlite::{Connection, params};

use crate::model::DownloadRecord;
use crate::paths::db_path;

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn open() -> Result<Self> {
        let path = db_path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create data dir {parent:?}"))?;
        }
        let conn = Connection::open(path).context("failed to open sqlite db")?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                output_path TEXT NOT NULL,
                status TEXT NOT NULL,
                total_bytes INTEGER,
                downloaded_bytes INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS downloads_url_idx ON downloads(url);
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        Ok(())
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
            "INSERT INTO downloads (url, output_path, status, total_bytes, downloaded_bytes, started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
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
             SET output_path = ?1, status = 'running', total_bytes = ?2, downloaded_bytes = ?3, finished_at = NULL
             WHERE id = ?4",
            params![
                output_path.display().to_string(),
                total_bytes,
                downloaded_bytes as i64,
                id,
            ],
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
            "UPDATE downloads SET total_bytes = ?1, downloaded_bytes = ?2 WHERE id = ?3",
            params![total_bytes, downloaded_bytes as i64, id],
        )?;
        Ok(())
    }

    pub fn update_download_finished(
        &self,
        id: i64,
        summary: &arkama_core::DownloadSummary,
    ) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads SET status = ?1, total_bytes = ?2, downloaded_bytes = ?3, finished_at = ?4 WHERE id = ?5",
            params!["finished", summary.total_bytes as i64, summary.downloaded_bytes as i64, finished_at, id],
        )?;
        Ok(())
    }

    pub fn update_download_failed(&self, id: i64, message: &str) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads SET status = ?1, finished_at = ?2 WHERE id = ?3",
            params![format!("failed: {message}"), finished_at, id],
        )?;
        Ok(())
    }

    pub fn update_download_paused(&self, id: i64, downloaded_bytes: u64) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused', downloaded_bytes = ?1 WHERE id = ?2",
            params![downloaded_bytes as i64, id],
        )?;
        Ok(())
    }

    pub fn update_download_cancelled(&self, id: i64) -> Result<()> {
        let finished_at = now_ts();
        self.conn.execute(
            "UPDATE downloads SET status = 'cancelled', finished_at = ?1 WHERE id = ?2",
            params![finished_at, id],
        )?;
        Ok(())
    }

    pub fn delete_download(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM downloads WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn normalize_running_to_paused(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused' WHERE status = 'running'",
            [],
        )?;
        Ok(())
    }

    pub fn normalize_queued_to_paused(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE downloads SET status = 'paused' WHERE status = 'queued'",
            [],
        )?;
        Ok(())
    }

    pub fn load_downloads(&self, query: &str) -> Result<Vec<DownloadRecord>> {
        let mut records = Vec::new();
        let like = format!("%{query}%");
        let mut stmt = if query.is_empty() {
            self.conn.prepare(
                "SELECT id, url, output_path, status, total_bytes, downloaded_bytes, started_at, finished_at
                 FROM downloads ORDER BY id DESC",
            )?
        } else {
            self.conn.prepare(
                "SELECT id, url, output_path, status, total_bytes, downloaded_bytes, started_at, finished_at
                 FROM downloads
                 WHERE url LIKE ?1 OR output_path LIKE ?1 OR status LIKE ?1
                 ORDER BY id DESC",
            )?
        };

        let mut rows = if query.is_empty() {
            stmt.query([])?
        } else {
            stmt.query(params![like])?
        };

        while let Some(row) = rows.next()? {
            let record = DownloadRecord {
                id: row.get(0)?,
                url: row.get(1)?,
                output_path: row.get(2)?,
                status: row.get(3)?,
                total_bytes: row.get(4)?,
                downloaded_bytes: row.get(5)?,
                started_at: row.get(6)?,
                finished_at: row.get(7)?,
            };
            records.push(record);
        }

        Ok(records)
    }

    pub fn load_downloads_limit(&self, limit: usize) -> Result<Vec<DownloadRecord>> {
        let mut records = Vec::new();
        let mut stmt = self.conn.prepare(
            "SELECT id, url, output_path, status, total_bytes, downloaded_bytes, started_at, finished_at
             FROM downloads ORDER BY id DESC LIMIT ?1",
        )?;
        let mut rows = stmt.query(params![limit as i64])?;

        while let Some(row) = rows.next()? {
            let record = DownloadRecord {
                id: row.get(0)?,
                url: row.get(1)?,
                output_path: row.get(2)?,
                status: row.get(3)?,
                total_bytes: row.get(4)?,
                downloaded_bytes: row.get(5)?,
                started_at: row.get(6)?,
                finished_at: row.get(7)?,
            };
            records.push(record);
        }

        Ok(records)
    }
}

fn now_ts() -> i64 {
    let now = SystemTime::now();
    let Ok(duration) = now.duration_since(UNIX_EPOCH) else {
        return 0;
    };
    duration.as_secs() as i64
}
