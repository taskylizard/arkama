//! Schema initialization and compatibility migrations, kept separate from record queries.

use eyre::Result;

use super::Db;

impl Db {
    pub(super) fn init(&self) -> Result<()> {
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
        self.migrate_downloads_table()?;
        Ok(())
    }

    fn migrate_downloads_table(&self) -> Result<()> {
        if !self.downloads_column_exists("daemon_request")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN daemon_request TEXT", [])?;
        }
        if !self.downloads_column_exists("error_message")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN error_message TEXT", [])?;
        }
        if !self.downloads_column_exists("queue_name")? {
            self.conn.execute(
                "ALTER TABLE downloads ADD COLUMN queue_name TEXT NOT NULL DEFAULT 'default'",
                [],
            )?;
        }
        if !self.downloads_column_exists("priority")? {
            self.conn.execute(
                "ALTER TABLE downloads ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !self.downloads_column_exists("created_at")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN created_at INTEGER", [])?;
            self.conn.execute(
                "UPDATE downloads SET created_at = started_at WHERE created_at IS NULL",
                [],
            )?;
        }
        if !self.downloads_column_exists("updated_at")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN updated_at INTEGER", [])?;
            self.conn.execute(
                "UPDATE downloads SET updated_at = COALESCE(finished_at, started_at) WHERE updated_at IS NULL",
                [],
            )?;
        }
        if !self.downloads_column_exists("etag")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN etag TEXT", [])?;
        }
        if !self.downloads_column_exists("last_modified")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN last_modified TEXT", [])?;
        }
        if !self.downloads_column_exists("mime_type")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN mime_type TEXT", [])?;
        }
        if !self.downloads_column_exists("final_url")? {
            self.conn
                .execute("ALTER TABLE downloads ADD COLUMN final_url TEXT", [])?;
        }
        Ok(())
    }

    fn downloads_column_exists(&self, column: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare("PRAGMA table_info(downloads)")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
