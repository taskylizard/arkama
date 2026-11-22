//! Synchronous SQLite persistence and application paths for Arkama.
//!
//! This crate stores settings, history, and serialized daemon requests. It does not schedule
//! transfers or validate download checkpoints. Callers keep database work off transfer tasks.

mod db;
mod model;
mod paths;

pub use db::Db;
pub use model::DownloadRecord;
pub use paths::{
    daemon_addr_path, daemon_log_path, data_dir, db_path, default_download_dir, reset_db,
};
