use std::path::{Path, PathBuf};

use directories::{ProjectDirs, UserDirs};
use eyre::{Context, ContextCompat, Result};
use tracing::info;

pub fn data_dir() -> Result<PathBuf> {
    match std::env::var("ARKAMA_DATA_DIR") {
        Ok(path) => Ok(PathBuf::from(path)),
        Err(std::env::VarError::NotPresent) => {
            let dirs = ProjectDirs::from("com", "arkama", "arkama")
                .context("failed to resolve app dirs")?;
            Ok(dirs.data_dir().to_path_buf())
        }
        Err(err) => Err(err).context("failed to read ARKAMA_DATA_DIR"),
    }
}

pub fn db_path() -> Result<PathBuf> {
    let path = data_dir()?.join("arkama.sqlite");
    Ok(path)
}

pub fn daemon_addr_path() -> Result<PathBuf> {
    let path = data_dir()?.join("daemon.addr");
    Ok(path)
}

pub fn daemon_log_path() -> Result<PathBuf> {
    let path = data_dir()?.join("daemon.log");
    Ok(path)
}

pub fn default_download_dir() -> PathBuf {
    let dir = UserDirs::new().and_then(|dirs| dirs.download_dir().map(Path::to_path_buf));
    match dir {
        Some(path) => path,
        None => std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
    }
}

pub fn reset_db() -> Result<()> {
    let path = db_path()?;
    match std::fs::remove_file(&path) {
        Ok(()) => info!("reset db at {}", path.display()),
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                info!("db already absent at {}", path.display());
            } else {
                return Err(err).context("failed to reset db");
            }
        }
    }
    Ok(())
}
