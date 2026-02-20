use std::path::{Path, PathBuf};

use directories::{ProjectDirs, UserDirs};
use eyre::{Context, ContextCompat, Result};
use tracing::info;

pub fn db_path() -> Result<PathBuf> {
    let dirs =
        ProjectDirs::from("com", "arkama", "arkama").context("failed to resolve app dirs")?;
    let path = dirs.data_dir().join("arkama.sqlite");
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
