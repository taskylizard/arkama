use super::output::sidecar_path;
#[cfg(feature = "serde")]
use super::output::sync_parent;
use crate::segment::Segment;
#[cfg(feature = "serde")]
use eyre::Context;
use eyre::Result;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use sha2::{Digest, Sha256};
#[cfg(feature = "serde")]
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex as TokioMutex;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub(crate) struct DownloadState {
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) version: u32,
    pub(crate) url: String,
    pub(crate) output: PathBuf,
    pub(crate) total_size: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) etag: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) last_modified: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) mime_type: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) final_url: Option<String>,
    pub(crate) segments: Vec<Segment>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) segment_size: Option<u64>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub(crate) experimental_entropy: bool,
}

#[cfg(feature = "serde")]
#[derive(Serialize, Deserialize)]
struct Checkpoint {
    data: String,
    sha256: String,
}

pub(crate) fn state_path(output: &Path) -> Result<PathBuf> {
    sidecar_path(output, ".arkama.state")
}

#[cfg(feature = "serde")]
pub(crate) fn load_state(path: &Path) -> Result<DownloadState> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read state {path:?}"))?;
    let checkpoint: Checkpoint =
        serde_json::from_str(&data).with_context(|| format!("invalid checkpoint {path:?}"))?;
    if checkpoint.sha256 != format!("{:x}", Sha256::digest(checkpoint.data.as_bytes())) {
        return Err(eyre::eyre!("checkpoint integrity check failed"));
    }
    serde_json::from_str(&checkpoint.data).with_context(|| format!("invalid state {path:?}"))
}

#[cfg(not(feature = "serde"))]
pub(crate) fn load_state(path: &Path) -> Result<DownloadState> {
    let _ = path;
    Err(eyre::eyre!(
        "resumable state loading requires the serde feature"
    ))
}

#[cfg(feature = "serde")]
pub(crate) async fn save_state(output: &Path, state: &TokioMutex<DownloadState>) -> Result<()> {
    let path = state_path(output)?;
    // Workers publish written progress only after flushing their Tokio file buffers.
    // Synchronize at least this snapshot's bytes before making it crash-reusable.
    let snapshot = state.lock().await.clone();
    let output = output.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::Write;
        let data = serde_json::to_string(&snapshot).context("failed to serialize state")?;
        let checkpoint = Checkpoint {
            sha256: format!("{:x}", Sha256::digest(data.as_bytes())),
            data,
        };
        let data = serde_json::to_vec(&checkpoint).context("failed to serialize checkpoint")?;
        fs::OpenOptions::new()
            .write(true)
            .open(&output)?
            .sync_all()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        temporary.write_all(&data)?;
        temporary.as_file().sync_all()?;
        temporary
            .persist(&path)
            .context("failed to replace checkpoint")?;
        sync_parent(&path)
    })
    .await
    .context("checkpoint task failed")?
}

#[cfg(not(feature = "serde"))]
pub(crate) async fn save_state(output: &Path, state: &TokioMutex<DownloadState>) -> Result<()> {
    let _ = output;
    let state = state.lock().await;
    let _ = (
        &state.url,
        &state.version,
        &state.output,
        &state.total_size,
        &state.etag,
        &state.last_modified,
        &state.mime_type,
        &state.final_url,
        &state.segments,
        &state.segment_size,
        &state.experimental_entropy,
    );
    Ok(())
}

pub(crate) async fn update_state(
    state: &TokioMutex<DownloadState>,
    updated: &Segment,
) -> Result<()> {
    let mut state = state.lock().await;
    state.segments[updated.id].downloaded = updated.downloaded;
    Ok(())
}
