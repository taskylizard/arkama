use crate::segment::Segment;
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, watch};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DownloadState {
    pub(crate) url: String,
    pub(crate) output: PathBuf,
    pub(crate) total_size: Option<u64>,
    pub(crate) segments: Vec<Segment>,
    #[serde(default)]
    pub(crate) segment_size: Option<u64>,
    #[serde(default)]
    pub(crate) experimental_entropy: bool,
}

pub(crate) fn state_path(output: &Path) -> Result<PathBuf> {
    let Some(name) = output.file_name() else {
        return Err(eyre::eyre!("output file name missing"));
    };
    let mut name = name.to_os_string();
    name.push(".arkama.state");
    Ok(output.with_file_name(name))
}

pub(crate) fn load_state(path: &Path) -> Result<DownloadState> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read state {path:?}"))?;
    let state = serde_json::from_str(&data).with_context(|| format!("invalid state {path:?}"))?;
    Ok(state)
}

pub(crate) async fn save_state(output: &Path, state: &TokioMutex<DownloadState>) -> Result<()> {
    let path = state_path(output)?;
    let state = state.lock().await;
    let data = serde_json::to_string_pretty(&*state).context("failed to serialize state")?;
    tokio::fs::write(&path, data)
        .await
        .with_context(|| format!("failed to write state {path:?}"))?;
    Ok(())
}

pub(crate) async fn update_state(
    state: &TokioMutex<DownloadState>,
    updated: &Segment,
) -> Result<()> {
    let mut state = state.lock().await;
    if let Some(segment) = state.segments.get_mut(updated.id)
        && segment.id == updated.id
    {
        segment.downloaded = updated.downloaded;
        return Ok(());
    }

    let mut index = 0usize;
    while index < state.segments.len() {
        if state.segments[index].id == updated.id {
            state.segments[index].downloaded = updated.downloaded;
            break;
        }
        index += 1;
    }
    Ok(())
}

pub(crate) async fn periodic_save(
    output: PathBuf,
    state: Arc<TokioMutex<DownloadState>>,
    mut stop_rx: watch::Receiver<bool>,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                save_state(&output, &state).await?;
            }
        }
    }
}
