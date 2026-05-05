use super::state::{DownloadState, save_state};
use eyre::Result;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, watch};
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopSignal {
    None,
    Pause,
    Cancel,
}

impl fmt::Display for StopSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopSignal::None => write!(f, "none"),
            StopSignal::Pause => write!(f, "interrupted"),
            StopSignal::Cancel => write!(f, "cancelled"),
        }
    }
}

impl Error for StopSignal {}

pub(crate) async fn wait_with_ctrl_c(
    handles: Vec<JoinHandle<Result<()>>>,
    state: Arc<TokioMutex<DownloadState>>,
    output: PathBuf,
    stop_tx: watch::Sender<StopSignal>,
) -> Result<()> {
    let mut handles = handles;
    tokio::select! {
        result = wait_all(handles.drain(..)) => result,
        _ = tokio::signal::ctrl_c() => {
            let _ = stop_tx.send(StopSignal::Pause);
            let _ = wait_all(handles.drain(..)).await;
            save_state(&output, &state).await?;
            Err(eyre::Report::new(StopSignal::Pause))
        }
    }
}

async fn wait_all(handles: std::vec::Drain<'_, JoinHandle<Result<()>>>) -> Result<()> {
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(eyre::eyre!("task join failed: {err}")),
        }
    }
    Ok(())
}
