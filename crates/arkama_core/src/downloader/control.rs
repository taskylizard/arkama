use super::state::{DownloadState, save_state};
use eyre::{Context, Result};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::path::Path;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopSignal {
    None,
    Pause,
    Cancel,
}

impl fmt::Display for StopSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Pause => "interrupted",
            Self::Cancel => "cancelled",
        })
    }
}

impl Error for StopSignal {}

// Only cancel network/timer futures here. Disk writes must finish before releasing ownership.
pub(crate) async fn controlled<T>(
    stop_rx: &mut watch::Receiver<StopSignal>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let signal = *stop_rx.borrow();
    if signal != StopSignal::None {
        return Err(eyre::Report::new(signal));
    }
    tokio::select! {
        result = future => result,
        _ = stop_rx.changed() => Err(eyre::Report::new(*stop_rx.borrow())),
    }
}

pub(crate) async fn wait_workers(
    workers: &mut JoinSet<Result<()>>,
    state: &Mutex<DownloadState>,
    output: &Path,
    mut controls: watch::Receiver<StopSignal>,
    worker_stop: watch::Sender<StopSignal>,
) -> Result<()> {
    let mut failure = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.tick().await;
    while !workers.is_empty() {
        let signal = *controls.borrow();
        if signal != StopSignal::None {
            worker_stop.send_replace(signal);
            failure = Some(eyre::Report::new(signal));
        }
        tokio::select! {
            _ = controls.changed(), if signal == StopSignal::None => {}
            result = workers.join_next() => {
                let result = result.expect("nonempty workers").context("worker task failed").and_then(|result| result);
                if let Err(err) = result {
                    if failure.is_none() {
                        failure = Some(err);
                        worker_stop.send_replace(StopSignal::Pause);
                    }
                }
            }
            _ = ticker.tick(), if failure.is_none() => {
                if let Err(err) = save_state(output, state).await {
                    failure = Some(err);
                    worker_stop.send_replace(StopSignal::Pause);
                }
            }
        }
    }
    let signal = *controls.borrow();
    if signal != StopSignal::None {
        return Err(eyre::Report::new(signal));
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}
