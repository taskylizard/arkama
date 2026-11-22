//! Events, results, and controls for a running download.

use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::downloader::StopSignal;
use crate::error::Result;

/// Reports lifecycle updates for a download.
///
/// Progress is sampled at most once every 500 ms per download, shared across workers.
/// The first sample and lifecycle events are immediate; completion is authoritative through `join`.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadEvent, DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> arkama_core::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let mut handle = start_download(request)?;
/// while let Some(event) = handle.events.recv().await {
///     match event {
///         DownloadEvent::Progress { downloaded_bytes, total_bytes } => {
///             println!("downloaded {downloaded_bytes:?} of {total_bytes:?}");
///         }
///         other => println!("{other:?}"),
///     }
/// }
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub enum DownloadEvent {
    Metadata {
        etag: Option<String>,
        last_modified: Option<String>,
        mime_type: Option<String>,
        final_url: String,
    },
    Started {
        output: PathBuf,
        total_bytes: Option<u64>,
        resumed_bytes: u64,
    },
    Progress {
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    Finished {
        summary: DownloadSummary,
    },
    Failed {
        message: String,
    },
}

/// Holds the completion details for a download.
#[derive(Debug, Clone)]
pub struct DownloadSummary {
    pub output: PathBuf,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub resumed_bytes: u64,
    pub elapsed: Duration,
}

/// Provides the event stream, completion task, and control handle for a background download.
///
/// Await `join` to acknowledge completion or shutdown. Dropping the handle detaches the task;
/// aborting `join` is not a coordinated pause/cancel and must not be used for shutdown.
pub struct DownloadHandle {
    pub events: mpsc::UnboundedReceiver<DownloadEvent>,
    pub join: JoinHandle<Result<DownloadSummary>>,
    pub control: DownloadControl,
}

/// Controls a running background download.
#[derive(Clone)]
pub struct DownloadControl {
    pub(crate) stop_tx: watch::Sender<StopSignal>,
}

impl DownloadControl {
    /// Pauses the download and keeps resumable state when possible.
    /// Await the completion task before starting another attempt. Cannot undo cancellation.
    pub fn pause(&self) {
        self.stop_tx.send_if_modified(|signal| {
            if *signal == StopSignal::Cancel {
                return false;
            }
            *signal = StopSignal::Pause;
            true
        });
    }

    /// Cancels the download and removes partial output/state when possible.
    pub fn cancel(&self) {
        let _ = self.stop_tx.send(StopSignal::Cancel);
    }
}
