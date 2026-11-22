#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod downloader;
mod error;
mod http;
mod observation;
mod recovery;
mod request;
mod segment;
mod task;

pub use downloader::{DownloadSession, download, start_download, start_download_with_handle};
pub use error::{Error, Result};
pub use observation::{
    Diagnostics, DiagnosticsConfig, DiagnosticsSnapshot, DownloadObservation, ObservationSnapshot,
    SpeedSample,
};
pub use recovery::RecoveryPolicy;
pub use request::{DEFAULT_CONNECTIONS, DownloadRequest, DownloadRequestBuilder};
pub use task::{DownloadControl, DownloadEvent, DownloadHandle, DownloadSummary};
