#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

mod downloader;
mod error;
mod http;
mod request;
mod segment;
mod task;

pub use downloader::{download, start_download, start_download_with_handle};
pub use error::{Error, Result};
pub use request::{DEFAULT_CONNECTIONS, DownloadRequest, DownloadRequestBuilder};
pub use task::{DownloadControl, DownloadEvent, DownloadHandle, DownloadSummary};
