mod api;
mod downloader;
mod error;
mod http;
mod segment;

pub use api::{
    DEFAULT_CONNECTIONS, DownloadControl, DownloadEvent, DownloadHandle, DownloadRequest,
    DownloadRequestBuilder, DownloadSummary,
};
pub use downloader::{download, start_download, start_download_with_handle};
pub use error::{Error, Result};
