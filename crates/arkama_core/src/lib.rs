mod downloader;
mod http;
mod segment;

pub use downloader::{
    DownloadControl, DownloadEvent, DownloadHandle, DownloadRequest, DownloadSummary, download,
    start_download, start_download_with_handle,
};
