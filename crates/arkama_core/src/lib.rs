//! Core library API for arkama downloads.
//!
//! `arkama_core` provides HTTP/HTTPS file downloads with filename inference, resume support,
//! segmented downloads, progress events, pause/cancel controls, and optional global speed limits.
//! The crate is intended for applications that want arkama's downloader without depending on the
//! arkama CLI or daemon.
//!
//! # Quick start
//!
//! ```no_run
//! use arkama_core::{DownloadRequest, download};
//!
//! # #[tokio::main]
//! # async fn main() -> arkama_core::Result<()> {
//! let request = DownloadRequest::new("https://example.com/file.bin")
//!     .output("./file.bin")
//!     .connections(8)
//!     .speed_limit(2_000_000);
//!
//! let summary = download(request).await?;
//! println!("saved {} bytes to {:?}", summary.downloaded_bytes, summary.output);
//! # Ok(()) }
//! ```
//!
//! # Tokio runtime model
//!
//! All download APIs are asynchronous and run on Tokio:
//!
//! - [`download`] awaits the download on the current Tokio runtime task and returns a
//!   [`DownloadSummary`] when it completes.
//! - [`start_download`] must be called from an existing Tokio runtime. It spawns the download on
//!   that runtime and returns a [`DownloadHandle`] containing an event stream, completion task, and
//!   [`DownloadControl`].
//! - [`start_download_with_handle`] is the same background API, but lets callers provide a specific
//!   [`tokio::runtime::Handle`].
//!
//! # Progress events and completion
//!
//! Background downloads report lifecycle updates through [`DownloadEvent`]: [`DownloadEvent::Started`],
//! [`DownloadEvent::Progress`], [`DownloadEvent::Finished`], and [`DownloadEvent::Failed`]. The
//! completion result is available from [`DownloadHandle::join`].
//!
//! # Pause and cancel
//!
//! [`DownloadControl::pause`] and [`DownloadControl::cancel`] request shutdown through the
//! background download's control channel. Pausing keeps resumable state when possible and the
//! completion path returns [`Error::Paused`]. Cancelling returns [`Error::Cancelled`] and removes
//! partial output/state when possible. [`Error::is_paused`] and [`Error::is_cancelled`] are provided
//! for callers that want to branch on those outcomes.
//!
//! # Resume behavior
//!
//! Resumable state is stored next to the output file as `<output file name>.arkama.state`. A paused
//! or interrupted segmented download can be resumed by starting another download with the same URL
//! and output path. State is cleaned up after successful completion. Servers that do not support
//! HTTP byte ranges fall back to single-stream downloads internally.
//!
//! # Segmented downloads
//!
//! [`DownloadRequest::connections`] controls the requested number of segmented download workers.
//! [`DEFAULT_CONNECTIONS`] is `4`. Segmentation is used only when the server reports a known size
//! and supports HTTP byte ranges; otherwise the request still succeeds through the single-stream
//! path when possible.
//!
//! # Feature flags
//!
//! The default feature set is `rustls-tls` and `serde`:
//!
//! - `rustls-tls` enables Reqwest's Rustls TLS backend.
//! - `native-tls` enables Reqwest's native TLS backend instead or in addition.
//! - `serde` enables `Serialize`/`Deserialize` for public request types and the internal persisted
//!   resume-state format. Disabling it still compiles the downloader, but persisted resume state is
//!   unavailable.
//!
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
