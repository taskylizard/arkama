use crate::downloader::StopSignal;
use crate::error::Result;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// The default number of connections used by [`DownloadRequest::new`].
pub const DEFAULT_CONNECTIONS: usize = 4;

/// Describes a download request.
///
/// `DownloadRequest::new` provides conservative defaults suitable for most downloads. Fields remain
/// public for now so existing callers can continue using struct literals.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, download};
///
/// # #[tokio::main]
/// # async fn main() -> arkama_core::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin")
///     .output("./file.bin")
///     .connections(8)
///     .speed_limit(2_000_000);
///
/// let summary = download(request).await?;
/// let _ = summary;
/// # Ok(()) }
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct DownloadRequest {
    pub url: String,
    pub output: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub connections: usize,
    pub user_agent: Option<String>,
    pub limit: Option<u64>,
    pub experimental_entropy: bool,
}

impl DownloadRequest {
    /// Starts a builder for a download request.
    pub fn builder(url: impl Into<String>) -> DownloadRequestBuilder {
        DownloadRequestBuilder::new(url)
    }

    /// Creates a download request with practical defaults.
    ///
    /// Defaults:
    ///
    /// - output path inferred from HTTP headers or URL path
    /// - [`DEFAULT_CONNECTIONS`] segmented connections when the server supports ranges
    /// - no custom user agent
    /// - no speed limit
    /// - experimental entropy mode disabled
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            output: None,
            output_dir: None,
            connections: DEFAULT_CONNECTIONS,
            user_agent: None,
            limit: None,
            experimental_entropy: false,
        }
    }

    /// Sets the exact output file path.
    pub fn output(mut self, output: impl Into<PathBuf>) -> Self {
        self.output = Some(output.into());
        self
    }

    /// Sets the directory used when inferring the output filename.
    pub fn output_dir(mut self, output_dir: impl Into<PathBuf>) -> Self {
        self.output_dir = Some(output_dir.into());
        self
    }

    /// Sets the number of connections used for segmented downloads.
    ///
    /// A value of `0` is accepted for backward compatibility and is normalized to `1` when the
    /// download starts.
    pub fn connections(mut self, connections: usize) -> Self {
        self.connections = connections;
        self
    }

    /// Sets the HTTP `User-Agent` header.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Sets a global download speed limit in bytes per second.
    pub fn speed_limit(mut self, bytes_per_second: u64) -> Self {
        self.limit = Some(bytes_per_second);
        self
    }

    /// Sets the optional global download speed limit in bytes per second.
    pub fn optional_speed_limit(mut self, bytes_per_second: Option<u64>) -> Self {
        self.limit = bytes_per_second;
        self
    }

    /// Enables or disables experimental connection entropy mode.
    ///
    /// This mode is intended for LACP/ECMP environments and may increase connection churn.
    pub fn experimental_entropy(mut self, enabled: bool) -> Self {
        self.experimental_entropy = enabled;
        self
    }

    /// Returns the output file path configured on the request, if any.
    pub fn output_path(&self) -> Option<&Path> {
        self.output.as_deref()
    }

    /// Returns the output directory configured on the request, if any.
    pub fn output_directory(&self) -> Option<&Path> {
        self.output_dir.as_deref()
    }

    /// Returns the configured speed limit in bytes per second, if any.
    pub fn speed_limit_bytes_per_second(&self) -> Option<u64> {
        self.limit
    }
}

/// Builder for [`DownloadRequest`].
///
/// This is equivalent to using the chainable methods on `DownloadRequest` directly, but makes the
/// construction style explicit for callers that prefer a named builder.
///
/// # Examples
///
/// ```
/// use arkama_core::DownloadRequest;
///
/// let request = DownloadRequest::builder("https://example.com/file.bin")
///     .output("./file.bin")
///     .connections(8)
///     .build();
///
/// assert_eq!(request.connections, 8);
/// ```
#[derive(Clone, Debug)]
pub struct DownloadRequestBuilder {
    request: DownloadRequest,
}

impl DownloadRequestBuilder {
    /// Creates a builder with [`DownloadRequest::new`] defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            request: DownloadRequest::new(url),
        }
    }

    /// Sets the exact output file path.
    pub fn output(mut self, output: impl Into<PathBuf>) -> Self {
        self.request = self.request.output(output);
        self
    }

    /// Sets the directory used when inferring the output filename.
    pub fn output_dir(mut self, output_dir: impl Into<PathBuf>) -> Self {
        self.request = self.request.output_dir(output_dir);
        self
    }

    /// Sets the number of connections used for segmented downloads.
    pub fn connections(mut self, connections: usize) -> Self {
        self.request = self.request.connections(connections);
        self
    }

    /// Sets the HTTP `User-Agent` header.
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.request = self.request.user_agent(user_agent);
        self
    }

    /// Sets a global download speed limit in bytes per second.
    pub fn speed_limit(mut self, bytes_per_second: u64) -> Self {
        self.request = self.request.speed_limit(bytes_per_second);
        self
    }

    /// Sets the optional global download speed limit in bytes per second.
    pub fn optional_speed_limit(mut self, bytes_per_second: Option<u64>) -> Self {
        self.request = self.request.optional_speed_limit(bytes_per_second);
        self
    }

    /// Enables or disables experimental connection entropy mode.
    pub fn experimental_entropy(mut self, enabled: bool) -> Self {
        self.request = self.request.experimental_entropy(enabled);
        self
    }

    /// Builds the request.
    pub fn build(self) -> DownloadRequest {
        self.request
    }
}

impl From<DownloadRequestBuilder> for DownloadRequest {
    fn from(builder: DownloadRequestBuilder) -> Self {
        builder.build()
    }
}

/// Reports lifecycle updates for a download.
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
    pub fn pause(&self) {
        let _ = self.stop_tx.send(StopSignal::Pause);
    }

    /// Cancels the download and removes partial output/state when possible.
    pub fn cancel(&self) {
        let _ = self.stop_tx.send(StopSignal::Cancel);
    }
}
