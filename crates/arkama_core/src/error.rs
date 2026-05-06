use std::error::Error as StdError;
use std::fmt;

/// Result type returned by arkama_core public APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type returned by arkama_core public APIs.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The download was paused and can be resumed when resumable state was saved.
    Paused,
    /// The download was cancelled and partial output/state was removed when possible.
    Cancelled,
    /// The request URL could not be parsed.
    InvalidUrl { message: String },
    /// An HTTP request or response failed.
    Http { message: String },
    /// File or filesystem I/O failed.
    Io { message: String },
    /// Saved resumable state could not be read, written, or decoded.
    State { message: String },
    /// A download could not be started because the runtime environment is unavailable.
    RuntimeUnavailable { message: String },
    /// The download failed.
    DownloadFailed { message: String },
}

impl Error {
    pub(crate) fn runtime_unavailable(message: impl Into<String>) -> Self {
        Self::RuntimeUnavailable {
            message: message.into(),
        }
    }

    pub(crate) fn download_failed(message: impl Into<String>) -> Self {
        Self::DownloadFailed {
            message: message.into(),
        }
    }

    /// Returns true if the download was paused.
    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Paused)
    }

    /// Returns true if the download was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Paused => write!(f, "interrupted"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::InvalidUrl { message }
            | Self::Http { message }
            | Self::Io { message }
            | Self::State { message }
            | Self::RuntimeUnavailable { message }
            | Self::DownloadFailed { message } => f.write_str(message),
        }
    }
}

impl StdError for Error {}
