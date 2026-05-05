use crate::api::{
    DownloadControl, DownloadEvent, DownloadHandle, DownloadRequest, DownloadSummary,
};
use eyre::{Context, Result};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};

mod control;
mod engine;
mod output;
mod plan;
mod rate_limit;
mod state;
mod worker;

pub(crate) use control::StopSignal;
use engine::download_inner;

/// Starts a download on a background task.
///
/// # Errors
///
/// Returns an error if the request is invalid before spawning.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let handle = start_download(request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
pub fn start_download(request: DownloadRequest) -> Result<DownloadHandle> {
    let handle = Handle::try_current().context("start_download requires a Tokio runtime")?;
    start_download_with_handle(handle, request)
}

/// Starts a download using the provided Tokio runtime handle.
///
/// # Errors
///
/// Returns an error if the request is invalid before spawning.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, start_download_with_handle};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let handle = start_download_with_handle(tokio::runtime::Handle::current(), request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
pub fn start_download_with_handle(
    handle: Handle,
    request: DownloadRequest,
) -> Result<DownloadHandle> {
    let (tx, rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(StopSignal::None);
    let join_stop_tx = stop_tx.clone();
    let join =
        handle.spawn(async move { download_inner(request, Some(tx), join_stop_tx, stop_rx).await });
    Ok(DownloadHandle {
        events: rx,
        join,
        control: DownloadControl { stop_tx },
    })
}

/// Runs a download without emitting events.
///
/// # Errors
///
/// Returns an error if the download fails.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let summary = download(request).await?;
/// let _ = summary;
/// # Ok(()) }
/// ```
pub async fn download(request: DownloadRequest) -> Result<DownloadSummary> {
    let (stop_tx, stop_rx) = watch::channel(StopSignal::None);
    download_inner(request, None, stop_tx, stop_rx).await
}

fn send_event(events: &Option<mpsc::UnboundedSender<DownloadEvent>>, event: DownloadEvent) {
    let Some(sender) = events.as_ref() else {
        return;
    };
    let _ = sender.send(event);
}

#[cfg(test)]
mod tests {
    use super::output::determine_output;
    use super::send_event;
    use super::state::{DownloadState, load_state, periodic_save};
    use super::worker::backoff_delay;
    use crate::api::DownloadEvent;
    use crate::http::HttpMeta;
    use crate::segment::Segment;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::Mutex as TokioMutex;
    use tokio::sync::{mpsc, watch};
    use url::Url;

    fn temp_dir_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("arkama-test-{name}-{nanos}"));
        path
    }

    #[test]
    fn test_determine_output_explicit_path() {
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: None,
        };
        let path = PathBuf::from("output.bin");

        let resolved = determine_output(&url, &meta, Some(path.clone()), None).expect("output");
        assert_eq!(resolved, path);
    }

    #[test]
    fn test_determine_output_rejects_directory() {
        let dir = temp_dir_path("output-dir");
        fs::create_dir_all(&dir).expect("create temp dir");
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: None,
        };

        let result = determine_output(&url, &meta, Some(dir), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_determine_output_uses_meta_filename() {
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: Some("meta.bin".to_string()),
        };

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("meta.bin"));
    }

    #[test]
    fn test_determine_output_uses_url_path() {
        let url = Url::parse("https://example.com/files/report.csv").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: None,
        };

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("report.csv"));
    }

    #[test]
    fn test_determine_output_uses_output_dir() {
        let url = Url::parse("https://example.com/files/report.csv").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: None,
        };
        let dir = PathBuf::from("/tmp");

        let resolved = determine_output(&url, &meta, None, Some(dir.clone())).expect("output");
        assert_eq!(resolved, dir.join("report.csv"));
    }

    #[test]
    fn test_determine_output_falls_back_to_default() {
        let url = Url::parse("https://example.com/files/").expect("url");
        let meta = HttpMeta {
            size: None,
            accept_ranges: false,
            filename: None,
        };

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("download"));
    }

    #[test]
    fn test_backoff_delay_caps_at_max() {
        let delay = backoff_delay(6);
        assert_eq!(delay, Duration::from_millis(10_000));
    }

    #[test]
    fn test_backoff_delay_first_attempt() {
        let delay = backoff_delay(1);
        assert_eq!(delay, Duration::from_millis(1_000));
    }

    #[test]
    fn test_send_event_dispatches() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let event = DownloadEvent::Failed {
            message: "boom".to_string(),
        };

        send_event(&Some(tx), event);
        let received = rx.blocking_recv().expect("event");
        let DownloadEvent::Failed { message } = received else {
            panic!("unexpected event");
        };
        assert_eq!(message, "boom");
    }

    #[tokio::test]
    async fn test_periodic_save_writes_state() {
        let dir = temp_dir_path("periodic-save");
        fs::create_dir_all(&dir).expect("create temp dir");
        let output = dir.join("file.bin");
        let state = DownloadState {
            url: "http://example.com/file.bin".to_string(),
            output: output.clone(),
            total_size: Some(100),
            segments: vec![Segment {
                id: 0,
                start: 0,
                end: 99,
                downloaded: 42,
            }],
            segment_size: None,
            experimental_entropy: false,
        };
        let state = Arc::new(TokioMutex::new(state));
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = tokio::spawn(periodic_save(
            output.clone(),
            Arc::clone(&state),
            stop_rx,
            Duration::from_millis(20),
        ));

        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = stop_tx.send(true);
        handle.await.expect("save task").expect("save ok");

        let state_path = output.with_file_name("file.bin.arkama.state");
        let saved = load_state(&state_path).expect("load state");
        assert_eq!(saved.total_size, Some(100));
        assert_eq!(saved.segments[0].downloaded, 42);
    }
}
