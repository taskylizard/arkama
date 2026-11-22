//! Public download entry points and private transfer orchestration.

use crate::DownloadObservation;
use crate::error::{Error, Result};
use crate::recovery::RetryBudget;
use crate::{DownloadControl, DownloadEvent, DownloadHandle, DownloadRequest, DownloadSummary};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

/// Discards partial artifacts only while holding output ownership. Never deletes a final file.
/// Call only for an output owned by the caller; an active attempt or existing final file refuses.
pub fn discard_partial(output: &std::path::Path) -> Result<()> {
    let _ownership = output::lock_output(output).map_err(map_download_error)?;
    let partial = output::sidecar_path(output, ".arkama.part").map_err(map_download_error)?;
    output::remove_if_exists(&state::state_path(&partial).map_err(map_download_error)?)
        .map_err(map_download_error)?;
    output::remove_if_exists(&partial).map_err(map_download_error)
}

/// Shared recovery admission for downloads owned by one host application.
///
/// Reuse a session across jobs to share rolling per-origin retry budgets. Separate sessions and
/// the standalone functions do not share budgets. The host still owns the runtime and lifecycle.
#[derive(Clone, Default)]
pub struct DownloadSession {
    budget: Arc<RetryBudget>,
    destination: crate::DestinationPolicy,
}

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
/// # async fn main() -> arkama_core::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let handle = start_download(request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
pub fn start_download(request: DownloadRequest) -> Result<DownloadHandle> {
    let handle = Handle::try_current()
        .map_err(|_| Error::runtime_unavailable("start_download requires a Tokio runtime"))?;
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
/// # async fn main() -> arkama_core::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let handle = start_download_with_handle(tokio::runtime::Handle::current(), request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
pub fn start_download_with_handle(
    handle: Handle,
    request: DownloadRequest,
) -> Result<DownloadHandle> {
    DownloadSession::default().start_with_handle(handle, request)
}

impl DownloadSession {
    /// Applies host-owned destination restrictions to every request, redirect, and DNS lookup.
    pub fn destination_policy(mut self, policy: crate::DestinationPolicy) -> Self {
        self.destination = policy;
        self
    }

    /// Starts a download on the current Tokio runtime, sharing this session's retry budgets.
    pub fn start(&self, request: DownloadRequest) -> Result<DownloadHandle> {
        let handle = Handle::try_current()
            .map_err(|_| Error::runtime_unavailable("start requires a Tokio runtime"))?;
        self.start_with_handle(handle, request)
    }

    /// Starts on the host-selected runtime. Invalid recovery settings fail before spawning.
    pub fn start_with_handle(
        &self,
        handle: Handle,
        request: DownloadRequest,
    ) -> Result<DownloadHandle> {
        request.recovery.validate()?;
        let url = url::Url::parse(&request.url).map_err(|_| Error::InvalidUrl {
            message: "invalid URL".into(),
        })?;
        self.destination.validate(&url)?;
        let budget = self.budget.clone();
        let destination = self.destination.clone();
        let (tx, rx) = mpsc::channel(8);
        let observation = DownloadObservation::new();
        let task_observation = observation.clone();
        let (stop_tx, stop_rx) = watch::channel(StopSignal::None);
        let join_stop_tx = stop_tx.clone();
        let join = handle.spawn(async move {
            let result = download_inner(
                request,
                Some(Arc::new(EventSender::new(tx, task_observation.clone()))),
                join_stop_tx,
                stop_rx,
                task_observation.clone(),
                budget,
                destination,
            )
            .await
            .map_err(map_download_error);
            task_observation.finish(match &result {
                Ok(_) => 1,
                Err(Error::Paused) => 2,
                Err(Error::Cancelled) => 3,
                Err(_) => 4,
            });
            result
        });
        Ok(DownloadHandle {
            events: rx,
            observation,
            join,
            control: DownloadControl { stop_tx },
        })
    }
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
/// # async fn main() -> arkama_core::Result<()> {
/// let request = DownloadRequest::new("https://example.com/file.bin");
/// let summary = download(request).await?;
/// let _ = summary;
/// # Ok(()) }
/// ```
pub async fn download(request: DownloadRequest) -> Result<DownloadSummary> {
    request.recovery.validate()?;
    let (stop_tx, stop_rx) = watch::channel(StopSignal::None);
    download_inner(
        request,
        None,
        stop_tx,
        stop_rx,
        DownloadObservation::new(),
        Arc::new(RetryBudget::default()),
        crate::DestinationPolicy::default(),
    )
    .await
    .map_err(map_download_error)
}

fn map_download_error(err: eyre::Report) -> Error {
    if let Some(stop_signal) = err.downcast_ref::<StopSignal>() {
        return match stop_signal {
            StopSignal::None => Error::download_failed(err.to_string()),
            StopSignal::Pause => Error::Paused,
            StopSignal::Cancel => Error::Cancelled,
        };
    }

    let message = format!("{err:#}");
    if err.downcast_ref::<url::ParseError>().is_some() {
        return Error::InvalidUrl { message };
    }
    if err.downcast_ref::<reqwest::Error>().is_some()
        || err.downcast_ref::<crate::http::HttpFailure>().is_some()
    {
        return Error::Http { message };
    }
    if err.downcast_ref::<std::io::Error>().is_some() {
        return Error::Io { message };
    }
    #[cfg(feature = "serde")]
    if err.downcast_ref::<serde_json::Error>().is_some() {
        return Error::State { message };
    }

    Error::DownloadFailed { message }
}

struct EventSender {
    sender: mpsc::Sender<DownloadEvent>,
    observation: DownloadObservation,
    last_progress: Mutex<Option<Instant>>,
}

impl EventSender {
    fn new(sender: mpsc::Sender<DownloadEvent>, observation: DownloadObservation) -> Self {
        Self {
            sender,
            observation,
            last_progress: Mutex::new(None),
        }
    }
}

fn send_event(events: &Option<Arc<EventSender>>, event: DownloadEvent) {
    let Some(sender) = events.as_ref() else {
        return;
    };
    if sender.sender.is_closed() {
        sender.observation.dropped();
        return;
    }
    if matches!(event, DownloadEvent::Progress { .. }) {
        // Skip optional samples on contention; workers never wait for reporting.
        let Ok(mut last_progress) = sender.last_progress.try_lock() else {
            sender.observation.dropped();
            return;
        };
        let now = Instant::now();
        if last_progress.is_some_and(|last| now.duration_since(last) < Duration::from_millis(500)) {
            sender.observation.suppressed();
            return;
        }
        *last_progress = Some(now);
        if sender.sender.try_send(event).is_err() {
            sender.observation.dropped();
        }
    } else if sender.sender.try_send(event).is_err() {
        sender.observation.dropped();
    }
}

#[cfg(test)]
mod tests {
    use super::EventSender;
    use super::output::determine_output;
    use super::send_event;
    #[cfg(feature = "serde")]
    use super::state::{DownloadState, load_state, save_state};
    use crate::DownloadEvent;
    use crate::http::HttpMeta;
    #[cfg(feature = "serde")]
    use crate::segment::Segment;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    #[cfg(feature = "serde")]
    use tokio::sync::Mutex as TokioMutex;
    use tokio::sync::mpsc;
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

    fn http_meta(filename: Option<&str>) -> HttpMeta {
        HttpMeta {
            size: None,
            accept_ranges: false,
            filename: filename.map(ToString::to_string),
            etag: None,
            last_modified: None,
            mime_type: None,
            final_url: "https://example.com/file.bin".to_string(),
        }
    }

    #[test]
    fn test_determine_output_explicit_path() {
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = http_meta(None);
        let path = PathBuf::from("output.bin");

        let resolved = determine_output(&url, &meta, Some(path.clone()), None).expect("output");
        assert_eq!(resolved, path);
    }

    #[test]
    fn test_determine_output_rejects_directory() {
        let dir = temp_dir_path("output-dir");
        fs::create_dir_all(&dir).expect("create temp dir");
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = http_meta(None);

        let result = determine_output(&url, &meta, Some(dir), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_determine_output_uses_meta_filename() {
        let url = Url::parse("https://example.com/file.bin").expect("url");
        let meta = http_meta(Some("meta.bin"));

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("meta.bin"));
    }

    #[test]
    fn test_determine_output_uses_url_path() {
        let url = Url::parse("https://example.com/files/report.csv").expect("url");
        let meta = http_meta(None);

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("report.csv"));
    }

    #[test]
    fn test_determine_output_uses_output_dir() {
        let url = Url::parse("https://example.com/files/report.csv").expect("url");
        let meta = http_meta(None);
        let dir = PathBuf::from("/tmp");

        let resolved = determine_output(&url, &meta, None, Some(dir.clone())).expect("output");
        assert_eq!(resolved, dir.join("report.csv"));
    }

    #[test]
    fn test_determine_output_falls_back_to_default() {
        let url = Url::parse("https://example.com/files/").expect("url");
        let meta = http_meta(None);

        let resolved = determine_output(&url, &meta, None, None).expect("output");
        assert_eq!(resolved, PathBuf::from("download"));
    }

    #[test]
    fn test_backoff_delay_caps_at_max() {
        let delay = crate::RecoveryPolicy {
            jitter_ms: 0,
            ..Default::default()
        }
        .delay(6);
        assert_eq!(delay, Duration::from_millis(10_000));
    }

    #[test]
    fn test_backoff_delay_first_attempt() {
        let delay = crate::RecoveryPolicy {
            jitter_ms: 0,
            ..Default::default()
        }
        .delay(1);
        assert_eq!(delay, Duration::from_millis(1_000));
    }

    #[test]
    fn test_send_event_dispatches() {
        let (tx, mut rx) = mpsc::channel(8);
        let event = DownloadEvent::Failed {
            message: "boom".to_string(),
        };

        send_event(
            &Some(Arc::new(EventSender::new(
                tx,
                crate::DownloadObservation::new(),
            ))),
            event,
        );
        let received = rx.blocking_recv().expect("event");
        let DownloadEvent::Failed { message } = received else {
            panic!("unexpected event");
        };
        assert_eq!(message, "boom");
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn test_save_writes_state() {
        let dir = temp_dir_path("periodic-save");
        fs::create_dir_all(&dir).expect("create temp dir");
        let output = dir.join("file.bin");
        fs::write(&output, [0; 100]).expect("create output");
        let state = DownloadState {
            version: 1,
            url: "http://example.com/file.bin".to_string(),
            output: output.clone(),
            total_size: Some(100),
            etag: None,
            last_modified: None,
            mime_type: None,
            final_url: None,
            segments: vec![Segment {
                id: 0,
                start: 0,
                end: 99,
                downloaded: 42,
            }],
            segment_size: None,
            experimental_entropy: false,
        };
        let state = TokioMutex::new(state);
        save_state(&output, &state, None).await.expect("save ok");

        let state_path = output.with_file_name("file.bin.arkama.state");
        let saved = load_state(&state_path).expect("load state");
        assert_eq!(saved.total_size, Some(100));
        assert_eq!(saved.segments[0].downloaded, 42);
    }
}
