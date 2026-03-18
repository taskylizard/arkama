use crate::http::{self, ClientFactory, HttpMeta};
use crate::segment::{Segment, build_segments, build_segments_with_chunk_size};
use eyre::{Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::{BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions as AsyncOpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::runtime::Handle;
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use url::Url;

/// Describes a download request.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
/// let handle = start_download(request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
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

/// Reports lifecycle updates for a download.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadEvent, DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
/// let mut handle = start_download(request)?;
/// while let Some(event) = handle.events.recv().await {
///     let event = event;
///     let _ = event;
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
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
/// let mut handle = start_download(request)?;
/// let summary = handle.join.await??;
/// let _ = summary;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct DownloadSummary {
    pub output: PathBuf,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub resumed_bytes: u64,
    pub elapsed: Duration,
}

/// Provides the event stream and task handle for a download.
///
/// # Examples
///
/// ```no_run
/// use arkama_core::{DownloadRequest, start_download};
///
/// # #[tokio::main]
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
/// let handle = start_download(request)?;
/// let _ = handle;
/// # Ok(()) }
/// ```
pub struct DownloadHandle {
    pub events: mpsc::UnboundedReceiver<DownloadEvent>,
    pub join: JoinHandle<Result<DownloadSummary>>,
    pub control: DownloadControl,
}

#[derive(Clone)]
pub struct DownloadControl {
    stop_tx: watch::Sender<StopSignal>,
}

impl DownloadControl {
    pub fn pause(&self) {
        let _ = self.stop_tx.send(StopSignal::Pause);
    }

    pub fn cancel(&self) {
        let _ = self.stop_tx.send(StopSignal::Cancel);
    }
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
/// # async fn main() -> eyre::Result<()> {
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
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
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
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
/// let request = DownloadRequest {
///     url: "https://example.com/file.bin".to_string(),
///     output: None,
///     output_dir: None,
///     connections: 4,
///     user_agent: None,
///     limit: None,
///     experimental_entropy: false,
/// };
/// let summary = download(request).await?;
/// let _ = summary;
/// # Ok(()) }
/// ```
pub async fn download(request: DownloadRequest) -> Result<DownloadSummary> {
    let (stop_tx, stop_rx) = watch::channel(StopSignal::None);
    download_inner(request, None, stop_tx, stop_rx).await
}

#[derive(Debug, Serialize, Deserialize)]
struct DownloadState {
    url: String,
    output: PathBuf,
    total_size: Option<u64>,
    segments: Vec<Segment>,
    #[serde(default)]
    segment_size: Option<u64>,
    #[serde(default)]
    experimental_entropy: bool,
}

struct DownloadPlan {
    url: Url,
    output: PathBuf,
    total_size: Option<u64>,
    accept_ranges: bool,
    segments: Vec<Segment>,
    connections: usize,
    segment_size: Option<u64>,
    experimental_entropy: bool,
    client_factory: ClientFactory,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
    stop_tx: watch::Sender<StopSignal>,
    stop_rx: watch::Receiver<StopSignal>,
}

struct DownloadPlanConfig {
    url: Url,
    output: PathBuf,
    meta: HttpMeta,
    connections: usize,
    resume: Option<DownloadState>,
    experimental_entropy: bool,
    client_factory: ClientFactory,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
    stop_tx: watch::Sender<StopSignal>,
    stop_rx: watch::Receiver<StopSignal>,
}

const EXPERIMENTAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024;
const STATE_SYNC_BATCH_BYTES: u64 = 84 * 1024;

#[derive(Debug)]
struct RangeUnsupported;

impl fmt::Display for RangeUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server does not support range requests")
    }
}

impl Error for RangeUnsupported {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopSignal {
    None,
    Pause,
    Cancel,
}

impl fmt::Display for StopSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopSignal::None => write!(f, "none"),
            StopSignal::Pause => write!(f, "interrupted"),
            StopSignal::Cancel => write!(f, "cancelled"),
        }
    }
}

impl Error for StopSignal {}

struct SlowestTracker {
    target_concurrency: usize,
    timings: BTreeSet<Duration>,
}

struct SpeedLimiter {
    rate: u64,
    next_at: Mutex<Instant>,
}

impl SpeedLimiter {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            next_at: Mutex::new(Instant::now()),
        }
    }

    async fn throttle(&self, bytes: u64) {
        if bytes == 0 || self.rate == 0 {
            return;
        }
        let nanos = (bytes.saturating_mul(1_000_000_000)) / self.rate;
        let wait = Duration::from_nanos(nanos.max(1));
        let now = Instant::now();
        let target = {
            let mut next_at = self.next_at.lock().unwrap();
            if *next_at < now {
                *next_at = now;
            }
            *next_at += wait;
            *next_at
        };
        if target > now {
            tokio::time::sleep(target - now).await;
        }
    }
}

#[derive(Clone)]
struct DownloadSegmentContext {
    client_factory: ClientFactory,
    url: Url,
    output: PathBuf,
    state: Arc<TokioMutex<DownloadState>>,
    stop_rx: watch::Receiver<StopSignal>,
    slow_tracker: Option<Arc<Mutex<SlowestTracker>>>,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
    total_size: Option<u64>,
}

#[derive(Clone)]
struct DownloadSingleContext {
    client_factory: ClientFactory,
    url: Url,
    output: PathBuf,
    stop_rx: watch::Receiver<StopSignal>,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    start: u64,
    accept_ranges: bool,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
    total_size: Option<u64>,
}

impl SlowestTracker {
    fn new(target_concurrency: usize) -> Self {
        let target_concurrency = if target_concurrency == 0 {
            1
        } else {
            target_concurrency
        };
        Self {
            target_concurrency,
            timings: BTreeSet::new(),
        }
    }

    fn should_recycle(&self, elapsed: Duration) -> bool {
        if self.timings.len() < self.target_concurrency {
            return false;
        }
        let Some(slowest) = self.timings.iter().next_back() else {
            return false;
        };
        elapsed > *slowest
    }

    fn record(&mut self, elapsed: Duration) {
        if self.timings.len() < self.target_concurrency {
            self.timings.insert(elapsed);
            return;
        }
        let Some(slowest) = self.timings.iter().next_back().cloned() else {
            return;
        };
        self.timings.remove(&slowest);
        self.timings.insert(elapsed);
    }
}

impl DownloadPlan {
    fn new(config: DownloadPlanConfig) -> Result<Self> {
        let DownloadPlanConfig {
            url,
            output,
            meta,
            connections,
            resume,
            experimental_entropy,
            client_factory,
            speed_limiter,
            events,
            total_downloaded,
            stop_tx,
            stop_rx,
        } = config;
        let mut accept_ranges = meta.accept_ranges;
        let total_size = meta.size;
        let segment_size = if experimental_entropy {
            Some(EXPERIMENTAL_SEGMENT_SIZE)
        } else {
            None
        };

        let mut segments = Vec::new();
        let output_exists = output.exists();

        if output_exists
            && let Some(state) = resume
            && state.url == url.as_str()
            && state.total_size == total_size
            && state.experimental_entropy == experimental_entropy
            && state.segment_size == segment_size
        {
            segments = state.segments;
        }

        if total_size.is_none() {
            accept_ranges = false;
        }

        if !output_exists
            && segments.is_empty()
            && accept_ranges
            && let Some(total) = total_size
        {
            segments = if let Some(segment_size) = segment_size {
                build_segments_with_chunk_size(total, segment_size)
            } else {
                build_segments(total, connections)
            };
        }

        Ok(Self {
            url,
            output,
            total_size,
            accept_ranges,
            segments,
            connections,
            segment_size,
            experimental_entropy,
            client_factory,
            speed_limiter,
            events,
            total_downloaded,
            stop_tx,
            stop_rx,
        })
    }

    fn already_downloaded(&self) -> Result<u64> {
        if self.accept_ranges && !self.segments.is_empty() {
            let mut total = 0u64;
            for segment in &self.segments {
                total += segment.downloaded;
            }
            return Ok(total);
        }

        if self.accept_ranges && self.output.exists() {
            let meta = fs::metadata(&self.output)
                .with_context(|| format!("failed to stat output {path:?}", path = &self.output))?;
            return Ok(meta.len());
        }

        Ok(0)
    }

    async fn execute(&mut self) -> Result<()> {
        if self.accept_ranges && !self.segments.is_empty() {
            return self.execute_segmented().await;
        }
        self.execute_single().await
    }

    async fn execute_segmented(&mut self) -> Result<()> {
        if self.experimental_entropy {
            return self.execute_segmented_experimental().await;
        }
        let Some(total) = self.total_size else {
            return self.execute_single().await;
        };

        let output = self.output.clone();
        prepare_output(&output, total)?;

        let mut total_done = 0u64;
        for segment in &self.segments {
            total_done += segment.downloaded;
        }
        self.total_downloaded.store(total_done, Ordering::Relaxed);

        let state = DownloadState {
            url: self.url.to_string(),
            output: self.output.clone(),
            total_size: self.total_size,
            segments: self.segments.clone(),
            segment_size: self.segment_size,
            experimental_entropy: self.experimental_entropy,
        };
        let state = Arc::new(TokioMutex::new(state));
        let (save_tx, save_rx) = watch::channel(false);
        let saver = tokio::spawn(periodic_save(
            output.clone(),
            Arc::clone(&state),
            save_rx,
            Duration::from_secs(1),
        ));
        save_state(&output, &state).await?;

        let stop_rx = self.stop_rx.clone();
        let base_context = DownloadSegmentContext {
            client_factory: self.client_factory.clone(),
            url: self.url.clone(),
            output: output.clone(),
            state: Arc::clone(&state),
            stop_rx,
            slow_tracker: None,
            speed_limiter: self.speed_limiter.clone(),
            events: self.events.clone(),
            total_downloaded: Arc::clone(&self.total_downloaded),
            total_size: self.total_size,
        };
        let mut handles = Vec::new();
        let mut index = 0usize;
        while index < self.segments.len() {
            let segment = self.segments[index].clone();
            let context = base_context.clone();
            let handle = tokio::spawn(async move { download_segment(context, segment).await });
            handles.push(handle);
            index += 1;
        }

        let result = wait_with_ctrl_c(
            handles,
            Arc::clone(&state),
            self.output.clone(),
            self.stop_tx.clone(),
        )
        .await;
        let _ = save_tx.send(true);
        let save_result = saver.await;
        if result.is_ok() {
            match save_result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(eyre::eyre!("state saver task failed: {err}")),
            }
        }
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Some(stop_signal) = err.downcast_ref::<StopSignal>() {
                    match stop_signal {
                        StopSignal::None => {}
                        StopSignal::Pause => {
                            save_state(&output, &state).await?;
                            return Err(err);
                        }
                        StopSignal::Cancel => {
                            let _ = fs::remove_file(&output);
                            let state_path = state_path(&output)?;
                            if state_path.exists() {
                                let _ = fs::remove_file(state_path);
                            }
                            return Err(err);
                        }
                    }
                }
                if err.downcast_ref::<RangeUnsupported>().is_some() {
                    self.accept_ranges = false;
                    let _ = fs::remove_file(&output);
                    let state_path = state_path(&output)?;
                    if state_path.exists() {
                        let _ = fs::remove_file(state_path);
                    }
                    return self.execute_single().await;
                }
                Err(err)
            }
        }
    }

    async fn execute_single(&mut self) -> Result<()> {
        let output = self.output.clone();
        let mut start = 0u64;

        if output.exists() {
            let existing = output.metadata().map(|m| m.len()).unwrap_or(0);
            if self.accept_ranges {
                let downloaded = existing;
                start = existing;
                self.total_downloaded.store(downloaded, Ordering::Relaxed);
            } else {
                let _ = fs::remove_file(&output);
            }
        }

        ensure_parent_dir(&output)?;

        let state = DownloadState {
            url: self.url.to_string(),
            output: self.output.clone(),
            total_size: self.total_size,
            segments: Vec::new(),
            segment_size: self.segment_size,
            experimental_entropy: self.experimental_entropy,
        };
        let state = Arc::new(TokioMutex::new(state));
        let stop_rx = self.stop_rx.clone();

        let url = self.url.clone();
        let output = output.clone();
        let client_factory = self.client_factory.clone();
        let speed_limiter = self.speed_limiter.clone();
        let accept_ranges = self.accept_ranges;
        let events = self.events.clone();
        let total_downloaded = Arc::clone(&self.total_downloaded);
        let total_size = self.total_size;
        let handle = tokio::spawn(async move {
            let context = DownloadSingleContext {
                client_factory,
                url,
                output,
                stop_rx,
                speed_limiter,
                start,
                accept_ranges,
                events,
                total_downloaded,
                total_size,
            };
            download_single(context).await
        });

        wait_with_ctrl_c(
            vec![handle],
            state,
            self.output.clone(),
            self.stop_tx.clone(),
        )
        .await
    }

    async fn execute_segmented_experimental(&mut self) -> Result<()> {
        let Some(total) = self.total_size else {
            return self.execute_single().await;
        };

        let output = self.output.clone();
        prepare_output(&output, total)?;

        let mut total_done = 0u64;
        for segment in &self.segments {
            total_done += segment.downloaded;
        }
        self.total_downloaded.store(total_done, Ordering::Relaxed);

        let state = DownloadState {
            url: self.url.to_string(),
            output: self.output.clone(),
            total_size: self.total_size,
            segments: self.segments.clone(),
            segment_size: self.segment_size,
            experimental_entropy: self.experimental_entropy,
        };
        let state = Arc::new(TokioMutex::new(state));
        let (save_tx, save_rx) = watch::channel(false);
        let saver = tokio::spawn(periodic_save(
            output.clone(),
            Arc::clone(&state),
            save_rx,
            Duration::from_secs(1),
        ));
        save_state(&output, &state).await?;
        let slow_tracker = Arc::new(Mutex::new(SlowestTracker::new(self.connections)));
        let queue = Arc::new(TokioMutex::new(VecDeque::from(std::mem::take(
            &mut self.segments,
        ))));

        let stop_rx = self.stop_rx.clone();
        let base_context = DownloadSegmentContext {
            client_factory: self.client_factory.clone(),
            url: self.url.clone(),
            output: output.clone(),
            state: Arc::clone(&state),
            stop_rx,
            slow_tracker: Some(Arc::clone(&slow_tracker)),
            speed_limiter: self.speed_limiter.clone(),
            events: self.events.clone(),
            total_downloaded: Arc::clone(&self.total_downloaded),
            total_size: self.total_size,
        };
        let mut handles = Vec::new();
        let mut worker_id = 0usize;
        while worker_id < self.connections {
            let queue = Arc::clone(&queue);
            let base_context = base_context.clone();
            let handle = tokio::spawn(async move {
                loop {
                    let segment = {
                        let mut queue = queue.lock().await;
                        queue.pop_front()
                    };
                    let Some(segment) = segment else {
                        break;
                    };
                    let context = base_context.clone();
                    download_segment(context, segment).await?;
                }
                Ok(())
            });
            handles.push(handle);
            worker_id += 1;
        }

        let result = wait_with_ctrl_c(
            handles,
            Arc::clone(&state),
            self.output.clone(),
            self.stop_tx.clone(),
        )
        .await;
        let _ = save_tx.send(true);
        let saver_result = saver.await;
        if result.is_ok() {
            match saver_result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(eyre::eyre!("state saver task failed: {err}")),
            }
        }
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Some(stop_signal) = err.downcast_ref::<StopSignal>() {
                    match stop_signal {
                        StopSignal::None => {}
                        StopSignal::Pause => {
                            save_state(&output, &state).await?;
                            return Err(err);
                        }
                        StopSignal::Cancel => {
                            let _ = fs::remove_file(&output);
                            let state_path = state_path(&output)?;
                            if state_path.exists() {
                                let _ = fs::remove_file(state_path);
                            }
                            return Err(err);
                        }
                    }
                }
                if err.downcast_ref::<RangeUnsupported>().is_some() {
                    self.accept_ranges = false;
                    let _ = fs::remove_file(&output);
                    let state_path = state_path(&output)?;
                    if state_path.exists() {
                        let _ = fs::remove_file(state_path);
                    }
                    return self.execute_single().await;
                }
                Err(err)
            }
        }
    }
}

async fn download_inner(
    request: DownloadRequest,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    stop_tx: watch::Sender<StopSignal>,
    stop_rx: watch::Receiver<StopSignal>,
) -> Result<DownloadSummary> {
    let DownloadRequest {
        url,
        output,
        output_dir,
        connections,
        user_agent,
        limit,
        experimental_entropy,
    } = request;

    let url = Url::parse(&url).context("invalid url")?;
    let client_factory = ClientFactory::new(user_agent.as_deref(), experimental_entropy)?;
    let client = client_factory.client()?;
    let meta = http::probe(&client, &url).await?;
    let speed_limiter = limit.map(SpeedLimiter::new).map(Arc::new);

    let output = determine_output(&url, &meta, output, output_dir)?;
    let connections = if connections == 0 { 1 } else { connections };

    let state_path = state_path(&output)?;
    let mut resume_state = None;
    if state_path.exists() && output.exists() {
        resume_state = load_state(&state_path).ok();
    }

    let total_downloaded = Arc::new(AtomicU64::new(0));
    let config = DownloadPlanConfig {
        url,
        output: output.clone(),
        meta,
        connections,
        resume: resume_state,
        experimental_entropy,
        client_factory,
        speed_limiter,
        events: events.clone(),
        total_downloaded: Arc::clone(&total_downloaded),
        stop_tx,
        stop_rx,
    };
    let mut download = DownloadPlan::new(config)?;
    let already_downloaded = download.already_downloaded()?;
    total_downloaded.store(already_downloaded, Ordering::Relaxed);

    send_event(
        &events,
        DownloadEvent::Started {
            output: output.clone(),
            total_bytes: download.total_size,
            resumed_bytes: already_downloaded,
        },
    );
    info!("download started");

    if output.exists()
        && let Some(total) = download.total_size
        && already_downloaded >= total
    {
        if state_path.exists() {
            let _ = fs::remove_file(state_path);
        }
        let summary = DownloadSummary {
            output: download.output.clone(),
            total_bytes: total,
            downloaded_bytes: total,
            resumed_bytes: total,
            elapsed: Duration::from_secs(0),
        };
        send_event(
            &events,
            DownloadEvent::Finished {
                summary: summary.clone(),
            },
        );
        info!("download finished");
        return Ok(summary);
    }

    let started = Instant::now();
    let result = download.execute().await;
    let elapsed = started.elapsed();

    match result {
        Ok(()) => {
            if state_path.exists() {
                let _ = fs::remove_file(state_path);
            }
            let meta = fs::metadata(&download.output).with_context(|| {
                format!("failed to stat output {path:?}", path = &download.output)
            })?;
            let summary = DownloadSummary {
                output: download.output.clone(),
                total_bytes: meta.len(),
                downloaded_bytes: meta.len(),
                resumed_bytes: already_downloaded,
                elapsed,
            };
            send_event(
                &events,
                DownloadEvent::Finished {
                    summary: summary.clone(),
                },
            );
            info!("download finished");
            Ok(summary)
        }
        Err(err) => {
            if let Some(stop_signal) = err.downcast_ref::<StopSignal>() {
                match stop_signal {
                    StopSignal::None => {}
                    StopSignal::Pause => {}
                    StopSignal::Cancel => {
                        let _ = fs::remove_file(&download.output);
                        if state_path.exists() {
                            let _ = fs::remove_file(state_path);
                        }
                    }
                }
            }
            let message = err.to_string();
            send_event(&events, DownloadEvent::Failed { message });
            warn!("download failed");
            Err(err)
        }
    }
}

fn send_event(events: &Option<mpsc::UnboundedSender<DownloadEvent>>, event: DownloadEvent) {
    let Some(sender) = events.as_ref() else {
        return;
    };
    let _ = sender.send(event);
}

fn infer_filename(url: &Url, meta: &HttpMeta) -> String {
    if let Some(name) = &meta.filename {
        return name.clone();
    }

    let path = url.path();
    if let Some(last) = path.rsplit('/').next()
        && !last.is_empty()
    {
        return last.to_string();
    }

    "download".to_string()
}

fn determine_output(
    url: &Url,
    meta: &HttpMeta,
    output: Option<PathBuf>,
    output_dir: Option<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = output {
        if path.is_dir() {
            return Err(eyre::eyre!("output path is a directory"));
        }
        return Ok(path);
    }

    let filename = infer_filename(url, meta);
    if let Some(dir) = output_dir {
        return Ok(dir.join(filename));
    }

    Ok(PathBuf::from(filename))
}

fn prepare_output(path: &Path, total: u64) -> Result<()> {
    ensure_parent_dir(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open output file {path:?}"))?;
    file.set_len(total)
        .with_context(|| format!("failed to allocate output file {path:?}"))?;
    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create output dir {parent:?}"))?;
    Ok(())
}

fn state_path(output: &Path) -> Result<PathBuf> {
    let Some(name) = output.file_name() else {
        return Err(eyre::eyre!("output file name missing"));
    };
    let mut name = name.to_os_string();
    name.push(".arkama.state");
    Ok(output.with_file_name(name))
}

fn load_state(path: &Path) -> Result<DownloadState> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read state {path:?}"))?;
    let state = serde_json::from_str(&data).with_context(|| format!("invalid state {path:?}"))?;
    Ok(state)
}

async fn save_state(output: &Path, state: &TokioMutex<DownloadState>) -> Result<()> {
    let path = state_path(output)?;
    let state = state.lock().await;
    let data = serde_json::to_string_pretty(&*state).context("failed to serialize state")?;
    tokio::fs::write(&path, data)
        .await
        .with_context(|| format!("failed to write state {path:?}"))?;
    Ok(())
}

async fn update_state(state: &TokioMutex<DownloadState>, updated: &Segment) -> Result<()> {
    let mut state = state.lock().await;
    if let Some(segment) = state.segments.get_mut(updated.id)
        && segment.id == updated.id
    {
        segment.downloaded = updated.downloaded;
        return Ok(());
    }

    let mut index = 0usize;
    while index < state.segments.len() {
        if state.segments[index].id == updated.id {
            state.segments[index].downloaded = updated.downloaded;
            break;
        }
        index += 1;
    }
    Ok(())
}

async fn periodic_save(
    output: PathBuf,
    state: Arc<TokioMutex<DownloadState>>,
    mut stop_rx: watch::Receiver<bool>,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                save_state(&output, &state).await?;
            }
        }
    }
}

async fn wait_with_ctrl_c(
    handles: Vec<JoinHandle<Result<()>>>,
    state: Arc<TokioMutex<DownloadState>>,
    output: PathBuf,
    stop_tx: watch::Sender<StopSignal>,
) -> Result<()> {
    let mut handles = handles;
    tokio::select! {
        result = wait_all(handles.drain(..)) => result,
        _ = tokio::signal::ctrl_c() => {
            let _ = stop_tx.send(StopSignal::Pause);
            let _ = wait_all(handles.drain(..)).await;
            save_state(&output, &state).await?;
            Err(eyre::Report::new(StopSignal::Pause))
        }
    }
}

async fn wait_all(handles: std::vec::Drain<'_, JoinHandle<Result<()>>>) -> Result<()> {
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(eyre::eyre!("task join failed: {err}")),
        }
    }
    Ok(())
}

async fn download_segment(context: DownloadSegmentContext, mut segment: Segment) -> Result<()> {
    let DownloadSegmentContext {
        client_factory,
        url,
        output,
        state,
        mut stop_rx,
        slow_tracker,
        speed_limiter,
        events,
        total_downloaded,
        total_size,
    } = context;
    let Some(mut start) = segment.remaining_start() else {
        return Ok(());
    };

    let mut attempts = 0u32;
    loop {
        let stop_signal = *stop_rx.borrow();
        if stop_signal != StopSignal::None {
            return Err(eyre::Report::new(stop_signal));
        }
        let client = client_factory.client()?;
        let started = Instant::now();
        let response = http::get_range(&client, &url, start, Some(segment.end)).await;
        let response = match response {
            Ok(resp) => resp,
            Err(err) => {
                attempts += 1;
                if attempts > 5 {
                    return Err(err);
                }
                let backoff = backoff_delay(attempts);
                tokio::time::sleep(backoff).await;
                continue;
            }
        };

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(eyre::eyre!(RangeUnsupported));
        }

        let file = AsyncOpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&output)
            .await
            .with_context(|| format!("failed to open output file {output:?}"))?;
        let mut file = file;
        let mut offset = start;
        let mut stream = response.bytes_stream();

        let mut recycle = false;
        let mut pending_state_sync = 0u64;
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    let stop_signal = *stop_rx.borrow();
                    if stop_signal != StopSignal::None {
                        if pending_state_sync > 0 {
                            update_state(&state, &segment).await?;
                        }
                        return Err(eyre::Report::new(stop_signal));
                    }
                }
                next = stream.next() => {
                    match next {
                        Some(Ok(bytes)) => {
                            if let Some(speed_limiter) = speed_limiter.as_ref() {
                                speed_limiter.throttle(bytes.len() as u64).await;
                            }
                            file.seek(SeekFrom::Start(offset)).await?;
                            file.write_all(&bytes).await?;
                            let len = bytes.len() as u64;
                            offset += len;
                            segment.downloaded += len;
                            pending_state_sync += len;
                            let downloaded = total_downloaded.fetch_add(len, Ordering::Relaxed) + len;
                            send_event(
                                &events,
                                DownloadEvent::Progress {
                                    downloaded_bytes: downloaded,
                                    total_bytes: total_size,
                                },
                            );
                            if pending_state_sync >= STATE_SYNC_BATCH_BYTES {
                                update_state(&state, &segment).await?;
                                pending_state_sync = 0;
                            }
                            if let Some(slow_tracker) = slow_tracker.as_ref() {
                                let elapsed = started.elapsed();
                                if elapsed > Duration::from_secs(1) {
                                    let should_recycle = {
                                        let tracker = slow_tracker.lock().unwrap();
                                        tracker.should_recycle(elapsed)
                                    };
                                    if should_recycle {
                                        if pending_state_sync > 0 {
                                            update_state(&state, &segment).await?;
                                        }
                                        recycle = true;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(err)) => {
                            if pending_state_sync > 0 {
                                update_state(&state, &segment).await?;
                            }
                            attempts += 1;
                            if attempts > 5 {
                                return Err(eyre::eyre!("segment download failed: {err}"));
                            }
                            let backoff = backoff_delay(attempts);
                            tokio::time::sleep(backoff).await;
                            start = segment.start + segment.downloaded;
                            break;
                        }
                        None => {
                            if pending_state_sync > 0 {
                                update_state(&state, &segment).await?;
                            }
                            let expected = segment.end.saturating_sub(segment.start) + 1;
                            if segment.downloaded < expected {
                                attempts += 1;
                                if attempts > 5 {
                                    return Err(eyre::eyre!("segment ended early"));
                                }
                                let backoff = backoff_delay(attempts);
                                tokio::time::sleep(backoff).await;
                                start = segment.start + segment.downloaded;
                                break;
                            }
                            if let Some(slow_tracker) = slow_tracker.as_ref() {
                                let elapsed = started.elapsed();
                                let mut tracker = slow_tracker.lock().unwrap();
                                tracker.record(elapsed);
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
        if recycle {
            attempts += 1;
            if attempts > 5 {
                return Err(eyre::eyre!("segment recycling exceeded attempts"));
            }
            start = segment.start + segment.downloaded;
            continue;
        }
    }
}

async fn download_single(context: DownloadSingleContext) -> Result<()> {
    let DownloadSingleContext {
        client_factory,
        url,
        output,
        mut stop_rx,
        speed_limiter,
        start,
        mut accept_ranges,
        events,
        total_downloaded,
        total_size,
    } = context;
    let mut attempts = 0u32;
    let mut offset = start;
    ensure_parent_dir(&output)?;
    loop {
        let stop_signal = *stop_rx.borrow();
        if stop_signal != StopSignal::None {
            return Err(eyre::Report::new(stop_signal));
        }

        let client = client_factory.client()?;
        let response = if offset > 0 {
            http::get_range(&client, &url, offset, None).await
        } else {
            http::get_full(&client, &url).await
        };
        let response = match response {
            Ok(resp) => resp,
            Err(err) => {
                attempts += 1;
                if attempts > 5 {
                    return Err(err);
                }
                let backoff = backoff_delay(attempts);
                tokio::time::sleep(backoff).await;
                continue;
            }
        };

        if offset > 0 && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            if accept_ranges {
                let _ = fs::remove_file(&output);
                offset = 0;
                total_downloaded.store(0, Ordering::Relaxed);
                accept_ranges = false;
                continue;
            }
            let _ = fs::remove_file(&output);
            offset = 0;
            total_downloaded.store(0, Ordering::Relaxed);
            continue;
        }

        let file = AsyncOpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&output)
            .await
            .with_context(|| format!("failed to open output file {output:?}"))?;
        let mut file = file;
        let mut stream = response.bytes_stream();

        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    let stop_signal = *stop_rx.borrow();
                    if stop_signal != StopSignal::None {
                        return Err(eyre::Report::new(stop_signal));
                    }
                }
                next = stream.next() => {
                    match next {
                        Some(Ok(bytes)) => {
                            if let Some(speed_limiter) = speed_limiter.as_ref() {
                                speed_limiter.throttle(bytes.len() as u64).await;
                            }
                            file.seek(SeekFrom::Start(offset)).await?;
                            file.write_all(&bytes).await?;
                            let len = bytes.len() as u64;
                            offset += len;
                            let downloaded = total_downloaded.fetch_add(len, Ordering::Relaxed) + len;
                            send_event(
                                &events,
                                DownloadEvent::Progress {
                                    downloaded_bytes: downloaded,
                                    total_bytes: total_size,
                                },
                            );
                        }
                        Some(Err(err)) => {
                            attempts += 1;
                            if attempts > 5 {
                                return Err(eyre::eyre!("download failed: {err}"));
                            }
                            if !accept_ranges {
                                let _ = fs::remove_file(&output);
                                offset = 0;
                                total_downloaded.store(0, Ordering::Relaxed);
                            }
                            let backoff = backoff_delay(attempts);
                            tokio::time::sleep(backoff).await;
                            break;
                        }
                        None => {
                            let downloaded = offset.saturating_sub(start);
                            if let Some(total) = total_size {
                                let expected = total.saturating_sub(start);
                                if downloaded < expected {
                                    attempts += 1;
                                    if attempts > 5 {
                                        return Err(eyre::eyre!("download ended early"));
                                    }
                                    let backoff = backoff_delay(attempts);
                                    tokio::time::sleep(backoff).await;
                                    break;
                                }
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

fn backoff_delay(attempt: u32) -> Duration {
    let max = 10_000u64;
    let base = 500u64;
    let pow = 1u64 << min(attempt, 5);
    let delay = base * pow;
    Duration::from_millis(min(delay, max))
}

#[cfg(test)]
mod tests {
    use super::{
        DownloadEvent, DownloadState, HttpMeta, backoff_delay, determine_output, load_state,
        periodic_save, send_event,
    };
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
