//! Fixed-cost transfer measurements and an optional dedicated diagnostic collector.

use crate::recovery::RecoveryReason;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

/// Inclusive durations. Worker timings sum across workers, not wall-clock phases.
#[derive(Clone, Copy)]
pub(crate) enum Phase {
    Setup = 0,
    Transfer = 1,
    Retry = 2,
    Throttle = 3,
    DiskWrite = 4,
    #[cfg(feature = "serde")]
    Checkpoint = 5,
    Verification = 6,
    Publication = 7,
}

const PHASES: [&str; 8] = [
    "setup",
    "transfer",
    "retry_wait",
    "throttle_wait",
    "disk_write",
    "checkpoint",
    "verification",
    "publication",
];

struct Counters {
    started: Instant,
    pub(crate) written: Arc<AtomicU64>,
    received: AtomicU64,
    disk_written: AtomicU64,
    crash_safe: AtomicU64,
    retries: AtomicU64,
    recovery_reason: AtomicU64,
    retry_delay_ms: AtomicU64,
    network_stalls: AtomicU64,
    active_workers: AtomicU64,
    pending_bytes: AtomicU64,
    dropped: AtomicU64,
    suppressed: AtomicU64,
    outcome: AtomicU64,
    durations: [AtomicU64; 8],
    metadata: OnceLock<DownloadMetadata>,
}

/// Resolved output and representation metadata, independent of optional event delivery.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct DownloadMetadata {
    pub output: PathBuf,
    pub total_bytes: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub mime_type: Option<String>,
    pub final_url: String,
}

/// Current state independent of optional event and diagnostic readers.
#[derive(Clone)]
pub struct DownloadObservation(Arc<Counters>);

/// Measurements contain no URLs, headers, cookies, filenames, or error strings.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
#[derive(Clone, Debug, Default)]
pub struct ObservationSnapshot {
    /// Age since this attempt was created, including time retained after completion.
    pub elapsed_ms: u64,
    /// Current accepted written coverage, including resumed bytes; can decrease on fallback.
    pub written_bytes: u64,
    /// Body bytes delivered by HTTP, including duplicate/retry traffic, excluding headers.
    pub received_bytes: u64,
    /// Bytes written during this attempt, including overwritten retry data.
    pub disk_written_bytes: u64,
    /// Coverage in the last durably published checkpoint; zero without persisted state.
    pub crash_safe_bytes: u64,
    pub retries: u64,
    /// Most recent recovery reason, retained after completion. No source or credential data.
    pub last_recovery_reason: Option<String>,
    pub last_retry_delay_ms: u64,
    pub network_stalls: u64,
    pub active_workers: u64,
    /// Body bytes currently waiting for limiting or disk writes, not total process buffers.
    pub pending_bytes: u64,
    pub dropped_events: u64,
    pub suppressed_samples: u64,
    /// running, complete, paused, cancelled, or failed.
    pub outcome: String,
    /// Inclusive milliseconds; disk/throttle/retry values are summed worker time.
    pub durations_ms: BTreeMap<String, u64>,
}

impl DownloadObservation {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Counters {
            started: Instant::now(),
            written: Arc::new(AtomicU64::new(0)),
            received: AtomicU64::new(0),
            disk_written: AtomicU64::new(0),
            crash_safe: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            recovery_reason: AtomicU64::new(0),
            retry_delay_ms: AtomicU64::new(0),
            network_stalls: AtomicU64::new(0),
            active_workers: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
            outcome: AtomicU64::new(0),
            durations: std::array::from_fn(|_| AtomicU64::new(0)),
            metadata: OnceLock::new(),
        }))
    }

    /// Reads counters without waiting for a collector or event reader.
    pub fn snapshot(&self) -> ObservationSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ObservationSnapshot {
            elapsed_ms: millis(self.0.started.elapsed()),
            written_bytes: load(&self.0.written),
            received_bytes: load(&self.0.received),
            disk_written_bytes: load(&self.0.disk_written),
            crash_safe_bytes: load(&self.0.crash_safe),
            retries: load(&self.0.retries),
            last_recovery_reason: RecoveryReason::label(load(&self.0.recovery_reason))
                .map(str::to_owned),
            last_retry_delay_ms: load(&self.0.retry_delay_ms),
            network_stalls: load(&self.0.network_stalls),
            active_workers: load(&self.0.active_workers),
            pending_bytes: load(&self.0.pending_bytes),
            dropped_events: load(&self.0.dropped),
            suppressed_samples: load(&self.0.suppressed),
            outcome: ["running", "complete", "paused", "cancelled", "failed"]
                [load(&self.0.outcome) as usize]
                .into(),
            durations_ms: PHASES
                .iter()
                .enumerate()
                .map(|(i, name)| ((*name).into(), load(&self.0.durations[i]) / 1_000_000))
                .collect(),
        }
    }

    /// Available once setup resolves and owns the output; retained after this attempt stops.
    /// Metadata is not included in credential-free numeric diagnostic snapshots.
    pub fn metadata(&self) -> Option<&DownloadMetadata> {
        self.0.metadata.get()
    }

    pub(crate) fn set_metadata(&self, metadata: DownloadMetadata) {
        let _ = self.0.metadata.set(metadata);
    }

    pub(crate) fn written(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.0.written)
    }
    pub(crate) fn received(&self, bytes: u64) {
        self.0.received.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn disk_written(&self, bytes: u64) {
        self.0.disk_written.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn checkpoint(&self, bytes: u64) {
        self.0.crash_safe.store(bytes, Ordering::Relaxed);
    }
    pub(crate) fn retry(&self, reason: RecoveryReason, delay: Duration) {
        self.0.retries.fetch_add(1, Ordering::Relaxed);
        self.0
            .recovery_reason
            .store(reason as u64, Ordering::Relaxed);
        self.0
            .retry_delay_ms
            .store(millis(delay), Ordering::Relaxed);
        if matches!(reason, RecoveryReason::ReadIdle) {
            self.0.network_stalls.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn dropped(&self) {
        self.0.dropped.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn suppressed(&self) {
        self.0.suppressed.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn finish(&self, outcome: u64) {
        self.0.outcome.store(outcome, Ordering::Relaxed);
    }

    pub(crate) fn timing(&self, phase: Phase) -> Timing {
        Timing {
            observation: self.clone(),
            phase,
            started: Instant::now(),
        }
    }

    pub(crate) fn worker(&self) -> WorkerActivity {
        self.0.active_workers.fetch_add(1, Ordering::Relaxed);
        WorkerActivity(self.clone())
    }

    pub(crate) fn pending(&self, bytes: u64) -> PendingBytes {
        self.0.pending_bytes.fetch_add(bytes, Ordering::Relaxed);
        PendingBytes(self.clone(), bytes)
    }
}

pub(crate) struct Timing {
    observation: DownloadObservation,
    phase: Phase,
    started: Instant,
}
impl Drop for Timing {
    fn drop(&mut self) {
        self.observation.0.durations[self.phase as usize].fetch_add(
            self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }
}
pub(crate) struct WorkerActivity(DownloadObservation);
impl Drop for WorkerActivity {
    fn drop(&mut self) {
        self.0.0.active_workers.fetch_sub(1, Ordering::Relaxed);
    }
}
pub(crate) struct PendingBytes(DownloadObservation, u64);
impl Drop for PendingBytes {
    fn drop(&mut self) {
        self.0.0.pending_bytes.fetch_sub(self.1, Ordering::Relaxed);
    }
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Collector retention and optional disk sampling. Invalid/unbounded settings are rejected.
#[derive(Clone, Debug)]
pub struct DiagnosticsConfig {
    pub interval: Duration,
    pub history_capacity: usize,
    /// Existing filesystem location to sample; never included in snapshots or exports.
    pub disk_path: Option<PathBuf>,
}
impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            history_capacity: 120,
            disk_path: None,
        }
    }
}

/// Unsmooth interval speed; gaps and coverage resets have no speed estimate.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct SpeedSample {
    pub elapsed_ms: u64,
    pub bytes_per_second: Option<u64>,
    pub gap: bool,
}

/// OS measurements are process-wide, not attributed to a particular job.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug, Default)]
pub struct DiagnosticsSnapshot {
    pub observation: ObservationSnapshot,
    pub history: VecDeque<SpeedSample>,
    pub collected_elapsed_ms: u64,
    pub collector_lag_ms: u64,
    pub evicted_samples: u64,
    pub sink_errors: u64,
    pub process_memory_bytes: Option<u64>,
    pub process_cpu_percent: Option<f32>,
    pub disk_available_bytes: Option<u64>,
    pub disk_allocated_bytes: Option<u64>,
}

/// Optional aggregation, OS sampling and sinks run on one dedicated OS thread.
/// Dropping this handle requests stop without waiting for a slow or stalled sink.
/// A sink must eventually return for its thread to exit. Downloads never call it.
pub struct Diagnostics {
    observation: DownloadObservation,
    snapshot: Arc<Mutex<DiagnosticsSnapshot>>,
    stop: mpsc::SyncSender<()>,
}
impl Diagnostics {
    pub fn start(observation: DownloadObservation, config: DiagnosticsConfig) -> io::Result<Self> {
        Self::with_sink(observation, config, |_| Ok(()))
    }

    pub fn with_sink(
        observation: DownloadObservation,
        config: DiagnosticsConfig,
        mut sink: impl FnMut(&DiagnosticsSnapshot) -> io::Result<()> + Send + 'static,
    ) -> io::Result<Self> {
        if config.interval < Duration::from_millis(50)
            || config.interval > Duration::from_secs(60)
            || !(1..=10_000).contains(&config.history_capacity)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid diagnostic interval or retention",
            ));
        }
        let snapshot = Arc::new(Mutex::new(DiagnosticsSnapshot::default()));
        let shared = Arc::clone(&snapshot);
        let counters = observation.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let (stop, receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("arkama-diagnostics".into())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    let mut system = sysinfo::System::new();
                    let pid = sysinfo::Pid::from_u32(std::process::id());
                    let mut current = DiagnosticsSnapshot::default();
                    let mut previous = counters.snapshot();
                    loop {
                        match receiver.recv_timeout(config.interval) {
                            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                            Err(mpsc::RecvTimeoutError::Timeout) => {}
                        }
                        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
                        let measured = counters.snapshot();
                        let delta = measured.elapsed_ms.saturating_sub(previous.elapsed_ms);
                        let gap = delta > millis(config.interval).saturating_mul(2);
                        let speed = measured
                            .written_bytes
                            .checked_sub(previous.written_bytes)
                            .filter(|_| delta > 0 && !gap)
                            .map(|bytes| bytes.saturating_mul(1000) / delta);
                        current.history.push_back(SpeedSample {
                            elapsed_ms: measured.elapsed_ms,
                            bytes_per_second: speed,
                            gap,
                        });
                        if current.history.len() > config.history_capacity {
                            current.history.pop_front();
                            current.evicted_samples += 1;
                        }
                        current.collected_elapsed_ms = measured.elapsed_ms;
                        if let Some(process) = system.process(pid) {
                            current.process_memory_bytes = Some(process.memory());
                            current.process_cpu_percent = Some(process.cpu_usage());
                        }
                        if let Some(path) = &config.disk_path {
                            current.disk_available_bytes = fs2::available_space(path).ok();
                            current.disk_allocated_bytes = allocated_bytes(path);
                        }
                        previous = measured.clone();
                        current.observation = measured;
                        *shared.lock().unwrap_or_else(|error| error.into_inner()) = current.clone();
                        tracing::info!(
                            written_bytes = current.observation.written_bytes,
                            received_bytes = current.observation.received_bytes,
                            retries = current.observation.retries,
                            outcome = %current.observation.outcome,
                            "download observation"
                        );
                        if sink(&current).is_err() {
                            current.sink_errors += 1;
                        }
                    }
                })
            })?;
        Ok(Self {
            observation,
            snapshot,
            stop,
        })
    }

    /// Copies bounded retained data. Formatting/export belongs to the host or collector sink.
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        let mut result = self
            .snapshot
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        result.collector_lag_ms = self
            .observation
            .snapshot()
            .elapsed_ms
            .saturating_sub(result.collected_elapsed_ms);
        result
    }
}
impl Drop for Diagnostics {
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
    }
}
fn allocated_bytes(path: &std::path::Path) -> Option<u64> {
    let metadata = path.metadata().ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(metadata.blocks().saturating_mul(512))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}
