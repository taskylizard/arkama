use super::control::{StopSignal, controlled};
use super::output::ensure_parent_dir;
use super::rate_limit::SpeedLimiter;
use super::state::{DownloadState, update_state};
use super::{EventSender, send_event};
use crate::DownloadEvent;
use crate::DownloadObservation;
use crate::http::{self, ClientFactory, HttpMeta};
use crate::observation::Phase;
use crate::segment::Segment;
use eyre::{Context, Result};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions as AsyncOpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex as TokioMutex, watch};
use url::Url;

#[derive(Debug)]
pub(crate) struct RangeUnsupported;

impl fmt::Display for RangeUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server does not support range requests")
    }
}

impl Error for RangeUnsupported {}

pub(crate) struct SlowestTracker {
    target_concurrency: usize,
    timings: BTreeSet<Duration>,
}

impl SlowestTracker {
    pub(crate) fn new(target_concurrency: usize) -> Self {
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

#[derive(Clone)]
pub(crate) struct DownloadSegmentContext {
    pub(crate) client_factory: ClientFactory,
    pub(crate) url: Url,
    pub(crate) output: PathBuf,
    pub(crate) state: Arc<TokioMutex<DownloadState>>,
    pub(crate) stop_rx: watch::Receiver<StopSignal>,
    pub(crate) slow_tracker: Option<Arc<Mutex<SlowestTracker>>>,
    pub(crate) meta: HttpMeta,
    pub(crate) speed_limiter: Option<Arc<SpeedLimiter>>,
    pub(crate) events: Option<Arc<EventSender>>,
    pub(crate) total_downloaded: Arc<AtomicU64>,
    pub(crate) total_size: Option<u64>,
    pub(crate) observation: DownloadObservation,
}

#[derive(Clone)]
pub(crate) struct DownloadSingleContext {
    pub(crate) client_factory: ClientFactory,
    pub(crate) url: Url,
    pub(crate) output: PathBuf,
    pub(crate) stop_rx: watch::Receiver<StopSignal>,
    pub(crate) speed_limiter: Option<Arc<SpeedLimiter>>,
    pub(crate) events: Option<Arc<EventSender>>,
    pub(crate) total_downloaded: Arc<AtomicU64>,
    pub(crate) total_size: Option<u64>,
    pub(crate) observation: DownloadObservation,
}

pub(crate) async fn download_segment(
    context: DownloadSegmentContext,
    mut segment: Segment,
) -> Result<()> {
    let DownloadSegmentContext {
        client_factory,
        url,
        output,
        state,
        mut stop_rx,
        slow_tracker,
        meta,
        speed_limiter,
        events,
        total_downloaded,
        total_size,
        observation,
    } = context;
    let _worker = observation.worker();
    let Some(mut start) = segment.remaining_start() else {
        return Ok(());
    };

    let mut attempts = 0u32;
    loop {
        let stop_signal = *stop_rx.borrow();
        if stop_signal != StopSignal::None {
            return Err(eyre::Report::new(stop_signal));
        }
        let started = Instant::now();
        let response = controlled(
            &mut stop_rx,
            http::get_range(
                &client_factory,
                &url,
                start,
                Some(segment.end),
                meta.strong_etag(),
            ),
        )
        .await;
        let response = match response {
            Ok(resp) => resp,
            Err(err) => {
                wait_for_retry(
                    &client_factory,
                    &mut stop_rx,
                    &mut attempts,
                    err,
                    &observation,
                )
                .await?;
                continue;
            }
        };

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(eyre::eyre!(RangeUnsupported));
        }
        http::validate_range(&response, start, segment.end, &meta)?;

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
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    let stop_signal = *stop_rx.borrow();
                    if stop_signal != StopSignal::None {
                        file.flush().await?;
                        return Err(eyre::Report::new(stop_signal));
                    }
                }
                next = http::next_chunk(&mut stream, client_factory.recovery.read_idle_timeout_ms) => {
                    match next {
                        Some(Ok(bytes)) => {
                            observation.received(bytes.len() as u64);
                            let _pending = observation.pending(bytes.len() as u64);
                            if bytes.len() as u64 > segment.end + 1 - offset {
                                return Err(eyre::eyre!("range body exceeds requested bounds"));
                            }
                            {
                                let _throttle = observation.timing(Phase::Throttle);
                                controlled(&mut stop_rx, async { client_factory.limits.throttle(bytes.len() as u64, speed_limiter.as_deref()).await; Ok(()) }).await?;
                            }
                            {
                                let _disk = observation.timing(Phase::DiskWrite);
                                file.seek(SeekFrom::Start(offset)).await?;
                                file.write_all(&bytes).await?;
                                file.flush().await?;
                            }
                            let len = bytes.len() as u64;
                            observation.disk_written(len);
                            offset += len;
                            segment.downloaded += len;
                            // Don't checkpoint complete coverage until the exact response ends.
                            if offset <= segment.end {
                                update_state(&state, &segment).await?;
                            }
                            let downloaded = total_downloaded.fetch_add(len, Ordering::Relaxed) + len;
                            send_event(
                                &events,
                                DownloadEvent::Progress {
                                    downloaded_bytes: downloaded,
                                    total_bytes: total_size,
                                },
                            );
                            if let Some(slow_tracker) = slow_tracker.as_ref() {
                                let elapsed = started.elapsed();
                                if elapsed > Duration::from_secs(1) {
                                    let should_recycle = {
                                        let tracker = slow_tracker.lock().unwrap();
                                        tracker.should_recycle(elapsed)
                                    };
                                    if should_recycle && offset <= segment.end {
                                        recycle = true;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(err)) => {
                            file.flush().await?;
                            if offset > segment.end {
                                let saved = state.lock().await.segments[segment.id].downloaded;
                                total_downloaded.fetch_sub(segment.downloaded - saved, Ordering::Relaxed);
                                segment.downloaded = saved;
                            }
                            wait_for_retry(&client_factory, &mut stop_rx, &mut attempts, err, &observation).await?;
                            start = segment.start + segment.downloaded;
                            break;
                        }
                        None => {
                            file.flush().await?;
                            let expected = segment.end.saturating_sub(segment.start) + 1;
                            if segment.downloaded < expected {
                                wait_for_retry(&client_factory, &mut stop_rx, &mut attempts, http::HttpFailure::ShortBody.into(), &observation).await?;
                                start = segment.start + segment.downloaded;
                                break;
                            }
                            update_state(&state, &segment).await?;
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
            if attempts > client_factory.recovery.max_retries {
                return Err(eyre::eyre!("segment recycling exceeded attempts"));
            }
            start = segment.start + segment.downloaded;
            continue;
        }
    }
}

pub(crate) async fn download_single(context: DownloadSingleContext) -> Result<()> {
    let DownloadSingleContext {
        client_factory,
        url,
        output,
        mut stop_rx,
        speed_limiter,
        events,
        total_downloaded,
        total_size,
        observation,
    } = context;
    let _worker = observation.worker();
    let mut attempts = 0u32;
    ensure_parent_dir(&output)?;
    loop {
        let stop_signal = *stop_rx.borrow();
        if stop_signal != StopSignal::None {
            return Err(eyre::Report::new(stop_signal));
        }

        let response = controlled(&mut stop_rx, http::get_full(&client_factory, &url)).await;
        let response = match response {
            Ok(resp) => resp,
            Err(err) => {
                wait_for_retry(
                    &client_factory,
                    &mut stop_rx,
                    &mut attempts,
                    err,
                    &observation,
                )
                .await?;
                continue;
            }
        };

        http::validate_identity_encoding(&response)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(eyre::eyre!("full download requires status 200"));
        }
        let expected = response.content_length().or(total_size);
        let mut offset = 0;
        total_downloaded.store(0, Ordering::Relaxed);

        let file = AsyncOpenOptions::new()
            .create(true)
            .truncate(true)
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
                        file.flush().await?;
                        return Err(eyre::Report::new(stop_signal));
                    }
                }
                next = http::next_chunk(&mut stream, client_factory.recovery.read_idle_timeout_ms) => {
                    match next {
                        Some(Ok(bytes)) => {
                            let len = bytes.len() as u64;
                            observation.received(len);
                            let _pending = observation.pending(len);
                            if expected.is_some_and(|total| len > total.saturating_sub(offset)) {
                                return Err(eyre::eyre!("full response exceeds expected length"));
                            }
                            {
                                let _throttle = observation.timing(Phase::Throttle);
                                controlled(&mut stop_rx, async { client_factory.limits.throttle(bytes.len() as u64, speed_limiter.as_deref()).await; Ok(()) }).await?;
                            }
                            {
                                let _disk = observation.timing(Phase::DiskWrite);
                                file.seek(SeekFrom::Start(offset)).await?;
                                file.write_all(&bytes).await?;
                                file.flush().await?;
                            }
                            observation.disk_written(len);
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
                            file.flush().await?;
                            wait_for_retry(&client_factory, &mut stop_rx, &mut attempts, err, &observation).await?;
                            break;
                        }
                        None => {
                            file.flush().await?;
                            if let Some(expected) = expected {
                                if offset != expected {
                                    wait_for_retry(&client_factory, &mut stop_rx, &mut attempts, http::HttpFailure::ShortBody.into(), &observation).await?;
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

pub(super) async fn wait_for_retry(
    factory: &ClientFactory,
    stop_rx: &mut watch::Receiver<StopSignal>,
    attempts: &mut u32,
    error: eyre::Report,
    observation: &DownloadObservation,
) -> Result<()> {
    *attempts += 1;
    let (reason, delay) = factory.retry_delay(*attempts, error)?;
    observation.retry(reason, delay);
    let _retry = observation.timing(Phase::Retry);
    controlled(stop_rx, async {
        tokio::time::sleep(delay).await;
        Ok(())
    })
    .await
}
