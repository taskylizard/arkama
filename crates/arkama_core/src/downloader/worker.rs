use super::control::StopSignal;
use super::output::ensure_parent_dir;
use super::rate_limit::SpeedLimiter;
use super::send_event;
use super::state::{DownloadState, update_state};
use crate::api::DownloadEvent;
use crate::http::{self, ClientFactory};
use crate::segment::Segment;
use eyre::{Context, Result};
use futures_util::StreamExt;
use std::cmp::min;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions as AsyncOpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};
use url::Url;

pub(crate) const STATE_SYNC_BATCH_BYTES: u64 = 84 * 1024;

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
    pub(crate) if_range: Option<String>,
    pub(crate) speed_limiter: Option<Arc<SpeedLimiter>>,
    pub(crate) events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    pub(crate) total_downloaded: Arc<AtomicU64>,
    pub(crate) total_size: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct DownloadSingleContext {
    pub(crate) client_factory: ClientFactory,
    pub(crate) url: Url,
    pub(crate) output: PathBuf,
    pub(crate) stop_rx: watch::Receiver<StopSignal>,
    pub(crate) speed_limiter: Option<Arc<SpeedLimiter>>,
    pub(crate) if_range: Option<String>,
    pub(crate) start: u64,
    pub(crate) accept_ranges: bool,
    pub(crate) events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    pub(crate) total_downloaded: Arc<AtomicU64>,
    pub(crate) total_size: Option<u64>,
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
        if_range,
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
        let response =
            http::get_range(&client, &url, start, Some(segment.end), if_range.as_deref()).await;
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
                            file.flush().await?;
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
                                file.flush().await?;
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
                                            file.flush().await?;
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
                                file.flush().await?;
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
                            file.flush().await?;
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

pub(crate) async fn download_single(context: DownloadSingleContext) -> Result<()> {
    let DownloadSingleContext {
        client_factory,
        url,
        output,
        mut stop_rx,
        speed_limiter,
        if_range,
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

        let request_start = offset;
        let client = client_factory.client()?;
        let response = if accept_ranges && request_start > 0 {
            http::get_range(&client, &url, request_start, None, if_range.as_deref()).await
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

        if request_start > 0 && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
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
                        file.flush().await?;
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
                            file.flush().await?;
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
                            file.flush().await?;
                            let downloaded = offset.saturating_sub(request_start);
                            if let Some(total) = total_size {
                                let expected = total.saturating_sub(request_start);
                                if downloaded < expected {
                                    attempts += 1;
                                    if attempts > 5 {
                                        return Err(eyre::eyre!("download ended early"));
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
                            }
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn backoff_delay(attempt: u32) -> Duration {
    let max = 10_000u64;
    let base = 500u64;
    let pow = 1u64 << min(attempt, 5);
    let delay = base * pow;
    Duration::from_millis(min(delay, max))
}
