use super::control::{StopSignal, wait_with_ctrl_c};
use super::output::{ensure_parent_dir, prepare_output};
use super::rate_limit::SpeedLimiter;
use super::state::{DownloadState, periodic_save, save_state, state_path};
use super::worker::{
    DownloadSegmentContext, DownloadSingleContext, RangeUnsupported, SlowestTracker,
    download_segment, download_single,
};
use crate::api::DownloadEvent;
use crate::http::{ClientFactory, HttpMeta};
use crate::segment::{Segment, build_segments, build_segments_with_chunk_size};
use eyre::{Context, Result};
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};
use url::Url;

const EXPERIMENTAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024;

pub(crate) struct DownloadPlan {
    pub(crate) url: Url,
    pub(crate) output: PathBuf,
    pub(crate) total_size: Option<u64>,
    accept_ranges: bool,
    segments: Vec<Segment>,
    connections: usize,
    segment_size: Option<u64>,
    etag: Option<String>,
    last_modified: Option<String>,
    mime_type: Option<String>,
    final_url: String,
    if_range: Option<String>,
    experimental_entropy: bool,
    client_factory: ClientFactory,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
    stop_tx: watch::Sender<StopSignal>,
    stop_rx: watch::Receiver<StopSignal>,
}

pub(crate) struct DownloadPlanConfig {
    pub(crate) url: Url,
    pub(crate) output: PathBuf,
    pub(crate) meta: HttpMeta,
    pub(crate) connections: usize,
    pub(crate) resume: Option<DownloadState>,
    pub(crate) experimental_entropy: bool,
    pub(crate) client_factory: ClientFactory,
    pub(crate) speed_limiter: Option<Arc<SpeedLimiter>>,
    pub(crate) events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    pub(crate) total_downloaded: Arc<AtomicU64>,
    pub(crate) stop_tx: watch::Sender<StopSignal>,
    pub(crate) stop_rx: watch::Receiver<StopSignal>,
}

impl DownloadPlan {
    pub(crate) fn new(config: DownloadPlanConfig) -> Result<Self> {
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
        let etag = meta.etag.clone();
        let last_modified = meta.last_modified.clone();
        let mime_type = meta.mime_type.clone();
        let final_url = meta.final_url.clone();
        let if_range = etag.clone().or_else(|| last_modified.clone());

        let mut segments = Vec::new();
        let output_exists = output.exists();

        if output_exists
            && let Some(state) = resume
            && state.url == url.as_str()
            && state.total_size == total_size
            && state.etag == etag
            && state.last_modified == last_modified
            && state.mime_type == mime_type
            && state.final_url.as_deref() == Some(final_url.as_str())
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
            etag,
            last_modified,
            mime_type,
            final_url,
            if_range,
            experimental_entropy,
            client_factory,
            speed_limiter,
            events,
            total_downloaded,
            stop_tx,
            stop_rx,
        })
    }

    pub(crate) fn already_downloaded(&self) -> Result<u64> {
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

    pub(crate) async fn execute(&mut self) -> Result<()> {
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
            etag: self.etag.clone(),
            last_modified: self.last_modified.clone(),
            mime_type: self.mime_type.clone(),
            final_url: Some(self.final_url.clone()),
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
            if_range: self.if_range.clone(),
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
        self.handle_segmented_result(result, &output, &state).await
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
            etag: self.etag.clone(),
            last_modified: self.last_modified.clone(),
            mime_type: self.mime_type.clone(),
            final_url: Some(self.final_url.clone()),
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
        let if_range = self.if_range.clone();
        let handle = tokio::spawn(async move {
            let context = DownloadSingleContext {
                client_factory,
                url,
                output,
                stop_rx,
                speed_limiter,
                if_range,
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
            etag: self.etag.clone(),
            last_modified: self.last_modified.clone(),
            mime_type: self.mime_type.clone(),
            final_url: Some(self.final_url.clone()),
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
            if_range: self.if_range.clone(),
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
        self.handle_segmented_result(result, &output, &state).await
    }

    async fn handle_segmented_result(
        &mut self,
        result: Result<()>,
        output: &PathBuf,
        state: &Arc<TokioMutex<DownloadState>>,
    ) -> Result<()> {
        match result {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Some(stop_signal) = err.downcast_ref::<StopSignal>() {
                    match stop_signal {
                        StopSignal::None => {}
                        StopSignal::Pause => {
                            save_state(output, state).await?;
                            return Err(err);
                        }
                        StopSignal::Cancel => {
                            let _ = fs::remove_file(output);
                            let state_path = state_path(output)?;
                            if state_path.exists() {
                                let _ = fs::remove_file(state_path);
                            }
                            return Err(err);
                        }
                    }
                }
                if err.downcast_ref::<RangeUnsupported>().is_some() {
                    self.accept_ranges = false;
                    let _ = fs::remove_file(output);
                    let state_path = state_path(output)?;
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
