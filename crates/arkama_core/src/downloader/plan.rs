use super::control::{StopSignal, wait_workers};
use super::output::{prepare_output, remove_if_exists};
use super::rate_limit::SpeedLimiter;
use super::state::{DownloadState, save_state, state_path};
use super::worker::{
    DownloadSegmentContext, DownloadSingleContext, RangeUnsupported, SlowestTracker,
    download_segment, download_single,
};
use crate::api::DownloadEvent;
use crate::http::{ClientFactory, HttpMeta};
use crate::segment::{Segment, build_segments, build_segments_with_chunk_size};
use eyre::Result;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as TokioMutex, mpsc, watch};
use tokio::task::JoinSet;
use url::Url;

const EXPERIMENTAL_SEGMENT_SIZE: u64 = 4 * 1024 * 1024;

pub(crate) struct DownloadPlan {
    pub(crate) output: PathBuf,
    pub(crate) total_size: Option<u64>,
    url: Url,
    meta: HttpMeta,
    state: Arc<TokioMutex<DownloadState>>,
    resumed_bytes: u64,
    connections: usize,
    client_factory: ClientFactory,
    speed_limiter: Option<Arc<SpeedLimiter>>,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    total_downloaded: Arc<AtomicU64>,
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
            stop_rx,
        } = config;
        let segment_size = experimental_entropy.then_some(EXPERIMENTAL_SEGMENT_SIZE);
        // Multiple requests can be joined only with a stable representation identity.
        let ranged = meta.accept_ranges && meta.strong_etag().is_some() && meta.size.is_some();
        let mut segments = if ranged {
            let total = meta.size.expect("known size");
            match segment_size {
                Some(size) => build_segments_with_chunk_size(total, size),
                None => build_segments(total, connections),
            }
        } else {
            Vec::new()
        };
        let mut resumed_bytes = 0;
        if let Some(saved) = resume {
            if ranged
                && saved.version == 1
                && saved.url == url.as_str()
                && saved.output == output
                && saved.total_size == meta.size
                && saved.etag == meta.etag
                && saved.final_url.as_deref() == Some(meta.final_url.as_str())
                && saved.experimental_entropy == experimental_entropy
                && saved.segment_size == segment_size
                && output.metadata().map(|m| m.len()).ok() == meta.size
                && valid_segments(&saved.segments, meta.size.expect("known size"))
            {
                segments = saved.segments;
                resumed_bytes = segments.iter().map(|s| s.downloaded).sum();
            }
        }
        if resumed_bytes == 0 {
            // Invalidate durable positions BEFORE any previously saved byte is overwritten.
            remove_if_exists(&state_path(&output)?)?;
            prepare_output(&output, 0)?;
        }
        let state = DownloadState {
            version: 1,
            url: url.to_string(),
            output: output.clone(),
            total_size: meta.size,
            etag: meta.etag.clone(),
            last_modified: meta.last_modified.clone(),
            mime_type: meta.mime_type.clone(),
            final_url: Some(meta.final_url.clone()),
            segments,
            segment_size,
            experimental_entropy,
        };
        Ok(Self {
            output,
            total_size: meta.size,
            url,
            meta,
            state: Arc::new(TokioMutex::new(state)),
            resumed_bytes,
            connections,
            client_factory,
            speed_limiter,
            events,
            total_downloaded,
            stop_rx,
        })
    }

    pub(crate) fn already_downloaded(&self) -> u64 {
        self.resumed_bytes
    }

    pub(crate) async fn execute(&mut self) -> Result<()> {
        let segmented = !self.state.lock().await.segments.is_empty();
        if segmented {
            let result = self.execute_segmented().await;
            if let Err(err) = &result {
                if err.downcast_ref::<RangeUnsupported>().is_some() {
                    // execute_segmented has joined every worker; no old writer can race this reset.
                    remove_if_exists(&state_path(&self.output)?)?;
                    prepare_output(&self.output, 0)?;
                    self.state.lock().await.segments.clear();
                    self.total_downloaded.store(0, Ordering::Relaxed);
                    self.resumed_bytes = 0;
                    return self.execute_single().await;
                }
            }
            result
        } else {
            self.execute_single().await
        }
    }

    async fn execute_segmented(&mut self) -> Result<()> {
        prepare_output(&self.output, self.total_size.expect("known size"))?;
        save_state(&self.output, &self.state).await?;
        let segments = self.state.lock().await.segments.clone();
        let queue = Arc::new(TokioMutex::new(VecDeque::from(segments)));
        let (stop_tx, stop_rx) = watch::channel(*self.stop_rx.borrow());
        let experimental = self.state.lock().await.experimental_entropy;
        let slow_tracker =
            experimental.then(|| Arc::new(Mutex::new(SlowestTracker::new(self.connections))));
        let context = DownloadSegmentContext {
            client_factory: self.client_factory.clone(),
            url: self.url.clone(),
            output: self.output.clone(),
            state: Arc::clone(&self.state),
            stop_rx,
            slow_tracker,
            meta: self.meta.clone(),
            speed_limiter: self.speed_limiter.clone(),
            events: self.events.clone(),
            total_downloaded: Arc::clone(&self.total_downloaded),
            total_size: self.total_size,
        };
        let mut workers = JoinSet::new();
        for _ in 0..self.connections.min(queue.lock().await.len()) {
            let queue = Arc::clone(&queue);
            let context = context.clone();
            workers.spawn(async move {
                loop {
                    let segment = queue.lock().await.pop_front();
                    match segment {
                        Some(segment) => download_segment(context.clone(), segment).await?,
                        None => return Ok(()),
                    }
                }
            });
        }
        let result = wait_workers(
            &mut workers,
            &self.state,
            &self.output,
            self.stop_rx.clone(),
            stop_tx,
        )
        .await;
        // Written positions become crash-reusable only after this durable publication.
        save_state(&self.output, &self.state).await?;
        result?;
        let state = self.state.lock().await;
        if !state
            .segments
            .iter()
            .all(|s| s.downloaded == s.end - s.start + 1)
        {
            return Err(eyre::eyre!("download has incomplete segment coverage"));
        }
        Ok(())
    }

    async fn execute_single(&mut self) -> Result<()> {
        let context = DownloadSingleContext {
            client_factory: self.client_factory.clone(),
            url: self.url.clone(),
            output: self.output.clone(),
            stop_rx: self.stop_rx.clone(),
            speed_limiter: self.speed_limiter.clone(),
            events: self.events.clone(),
            total_downloaded: Arc::clone(&self.total_downloaded),
            total_size: self.total_size,
        };
        download_single(context).await
    }
}

fn valid_segments(segments: &[Segment], total: u64) -> bool {
    let mut next = 0;
    for (id, segment) in segments.iter().enumerate() {
        if segment.id != id
            || segment.start != next
            || segment.end < segment.start
            || segment.end >= total
        {
            return false;
        }
        let length = segment.end - segment.start + 1;
        if segment.downloaded > length {
            return false;
        }
        next = segment.end + 1;
    }
    next == total && (total == 0 || !segments.is_empty())
}
