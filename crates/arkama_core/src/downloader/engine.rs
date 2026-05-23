use super::control::StopSignal;
use super::output::determine_output;
use super::plan::{DownloadPlan, DownloadPlanConfig};
use super::rate_limit::SpeedLimiter;
use super::send_event;
use super::state::{load_state, state_path};
use crate::api::{DownloadEvent, DownloadRequest, DownloadSummary};
use crate::http::{self, ClientFactory};
use eyre::{Context, Result};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use url::Url;

pub(super) async fn download_inner(
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
    send_event(
        &events,
        DownloadEvent::Metadata {
            etag: meta.etag.clone(),
            last_modified: meta.last_modified.clone(),
            mime_type: meta.mime_type.clone(),
            final_url: meta.final_url.clone(),
        },
    );
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
