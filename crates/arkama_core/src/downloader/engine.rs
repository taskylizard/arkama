use super::control::{StopSignal, controlled};
use super::output::{
    determine_output, lock_output, publish_output, remove_if_exists, sidecar_path,
};
use super::plan::{DownloadPlan, DownloadPlanConfig};
use super::rate_limit::SpeedLimiter;
use super::send_event;
use super::state::{load_state, state_path};
use crate::api::{DownloadEvent, DownloadRequest, DownloadSummary};
use crate::http::{self, ClientFactory};
use eyre::{Context, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};
use url::Url;

pub(super) async fn download_inner(
    request: DownloadRequest,
    events: Option<mpsc::UnboundedSender<DownloadEvent>>,
    _keep_controls_alive: watch::Sender<StopSignal>,
    mut stop_rx: watch::Receiver<StopSignal>,
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
    let meta = controlled(&mut stop_rx, http::probe(&client, &url)).await?;
    send_event(
        &events,
        DownloadEvent::Metadata {
            etag: meta.etag.clone(),
            last_modified: meta.last_modified.clone(),
            mime_type: meta.mime_type.clone(),
            final_url: meta.final_url.clone(),
        },
    );
    let output = determine_output(&url, &meta, output, output_dir)?;
    let _ownership = lock_output(&output)?;
    let partial = sidecar_path(&output, ".arkama.part")?;
    let checkpoint = state_path(&partial)?;
    let resume = load_state(&checkpoint).ok();
    let total_downloaded = Arc::new(AtomicU64::new(0));
    let mut plan = DownloadPlan::new(DownloadPlanConfig {
        url,
        output: partial.clone(),
        meta,
        connections: connections.max(1),
        resume,
        experimental_entropy,
        client_factory,
        speed_limiter: limit.map(SpeedLimiter::new).map(Arc::new),
        events: events.clone(),
        total_downloaded: Arc::clone(&total_downloaded),
        stop_rx: stop_rx.clone(),
    })?;
    let resumed = plan.already_downloaded();
    total_downloaded.store(resumed, Ordering::Relaxed);
    send_event(
        &events,
        DownloadEvent::Started {
            output: output.clone(),
            total_bytes: plan.total_size,
            resumed_bytes: resumed,
        },
    );
    info!("download started");
    let started = Instant::now();
    let result = plan.execute().await;
    let signal = *stop_rx.borrow();
    if signal == StopSignal::Cancel {
        remove_if_exists(&checkpoint)?;
        remove_if_exists(&partial)?;
    }
    let result = if signal == StopSignal::Cancel || (signal == StopSignal::Pause && result.is_ok())
    {
        Err(eyre::Report::new(signal))
    } else {
        result
    };
    match result {
        Ok(()) => {
            let written = total_downloaded.load(Ordering::Relaxed);
            if partial.metadata()?.len() != written
                || plan.total_size.is_some_and(|total| total != written)
            {
                return Err(eyre::eyre!(
                    "output length does not match verified transfer coverage"
                ));
            }
            publish_output(&partial, &output)?;
            remove_if_exists(&checkpoint)?;
            let summary = DownloadSummary {
                output,
                total_bytes: written,
                downloaded_bytes: written,
                resumed_bytes: plan.already_downloaded(),
                elapsed: started.elapsed(),
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
            send_event(
                &events,
                DownloadEvent::Failed {
                    message: err.to_string(),
                },
            );
            warn!("download failed");
            Err(err)
        }
    }
}
