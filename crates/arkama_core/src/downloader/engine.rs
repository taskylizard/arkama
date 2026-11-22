use super::control::{StopSignal, controlled};
use super::output::{
    determine_output, lock_output, publish_output, remove_if_exists, sidecar_path,
};
use super::plan::{DownloadPlan, DownloadPlanConfig};
use super::rate_limit::SpeedLimiter;
use super::state::{load_state, state_path};
use super::{EventSender, send_event};
use crate::DownloadObservation;
use crate::http::{self, ClientFactory};
use crate::observation::Phase;
use crate::{DownloadEvent, DownloadRequest, DownloadSummary};
use eyre::{Context, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::watch;
use url::Url;

pub(super) async fn download_inner(
    request: DownloadRequest,
    events: Option<Arc<EventSender>>,
    _keep_controls_alive: watch::Sender<StopSignal>,
    mut stop_rx: watch::Receiver<StopSignal>,
    observation: DownloadObservation,
) -> Result<DownloadSummary> {
    let setup = observation.timing(Phase::Setup);
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
    let total_downloaded = observation.written();
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
        observation: observation.clone(),
        stop_rx: stop_rx.clone(),
    })?;
    let resumed = plan.already_downloaded();
    total_downloaded.store(resumed, Ordering::Relaxed);
    observation.checkpoint(resumed);
    drop(setup);
    send_event(
        &events,
        DownloadEvent::Started {
            output: output.clone(),
            total_bytes: plan.total_size,
            resumed_bytes: resumed,
        },
    );
    let started = Instant::now();
    let result = {
        let _transfer = observation.timing(Phase::Transfer);
        plan.execute().await
    };
    let signal = *stop_rx.borrow();
    if signal == StopSignal::Cancel {
        remove_if_exists(&checkpoint)?;
        remove_if_exists(&partial)?;
        observation.checkpoint(0);
    }
    let result = if signal == StopSignal::Cancel || (signal == StopSignal::Pause && result.is_ok())
    {
        Err(eyre::Report::new(signal))
    } else {
        result
    };
    match result {
        Ok(()) => {
            let verification = observation.timing(Phase::Verification);
            let written = total_downloaded.load(Ordering::Relaxed);
            if partial.metadata()?.len() != written
                || plan.total_size.is_some_and(|total| total != written)
            {
                return Err(eyre::eyre!(
                    "output length does not match verified transfer coverage"
                ));
            }
            drop(verification);
            {
                let _publication = observation.timing(Phase::Publication);
                publish_output(&partial, &output)?;
                remove_if_exists(&checkpoint)?;
            }
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
            Ok(summary)
        }
        Err(err) => {
            send_event(
                &events,
                DownloadEvent::Failed {
                    message: err.to_string(),
                },
            );
            Err(err)
        }
    }
}
