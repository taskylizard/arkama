use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arkama_core::{DownloadEvent, DownloadRequest, DownloadSummary, download, start_download};
use arkama_data::{Db, default_download_dir};
use eyre::{Context, Result};
use serde_json::json;
use tracing::warn;
use tracing_subscriber::EnvFilter;

use crate::cli::DownloadArgs;
use crate::daemon;
use crate::progress;
use crate::util::{format_duration, format_speed};

pub async fn run(args: DownloadArgs) -> Result<()> {
    let output_mode = OutputMode::from_args(&args);
    init_tracing(output_mode);

    if args.daemon {
        return queue_args(args, output_mode).await;
    }

    run_args(args, output_mode).await
}

async fn run_args(args: DownloadArgs, output_mode: OutputMode) -> Result<()> {
    let yes = args.yes;
    if yes {
        warn!("--yes is currently a no-op");
    }

    let db = Db::open()?;
    let requests = resolve_requests(&db, args)?;

    for request in requests {
        let url = request.url.clone();
        run_single(&db, request, output_mode, url).await?;
    }

    Ok(())
}

async fn queue_args(args: DownloadArgs, output_mode: OutputMode) -> Result<()> {
    let yes = args.yes;
    if yes {
        warn!("--yes is currently a no-op");
    }

    let db = Db::open()?;
    let requests = resolve_requests(&db, args)?;

    for request in requests {
        let url = request.url.clone();
        let id = daemon::enqueue_download(request).await?;
        emit_queued(output_mode, id, &url)?;
    }

    Ok(())
}

fn resolve_requests(db: &Db, args: DownloadArgs) -> Result<Vec<DownloadRequest>> {
    let DownloadArgs {
        url,
        links_file,
        output,
        connections,
        user_agent,
        limit,
        experimental_entropy,
        daemon: _,
        silent: _,
        json: _,
        yes: _,
    } = args;

    let output_dir = resolve_output_dir(db)?;
    let connections = resolve_connections(connections, db.get_setting("connections")?)?;
    let limit = resolve_speed_limit(limit, db.get_setting("speed_limit")?)?;

    let Some(links_file) = links_file else {
        let Some(url) = url else {
            return Err(eyre::eyre!("missing url"));
        };
        return Ok(vec![DownloadRequest {
            url,
            output,
            output_dir: Some(output_dir),
            connections,
            user_agent,
            limit,
            experimental_entropy,
        }]);
    };

    if output.is_some() {
        return Err(eyre::eyre!("--output not allowed with --links-file"));
    }

    let links = read_links(&links_file)?;
    if links.is_empty() {
        return Err(eyre::eyre!("no links found in {links_file:?}"));
    }

    let mut requests = Vec::new();
    for url in links {
        requests.push(DownloadRequest {
            url,
            output: None,
            output_dir: Some(output_dir.clone()),
            connections,
            user_agent: user_agent.clone(),
            limit,
            experimental_entropy,
        });
    }

    Ok(requests)
}

fn resolve_output_dir(db: &Db) -> Result<PathBuf> {
    let dir = db
        .get_setting("download_dir")?
        .map(PathBuf::from)
        .unwrap_or_else(default_download_dir);
    Ok(dir)
}

fn resolve_connections(connections: Option<usize>, configured: Option<String>) -> Result<usize> {
    let Some(connections) = connections else {
        let connections = match configured {
            Some(connections) => connections
                .parse()
                .with_context(|| format!("invalid connections setting: {connections}"))?,
            None => 4,
        };
        return Ok(connections);
    };

    Ok(connections)
}

fn resolve_speed_limit(limit: Option<u64>, configured: Option<String>) -> Result<Option<u64>> {
    let Some(limit) = limit else {
        let Some(limit) = configured else {
            return Ok(None);
        };
        if limit.is_empty() {
            return Ok(None);
        }

        let limit = bytefmt::parse(&limit)
            .map_err(|err| eyre::eyre!("invalid speed_limit setting: {limit}: {err}"))?;
        return Ok(Some(limit));
    };

    Ok(Some(limit))
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum OutputMode {
    Default,
    Silent,
    Json,
}

impl OutputMode {
    fn from_args(args: &DownloadArgs) -> Self {
        if args.json {
            Self::Json
        } else if args.silent {
            Self::Silent
        } else {
            Self::Default
        }
    }
}

fn init_tracing(output_mode: OutputMode) {
    let filter = match output_mode {
        OutputMode::Default => EnvFilter::from_default_env(),
        OutputMode::Silent | OutputMode::Json => EnvFilter::new("error"),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

async fn run_single(
    db: &Db,
    request: DownloadRequest,
    output_mode: OutputMode,
    url: String,
) -> Result<()> {
    match output_mode {
        OutputMode::Default => run_single_console(db, request, &url).await,
        OutputMode::Silent => run_single_silent(db, request, &url).await,
        OutputMode::Json => run_single_json(db, request, url).await,
    }
}

async fn run_single_silent(db: &Db, request: DownloadRequest, url: &str) -> Result<()> {
    let id = db.insert_download(url, Path::new(""))?;
    match download(request).await {
        Ok(summary) => {
            db.update_download_finished(id, &summary)?;
        }
        Err(err) => {
            db.update_download_failed(id, &err.to_string())?;
            return Err(err.into());
        }
    }
    Ok(())
}

async fn run_single_console(db: &Db, request: DownloadRequest, url: &str) -> Result<()> {
    let id = db.insert_download(url, Path::new(""))?;
    let mut handle = start_download(request)?;
    let mut progress = None;

    loop {
        tokio::select! {
            event = handle.events.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    DownloadEvent::Started { output, total_bytes, resumed_bytes } => {
                        db.update_download_started(id, &output, total_bytes, resumed_bytes)?;
                        let bar = progress::build_progress(total_bytes, &output);
                        if resumed_bytes > 0 {
                            bar.set_position(resumed_bytes);
                        }
                        progress = Some(bar);
                    }
                    DownloadEvent::Progress { downloaded_bytes, total_bytes } => {
                        if let Some(bar) = progress.as_ref() {
                            if let Some(total) = total_bytes {
                                bar.set_length(total);
                            }
                            bar.set_position(downloaded_bytes);
                        }
                    }
                    DownloadEvent::Finished { summary } => {
                        if let Some(bar) = progress.as_ref() {
                            bar.finish_and_clear();
                        }
                        db.update_download_finished(id, &summary)?;
                    }
                    DownloadEvent::Failed { message } => {
                        if let Some(bar) = progress.as_ref() {
                            bar.abandon();
                        }
                        db.update_download_failed(id, &message)?;
                        eprintln!("download failed: {message}");
                    }
                }
            }
            result = &mut handle.join => {
                match result? {
                    Ok(summary) => {
                        if let Some(bar) = progress.as_ref() {
                            bar.finish_and_clear();
                        }
                        db.update_download_finished(id, &summary)?;
                        print_summary(&summary)?;
                    }
                    Err(err) => {
                        db.update_download_failed(id, &err.to_string())?;
                        return Err(err.into());
                    }
                }
                break;
            }
        }
    }

    Ok(())
}

async fn run_single_json(db: &Db, request: DownloadRequest, url: String) -> Result<()> {
    let id = db.insert_download(&url, Path::new(""))?;
    let mut handle = start_download(request)?;
    let mut finished_emitted = false;

    loop {
        tokio::select! {
            event = handle.events.recv() => {
                let Some(event) = event else {
                    break;
                };
                match event {
                    DownloadEvent::Started { output, total_bytes, resumed_bytes } => {
                        db.update_download_started(id, &output, total_bytes, resumed_bytes)?;
                        emit_json(json!({
                            "event": "started",
                            "url": &url,
                            "output": output.to_string_lossy(),
                            "total_bytes": total_bytes,
                            "resumed_bytes": resumed_bytes
                        }))?;
                    }
                    DownloadEvent::Progress { downloaded_bytes, total_bytes } => {
                        emit_json(json!({
                            "event": "progress",
                            "url": &url,
                            "downloaded_bytes": downloaded_bytes,
                            "total_bytes": total_bytes
                        }))?;
                    }
                    DownloadEvent::Finished { summary } => {
                        db.update_download_finished(id, &summary)?;
                        emit_json(summary_json("finished", &url, &summary))?;
                        finished_emitted = true;
                    }
                    DownloadEvent::Failed { message } => {
                        db.update_download_failed(id, &message)?;
                        emit_json(json!({
                            "event": "failed",
                            "url": &url,
                            "message": message
                        }))?;
                        finished_emitted = true;
                    }
                }
            }
            result = &mut handle.join => {
                match result {
                    Ok(Ok(summary)) => {
                        if !finished_emitted {
                            db.update_download_finished(id, &summary)?;
                            emit_json(summary_json("finished", &url, &summary))?;
                        }
                        break;
                    }
                    Ok(Err(err)) => {
                        if !finished_emitted {
                            db.update_download_failed(id, &err.to_string())?;
                            emit_json(json!({
                                "event": "failed",
                                "url": &url,
                                "message": err.to_string()
                            }))?;
                        }
                        return Err(err.into());
                    }
                    Err(err) => {
                        if !finished_emitted {
                            db.update_download_failed(id, &err.to_string())?;
                            emit_json(json!({
                                "event": "failed",
                                "url": &url,
                                "message": err.to_string()
                            }))?;
                        }
                        return Err(err.into());
                    }
                }
            }
        }
    }

    Ok(())
}

fn read_links(path: &Path) -> Result<Vec<String>> {
    let data =
        fs::read_to_string(path).with_context(|| format!("failed to read links file {path:?}"))?;
    let mut links = Vec::new();
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        links.push(line.to_string());
    }
    Ok(links)
}

fn print_summary(summary: &DownloadSummary) -> Result<()> {
    use owo_colors::OwoColorize;

    let downloaded = summary
        .downloaded_bytes
        .saturating_sub(summary.resumed_bytes);
    let elapsed = summary.elapsed;
    let speed = average_speed(downloaded, elapsed);
    let duration = format_duration(elapsed);
    let file_name = match summary.output.file_name() {
        Some(name) => name.to_str().unwrap_or("download"),
        None => "download",
    };

    println!(
        "{} {} in {} avg {}/s",
        "✓".green().bold(),
        file_name.bold(),
        duration.cyan(),
        format_speed(speed).yellow()
    );
    Ok(())
}

fn emit_json(value: serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

fn emit_queued(output_mode: OutputMode, id: i64, url: &str) -> Result<()> {
    match output_mode {
        OutputMode::Default => {
            println!("queued job {id} for {url}");
            Ok(())
        }
        OutputMode::Silent => Ok(()),
        OutputMode::Json => emit_json(json!({
            "event": "queued",
            "job_id": id,
            "url": url,
        })),
    }
}

fn summary_json(event: &str, url: &str, summary: &DownloadSummary) -> serde_json::Value {
    let speed = average_speed(
        summary
            .downloaded_bytes
            .saturating_sub(summary.resumed_bytes),
        summary.elapsed,
    );
    json!({
        "event": event,
        "url": url,
        "output": summary.output.to_string_lossy(),
        "total_bytes": summary.total_bytes,
        "downloaded_bytes": summary.downloaded_bytes,
        "resumed_bytes": summary.resumed_bytes,
        "elapsed_ms": summary.elapsed.as_millis(),
        "speed_bytes_per_sec": speed
    })
}

fn average_speed(downloaded: u64, elapsed: Duration) -> u64 {
    let elapsed_ms = elapsed.as_millis();
    if elapsed_ms == 0 {
        return 0;
    }
    let elapsed_ms = match u64::try_from(elapsed_ms) {
        Ok(value) => value,
        Err(err) => {
            let _ = err;
            u64::MAX
        }
    };
    downloaded.saturating_mul(1000) / elapsed_ms
}

#[cfg(test)]
mod tests {
    use super::{resolve_connections, resolve_speed_limit};

    #[test]
    fn test_resolve_connections_uses_config_when_flag_missing() {
        let connections = resolve_connections(None, Some("8".to_string())).unwrap();

        assert_eq!(connections, 8);
    }

    #[test]
    fn test_resolve_connections_prefers_flag_over_config() {
        let connections = resolve_connections(Some(6), Some("8".to_string())).unwrap();

        assert_eq!(connections, 6);
    }

    #[test]
    fn test_resolve_speed_limit_uses_config_when_flag_missing() {
        let limit = resolve_speed_limit(None, Some("2MB".to_string())).unwrap();

        assert_eq!(limit, Some(2_000_000));
    }

    #[test]
    fn test_resolve_speed_limit_prefers_flag_over_config() {
        let limit = resolve_speed_limit(Some(512_000), Some("2MB".to_string())).unwrap();

        assert_eq!(limit, Some(512_000));
    }
}
