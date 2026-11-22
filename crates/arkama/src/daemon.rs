//! Host startup and the local transport. Scheduling and SQLite live in arkama_manager.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use arkama_data::{daemon_addr_path, daemon_log_path, db_path};
use arkama_manager::{Command, Manager, Response};
use clap::Subcommand;
use eyre::{Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod client;
mod protocol;

pub(crate) use client::{
    cancel_job, enqueue_download, list_jobs, manager_command, pause_job, resume_job, retry_job,
    show_job,
};
use client::{send_request, status};
use protocol::{DaemonRequest, DaemonResponse, DaemonStatus};

#[derive(Subcommand, Debug)]
pub enum DaemonCommand {
    #[command(about = "Run the download host in the foreground.")]
    Run {
        #[arg(long, help = "Run the shared manager without a control listener.")]
        no_server: bool,
    },
    #[command(about = "Start the daemon in the background.")]
    Start,
    #[command(about = "Show daemon status.")]
    Status,
    #[command(about = "Ask the daemon to stop and join its downloads.")]
    Stop,
}

struct AddrFileGuard {
    path: PathBuf,
}
impl Drop for AddrFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub async fn run(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Run { no_server } => run_foreground(no_server).await,
        DaemonCommand::Start => start_background().await,
        DaemonCommand::Status => status_command().await,
        DaemonCommand::Stop => stop_command().await,
    }
}

fn init_tracing() -> Result<()> {
    let path = daemon_log_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .with_writer(file)
        .init();
    Ok(())
}

async fn run_foreground(no_server: bool) -> Result<()> {
    init_tracing()?;
    let manager = Manager::open(db_path()?, Handle::current()).await?;
    if no_server {
        tokio::select! {
            _ = manager.closed() => {}
            signal = tokio::signal::ctrl_c() => { signal?; }
        }
        return manager.shutdown().await.map_err(Into::into);
    }
    let result = serve(manager.clone()).await;
    manager.shutdown().await?;
    result
}

async fn serve(manager: Manager) -> Result<()> {
    let bind_addr = daemon_bind_addr()?;
    let listener = TcpListener::bind(bind_addr)
        .await
        .context("failed to bind daemon listener")?;
    let listen_addr = listener.local_addr()?;
    let addr_path = daemon_addr_path()?;
    write_daemon_addr(&addr_path, listen_addr)?;
    let _guard = AddrFileGuard { path: addr_path };
    let slots = Arc::new(Semaphore::new(32));
    let mut connections = JoinSet::new();
    info!("daemon listening on {listen_addr}");
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(slot) = slots.clone().try_acquire_owned() else { continue; };
                let manager = manager.clone();
                connections.spawn(async move {
                    let _slot = slot;
                    let _ = tokio::time::timeout(Duration::from_secs(30), handle_connection(stream, manager)).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            _ = manager.closed() => break,
            signal = tokio::signal::ctrl_c() => { signal?; manager.shutdown().await?; break; }
        }
    }
    drop(listener);
    // Allow the connection that requested shutdown to deliver its acknowledged reply.
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    info!("daemon stopped");
    Ok(())
}

async fn handle_connection(stream: TcpStream, manager: Manager) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half.take(65_537));
    let mut line = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), reader.read_until(b'\n', &mut line)).await??;
    if line.len() > 65_536 || line.last() != Some(&b'\n') {
        return Err(eyre::eyre!("invalid request size or framing"));
    }
    let request: DaemonRequest =
        serde_json::from_slice(&line).map_err(|_| eyre::eyre!("invalid daemon request"))?;
    let response = adapt_request(&manager, request).await;
    let response = serde_json::to_vec(&response)?;
    if response.len() > 2 * 1024 * 1024 {
        return Err(eyre::eyre!("response exceeds limit"));
    }
    write_half.write_all(&response).await?;
    write_half.write_all(b"\n").await?;
    Ok(())
}

async fn adapt_request(manager: &Manager, request: DaemonRequest) -> DaemonResponse {
    if matches!(request, DaemonRequest::Shutdown) {
        return match manager.shutdown().await {
            Ok(()) => DaemonResponse::ShuttingDown,
            Err(error) => DaemonResponse::Error {
                message: error.to_string(),
            },
        };
    }
    let action = match request {
        DaemonRequest::Pause { .. } => "paused",
        DaemonRequest::Resume { .. } => "resumed",
        DaemonRequest::Cancel { .. } => "cancelled",
        DaemonRequest::Retry { .. } => "retried",
        _ => "",
    };
    let managed = matches!(request, DaemonRequest::Manage { .. });
    let command = match request {
        DaemonRequest::Ping => Command::Status,
        DaemonRequest::Enqueue(request) => Command::Enqueue {
            request: request.into(),
        },
        DaemonRequest::List { limit, query } => Command::List {
            limit,
            offset: 0,
            query: query.unwrap_or_default(),
        },
        DaemonRequest::Show { id } => Command::Show { id },
        DaemonRequest::Pause { id } => Command::Pause { id },
        DaemonRequest::Resume { id } => Command::Resume { id },
        DaemonRequest::Cancel { id } => Command::Cancel { id },
        DaemonRequest::Retry { id } => Command::Retry { id },
        DaemonRequest::Manage { command } => command,
        DaemonRequest::Shutdown => unreachable!("shutdown handled before command translation"),
    };
    match manager.execute(command).await {
        Ok(response) if managed => DaemonResponse::Managed(response),
        Ok(Response::Status(status)) => DaemonResponse::Pong(DaemonStatus {
            pid: status.pid,
            active: status.active,
            queued: status.queued,
        }),
        Ok(Response::Accepted { id }) => DaemonResponse::Enqueued { id },
        Ok(Response::Jobs { jobs, .. }) => {
            DaemonResponse::Jobs(jobs.into_iter().map(|job| job.record).collect())
        }
        Ok(Response::Job(job)) if !action.is_empty() => DaemonResponse::Ok {
            message: format!("{action} job {}", job.record.id),
        },
        Ok(Response::Job(job)) => DaemonResponse::Job(job.record),
        Ok(_) => DaemonResponse::Error {
            message: "unsupported legacy response".into(),
        },
        Err(error) => DaemonResponse::Error {
            message: error.to_string(),
        },
    }
}

async fn start_background() -> Result<()> {
    if let Ok(status) = status().await {
        println!(
            "daemon already running (pid {}): {} active, {} queued",
            status.pid, status.active, status.queued
        );
        return Ok(());
    }
    let exe = std::env::current_exe().context("failed to locate current executable")?;
    let mut child = std::process::Command::new(exe)
        .arg("daemon")
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to spawn daemon")?;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok(status) = status().await {
            println!(
                "daemon started (pid {}): {} active, {} queued",
                status.pid, status.active, status.queued
            );
            return Ok(());
        }
        if let Some(exit) = child.try_wait()? {
            return Err(eyre::eyre!("daemon exited early with status {exit}"));
        }
    }
    Err(eyre::eyre!("daemon did not become ready"))
}

async fn status_command() -> Result<()> {
    let status = status().await?;
    println!(
        "daemon running (pid {}): {} active, {} queued",
        status.pid, status.active, status.queued
    );
    Ok(())
}

async fn stop_command() -> Result<()> {
    match send_request(DaemonRequest::Shutdown).await? {
        DaemonResponse::ShuttingDown => {
            println!("daemon stopped");
            Ok(())
        }
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        _ => Err(eyre::eyre!("unexpected daemon response")),
    }
}

fn daemon_bind_addr() -> Result<SocketAddr> {
    let addr: SocketAddr = match std::env::var("ARKAMA_DAEMON_ADDR") {
        Ok(addr) => addr.parse().context("invalid ARKAMA_DAEMON_ADDR"),
        Err(std::env::VarError::NotPresent) => {
            Ok(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        }
        Err(error) => Err(error).context("failed to read ARKAMA_DAEMON_ADDR"),
    }?;
    if !addr.ip().is_loopback() {
        return Err(eyre::eyre!(
            "remote binding requires the secure server; this local transport only accepts loopback"
        ));
    }
    Ok(addr)
}

fn write_daemon_addr(path: &PathBuf, addr: SocketAddr) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, addr.to_string())?;
    Ok(())
}

fn read_daemon_addr() -> Result<SocketAddr> {
    match std::env::var("ARKAMA_DAEMON_ADDR") {
        Ok(addr) => addr.parse().context("invalid ARKAMA_DAEMON_ADDR"),
        Err(std::env::VarError::NotPresent) => std::fs::read_to_string(daemon_addr_path()?)
            .context("daemon address unavailable")?
            .trim()
            .parse()
            .context("invalid daemon address"),
        Err(error) => Err(error).context("failed to read ARKAMA_DAEMON_ADDR"),
    }
}
