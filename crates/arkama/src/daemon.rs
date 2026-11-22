//! Host startup and the local transport. Scheduling and SQLite live in arkama_manager.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use arkama_data::{daemon_addr_path, daemon_log_path, db_path};
use arkama_manager::{Command, Manager, ManagerPolicy, Response};
use clap::{Args, Subcommand};
use eyre::{Context, Result};
use tokio::runtime::Handle;
use tracing_subscriber::EnvFilter;

mod auth;
mod client;
mod protocol;
mod server;

pub(crate) use client::{
    cancel_job, enqueue_download, list_jobs, manager_command, pause_job, resume_job, retry_job,
    show_job,
};
use client::{send_request, status};
use protocol::{DaemonRequest, DaemonResponse, DaemonStatus};

#[derive(Args, Debug, Default)]
pub struct ServerOptions {
    #[arg(
        long,
        help = "Explicitly disable API keys; reachable callers can control downloads."
    )]
    no_auth: bool,
    #[arg(
        long,
        requires = "no_auth",
        help = "Allow non-loopback credential-free binding."
    )]
    allow_remote_no_auth: bool,
    #[arg(
        long = "download-root",
        help = "Authorized output root (repeatable); defaults to Downloads."
    )]
    roots: Vec<PathBuf>,
    #[arg(
        long = "allow-private-source",
        help = "Exact owner-authorized private HTTP origin (repeatable)."
    )]
    private_origins: Vec<String>,
    #[arg(
        long = "allow-origin",
        help = "Exact HTTPS browser/TLS-proxy origin (repeatable)."
    )]
    origins: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum KeyCommand {
    #[command(about = "Replace the owner key; existing keys stop working immediately.")]
    Rotate,
    #[command(about = "Revoke the owner key until explicitly rotated.")]
    Revoke,
}

#[derive(Subcommand, Debug)]
pub enum DaemonCommand {
    #[command(about = "Run the download host in the foreground.")]
    Run {
        #[arg(long, help = "Run the shared manager without a control listener.")]
        no_server: bool,
        #[command(flatten)]
        server: ServerOptions,
    },
    #[command(about = "Start the daemon in the background.")]
    Start,
    #[command(about = "Show daemon status.")]
    Status,
    #[command(about = "Ask the daemon to stop and join its downloads.")]
    Stop,
    #[command(about = "Rotate or revoke the locally stored API key.")]
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
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
        DaemonCommand::Run { no_server, server } => run_foreground(no_server, server).await,
        DaemonCommand::Start => start_background().await,
        DaemonCommand::Status => status_command().await,
        DaemonCommand::Stop => stop_command().await,
        DaemonCommand::Key { command } => {
            let value = match command {
                KeyCommand::Rotate => auth::random_id()?,
                KeyCommand::Revoke => "revoked".into(),
            };
            auth::write_key(&value)?;
            println!("credential updated at {}", auth::key_path()?.display());
            Ok(())
        }
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

async fn run_foreground(no_server: bool, mut options: ServerOptions) -> Result<()> {
    auth::protect_directory(&arkama_data::data_dir()?)?;
    init_tracing()?;
    let policy = if no_server {
        ManagerPolicy::default()
    } else {
        if options.roots.is_empty() {
            options.roots.push(arkama_data::default_download_dir());
        }
        for root in &options.roots {
            std::fs::create_dir_all(root)?;
        }
        ManagerPolicy::restricted(options.roots.clone(), &options.private_origins)?
    };
    let manager = Manager::open_with_policy(db_path()?, Handle::current(), policy).await?;
    if no_server {
        tokio::select! {
            _ = manager.closed() => {}
            signal = tokio::signal::ctrl_c() => { signal?; }
        }
        return manager.shutdown().await.map_err(Into::into);
    }
    let result = server::serve(manager.clone(), options).await;
    manager.shutdown().await?;
    result
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
        DaemonRequest::Enqueue(request) => Command::Enqueue { request },
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
