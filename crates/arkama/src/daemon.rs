use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use arkama_core::{
    DownloadControl, DownloadEvent, DownloadRequest, DownloadSummary, start_download_with_handle,
};
use arkama_data::{Db, daemon_addr_path, daemon_log_path};
use clap::Subcommand;
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::util::should_persist_progress;

#[derive(Subcommand, Debug)]
pub enum DaemonCommand {
    #[command(about = "Run the daemon in the foreground.")]
    Run,

    #[command(about = "Start the daemon in the background.")]
    Start,

    #[command(about = "Show daemon status.")]
    Status,

    #[command(about = "Ask the daemon to stop.")]
    Stop,
}

#[derive(Debug, Serialize, Deserialize)]
enum DaemonRequest {
    Ping,
    Shutdown,
    Enqueue(QueuedDownloadRequest),
}

#[derive(Debug, Serialize, Deserialize)]
enum DaemonResponse {
    Pong(DaemonStatus),
    Enqueued { id: i64 },
    ShuttingDown,
    Error { message: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct QueuedDownloadRequest {
    url: String,
    output: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    connections: usize,
    user_agent: Option<String>,
    limit: Option<u64>,
    experimental_entropy: bool,
}

impl From<DownloadRequest> for QueuedDownloadRequest {
    fn from(request: DownloadRequest) -> Self {
        let DownloadRequest {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
        } = request;
        Self {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
        }
    }
}

impl From<QueuedDownloadRequest> for DownloadRequest {
    fn from(request: QueuedDownloadRequest) -> Self {
        let QueuedDownloadRequest {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
        } = request;
        Self {
            url,
            output,
            output_dir,
            connections,
            user_agent,
            limit,
            experimental_entropy,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub pid: u32,
    pub active: usize,
    pub queued: usize,
}

struct ActiveDownload {
    id: i64,
    request: DownloadRequest,
    receiver: tokio_mpsc::UnboundedReceiver<DownloadEvent>,
    done_rx: mpsc::Receiver<Result<DownloadSummary>>,
    control: DownloadControl,
    downloaded_bytes: u64,
    last_db_update: Instant,
    last_db_bytes: u64,
}

struct CommandMessage {
    request: DaemonRequest,
    response_tx: oneshot::Sender<DaemonResponse>,
}

struct AddrFileGuard {
    path: PathBuf,
}

impl Drop for AddrFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct DaemonState {
    runtime: Handle,
    db: Db,
    max_concurrent: usize,
    active: Vec<ActiveDownload>,
    stopping: bool,
}

impl DaemonState {
    fn new(runtime: Handle) -> Result<Self> {
        let db = Db::open()?;
        db.recover_daemon_downloads()?;
        let max_concurrent = db
            .get_setting("max_concurrent")?
            .and_then(|value| value.parse().ok())
            .unwrap_or(3usize)
            .max(1);

        Ok(Self {
            runtime,
            db,
            max_concurrent,
            active: Vec::new(),
            stopping: false,
        })
    }

    fn status(&self) -> Result<DaemonStatus> {
        Ok(DaemonStatus {
            pid: std::process::id(),
            active: self.active.len(),
            queued: self.db.count_queued_daemon_downloads()?,
        })
    }

    fn handle_request(&mut self, request: DaemonRequest) -> Result<DaemonResponse> {
        match request {
            DaemonRequest::Ping => Ok(DaemonResponse::Pong(self.status()?)),
            DaemonRequest::Shutdown => {
                self.begin_shutdown()?;
                Ok(DaemonResponse::ShuttingDown)
            }
            DaemonRequest::Enqueue(request) => {
                let id = self.enqueue(request.into())?;
                Ok(DaemonResponse::Enqueued { id })
            }
        }
    }

    fn enqueue(&mut self, request: DownloadRequest) -> Result<i64> {
        if self.stopping {
            return Ok(-1);
        }

        let id = self.db.insert_daemon_download(&request)?;
        self.schedule()?;
        Ok(id)
    }

    fn schedule(&mut self) -> Result<()> {
        if self.stopping {
            return Ok(());
        }

        while self.active.len() < self.max_concurrent {
            let Some((id, request)) = self.db.claim_next_daemon_download()? else {
                break;
            };
            if let Err(err) = self.start_one(id, request) {
                self.db.update_download_failed(id, &err.to_string())?;
                error!("failed to start queued download {id}: {err:#}");
            }
        }
        Ok(())
    }

    fn start_one(&mut self, id: i64, request: DownloadRequest) -> Result<()> {
        let handle = start_download_with_handle(self.runtime.clone(), request.clone())?;

        let (done_tx, done_rx) = mpsc::channel();
        let join = handle.join;
        self.runtime.spawn(async move {
            let result = match join.await {
                Ok(inner) => inner,
                Err(err) => Err(eyre::eyre!(err)),
            };
            let _ = done_tx.send(result);
        });

        self.active.push(ActiveDownload {
            id,
            request,
            receiver: handle.events,
            done_rx,
            control: handle.control,
            downloaded_bytes: 0,
            last_db_update: Instant::now(),
            last_db_bytes: 0,
        });

        Ok(())
    }

    fn begin_shutdown(&mut self) -> Result<()> {
        if self.stopping {
            return Ok(());
        }

        self.stopping = true;

        for active in &mut self.active {
            self.db
                .requeue_download(active.id, active.downloaded_bytes)?;
            active.control.pause();
        }

        Ok(())
    }

    fn poll_active(&mut self) -> Result<()> {
        let mut finished: Vec<(i64, DownloadSummary)> = Vec::new();
        let mut failed: Vec<(i64, String, u64)> = Vec::new();

        for active in &mut self.active {
            let mut processed = 0usize;
            while processed < 200 {
                match active.receiver.try_recv() {
                    Ok(event) => match event {
                        DownloadEvent::Started {
                            output,
                            total_bytes,
                            resumed_bytes,
                        } => {
                            active.downloaded_bytes = resumed_bytes;
                            active.request.output = Some(output.clone());
                            self.db.update_daemon_request(active.id, &active.request)?;
                            self.db.update_download_started(
                                active.id,
                                &output,
                                total_bytes,
                                resumed_bytes,
                            )?;
                        }
                        DownloadEvent::Progress {
                            downloaded_bytes,
                            total_bytes,
                        } => {
                            active.downloaded_bytes = downloaded_bytes;
                            if should_persist_progress(
                                &active.last_db_update,
                                active.last_db_bytes,
                                downloaded_bytes,
                            ) {
                                active.last_db_update = Instant::now();
                                active.last_db_bytes = downloaded_bytes;
                                self.db.update_download_progress(
                                    active.id,
                                    total_bytes,
                                    downloaded_bytes,
                                )?;
                            }
                        }
                        DownloadEvent::Finished { summary } => {
                            finished.push((active.id, summary));
                            break;
                        }
                        DownloadEvent::Failed { message } => {
                            failed.push((active.id, message, active.downloaded_bytes));
                            break;
                        }
                    },
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
                processed += 1;
            }

            match active.done_rx.try_recv() {
                Ok(Ok(summary)) => finished.push((active.id, summary)),
                Ok(Err(err)) => failed.push((active.id, err.to_string(), active.downloaded_bytes)),
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {}
            }
        }

        for (id, summary) in finished {
            self.db.update_download_finished(id, &summary)?;
            self.active.retain(|active| active.id != id);
        }

        for (id, message, downloaded_bytes) in failed {
            if message == "interrupted" {
                self.db.requeue_download(id, downloaded_bytes)?;
            } else {
                self.db.update_download_failed(id, &message)?;
            }
            self.active.retain(|active| active.id != id);
        }

        self.schedule()?;
        Ok(())
    }
}

pub async fn run(command: DaemonCommand) -> Result<()> {
    match command {
        DaemonCommand::Run => run_foreground().await,
        DaemonCommand::Start => start_background().await,
        DaemonCommand::Status => status_command().await,
        DaemonCommand::Stop => stop_command().await,
    }
}

pub async fn enqueue_download(request: DownloadRequest) -> Result<i64> {
    let response = send_request(DaemonRequest::Enqueue(request.into())).await?;
    match response {
        DaemonResponse::Enqueued { id } if id >= 0 => Ok(id),
        DaemonResponse::Enqueued { .. } => Err(eyre::eyre!("daemon is stopping")),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        DaemonResponse::Pong(_) => Err(eyre::eyre!("unexpected daemon response")),
        DaemonResponse::ShuttingDown => Err(eyre::eyre!("daemon is shutting down")),
    }
}

fn init_tracing() -> Result<()> {
    let path = daemon_log_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create daemon log dir {parent:?}"))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("failed to open daemon log {path:?}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .with_writer(file)
        .init();
    Ok(())
}

async fn run_foreground() -> Result<()> {
    init_tracing()?;

    let addr_path = daemon_addr_path()?;
    let listener = prepare_listener().await?;
    let listen_addr = listener
        .local_addr()
        .context("failed to resolve daemon listener address")?;
    write_daemon_addr(&addr_path, listen_addr)?;
    let _guard = AddrFileGuard { path: addr_path };
    let runtime = Handle::current();
    let mut daemon = DaemonState::new(runtime)?;
    daemon.schedule()?;
    let (command_tx, mut command_rx) = tokio_mpsc::unbounded_channel::<CommandMessage>();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    info!("daemon listening on {listen_addr}");

    loop {
        if daemon.stopping && daemon.active.is_empty() {
            break;
        }

        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result.context("failed to accept daemon client")?;
                let command_tx = command_tx.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, command_tx).await {
                        error!("daemon client error: {err:#}");
                    }
                });
            }
            Some(message) = command_rx.recv() => {
                let response = match daemon.handle_request(message.request) {
                    Ok(response) => response,
                    Err(err) => DaemonResponse::Error { message: err.to_string() },
                };
                let _ = message.response_tx.send(response);
            }
            _ = ticker.tick() => {
                daemon.poll_active()?;
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for ctrl-c")?;
                daemon.begin_shutdown()?;
            }
        }
    }

    info!("daemon stopped");
    Ok(())
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

        if let Some(exit_status) = child.try_wait().context("failed to poll daemon process")? {
            return Err(eyre::eyre!("daemon exited early with status {exit_status}"));
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
    let response = send_request(DaemonRequest::Shutdown).await?;
    match response {
        DaemonResponse::ShuttingDown => {
            println!("daemon stopping");
            Ok(())
        }
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        DaemonResponse::Pong(_) => Err(eyre::eyre!("unexpected daemon response")),
        DaemonResponse::Enqueued { .. } => Err(eyre::eyre!("unexpected daemon response")),
    }
}

async fn status() -> Result<DaemonStatus> {
    let response = send_request(DaemonRequest::Ping).await?;
    match response {
        DaemonResponse::Pong(status) => Ok(status),
        DaemonResponse::Error { message } => Err(eyre::eyre!(message)),
        DaemonResponse::Enqueued { .. } => Err(eyre::eyre!("unexpected daemon response")),
        DaemonResponse::ShuttingDown => Err(eyre::eyre!("daemon is shutting down")),
    }
}

async fn prepare_listener() -> Result<TcpListener> {
    if status().await.is_ok() {
        return Err(eyre::eyre!("daemon already running"));
    }

    let bind_addr = daemon_bind_addr()?;
    TcpListener::bind(bind_addr)
        .await
        .with_context(|| format!("failed to bind daemon listener to {bind_addr}"))
}

async fn handle_connection(
    stream: TcpStream,
    command_tx: tokio_mpsc::UnboundedSender<CommandMessage>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .context("failed to read daemon request")?;
    if read == 0 {
        return Ok(());
    }

    let request = serde_json::from_str::<DaemonRequest>(&line).context("invalid daemon request")?;
    let (response_tx, response_rx) = oneshot::channel();
    command_tx
        .send(CommandMessage {
            request,
            response_tx,
        })
        .map_err(|_| eyre::eyre!("daemon command channel closed"))?;
    let response = response_rx
        .await
        .map_err(|_| eyre::eyre!("daemon response channel closed"))?;
    let response = serde_json::to_vec(&response).context("failed to serialize daemon response")?;
    write_half
        .write_all(&response)
        .await
        .context("failed to write daemon response")?;
    write_half
        .write_all(b"\n")
        .await
        .context("failed to finish daemon response")?;
    Ok(())
}

async fn send_request(request: DaemonRequest) -> Result<DaemonResponse> {
    let daemon_addr = read_daemon_addr()?;
    let stream = TcpStream::connect(daemon_addr)
        .await
        .with_context(|| format!("failed to connect to daemon at {daemon_addr}"))?;
    let (read_half, mut write_half) = stream.into_split();
    let request = serde_json::to_vec(&request).context("failed to serialize daemon request")?;
    write_half
        .write_all(&request)
        .await
        .context("failed to write daemon request")?;
    write_half
        .write_all(b"\n")
        .await
        .context("failed to finish daemon request")?;
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .context("failed to read daemon response")?;
    serde_json::from_str(&line).context("invalid daemon response")
}

fn daemon_bind_addr() -> Result<SocketAddr> {
    match std::env::var("ARKAMA_DAEMON_ADDR") {
        Ok(addr) => addr
            .parse()
            .with_context(|| format!("failed to parse ARKAMA_DAEMON_ADDR: {addr}")),
        Err(std::env::VarError::NotPresent) => {
            Ok(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
        }
        Err(err) => Err(err).context("failed to read ARKAMA_DAEMON_ADDR"),
    }
}

fn write_daemon_addr(path: &PathBuf, addr: SocketAddr) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create daemon dir {parent:?}"))?;
    }
    std::fs::write(path, addr.to_string())
        .with_context(|| format!("failed to write daemon address file {path:?}"))?;
    Ok(())
}

fn read_daemon_addr() -> Result<SocketAddr> {
    match std::env::var("ARKAMA_DAEMON_ADDR") {
        Ok(addr) => addr
            .parse()
            .with_context(|| format!("failed to parse ARKAMA_DAEMON_ADDR: {addr}")),
        Err(std::env::VarError::NotPresent) => {
            let path = daemon_addr_path()?;
            let addr = std::fs::read_to_string(&path)
                .with_context(|| format!("failed to read daemon address file {path:?}"))?;
            let addr = addr.trim();
            addr.parse()
                .with_context(|| format!("failed to parse daemon address from {path:?}: {addr}"))
        }
        Err(err) => Err(err).context("failed to read ARKAMA_DAEMON_ADDR"),
    }
}
