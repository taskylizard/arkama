//! Persistent download management without a listener, runtime, signals, or logging setup.
//!
//! A dedicated OS thread owns SQLite and scheduling. Transfers use the host's Tokio runtime.
//! Keep that runtime alive until [`Manager::shutdown`] returns. Clones share one owner; dropping
//! the last clone requests shutdown but cannot await it. Prefer explicit shutdown before exit.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

mod actor;
mod model;
mod policy;

use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

use tokio::runtime::Handle;
use tokio::sync::{oneshot, watch};

pub use model::{Command, JobSnapshot, Response, Status};
pub use policy::ManagerPolicy;

/// Stable error categories for local and transport consumers. Messages never contain requests.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Error {
    pub code: String,
    pub message: String,
}

impl Error {
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}
impl From<eyre::Report> for Error {
    fn from(_: eyre::Report) -> Self {
        Self::new("storage", "persistent store operation failed")
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub(crate) type Reply = oneshot::Sender<Result<Response>>;
pub(crate) struct Message {
    command: Command,
    reply: Reply,
}

struct Owner {
    sender: mpsc::SyncSender<Message>,
    stop: Arc<AtomicBool>,
    closed: watch::Receiver<Option<Result<()>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// A bounded asynchronous command handle to one persistent execution owner.
#[derive(Clone)]
pub struct Manager(Arc<Owner>);

impl Manager {
    /// Opens a store on the manager thread. A retained cross-process lock refuses a second host.
    /// Do not mutate this store directly while a manager owns it.
    pub async fn open(path: impl Into<PathBuf>, runtime: Handle) -> Result<Self> {
        Self::open_with_policy(path, runtime, ManagerPolicy::default()).await
    }

    /// Opens with immutable owner-selected boundaries. Requests and settings cannot relax them.
    pub async fn open_with_policy(
        path: impl Into<PathBuf>,
        runtime: Handle,
        policy: ManagerPolicy,
    ) -> Result<Self> {
        let path = path.into();
        let (sender, receiver) = mpsc::sync_channel(128);
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let (closed_tx, closed) = watch::channel(None);
        let (ready_tx, ready_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("arkama-manager".into())
            .spawn(move || {
                let result = match actor::Actor::open(path, runtime, policy) {
                    Ok(mut actor) => {
                        let _ = ready_tx.send(Ok(()));
                        actor.run(receiver, thread_stop)
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.clone()));
                        Err(error)
                    }
                };
                closed_tx.send_replace(Some(result));
            })
            .map_err(|_| Error::new("runtime", "failed to start manager thread"))?;
        ready_rx
            .await
            .map_err(|_| Error::new("stopped", "manager stopped during startup"))??;
        Ok(Self(Arc::new(Owner {
            sender,
            stop,
            closed,
        })))
    }

    /// Admission is bounded and non-waiting. Once admitted, client disconnection does not undo
    /// a command. Pause/cancel responses wait for the attempt to join and persist its outcome.
    pub async fn execute(&self, command: Command) -> Result<Response> {
        if !command.size_valid() {
            return Err(Error::new("invalid", "command exceeds field limits"));
        }
        if self.0.stop.load(Ordering::Acquire) || self.0.closed.borrow().is_some() {
            return Err(Error::new("stopped", "manager is stopping"));
        }
        let (reply, receiver) = oneshot::channel();
        self.0
            .sender
            .try_send(Message { command, reply })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::new("busy", "manager admission is full"),
                mpsc::TrySendError::Disconnected(_) => Error::new("stopped", "manager has stopped"),
            })?;
        receiver
            .await
            .map_err(|_| Error::new("stopped", "manager has stopped"))?
    }

    /// Rejects admission, checkpoints and joins active workers, then releases store ownership.
    /// Intentionally paused jobs remain paused; interrupted running jobs return to the queue.
    pub async fn shutdown(&self) -> Result<()> {
        self.0.stop.store(true, Ordering::Release);
        let mut closed = self.0.closed.clone();
        let result = closed
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::new("stopped", "manager thread unavailable"))?;
        result.as_ref().expect("closed manager result").clone()
    }

    pub async fn closed(&self) {
        let mut closed = self.0.closed.clone();
        let _ = closed.wait_for(Option::is_some).await;
    }
}
