//! Host-owned live limits, shared by every download in a session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::downloader::SpeedLimiter;

/// Shared per-host HTTP admission and aggregate bytes/second limit.
///
/// Zero disables a limit. Lowering the connection limit admits no new responses until existing
/// responses drain; it does not interrupt downloads. Bandwidth changes wake pending throttles.
/// Host identity is the normalized hostname, across ports and schemes. Admission waiting is not
/// a network timeout. Limits apply to received file chunks, not TLS/HTTP overhead or probe bodies.
#[derive(Clone, Default)]
pub struct DownloadLimits {
    connections: Arc<Connections>,
    speed: Arc<SpeedLimiter>,
}

#[derive(Default)]
struct Connections {
    state: Mutex<ConnectionState>,
    changed: Notify,
}

#[derive(Default)]
struct ConnectionState {
    limit: usize,
    active: HashMap<String, usize>,
}

impl DownloadLimits {
    /// Sets a shared limit of 1–256 active HTTP responses per hostname, or zero for unlimited.
    pub fn set_connections_per_host(&self, limit: usize) -> crate::Result<()> {
        if limit > 256 {
            return Err(crate::Error::InvalidRequest {
                message: "host connection limit must be 0–256".into(),
            });
        }
        self.connections.state.lock().unwrap().limit = limit;
        self.connections.changed.notify_waiters();
        Ok(())
    }

    /// Sets the aggregate file-byte budget shared by all jobs, or zero for unlimited.
    pub fn set_bytes_per_second(&self, limit: u64) {
        self.speed.set_rate(limit);
    }

    pub(crate) async fn throttle(&self, bytes: u64, job: Option<&SpeedLimiter>) {
        // Reserve both budgets concurrently, rather than adding two delays for the same bytes.
        tokio::join!(self.speed.throttle(bytes), async {
            if let Some(job) = job {
                job.throttle(bytes).await;
            }
        });
    }

    pub(crate) async fn acquire(&self, url: &url::Url) -> ConnectionPermit {
        let host = url.host_str().expect("validated HTTP host").to_owned();
        loop {
            let changed = self.connections.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.connections.state.lock().unwrap();
                let active = state.active.get(&host).copied().unwrap_or_default();
                if state.limit == 0 || active < state.limit {
                    state.active.insert(host.clone(), active + 1);
                    return ConnectionPermit {
                        connections: self.connections.clone(),
                        host,
                    };
                }
            }
            changed.await;
        }
    }
}

pub(crate) struct ConnectionPermit {
    connections: Arc<Connections>,
    host: String,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let mut state = self.connections.state.lock().unwrap();
        let active = state.active.get_mut(&self.host).expect("admitted host");
        *active -= 1;
        if *active == 0 {
            state.active.remove(&self.host);
        }
        drop(state);
        self.connections.changed.notify_waiters();
    }
}
