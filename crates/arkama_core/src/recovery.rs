//! Recovery configuration and host-scoped retry admission.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Deadlines and retry bounds for replayable HTTP downloads.
///
/// Durations are milliseconds. Zero retries disables recovery; zero deadlines are invalid.
/// The setup deadline covers connecting, redirects, and receiving response headers, not the body.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[derive(Clone, Debug)]
pub struct RecoveryPolicy {
    pub connect_timeout_ms: u64,
    pub setup_timeout_ms: u64,
    pub read_idle_timeout_ms: u64,
    /// Maximum retries per metadata probe or transfer worker, including body failures.
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// Uniform additional delay in 0..=jitter_ms, capped at max_delay_ms.
    pub jitter_ms: u64,
    /// Retries allowed per origin in the rolling host_window_ms window.
    pub max_host_retries: usize,
    pub host_window_ms: u64,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            setup_timeout_ms: 30_000,
            read_idle_timeout_ms: 30_000,
            max_retries: 5,
            base_delay_ms: 500,
            max_delay_ms: 10_000,
            jitter_ms: 250,
            max_host_retries: 30,
            host_window_ms: 60_000,
        }
    }
}

impl RecoveryPolicy {
    /// Checks configuration before any network or filesystem work starts.
    pub fn validate(&self) -> crate::Result<()> {
        let durations = [
            self.connect_timeout_ms,
            self.setup_timeout_ms,
            self.read_idle_timeout_ms,
            self.max_delay_ms,
            self.host_window_ms,
        ];
        if durations
            .iter()
            .any(|value| !(1..=3_600_000).contains(value))
            || self.base_delay_ms > self.max_delay_ms
            || self.jitter_ms > self.max_delay_ms
            || self.max_retries > 1_000
            || self.max_host_retries > 10_000
        {
            return Err(crate::Error::InvalidRequest {
                message: "invalid recovery policy: deadlines/window/max delay must be 1 ms–1 hour, base delay/jitter at most max delay, retries at most 1000, host retries at most 10000".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        let delay = self.base_delay_ms.saturating_mul(1u64 << attempt.min(32));
        let jitter = fastrand::u64(0..=self.jitter_ms);
        Duration::from_millis(delay.saturating_add(jitter).min(self.max_delay_ms))
    }
}

#[derive(Default)]
pub(crate) struct RetryBudget(Mutex<HashMap<String, VecDeque<Instant>>>);

impl RetryBudget {
    pub(crate) fn reserve(&self, origin: &str, policy: &RecoveryPolicy) -> eyre::Result<()> {
        let now = Instant::now();
        let mut hosts = self.0.lock().unwrap();
        // Bound a long-lived session, including origins that stopped failing long ago.
        hosts.retain(|_, retries| {
            while retries
                .front()
                .is_some_and(|time| now.duration_since(*time) >= Duration::from_secs(3600))
            {
                retries.pop_front();
            }
            !retries.is_empty()
        });
        if !hosts.contains_key(origin) && hosts.len() >= 1024 {
            return Err(eyre::eyre!(
                "retry origin capacity exhausted; wait or use another session"
            ));
        }
        let retries = hosts.entry(origin.into()).or_default();
        let window = Duration::from_millis(policy.host_window_ms);
        let used = retries
            .iter()
            .filter(|time| now.duration_since(**time) < window)
            .count();
        if used >= policy.max_host_retries {
            return Err(eyre::eyre!(
                "host retry budget exhausted; wait before retrying"
            ));
        }
        if retries.len() >= 10_000 {
            return Err(eyre::eyre!(
                "retry history capacity exhausted; wait before retrying"
            ));
        }
        retries.push_back(now);
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RecoveryReason {
    Connection = 1,
    SetupTimeout,
    ReadIdle,
    Body,
    Throttled,
    TemporaryServer,
    ShortBody,
}

impl RecoveryReason {
    pub(crate) fn label(value: u64) -> Option<&'static str> {
        match value {
            1 => Some("connection failure"),
            2 => Some("connection/header deadline"),
            3 => Some("network read idle deadline"),
            4 => Some("body disconnected"),
            5 => Some("server throttling"),
            6 => Some("temporary HTTP failure"),
            7 => Some("body ended early"),
            _ => None,
        }
    }
}
