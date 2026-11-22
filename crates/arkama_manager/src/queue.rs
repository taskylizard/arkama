//! Persisted admission policy. Pause/windows never preempt active transfers.

use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// A named queue. Higher queue priority runs first; ties choose the oldest eligible job.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Queue {
    pub name: String,
    pub paused: bool,
    pub max_active: usize,
    pub priority: i64,
    pub newest_first: bool,
    pub window: Option<DownloadWindow>,
}

impl Queue {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            paused: false,
            max_active: 3,
            priority: 0,
            newest_first: false,
            window: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_queue_name(&self.name)?;
        if !(1..=64).contains(&self.max_active) || self.priority.abs_diff(0) > 1_000_000 {
            return Err(Error::new("invalid", "invalid queue limits or priority"));
        }
        if let Some(window) = &self.window {
            window.validate()?;
        }
        Ok(())
    }

    pub(crate) fn eligible(&self) -> bool {
        !self.paused
            && self
                .window
                .as_ref()
                .is_none_or(|window| window.allows_at(Utc::now()))
    }
}

pub(crate) fn validate_queue_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
    {
        return Err(Error::new(
            "invalid",
            "queue names use 1–64 ASCII letters, digits, _ or -",
        ));
    }
    Ok(())
}

/// Weekly start-inclusive/end-exclusive local windows, with an IANA timezone.
/// Weekdays are Monday=0 through Sunday=6 and denote the starting day for overnight windows.
/// Repeated DST minutes are eligible both times; skipped minutes never occur. Active jobs keep
/// running when the window closes. Restart simply reevaluates eligibility at the current instant.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadWindow {
    pub timezone: String,
    pub weekdays: Vec<u32>,
    pub start_minute: u32,
    pub end_minute: u32,
}

impl DownloadWindow {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.timezone.parse::<chrono_tz::Tz>().is_err()
            || self.weekdays.is_empty()
            || self.weekdays.len() > 7
            || self.weekdays.iter().any(|day| *day > 6)
            || self.start_minute >= 1440
            || self.end_minute >= 1440
            || self.start_minute == self.end_minute
        {
            return Err(Error::new("invalid", "invalid timezone or download window"));
        }
        Ok(())
    }

    /// Checks eligibility without modifying the queue or relying on a client's clock.
    /// Invalid windows are closed; manager commands reject them before persistence.
    pub fn allows_at(&self, instant: DateTime<Utc>) -> bool {
        if self.validate().is_err() {
            return false;
        }
        let local = instant.with_timezone(
            &self
                .timezone
                .parse::<chrono_tz::Tz>()
                .expect("validated timezone"),
        );
        let minute = local.hour() * 60 + local.minute();
        let day = local.weekday().num_days_from_monday();
        if self.start_minute < self.end_minute {
            self.weekdays.contains(&day) && (self.start_minute..self.end_minute).contains(&minute)
        } else {
            (minute >= self.start_minute && self.weekdays.contains(&day))
                || (minute < self.end_minute && self.weekdays.contains(&((day + 6) % 7)))
        }
    }
}

/// First matching rule wins. Explicit output/output_dir bypass automatic rules.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FolderRule {
    pub category: Option<String>,
    pub source_host: Option<String>,
    pub directory: std::path::PathBuf,
}

/// No collision mode ever overwrites an unrelated file. Unique tries at most 100 suffixes.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Collision {
    #[default]
    Fail,
    Unique,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub request: arkama_core::DownloadRequest,
    #[serde(default = "default_queue")]
    pub queue: String,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub collision: Collision,
}

fn default_queue() -> String {
    "default".into()
}

impl From<arkama_core::DownloadRequest> for Submission {
    fn from(request: arkama_core::DownloadRequest) -> Self {
        Self {
            request,
            queue: default_queue(),
            priority: 0,
            category: None,
            collision: Collision::Fail,
        }
    }
}

impl Submission {
    pub(crate) fn size_valid(&self) -> bool {
        self.queue.len() <= 64
            && self.category.as_ref().is_none_or(|value| value.len() <= 64)
            && crate::model::request_size_valid(&self.request)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchResult {
    Accepted { id: i64 },
    Rejected { error: Error },
}

#[cfg(test)]
mod tests {
    use super::DownloadWindow;
    use chrono::{DateTime, Utc};

    #[test]
    fn windows_use_timezone_boundaries_overnight_days_and_dst() {
        let at = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
        let overnight = DownloadWindow {
            timezone: "Asia/Tokyo".into(),
            weekdays: vec![0],
            start_minute: 1410,
            end_minute: 30,
        };
        assert!(!overnight.allows_at(at("2026-11-23T14:29:59Z")));
        assert!(overnight.allows_at(at("2026-11-23T14:30:00Z")));
        assert!(overnight.allows_at(at("2026-11-23T15:29:59Z")));
        assert!(!overnight.allows_at(at("2026-11-23T15:30:00Z")));
        assert!(!overnight.allows_at(at("2026-11-24T14:30:00Z")));
        let repeated = DownloadWindow {
            timezone: "America/New_York".into(),
            weekdays: vec![6],
            start_minute: 75,
            end_minute: 105,
        };
        assert!(repeated.allows_at(at("2026-11-01T05:30:00Z")));
        assert!(repeated.allows_at(at("2026-11-01T06:30:00Z")));
        assert!(!repeated.allows_at(at("2026-11-01T06:45:00Z")));
        let skipped = DownloadWindow {
            timezone: "America/New_York".into(),
            weekdays: vec![6],
            start_minute: 120,
            end_minute: 180,
        };
        assert!(!skipped.allows_at(at("2026-03-08T06:59:59Z")));
        assert!(!skipped.allows_at(at("2026-03-08T07:00:00Z")));
    }
}
