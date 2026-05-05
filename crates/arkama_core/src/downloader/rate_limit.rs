use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) struct SpeedLimiter {
    rate: u64,
    next_at: Mutex<Instant>,
}

impl SpeedLimiter {
    pub(crate) fn new(rate: u64) -> Self {
        Self {
            rate,
            next_at: Mutex::new(Instant::now()),
        }
    }

    pub(crate) async fn throttle(&self, bytes: u64) {
        if bytes == 0 || self.rate == 0 {
            return;
        }
        let nanos = (bytes.saturating_mul(1_000_000_000)) / self.rate;
        let wait = Duration::from_nanos(nanos.max(1));
        let now = Instant::now();
        let target = {
            let mut next_at = self.next_at.lock().unwrap();
            if *next_at < now {
                *next_at = now;
            }
            *next_at += wait;
            *next_at
        };
        if target > now {
            tokio::time::sleep(target - now).await;
        }
    }
}
