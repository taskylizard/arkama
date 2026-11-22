use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

pub(crate) struct SpeedLimiter {
    state: Mutex<Rate>,
    changed: Notify,
}

struct Rate {
    rate: u64,
    next_at: Instant,
    generation: u64,
}

impl Default for SpeedLimiter {
    fn default() -> Self {
        Self::new(0)
    }
}

impl SpeedLimiter {
    pub(crate) fn new(rate: u64) -> Self {
        Self {
            state: Mutex::new(Rate {
                rate,
                next_at: Instant::now(),
                generation: 0,
            }),
            changed: Notify::new(),
        }
    }

    pub(crate) fn set_rate(&self, rate: u64) {
        let mut state = self.state.lock().unwrap();
        state.rate = rate;
        state.next_at = Instant::now();
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) async fn throttle(&self, bytes: u64) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (target, generation) = {
                let mut state = self.state.lock().unwrap();
                if bytes == 0 || state.rate == 0 {
                    return;
                }
                let nanos = ((u128::from(bytes) * 1_000_000_000) / u128::from(state.rate))
                    .min(u128::from(u64::MAX)) as u64;
                let now = Instant::now();
                state.next_at = state.next_at.max(now) + Duration::from_nanos(nanos.max(1));
                (state.next_at, state.generation)
            };
            tokio::select! {
                _ = tokio::time::sleep_until(target.into()) => {
                    if self.state.lock().unwrap().generation == generation { return; }
                }
                _ = &mut changed => {}
            }
        }
    }
}
