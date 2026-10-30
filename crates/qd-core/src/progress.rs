use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::util::{human_bytes, human_rate};

/// Shared byte counter plus a live progress line on stderr.
pub struct Progress {
    total: u64,
    done: AtomicU64,
    started: Instant,
}

impl Progress {
    pub fn new(total: u64) -> Self {
        Self {
            total,
            done: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn add(&self, n: u64) {
        self.done.fetch_add(n, Ordering::Relaxed);
    }

    pub fn done(&self) -> u64 {
        self.done.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub async fn run(&self, quiet: bool) {
        if quiet || self.total == 0 || !std::io::stderr().is_terminal() {
            return;
        }
        let mut last = 0u64;
        let mut last_t = Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let done = self.done();
            let now = Instant::now();
            let dt = now.duration_since(last_t).as_secs_f64().max(0.001);
            let rate = done.saturating_sub(last) as f64 / dt;
            last = done;
            last_t = now;
            let pct = (done as f64 / self.total as f64 * 100.0).min(100.0);
            let mut err = std::io::stderr();
            let _ = write!(
                err,
                "\r  {pct:5.1}%  {} / {}  {}   ",
                human_bytes(done),
                human_bytes(self.total),
                human_rate(rate)
            );
            let _ = err.flush();
            if done >= self.total {
                let _ = writeln!(err);
                return;
            }
        }
    }
}
