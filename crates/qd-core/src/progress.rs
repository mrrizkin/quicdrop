use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::util::{human_bytes, human_rate};

/// Observer for transfer progress, called with `(done, total)`.
///
/// Sinks are throttled to ~10 updates per second (plus one final update) so a
/// GUI or event stream is not flooded by 512 KiB chunks.
pub type ProgressSink = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Shared byte counter plus a live progress line on stderr.
pub struct Progress {
    total: u64,
    done: AtomicU64,
    started: Instant,
    sink: Option<ProgressSink>,
    last_emit_ms: AtomicU64,
}

impl Progress {
    pub fn new(total: u64) -> Self {
        Self {
            total,
            done: AtomicU64::new(0),
            started: Instant::now(),
            sink: None,
            last_emit_ms: AtomicU64::new(0),
        }
    }

    pub fn with_sink(total: u64, sink: ProgressSink) -> Self {
        Self {
            sink: Some(sink),
            ..Self::new(total)
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn add(&self, n: u64) {
        let done = self.done.fetch_add(n, Ordering::Relaxed) + n;
        if let Some(sink) = &self.sink {
            let now_ms = self.started.elapsed().as_millis() as u64;
            let last = self.last_emit_ms.load(Ordering::Relaxed);
            if done >= self.total || now_ms.saturating_sub(last) >= 100 {
                self.last_emit_ms.store(now_ms, Ordering::Relaxed);
                sink(done, self.total);
            }
        }
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
