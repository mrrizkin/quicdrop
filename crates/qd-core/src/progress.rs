//! Byte counting for the terminal meter and for embedders that want live progress.

use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::util::{human_bytes, human_rate};

/// Observer for transfer progress, called with `(done, total)`.
///
/// Throttled to ~10 updates per second (plus one final update) so a GUI is not flooded.
pub type ProgressSink = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Per-file observer: `(file id, bytes done, file size)`.
pub type FileSink = Arc<dyn Fn(u32, u64, u64) + Send + Sync>;

/// Minimum interval between per-file progress updates.
pub const FILE_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy)]
struct FileStamp {
    at_ms: u64,
    done: u64,
}

/// Throttles per-file progress. A completed file always emits.
pub struct FileThrottle {
    started: Instant,
    interval: Duration,
    seen: Mutex<HashMap<u32, FileStamp>>,
}

impl Default for FileThrottle {
    fn default() -> Self {
        Self::new(FILE_PROGRESS_INTERVAL)
    }
}

impl FileThrottle {
    pub fn new(interval: Duration) -> Self {
        Self {
            started: Instant::now(),
            interval,
            seen: Mutex::new(HashMap::new()),
        }
    }

    pub fn should_emit(&self, id: u32, done: u64, size: u64) -> bool {
        self.should_emit_at(id, done, size, self.started.elapsed().as_millis() as u64)
    }

    fn should_emit_at(&self, id: u32, done: u64, size: u64, now_ms: u64) -> bool {
        let interval = self.interval.as_millis() as u64;
        let mut seen = match self.seen.lock() {
            Ok(seen) => seen,
            Err(poisoned) => poisoned.into_inner(),
        };
        let complete = size == 0 || done >= size;
        match seen.get_mut(&id) {
            None => {
                seen.insert(
                    id,
                    FileStamp {
                        at_ms: now_ms,
                        done,
                    },
                );
                true
            }
            Some(stamp) => {
                let reset = done < stamp.done;
                let finished = complete && stamp.done < size;
                let moved = done > stamp.done && now_ms.saturating_sub(stamp.at_ms) >= interval;
                if reset || finished || moved {
                    *stamp = FileStamp {
                        at_ms: now_ms,
                        done,
                    };
                    true
                } else {
                    false
                }
            }
        }
    }
}

/// Wrap a per-file observer with [`FileThrottle`].
pub fn throttle_file_sink(sink: FileSink) -> FileSink {
    let throttle = FileThrottle::default();
    Arc::new(move |id, done, size| {
        if throttle.should_emit(id, done, size) {
            sink(id, done, size);
        }
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_and_final_file_updates_always_pass() {
        let throttle = FileThrottle::new(Duration::from_millis(100));
        assert!(throttle.should_emit_at(1, 10, 100, 0));
        assert!(!throttle.should_emit_at(1, 20, 100, 50));
        assert!(throttle.should_emit_at(1, 100, 100, 60));
        assert!(!throttle.should_emit_at(1, 100, 100, 500));
    }

    #[test]
    fn updates_are_limited_to_the_interval() {
        let throttle = FileThrottle::new(Duration::from_millis(100));
        assert!(throttle.should_emit_at(7, 1, 1000, 0));
        assert!(!throttle.should_emit_at(7, 2, 1000, 99));
        assert!(throttle.should_emit_at(7, 3, 1000, 100));
        assert!(!throttle.should_emit_at(7, 4, 1000, 199));
        assert!(throttle.should_emit_at(7, 5, 1000, 200));
    }

    #[test]
    fn a_backwards_update_reopens_the_gate() {
        let throttle = FileThrottle::new(Duration::from_millis(100));
        assert!(throttle.should_emit_at(3, 500, 1000, 0));
        assert!(throttle.should_emit_at(3, 10, 1000, 1));
    }
}
