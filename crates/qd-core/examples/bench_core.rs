//! Micro-benchmarks for the parts of a transfer that are not the wire itself.
//!
//! ```text
//! cargo run --release -p qd-core --example bench_core -- [reps]
//! ```
//!
//! Deliberately dependency free: a benchmark crate would grow the lockfile and
//! `packaging/flatpak/cargo-sources.json`. Numbers are the best of `reps` runs.

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use qd_core::proto::{FileMeta, MAX_NAME_LEN};

const HASH_BYTES: usize = 64 * 1024 * 1024;
const FILE_COUNT: usize = 10_000;

fn main() -> anyhow::Result<()> {
    let reps: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(5);
    let dir = std::env::temp_dir().join(format!("qd-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;

    println!("reps = {reps}, scratch = {}", dir.display());

    let big = dir.join("payload.bin");
    write_payload(&big, HASH_BYTES)?;
    let best = best_of(reps, || {
        let hash = qd_core::hash::hash_file_sync(&big).unwrap();
        black_box(hash);
    });
    report_bytes("hash_file_sync (64 MiB)", HASH_BYTES as u64, best);

    let tree = dir.join("tree");
    build_tree(&tree)?;
    let best = best_of(reps, || {
        let files = qd_core::source::expand(std::slice::from_ref(&tree)).unwrap();
        assert_eq!(files.len(), FILE_COUNT);
        black_box(files);
    });
    report_count("source::expand (10k files)", FILE_COUNT as u64, best);

    let offer = sample_offer();
    let encoded = postcard::to_stdvec(&offer)?;
    println!(
        "  offer of {FILE_COUNT} files encodes to {} KiB",
        encoded.len() / 1024
    );
    let best = best_of(reps, || {
        let bytes = postcard::to_stdvec(&offer).unwrap();
        black_box(bytes);
    });
    report_count("offer serialize", FILE_COUNT as u64, best);
    let best = best_of(reps, || {
        let decoded: Vec<FileMeta> = postcard::from_bytes(&encoded).unwrap();
        black_box(decoded);
    });
    report_count("offer deserialize", FILE_COUNT as u64, best);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn best_of(reps: usize, mut body: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..reps.max(1) {
        let started = Instant::now();
        body();
        best = best.min(started.elapsed());
    }
    best
}

fn report_bytes(label: &str, bytes: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "  {label:<28} {:>9.2} MiB/s  ({:.1} ms)",
        bytes as f64 / secs / (1024.0 * 1024.0),
        elapsed.as_secs_f64() * 1000.0
    );
}

fn report_count(label: &str, count: u64, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "  {label:<28} {:>9.0} /s      ({:.1} ms)",
        count as f64 / secs,
        elapsed.as_secs_f64() * 1000.0
    );
}

fn write_payload(path: &Path, bytes: usize) -> anyhow::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    let block = vec![0xABu8; 1024 * 1024];
    for _ in 0..bytes.div_ceil(block.len()) {
        file.write_all(&block)?;
    }
    file.set_len(bytes as u64)?;
    Ok(())
}

fn build_tree(root: &Path) -> anyhow::Result<()> {
    for index in 0..FILE_COUNT {
        let dir = root.join(format!("d{:02}", index % 20));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join(format!("f{index:05}.bin")), b"x")?;
    }
    Ok(())
}

/// A worst-case offer: the file limit, with names near the per-name limit.
fn sample_offer() -> Vec<FileMeta> {
    let long = "a".repeat(MAX_NAME_LEN - 16);
    (0..FILE_COUNT as u32)
        .map(|id| FileMeta {
            id,
            name: format!("{long}/{id:05}.bin"),
            size: 4096,
            hash: None,
        })
        .collect()
}
