#!/usr/bin/env bash
# End-to-end transfer benchmark over loopback.
#
#   scripts/bench-transfer.sh            # 512 MiB, 2 GiB and 10k small files
#   RUNS=5 scripts/bench-transfer.sh     # more repetitions
#   PERF=1 scripts/bench-transfer.sh     # also record perf counters
#   REMOTE=10.0.0.5:53320 scripts/bench-transfer.sh
#                                        # send to a peer instead of loopback
#
# Payloads live on tmpfs when it exists, so the run measures the transfer path
# instead of the disk. Point SRC_ROOT at a real directory to include the disk.
#
# Reports MiB/s, and with PERF=1 the perf counters for the sending side.

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RUNS=${RUNS:-3}
QUIC_PORT=${QUIC_PORT:-54320}
API_PORT=${API_PORT:-54319}
DISC_PORT=${DISC_PORT:-54321}
WORK=${WORK:-$(mktemp -d)}
PERF=${PERF:-0}
REMOTE=${REMOTE:-}

BIN="$ROOT/target/release"
if [[ ! -x "$BIN/qd" || ! -x "$BIN/quicdropd" ]]; then
    echo "building release binaries..." >&2
    cargo build --release --manifest-path "$ROOT/Cargo.toml" >/dev/null
fi

if [[ -d /dev/shm && -w /dev/shm ]]; then
    SRC_ROOT=${SRC_ROOT:-/dev/shm/qd-bench-src}
else
    SRC_ROOT=${SRC_ROOT:-$WORK/src}
fi
OUT="$WORK/out"
mkdir -p "$SRC_ROOT" "$OUT"

DAEMON_PID=""
cleanup() {
    if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    if [[ "${KEEP:-0}" != "1" ]]; then
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT

payload() {
    local path=$1 bytes=$2
    if [[ ! -f "$path" || $(stat -c %s "$path" 2>/dev/null || echo 0) -ne $bytes ]]; then
        head -c "$bytes" /dev/urandom >"$path"
    fi
}

small_files() {
    local dir=$1 count=$2
    if [[ -d "$dir" && $(find "$dir" -type f | wc -l) -eq $count ]]; then
        return
    fi
    rm -rf "$dir"
    for i in $(seq 1 "$count"); do
        local sub="$dir/d$(printf '%02d' $((i % 20)))"
        mkdir -p "$sub"
        printf 'x' >"$sub/f$(printf '%05d' "$i").bin"
    done
}

# With REMOTE set the receiver is another machine: nothing local to start, and the
# output directory and CPU counters belong to that machine.
start_daemon() {
    if [[ -n "$REMOTE" ]]; then
        TARGET=$REMOTE
        return
    fi
    TARGET="127.0.0.1:$QUIC_PORT"
    "$BIN/quicdropd" \
        --listen "127.0.0.1:$API_PORT" \
        --port "$QUIC_PORT" \
        --discovery-port "$DISC_PORT" \
        --out "$OUT" \
        --policy auto \
        --quiet >"$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    for _ in $(seq 1 100); do
        if grep -q "api:" "$WORK/daemon.log" 2>/dev/null; then
            return
        fi
        sleep 0.05
    done
    echo "daemon did not start; see $WORK/daemon.log" >&2
    cat "$WORK/daemon.log" >&2
    exit 1
}

# One transfer: `send <label> <bytes> <path...>`.
send() {
    local label=$1 bytes=$2
    shift 2
    local best="" total=0
    for run in $(seq 1 "$RUNS"); do
        rm -rf "$OUT"
        mkdir -p "$OUT"
        local started ended elapsed
        started=$(date +%s%N)
        if [[ "$PERF" == "1" ]] && command -v perf >/dev/null; then
            perf stat -e task-clock,cycles,instructions,context-switches \
                -o "$WORK/perf.run" \
                "$BIN/qd" --quiet --port "$QUIC_PORT" send --to "$TARGET" "$@" \
                >"$WORK/send.log" 2>&1
        else
            "$BIN/qd" --quiet --port "$QUIC_PORT" send --to "$TARGET" "$@" \
                >"$WORK/send.log" 2>&1
        fi
        ended=$(date +%s%N)
        elapsed=$((ended - started))
        total=$((total + elapsed))
        if [[ -z "$best" || $elapsed -lt $best ]]; then
            best=$elapsed
        fi
        if [[ "$PERF" == "1" && -f "$WORK/perf.run" ]]; then
            grep -E "task-clock|cycles|instructions|context-switches" "$WORK/perf.run" |
                sed "s/^/    run $run: /" || true
        fi
    done
    awk -v label="$label" -v bytes="$bytes" -v best="$best" -v total="$total" -v runs="$RUNS" 'BEGIN {
        mib = bytes / 1048576
        printf "%-22s best %7.1f MiB/s   mean %7.1f MiB/s   (%.2fs best of %d)\n", \
            label, mib / (best / 1e9), mib / (total / runs / 1e9), best / 1e9, runs
    }'
}

echo "source : $SRC_ROOT"
echo "out    : $OUT"
echo "runs   : $RUNS"
echo

payload "$SRC_ROOT/medium.bin" $((512 * 1024 * 1024))
payload "$SRC_ROOT/large.bin" $((2 * 1024 * 1024 * 1024))
small_files "$SRC_ROOT/small" 10000

start_daemon
if [[ -n "$REMOTE" ]]; then
    echo "target : $TARGET"
else
    echo "daemon : pid $DAEMON_PID on udp/$QUIC_PORT"
fi
echo

send "512 MiB file" $((512 * 1024 * 1024)) "$SRC_ROOT/medium.bin"
send "2 GiB file" $((2 * 1024 * 1024 * 1024)) "$SRC_ROOT/large.bin"
send "10k small files" $((10 * 1024 * 1024)) "$SRC_ROOT/small"

echo
echo "core paths (no wire):"
cargo run --release --quiet --manifest-path "$ROOT/Cargo.toml" \
    -p qd-core --example bench_core -- "$RUNS"
