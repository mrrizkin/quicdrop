#!/bin/sh
# Build the CLI and copy it where the desktop bundler picks it up as a sidecar.
# The files are named with the host target triple, as `externalBin` requires.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
dest="$root/apps/desktop/binaries"
triple=$(rustc -vV | sed -n 's|^host: ||p')

cargo build --release --locked --manifest-path "$root/Cargo.toml" -p qd -p qd-daemon
mkdir -p "$dest"
install -m755 "$root/target/release/qd" "$dest/qd-$triple"
install -m755 "$root/target/release/quicdropd" "$dest/quicdropd-$triple"
echo "$triple"
