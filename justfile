set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

desktop_manifest := "apps/desktop/Cargo.toml"

default:
    @just --list

fmt:
    cargo fmt --all
    cargo fmt --manifest-path {{desktop_manifest}}

fmt-check:
    cargo fmt --all -- --check
    cargo fmt --manifest-path {{desktop_manifest}} -- --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings
    cargo clippy --manifest-path {{desktop_manifest}} --all-targets -- -D warnings

test:
    cargo test --workspace

verify: fmt-check lint test

# Next release tag for the current UTC ISO week.
next-version:
    ./scripts/next-version.sh

# Stamp that version, commit it and tag it.
tag:
    ./scripts/next-version.sh --create

build:
    cargo build --release

desktop:
    cargo run --manifest-path {{desktop_manifest}}
