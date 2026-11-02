#!/usr/bin/env python3
"""Check that packaging/flatpak/cargo-sources.json matches apps/desktop/Cargo.lock.

The flatpak manifest builds offline with `cargo --offline build --locked`, so the
vendored crate set must track the desktop lockfile exactly. The flatpak workflow
only runs on tags, which means a stale source list would otherwise surface at
release time.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOCK = ROOT / "apps/desktop/Cargo.lock"
SOURCES = ROOT / "packaging/flatpak/cargo-sources.json"
VENDOR = "cargo/vendor/"


def lock_packages(text):
    """Registry packages with a checksum, keyed by (name, version)."""
    pkgs = {}
    for block in text.split("[[package]]")[1:]:
        name = re.search(r'^name = "(.*)"', block, re.M)
        version = re.search(r'^version = "(.*)"', block, re.M)
        source = re.search(r'^source = "(.*)"', block, re.M)
        checksum = re.search(r'^checksum = "(.*)"', block, re.M)
        if name and version and source and checksum:
            pkgs[(name.group(1), version.group(1))] = checksum.group(1)
    return pkgs


def main():
    pkgs = lock_packages(LOCK.read_text())
    entries = json.loads(SOURCES.read_text())

    archives = {}
    inline = {}
    for entry in entries:
        dest = entry.get("dest", "")
        if not dest.startswith(VENDOR):
            continue
        crate = dest[len(VENDOR):]
        if entry.get("type") == "archive":
            archives[crate] = entry
        elif entry.get("type") == "inline" and entry.get("dest-filename") == ".cargo-checksum.json":
            inline[crate] = entry

    expected = {"%s-%s" % (name, version): checksum for (name, version), checksum in pkgs.items()}

    missing = sorted(set(expected) - set(archives))
    extra = sorted(set(archives) - set(expected))
    bad_sha = []
    bad_inline = []
    for crate, checksum in expected.items():
        archive = archives.get(crate)
        if archive is None:
            continue
        if archive.get("sha256") != checksum:
            bad_sha.append(crate)
        record = inline.get(crate)
        if record is None:
            bad_inline.append(crate + " (missing)")
            continue
        try:
            packaged = json.loads(record["contents"]).get("package")
        except (KeyError, json.JSONDecodeError):
            packaged = None
        if packaged != checksum:
            bad_inline.append(crate)

    failed = False
    if missing:
        failed = True
        print("missing from cargo-sources.json: " + ", ".join(missing))
    if extra:
        failed = True
        print("stale entries in cargo-sources.json: " + ", ".join(extra))
    if bad_sha:
        failed = True
        print("sha256 mismatch: " + ", ".join(bad_sha))
    if bad_inline:
        failed = True
        print("bad .cargo-checksum.json: " + ", ".join(bad_inline))

    if failed:
        print(
            "\nRegenerate with flatpak-cargo-generator against apps/desktop/Cargo.lock:\n"
            "  flatpak-cargo-generator -o packaging/flatpak/cargo-sources.json apps/desktop/Cargo.lock"
        )
        return 1

    print("cargo-sources.json matches apps/desktop/Cargo.lock (%d crates)" % len(expected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
