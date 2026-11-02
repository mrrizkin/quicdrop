# Contributing

QuicDrop is a Rust workspace plus a Tauri desktop shell. Behaviour lives in `qd-core`;
`qd` (CLI) and `quicdropd` (loopback API) only wire it up.

## Setup

- Rust 1.82 or newer, with the `rustfmt` and `clippy` components.
- [just](https://github.com/casey/just) for the repository recipes.
- Linux desktop builds need the `webkit2gtk-4.1`, `gtk+-3.0`, `libsoup-3.0` and
  `javascriptcoregtk-4.1` development packages.

```bash
just verify   # fmt check, clippy with -D warnings, workspace tests, flatpak source check
just build    # release build of qd and quicdropd
just desktop  # run the Tauri app
```

`just bench` runs a loopback transfer end to end; `just bench-core` covers hashing, the
folder walk and offer framing.

## Layout

- `crates/qd-core` — protocol, discovery, sender, receiver.
- `crates/qd` — the `qd` CLI.
- `crates/qd-daemon` — `quicdropd`: loopback HTTP/SSE API, history, approval plumbing.
- `apps/desktop` — Tauri shell and the buildless `ui/`.
- `packaging` — Arch/AUR recipes, Flatpak manifest, desktop entry and AppStream data.

## Rules

- Run `just verify` before sending a change. CI additionally runs a fmt check, clippy and
  a `--locked` build for the desktop crate, and verifies that
  `packaging/flatpak/cargo-sources.json` matches `apps/desktop/Cargo.lock`.
- Changing a frame shape or its meaning bumps `PROTO_VERSION` in
  `crates/qd-core/src/proto.rs`.
- Everything a peer sends is untrusted input. A new limit gets a constant and a check.
- Keep `apps/desktop/ui` buildless: no npm, no bundler, colors come from CSS tokens.
- Docs and comments describe what is true now, not status, roadmap or history.
- Commit no build output, keys, `.env`, or local state.

## Pull requests

- Use the prefixes the history already follows: `feat`, `fix`, `build`, `chore`, `docs`,
  `refactor`, `packaging`, with a scope such as `fix(desktop):`.
- Keep a pull request to one topic and say what changed and why.
- Update the README when user-visible behaviour changes.
- Releases are cut from `main` by maintainers with `just tag`; tags are
  `v<YEAR>.<ISO week>.<patch>` in UTC.

## Conduct

Participation is covered by the [Code of Conduct](CODE_OF_CONDUCT.md).
