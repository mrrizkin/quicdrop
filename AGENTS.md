# Agent Instructions

```bash
just verify    # fmt check, clippy, workspace tests
just build     # release build of qd and quicdropd
just desktop   # run the Tauri app
```

## Layout

- `crates/qd-core` — engine: protocol, discovery, sender, receiver. No CLI or daemon concerns.
- `crates/qd` — `qd` CLI.
- `crates/qd-daemon` — `quicdropd`: loopback HTTP/SSE API, history, approval plumbing.
- `apps/desktop` — Tauri shell (outside the workspace) and the buildless `ui/`.
- `packaging/` — PKGBUILD, Flatpak manifest, shared desktop entry and AppStream data.
- `.github/workflows/` — `ci` gates, `release` artifacts on `v*` tags, `flatpak` bundle.

## Rules

- Behavior belongs in `qd-core`; hosts only wire it up.
- Changing a frame shape or its meaning bumps `PROTO_VERSION`.
- Everything a peer sends is untrusted input; new limits get a constant and a check.
- Keep `apps/desktop/ui` buildless: no npm, no bundler, colors come from CSS tokens.
- Docs and comments state what is true now: no status, roadmap, or change history.
- Commit no build output, keys, `.env`, or local state.
- Never `pkill -f` a pattern that matches the invoking shell.
- Version tags are `v<YEAR>.<ISO week>.<patch>` in UTC, cut with `just tag`.
