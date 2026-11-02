# Security Policy

## Supported versions

The latest tagged release is supported. Fixes land on `main` and ship in the next
`v<YEAR>.<ISO week>.<patch>` tag; older releases are not patched separately.

## Reporting a vulnerability

Report privately, not in a public issue:

- Use GitHub's [private vulnerability report](https://github.com/mrrizkin/quicdrop/security/advisories/new)
  (Security → Advisories → Report a vulnerability).
- Or email <nugraha07rizki@gmail.com>.

Include the version, platform, the impact you observed, and steps to reproduce. A proof
of concept helps but is not required. Reports are acknowledged and handled with a fix and
a coordinated advisory before details are published.

## Scope

In scope: `qd`, `quicdropd`, `qd-core` (protocol, discovery, sender, receiver), the desktop
app under `apps/desktop`, and the packaging in `packaging/`.

Useful to report:

- A peer causing a crash, unbounded memory or disk use, or a path escape outside the
  receive directory.
- Bypassing the receive-side approval, resume offsets, or certificate pinning.
- Reaching the `quicdropd` loopback API without its bearer token.
- Leaking `identity.key` or the daemon token to another local user.

## Known limitations

These are documented behaviour, not vulnerabilities on their own:

- The receiver does not authenticate the sender's certificate. A trusted device id is
  scoped to the channel the user approved, not to a signed long-term identity.
- mDNS and the UDP beacon are unauthenticated; peers found that way are only shortcuts to
  an address that still has to complete the handshake.
- Sending by raw address skips certificate pinning by design.

Vulnerabilities in third-party crates should go to the upstream project. We track them
through Dependabot.
