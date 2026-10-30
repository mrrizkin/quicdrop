# quicdrop

Fast peer-to-peer file transfer over your local network. Think AirDrop/LocalSend, but the
wire protocol is **QUIC** instead of HTTP/1.1 over TLS, and the core is a small Rust library
so any UI (CLI, Tauri, mobile) can sit on top of it.

```
$ qd receive
qd receiving as "laptop" (4d915ba1) on 192.168.1.42:53320
saving to /home/you/Downloads/QuicDrop

$ qd send photos/*.jpg --to laptop
looking for "laptop" (up to 5s)...
connecting to laptop (4d915ba1) at 192.168.1.42:53320
  ok      IMG_4021.jpg (4.20 MiB)
  ok      IMG_4022.jpg (3.98 MiB)
sent 2 / 2 files in 0.4s (20.4 MiB/s)
```

Status: **desktop MVP, working end to end.** Mobile is intentionally out of scope for now.

## Why QUIC

LocalSend's bottleneck is rarely raw bandwidth; it is per-file serialization, TCP
head-of-line blocking, and a congestion controller that assumes a big pipe. QUIC gives us
something better for free:

| Workload | Why QUIC wins |
| --- | --- |
| Thousands of small files | one stream per file, multiplexed — no queue of request/response round trips |
| Lossy Wi-Fi / far from the AP | stream-level loss recovery: a lost packet stalls one file, not the connection |
| Folder sync / resume | `RESET_STREAM` + byte offsets, no re-negotiation |
| Reconnects (browse → send → send) | 0-RTT with a cached session ticket |
| Wi-Fi ↔ hotspot switch | connection migration via connection IDs |

Measured on loopback with the release build (not representative of Wi-Fi, but it shows where
the parallelism helps):

| Scenario | Result |
| --- | --- |
| 512 MiB, single file | 0.6 s (799 MiB/s) |
| 2000 × 4 KiB, `-j 1` | 0.3 s |
| 2000 × 4 KiB, `-j 4` | 0.2 s |
| 2000 × 4 KiB, `-j 16` | 0.2 s |
| Resume 64 MiB file from a 20 MB partial | only 44.93 MiB transferred |

## Install / build

```bash
cargo build --release
./target/release/qd --help
```

Needs a Rust toolchain (1.82+). Linux, macOS, Windows.

## Usage

```bash
qd receive [--out DIR]        # advertise + accept transfers
qd peers [--timeout 4s]       # who is on this network
qd send <FILES...> --to PEER  # send files
qd id                         # print this device's name / id / config dir
```

`--to` accepts a device name (`laptop`), a device id prefix (`4d915ba1`), an IP
(`192.168.1.42`) or `ip:port`. Connecting by raw `ip:port` skips identity pinning (it is
warned about).

Global flags: `--port` (QUIC, default `53320`), `--discovery-port` (default `53321`),
`--name`, `-q/--quiet`, `-v` (repeatable).

Sending options: `-j/--concurrency` (parallel file streams, default 4),
`--timeout` (how long to wait for the peer to appear, default `5s`).

State lives in `~/.config/quicdrop` (override with `QD_CONFIG_DIR`): `identity.key`,
`identity.crt`, `name.txt`. Delete the directory to become a new device.

## Architecture

```
crates/
  qd-core/          protocol + transfer engine (no UI, no CLI)
    identity.rs     self-signed cert, device id = blake3(cert) fingerprint
    discovery.rs    mDNS + UDP broadcast beacon, dedup by device id
    proto.rs        postcard frames, framing, file-stream layout
    net.rs          QUIC endpoints, TLS 1.3, certificate pinning verifier
    client.rs       sender
    server.rs       receiver
    progress.rs     byte counter + live meter
  qd/               clap CLI
```

### Discovery

Two mechanisms run in parallel because neither works everywhere:

* **mDNS** (`_quicdrop._udp.local.`) with TXT records `id`, `name`, `platform`, `v`.
  Zero config, but dies on networks that filter multicast.
* **UDP beacon** to `255.255.255.255:53321` and `224.0.0.167:53321` every 3 s, carrying the
  full peer record in the payload (`QDB1` + postcard). Survives most campus/office LANs and
  needs no name resolution.

Sightings are merged by `device_id`; loopback addresses are ranked last so a peer that is
also reachable on the LAN is preferred.

### Protocol

One QUIC connection per transfer session, ALPN `quicdrop/1`.

* **Control stream** (first bidirectional stream opened by the sender), length-prefixed
  postcard frames:

  | Frame | Direction | Purpose |
  | --- | --- | --- |
  | `Hello` | sender → receiver → sender | protocol version, device id, display name |
  | `Offer` | sender → receiver | session id + `FileMeta{id,name,size,hash?}` list |
  | `Accept` / `Reject` | receiver → sender | per-file byte offset to resume from |
  | `Complete` | receiver → sender | total bytes written |
  | `Error` / `Bye` | either | terminal states |

* **One bidirectional stream per file**, concurrently (default 4):
  `FileHeader{session,id,offset,size}` → raw payload → `FileTrailer{hash}` → receiver
  replies `FileAck{ok,hash,message}`.

  The digest travels *after* the payload, so the sender never does a second read pass over
  the file: it hashes while streaming. On resume it hashes the skipped prefix from disk so
  the final digest still covers the whole file.

### Security model

No PKI, no pairing codes. Identity is the certificate:

1. Each device generates an ECDSA P-256 key once and keeps a self-signed cert.
2. `device_id = hex(blake3(cert_der)[..16])`, advertised over discovery.
3. The sender builds a `rustls` verifier that pins that exact fingerprint
   (`net.rs::PinnedVerifier`) — a peer that announces id `4d915ba1` but presents a different
   certificate is rejected during the handshake, before any file is offered.

Transport is TLS 1.3 with ALPN. Incoming file names are sanitized (`util::sanitize_file_name`)
so a sender cannot write outside the destination directory, and existing files are never
overwritten — `name (1).ext` is chosen instead.

Known gap: the receiver learns the sender's `device_id` from the `Hello` frame without
cryptographic proof (the sender does not present a client certificate yet). See roadmap.

## Roadmap

- [ ] Receiver approval prompt (`--ask`) with a per-transfer allow/deny, plus a trust store
      so known devices skip it.
- [ ] Mutual authentication: sender signs a session nonce with its identity key, receiver
      pins it too.
- [ ] Directory transfer (recursive walk, preserve relative paths).
- [ ] Persisted session tickets → 0-RTT on reconnect.
- [ ] Clipboard/text transfer.
- [ ] Long-lived daemon (`quicdropd`) with a local JSON API for a GUI.
- [ ] Tauri v2 shell (desktop first, then Android/iOS).
- [ ] Benchmark harness comparing against LocalSend on real Wi-Fi.
- [ ] Fallback to TCP+TLS when UDP is blocked.

## Platform notes (for the future mobile port)

* **Android**: receiving multicast requires `WifiManager.MulticastLock`; background transfers
  need a `dataSync` foreground service; saving needs SAF.
* **iOS**: `NSLocalNetworkUsageDescription` + `NSBonjourServices`; effectively foreground-only.
* **Windows**: first bind triggers a firewall prompt for inbound UDP.

## License

MIT OR Apache-2.0.
