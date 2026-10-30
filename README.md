# quicdrop

Peer-to-peer file transfer on your local network, over QUIC instead of HTTP.

```
$ qd receive
qd receiving as "laptop" (4d915ba1) on 192.168.1.42:53320

$ qd send photos --to laptop
  ok      photos/IMG_4021.jpg (4.20 MiB)
  ok      photos/2024/beach.jpg (3.98 MiB)
sent 2 / 2 files in 0.4s (20.4 MiB/s)
```

![quicdrop](docs/screenshot.png)

## Why QUIC

One stream per file removes the request queue and TCP head-of-line blocking that per-file
HTTP transfers pay. A lost packet stalls one file, not the session. Byte offsets make resume
free, and connection migration survives a Wi-Fi to hotspot switch.

## Install

```bash
cargo build --release
./target/release/qd --help
```

Rust 1.82+. Linux, macOS, Windows. Prebuilt packages for each platform are on the
[releases page](https://github.com/mrrizkin/quicdrop/releases).

## Usage

```bash
qd receive [--out DIR] [--policy ask|auto|deny]
qd peers [--timeout 4s]
qd send <PATHS...> --to <PEER> [-j 4]
qd trust [add <ID> | rm <ID> | list]
qd id
```

`--to` takes a device name, device id prefix, IP, or `ip:port`. Sending by address skips
discovery and certificate pinning. Folders are walked recursively, symlinks skipped, relative
paths recreated on the receiver; an existing file becomes `name (1).ext`.

State is in `~/.config/quicdrop` (override `QD_CONFIG_DIR`): identity, `name.txt`,
`trusted.json`. Incoming offers from unknown devices prompt on the terminal, or are rejected
when there is no terminal. Deleting the directory creates a new device.

Desktop app:

```bash
cargo run --manifest-path apps/desktop/Cargo.toml
```

It embeds the daemon (`quicdropd`, loopback API on 53319) and serves a static `ui/`.
Linux needs `webkit2gtk-4.1`, `gtk+-3.0`, `libsoup-3.0`, `javascriptcoregtk-4.1`.

## Protocol

QUIC on UDP 53320, discovery on 53321, ALPN `quicdrop/1`. One connection per transfer,
length-prefixed postcard frames on the control stream:

| Frame | Direction | Purpose |
| --- | --- | --- |
| `Hello` | sender → receiver → sender | version, device id, name, platform |
| `Offer` | sender → receiver | session id, `FileMeta{id,name,size,hash?}` |
| `Accept` / `Reject` | receiver → sender | per-file resume offset, or reason |
| `Complete` | receiver → sender | bytes written |
| `Error` / `Bye` | either | terminal |

Each file is its own bidirectional stream: `FileHeader{session,id,offset,size}`, payload,
`FileTrailer{hash}`, then `FileAck{ok,hash,message}`. The digest follows the payload, so the
sender hashes while streaming. Peer discovery uses mDNS and a UDP beacon in parallel.

## Security

- ECDSA P-256 identity, stored once; `device_id` is the blake3 fingerprint of the cert.
- Senders pin the receiver's fingerprint, so a spoofed device id fails the handshake.
- The receiver approves or rejects every offer before touching the disk.
- Sender-supplied names are relative paths: `..` is stripped, existing files are not
  overwritten.

The receiver does not authenticate the *sender's* certificate, so a trusted device id is
scoped to the channel the user approved, not to a signed long-term identity.

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).
