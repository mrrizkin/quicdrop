//! Receiver side: advertise, accept QUIC connections, write files to disk.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::discovery::{self, Discovery};
use crate::identity::Identity;
use crate::progress::Progress;
use crate::proto::{self, AcceptFile, FileAck, FileHeader, FileMeta, FileTrailer, Frame};
use crate::util::{human_bytes, human_rate, sanitize_file_name, unique_path};

#[derive(Debug, Clone)]
pub struct ServerOpts {
    pub quic_port: u16,
    pub discovery_port: u16,
    pub out_dir: PathBuf,
    pub name: Option<String>,
    /// Answer discovery probes with a summary of what is on disk (unused placeholder).
    pub quiet: bool,
}

pub struct RunningServer {
    pub identity: Arc<Identity>,
    pub endpoint: quinn::Endpoint,
    pub discovery: Discovery,
}

impl RunningServer {
    pub fn addr_hint(&self, quic_port: u16) -> String {
        let ip = self
            .discovery
            .local_addrs
            .first()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        format!("{ip}:{quic_port}")
    }
}

/// Build the endpoint and start advertising. Use [`serve`] to run the accept loop.
pub async fn start(opts: ServerOpts) -> Result<RunningServer> {
    let identity = Arc::new(Identity::load_or_create(opts.name.clone())?);
    tokio::fs::create_dir_all(&opts.out_dir)
        .await
        .with_context(|| format!("create output dir {}", opts.out_dir.display()))?;

    let config = crate::net::server_config(&identity)?;
    let endpoint = quinn::Endpoint::server(
        config,
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, opts.quic_port)),
    )
    .with_context(|| format!("bind QUIC udp/{}", opts.quic_port))?;
    let discovery =
        discovery::start(identity.clone(), opts.quic_port, opts.discovery_port, true).await?;

    Ok(RunningServer {
        identity,
        endpoint,
        discovery,
    })
}

pub async fn serve(mut server: RunningServer, opts: ServerOpts) -> Result<()> {
    {
        let mut peers = std::mem::replace(
            &mut server.discovery.peers,
            tokio::sync::mpsc::unbounded_channel().1,
        );
        tokio::spawn(async move {
            while let Some(peer) = peers.recv().await {
                debug!("peer seen: {peer}");
            }
        });
    }

    info!(
        "receiving as \"{}\" ({}) on {}",
        server.identity.name,
        server.identity.short_id(),
        server.addr_hint(opts.quic_port)
    );
    info!("saving to {}", opts.out_dir.display());

    let opts = Arc::new(opts);
    while let Some(incoming) = server.endpoint.accept().await {
        let opts = opts.clone();
        let identity = server.identity.clone();
        tokio::spawn(async move {
            let remote = incoming.remote_address();
            let conn = match incoming.accept() {
                Ok(connecting) => match connecting.await {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("handshake with {remote} failed: {e}");
                        return;
                    }
                },
                Err(e) => {
                    warn!("rejecting {remote}: {e}");
                    return;
                }
            };
            if let Err(e) = handle_connection(conn, identity, opts).await {
                warn!("session with {remote} ended: {e:#}");
            }
        });
    }
    Ok(())
}

struct PlannedFile {
    meta: FileMeta,
    dest: PathBuf,
    offset: u64,
}

async fn handle_connection(
    conn: quinn::Connection,
    identity: Arc<Identity>,
    opts: Arc<ServerOpts>,
) -> Result<()> {
    let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.context("accept control stream")?;

    let hello: Frame = proto::read_msg(&mut ctrl_recv).await?;
    let (sender_id, sender_name) = match hello {
        Frame::Hello {
            version,
            device_id,
            name,
            ..
        } => {
            if version != proto::PROTO_VERSION {
                let msg = format!("unsupported protocol version {version}");
                let _ = proto::write_frame(
                    &mut ctrl_send,
                    &Frame::Error {
                        message: msg.clone(),
                    },
                )
                .await;
                bail!(msg);
            }
            (device_id, name)
        }
        other => bail!("expected hello, got {other:?}"),
    };
    info!("incoming transfer from \"{sender_name}\" ({sender_id})");

    proto::write_frame(
        &mut ctrl_send,
        &Frame::Hello {
            version: proto::PROTO_VERSION,
            device_id: identity.device_id.clone(),
            name: identity.name.clone(),
            platform: Identity::platform().to_string(),
        },
    )
    .await?;

    let offer = proto::read_msg(&mut ctrl_recv).await?;
    let (session, files) = match offer {
        Frame::Offer { session, files } => (session, files),
        Frame::Bye => {
            info!("sender disconnected before offering files");
            return Ok(());
        }
        other => bail!("expected offer, got {other:?}"),
    };
    if files.is_empty() {
        let _ = proto::write_frame(
            &mut ctrl_send,
            &Frame::Reject {
                session,
                reason: "no files offered".into(),
            },
        )
        .await;
        bail!("empty offer");
    }

    // Decide destination paths *before* accepting so the offsets we report are the
    // offsets we will actually append to.
    let mut plan: Vec<PlannedFile> = Vec::with_capacity(files.len());
    let mut accepted: Vec<AcceptFile> = Vec::with_capacity(files.len());
    let mut total: u64 = 0;
    for meta in files {
        let name = sanitize_file_name(&meta.name);
        // Resume: continue from a leftover partial file instead of creating "name (1)".
        let (dest, offset) = match same_partial(&opts.out_dir, &name, meta.size).await? {
            Some(existing) => {
                let len = tokio::fs::metadata(&existing)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                (existing, len)
            }
            None => (unique_path(&opts.out_dir, &name), 0),
        };
        total += meta.size.saturating_sub(offset);
        accepted.push(AcceptFile {
            id: meta.id,
            offset,
        });
        plan.push(PlannedFile { meta, dest, offset });
    }

    proto::write_frame(
        &mut ctrl_send,
        &Frame::Accept {
            session,
            files: accepted,
        },
    )
    .await?;

    let plan = Arc::new(plan);
    let index: Arc<HashMap<u32, Arc<PlannedFile>>> = Arc::new(
        plan.iter()
            .map(|p| {
                (
                    p.meta.id,
                    Arc::new(PlannedFile {
                        meta: p.meta.clone(),
                        dest: p.dest.clone(),
                        offset: p.offset,
                    }),
                )
            })
            .collect(),
    );

    let progress = Arc::new(Progress::new(total));
    let reporter = progress.clone();
    let quiet = opts.quiet;
    let reporter_handle = tokio::spawn(async move { reporter.run(quiet).await });

    let expected = plan.len();
    let mut finished = 0usize;
    let mut transferred = 0u64;
    let mut failures = 0usize;
    let mut set: JoinSet<Result<(String, u64, bool)>> = JoinSet::new();

    loop {
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() && finished < expected => {
                match joined {
                    Some(Ok(Ok((name, bytes, ok)))) => {
                        finished += 1;
                        transferred += bytes;
                        if !ok { failures += 1; }
                        let status = if ok { "ok" } else { "FAILED" };
                        println!("  {status:<7} {name} ({})", human_bytes(bytes));
                    }
                    Some(Ok(Err(e))) => {
                        finished += 1;
                        failures += 1;
                        warn!("file failed: {e:#}");
                    }
                    Some(Err(e)) => {
                        finished += 1;
                        failures += 1;
                        warn!("file task panicked: {e}");
                    }
                    None => break,
                }
                if finished == expected {
                    break;
                }
            }
            accepted = conn.accept_bi() => {
                match accepted {
                    Ok((send, recv)) => {
                        let index = index.clone();
                        let progress = progress.clone();
                        set.spawn(async move { receive_file(send, recv, index, progress).await });
                    }
                    Err(e) => {
                        debug!("no more streams: {e}");
                        break;
                    }
                }
            }
        }
    }

    let _ = proto::write_frame(
        &mut ctrl_send,
        &Frame::Complete {
            session,
            transferred,
        },
    )
    .await;
    reporter_handle.abort();

    let secs = progress.elapsed().as_secs_f64().max(0.001);
    println!(
        "done: {} in {:.1}s ({}){}",
        human_bytes(transferred),
        secs,
        human_rate(transferred as f64 / secs),
        if failures > 0 {
            format!(", {failures} failed")
        } else {
            String::new()
        }
    );

    let _ = ctrl_send.finish();
    conn.close(0u32.into(), b"done");
    Ok(())
}

/// Find an existing partial file with the expected final size.
async fn same_partial(dir: &std::path::Path, name: &str, size: u64) -> Result<Option<PathBuf>> {
    let candidate = dir.join(name);
    match tokio::fs::metadata(&candidate).await {
        Ok(meta) if meta.is_file() && meta.len() < size => Ok(Some(candidate)),
        _ => Ok(None),
    }
}

async fn receive_file(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    index: Arc<HashMap<u32, Arc<PlannedFile>>>,
    progress: Arc<Progress>,
) -> Result<(String, u64, bool)> {
    let header: FileHeader = proto::read_msg(&mut recv)
        .await
        .context("read file header")?;
    let planned = index.get(&header.id).context("unknown file id")?.clone();
    let expected = planned.meta.size;
    let offset = planned.offset.min(expected);

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&planned.dest)
        .await
        .with_context(|| format!("open {}", planned.dest.display()))?;

    // Hash whatever prefix already exists so resume produces the same digest as a
    // one-shot transfer.
    let mut hasher = blake3::Hasher::new();
    if offset > 0 {
        file.set_len(offset).await?;
        let mut prefix = vec![0u8; 1024 * 1024];
        let mut read = 0u64;
        file.seek(std::io::SeekFrom::Start(0)).await?;
        while read < offset {
            let want = ((offset - read) as usize).min(prefix.len());
            let n = file.read(&mut prefix[..want]).await?;
            if n == 0 {
                break;
            }
            hasher.update(&prefix[..n]);
            read += n as u64;
        }
        if read != offset {
            // Disk changed under us; restart from scratch.
            file.set_len(0).await?;
            hasher = blake3::Hasher::new();
        }
    }
    file.set_len(offset).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;

    let mut written = 0u64;
    let remaining = expected.saturating_sub(offset);
    let mut buf = vec![0u8; proto::CHUNK_SIZE];
    while written < remaining {
        let want = ((remaining - written) as usize).min(buf.len());
        let n = match recv
            .read(&mut buf[..want])
            .await
            .context("read file chunk")?
        {
            Some(n) if n > 0 => n,
            _ => break,
        };
        file.write_all(&buf[..n])
            .await
            .context("write file chunk")?;
        hasher.update(&buf[..n]);
        written += n as u64;
        progress.add(n as u64);
    }
    file.flush().await?;
    let hash = hasher.finalize().to_hex().to_string();

    let complete = written == remaining;
    let trailer: Option<FileTrailer> = match proto::read_msg(&mut recv).await {
        Ok(t) => Some(t),
        Err(e) => {
            debug!("missing trailer: {e}");
            None
        }
    };
    let expected_hash = planned
        .meta
        .hash
        .clone()
        .or_else(|| trailer.as_ref().map(|t| t.hash.clone()));
    let hash_ok = complete && expected_hash.as_deref() == Some(hash.as_str());
    let ack = if !complete {
        FileAck {
            id: planned.meta.id,
            ok: false,
            hash,
            message: format!("incomplete: {written}/{remaining}"),
        }
    } else if !hash_ok {
        FileAck {
            id: planned.meta.id,
            ok: false,
            hash,
            message: "checksum mismatch".into(),
        }
    } else {
        FileAck {
            id: planned.meta.id,
            ok: true,
            hash,
            message: String::new(),
        }
    };
    let ok = ack.ok;
    if !ok {
        error!(
            "{} rejected: {} (expected {:?} got {})",
            planned.dest.display(),
            ack.message,
            expected_hash,
            ack.hash
        );
    }
    proto::write_msg(&mut send, &ack).await.ok();
    let _ = send.finish();
    Ok((
        planned
            .dest
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string(),
        written,
        ok,
    ))
}
