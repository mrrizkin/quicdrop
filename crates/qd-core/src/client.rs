//! Sender side.

use anyhow::{bail, Context, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tracing::{debug, info};

use crate::progress::{Progress, ProgressSink};
use crate::proto::{self, AcceptFile, FileAck, FileHeader, FileMeta, FileTrailer, Frame};
use crate::util::{human_bytes, human_rate};

#[derive(Clone)]
pub struct SendOpts {
    pub addr: SocketAddr,
    /// Pinned device id of the receiver (prefix allowed). `None` accepts any peer.
    pub device_id: Option<String>,
    pub files: Vec<PathBuf>,
    pub concurrency: usize,
    pub quiet: bool,
    /// Name announced to the receiver (defaults to the persisted device name).
    pub name: Option<String>,
    /// Optional live progress observer for embedders.
    pub progress: Option<ProgressSink>,
}

impl std::fmt::Debug for SendOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendOpts")
            .field("addr", &self.addr)
            .field("device_id", &self.device_id)
            .field("files", &self.files)
            .field("concurrency", &self.concurrency)
            .field("name", &self.name)
            .field("progress", &self.progress.is_some())
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct Summary {
    pub files: usize,
    pub ok: usize,
    pub failed: usize,
    pub bytes: u64,
    pub duration: Duration,
}

pub async fn send(opts: SendOpts) -> Result<Summary> {
    if opts.files.is_empty() {
        bail!("nothing to send");
    }

    let mut metas = Vec::new();
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut total = 0u64;
    // Walking a folder can touch thousands of entries: keep it off the runtime.
    let inputs = opts.files.clone();
    let sources = tokio::task::spawn_blocking(move || crate::source::expand(&inputs))
        .await
        .context("scan input paths")??;
    for (i, src) in sources.iter().enumerate() {
        total += src.size;
        metas.push(FileMeta {
            id: i as u32,
            name: src.name.clone(),
            size: src.size,
            hash: None,
        });
        paths.push(src.path.clone());
    }
    if metas.is_empty() {
        bail!("nothing to send");
    }

    let client_cfg = crate::net::client_config(opts.device_id.clone())?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
        .context("bind QUIC client socket")?;
    endpoint.set_default_client_config(client_cfg);

    let connecting = endpoint
        .connect(opts.addr, "quicdrop.local")
        .with_context(|| format!("connect to {}", opts.addr))?;
    let conn = match connecting.await {
        Ok(conn) => conn,
        Err(quinn::ConnectionError::TimedOut) => bail!(
            "{} did not answer. The receiver may be stopped, or a firewall is blocking UDP {}. \
             Check that both sides use the same port and that inbound UDP is allowed.",
            opts.addr,
            opts.addr.port()
        ),
        Err(e) => bail!("handshake with {} failed: {e}", opts.addr),
    };
    debug!("connected to {} in one RTT", conn.remote_address());

    let session = new_session_id();
    let identity = crate::identity::Identity::load_or_create(opts.name.clone())?;
    let (mut ctrl_send, mut ctrl_recv) = conn.open_bi().await.context("open control stream")?;
    proto::write_frame(
        &mut ctrl_send,
        &Frame::Hello {
            version: proto::PROTO_VERSION,
            device_id: identity.device_id.clone(),
            name: identity.name.clone(),
            platform: crate::identity::Identity::platform().to_string(),
        },
    )
    .await?;

    let peer_hello: Frame = proto::read_msg(&mut ctrl_recv).await?;
    let peer_name = match peer_hello {
        Frame::Hello { name, version, .. } => {
            if version != proto::PROTO_VERSION {
                bail!(
                    "peer speaks protocol v{version}, we speak v{}",
                    proto::PROTO_VERSION
                );
            }
            name
        }
        Frame::Error { message } => bail!("peer error: {message}"),
        other => bail!("unexpected reply: {other:?}"),
    };
    info!(
        "sending {} file(s), {} to \"{peer_name}\"",
        metas.len(),
        human_bytes(total)
    );

    proto::write_frame(
        &mut ctrl_send,
        &Frame::Offer {
            session,
            files: metas.clone(),
        },
    )
    .await?;
    let accepted: Vec<AcceptFile> = match proto::read_msg(&mut ctrl_recv).await? {
        Frame::Accept { files, .. } => files,
        Frame::Reject { reason, .. } => bail!("transfer rejected: {reason}"),
        Frame::Error { message } => bail!("peer error: {message}"),
        other => bail!("unexpected reply: {other:?}"),
    };

    let accepted_total: u64 = accepted
        .iter()
        .filter_map(|a| {
            metas
                .iter()
                .find(|m| m.id == a.id)
                .map(|m| m.size.saturating_sub(a.offset))
        })
        .sum();

    let progress = Arc::new(match &opts.progress {
        Some(sink) => Progress::with_sink(accepted_total, sink.clone()),
        None => Progress::new(accepted_total),
    });
    let reporter = progress.clone();
    let quiet = opts.quiet;
    let reporter_handle = tokio::spawn(async move { reporter.run(quiet).await });

    let semaphore = Arc::new(Semaphore::new(opts.concurrency.max(1)));
    let mut tasks = Vec::new();
    for item in accepted {
        let path = match paths.get(item.id as usize) {
            Some(p) => p.clone(),
            None => continue,
        };
        let meta = metas[item.id as usize].clone();
        let conn = conn.clone();
        let progress = progress.clone();
        let permit = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit.acquire_owned().await;
            let result =
                send_one(&conn, session, &path, &meta, item.offset, progress.clone()).await;
            (meta, item.offset, result)
        }));
    }

    let mut summary = Summary::default();
    for task in tasks {
        match task.await {
            Ok((meta, offset, Ok(bytes))) => {
                summary.files += 1;
                summary.ok += 1;
                summary.bytes += bytes;
                let name = meta.name.clone();
                if offset > 0 {
                    println!(
                        "  ok      {name} ({} resumed)",
                        human_bytes(meta.size - offset)
                    );
                } else {
                    println!("  ok      {name} ({})", human_bytes(meta.size));
                }
            }
            Ok((meta, _, Err(e))) => {
                summary.files += 1;
                summary.failed += 1;
                println!("  failed  {}: {e:#}", meta.name);
            }
            Err(e) => {
                summary.files += 1;
                summary.failed += 1;
                println!("  failed  task join error: {e}");
            }
        }
    }

    reporter_handle.abort();
    let _ = proto::write_frame(&mut ctrl_send, &Frame::Bye).await;
    let _ = ctrl_send.finish();

    summary.duration = progress.elapsed().max(Duration::from_millis(1));
    let secs = summary.duration.as_secs_f64();
    println!(
        "sent {} / {} files in {:.1}s ({}){}",
        summary.ok,
        summary.files,
        secs,
        human_rate(summary.bytes as f64 / secs),
        if summary.failed > 0 {
            format!(", {} failed", summary.failed)
        } else {
            String::new()
        }
    );

    endpoint.wait_idle().await;
    conn.close(0u32.into(), b"done");
    Ok(summary)
}

async fn send_one(
    conn: &quinn::Connection,
    session: [u8; 16],
    path: &Path,
    meta: &FileMeta,
    offset: u64,
    progress: Arc<Progress>,
) -> Result<u64> {
    let started = Instant::now();
    let (mut send, mut recv) = conn.open_bi().await.context("open file stream")?;
    proto::write_msg(
        &mut send,
        &FileHeader {
            session,
            id: meta.id,
            offset,
            size: meta.size,
        },
    )
    .await?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    // One sequential pass: hash everything (needed for resume correctness) but only put
    // bytes past `offset` on the wire.
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; proto::CHUNK_SIZE];
    let mut pos = 0u64;
    let mut sent = 0u64;
    loop {
        let n = file.read(&mut buf).await.context("read source file")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        let chunk_end = pos + n as u64;
        if chunk_end > offset {
            let skip = offset.saturating_sub(pos) as usize;
            let slice = &buf[skip..n];
            send.write_all(slice).await.context("write file chunk")?;
            sent += slice.len() as u64;
            progress.add(slice.len() as u64);
        }
        pos = chunk_end;
    }
    let hash = hasher.finalize().to_hex().to_string();
    proto::write_msg(
        &mut send,
        &FileTrailer {
            id: meta.id,
            hash: hash.clone(),
        },
    )
    .await?;
    send.finish().context("finish file stream")?;

    let ack: FileAck = proto::read_msg(&mut recv).await.context("read file ack")?;
    if !ack.ok {
        bail!("receiver rejected file: {}", ack.message);
    }
    if ack.hash != hash {
        bail!("peer computed a different digest");
    }
    debug!("{} sent in {:?}", meta.name, started.elapsed());
    Ok(sent)
}

fn new_session_id() -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .to_le_bytes(),
    );
    hasher.update(&crate::util::hostname().into_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    out
}
