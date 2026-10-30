//! Sender side.

use anyhow::{bail, Context, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tracing::{debug, info};

use crate::cancel::Cancel;
use crate::progress::{throttle_file_sink, FileSink, Progress, ProgressSink};
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
    /// Optional per-file progress observer, keyed by `FileMeta::id`.
    pub file_progress: Option<FileSink>,
    /// Abort the transfer when signalled.
    pub cancel: Option<Cancel>,
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
            .field("file_progress", &self.file_progress.is_some())
            .field("cancel", &self.cancel.is_some())
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
    let watcher = opts.cancel.as_ref().map(|flag| {
        let conn = conn.clone();
        let flag = flag.clone();
        tokio::spawn(async move {
            while !flag.is_cancelled() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            conn.close(0u32.into(), b"cancelled");
        })
    });
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
    let file_progress = opts.file_progress.clone().map(throttle_file_sink);
    let mut tasks = Vec::new();
    for item in accepted {
        let path = match paths.get(item.id as usize) {
            Some(p) => p.clone(),
            None => continue,
        };
        let meta = metas[item.id as usize].clone();
        let conn = conn.clone();
        let progress = progress.clone();
        let file_progress = file_progress.clone();
        let permit = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit.acquire_owned().await;
            let result = send_one(
                &conn,
                session,
                &path,
                &meta,
                item.offset,
                progress.clone(),
                file_progress.clone(),
            )
            .await;
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
    if let Some(watcher) = watcher {
        watcher.abort();
    }
    if opts.cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
        endpoint.wait_idle().await;
        bail!("cancelled");
    }
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
    file_progress: Option<FileSink>,
) -> Result<u64> {
    let started = Instant::now();
    // Hash once so every chunk carries the same whole-file digest, then let a pool of
    // workers pull 8 MiB chunks until the remaining range is on the wire.
    let hash = hash_file(path).await?;
    let count = proto::chunk_count(offset, meta.size);
    let workers = proto::stream_count(count) as usize;
    let next = Arc::new(AtomicUsize::new(0));
    // Chunks finish out of order; count the whole file so progress stays monotonic.
    let file_done = Arc::new(AtomicU64::new(offset));

    let mut tasks = Vec::with_capacity(workers);
    for _ in 0..workers {
        tasks.push(tokio::spawn(send_worker(
            conn.clone(),
            session,
            path.to_path_buf(),
            meta.clone(),
            offset,
            count,
            next.clone(),
            file_done.clone(),
            hash.clone(),
            progress.clone(),
            file_progress.clone(),
        )));
    }

    let mut sent = 0u64;
    let mut failure: Option<String> = None;
    for task in tasks {
        match task.await {
            Ok(Ok(bytes)) => sent += bytes,
            Ok(Err(e)) => failure = failure.or_else(|| Some(format!("{e:#}"))),
            Err(e) => failure = failure.or_else(|| Some(format!("chunk worker: {e}"))),
        }
    }
    if let Some(message) = failure {
        bail!("{message}");
    }
    debug!("{} sent in {:?}", meta.name, started.elapsed());
    Ok(sent)
}

#[allow(clippy::too_many_arguments)]
async fn send_worker(
    conn: quinn::Connection,
    session: [u8; 16],
    path: PathBuf,
    meta: FileMeta,
    offset: u64,
    count: u16,
    next: Arc<AtomicUsize>,
    file_done: Arc<AtomicU64>,
    hash: String,
    progress: Arc<Progress>,
    file_progress: Option<FileSink>,
) -> Result<u64> {
    let mut sent = 0u64;
    loop {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= count as usize {
            return Ok(sent);
        }
        let index = index as u16;
        let (start, len) = proto::chunk_range(offset, meta.size, index);
        sent += send_chunk(
            &conn,
            session,
            &path,
            &meta,
            start,
            len,
            index,
            count,
            &hash,
            &progress,
            &file_progress,
            &file_done,
        )
        .await?;
    }
}

async fn hash_file(path: &Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; proto::CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await.context("read source file")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[allow(clippy::too_many_arguments)]
async fn send_chunk(
    conn: &quinn::Connection,
    session: [u8; 16],
    path: &Path,
    meta: &FileMeta,
    start: u64,
    len: u64,
    index: u16,
    count: u16,
    hash: &str,
    progress: &Progress,
    file_progress: &Option<FileSink>,
    file_done: &AtomicU64,
) -> Result<u64> {
    let (mut send, mut recv) = conn.open_bi().await.context("open file stream")?;
    proto::write_msg(
        &mut send,
        &FileHeader {
            session,
            id: meta.id,
            offset: start,
            size: meta.size,
            index,
            count,
        },
    )
    .await?;

    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut remaining = len;
    let mut buf = vec![0u8; proto::CHUNK_SIZE];
    let mut written = 0u64;
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = file
            .read(&mut buf[..want])
            .await
            .context("read source file")?;
        if n == 0 {
            bail!("{} ended early, {remaining} bytes missing", path.display());
        }
        send.write_all(&buf[..n])
            .await
            .context("write file chunk")?;
        remaining -= n as u64;
        written += n as u64;
        progress.add(n as u64);
        if let Some(sink) = file_progress {
            let done = file_done.fetch_add(n as u64, Ordering::Relaxed) + n as u64;
            sink(meta.id, done, meta.size);
        }
    }
    proto::write_msg(
        &mut send,
        &FileTrailer {
            id: meta.id,
            hash: hash.to_string(),
        },
    )
    .await?;
    send.finish().context("finish file stream")?;

    let ack: FileAck = proto::read_msg(&mut recv).await.context("read file ack")?;
    if !ack.ok {
        bail!("receiver rejected file: {}", ack.message);
    }
    // Empty hash means "chunk written"; the last chunk carries the file verdict.
    if !ack.hash.is_empty() && ack.hash != hash {
        bail!("peer computed a different digest");
    }
    Ok(written)
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
