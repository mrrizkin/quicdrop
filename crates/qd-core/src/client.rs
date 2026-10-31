//! Sender side.

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use std::io::{Read, Seek, SeekFrom};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{debug, info};

use crate::cancel::Cancel;
use crate::hash;
use crate::identity::Identity;
use crate::progress::{throttle_file_sink, FileSink, FileVerdictSink, Progress, ProgressSink};
use crate::proto::{self, AcceptFile, FileAck, FileHeader, FileMeta, FileTrailer, Frame};
use crate::source::SourceFile;
use crate::util::{human_bytes, human_rate};

/// Pieces of one chunk buffered ahead of the wire.
const READ_AHEAD: usize = 2;

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
    /// Optional per-file verdict observer, called once per file when the receiver
    /// reports a verdict.
    pub file_verdict: Option<FileVerdictSink>,
    /// Abort the transfer when signalled.
    pub cancel: Option<Cancel>,
    /// Identity to announce. Read from the config dir when absent.
    pub identity: Option<Arc<Identity>>,
    /// Expanded inputs. Walked here when absent.
    pub sources: Option<Vec<SourceFile>>,
}

impl Default for SendOpts {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            device_id: None,
            files: Vec::new(),
            concurrency: 8,
            quiet: true,
            name: None,
            progress: None,
            file_progress: None,
            file_verdict: None,
            cancel: None,
            identity: None,
            sources: None,
        }
    }
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
            .field("file_verdict", &self.file_verdict.is_some())
            .field("cancel", &self.cancel.is_some())
            .field("identity", &self.identity.is_some())
            .field("sources", &self.sources.as_ref().map(Vec::len))
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

/// Result of the background hash, including the failure case.
type HashState = std::result::Result<String, String>;

pub async fn send(opts: SendOpts) -> Result<Summary> {
    if opts.files.is_empty() && opts.sources.is_none() {
        bail!("nothing to send");
    }

    let sources = match opts.sources.clone() {
        Some(sources) => sources,
        None => {
            let inputs = opts.files.clone();
            // Walking a folder can touch thousands of entries: keep it off the runtime.
            tokio::task::spawn_blocking(move || crate::source::expand(&inputs))
                .await
                .context("scan input paths")??
        }
    };
    if sources.is_empty() {
        bail!("nothing to send");
    }

    let mut metas = Vec::with_capacity(sources.len());
    let mut paths: Vec<PathBuf> = Vec::with_capacity(sources.len());
    let mut total = 0u64;
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
    proto::validate_offer(&metas)?;

    let client_cfg = crate::net::client_config(opts.device_id.clone())?;
    let endpoint = crate::net::client_endpoint(client_cfg)?;

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
    let identity = match &opts.identity {
        Some(identity) => identity.as_ref().clone(),
        None => {
            let name = opts.name.clone();
            // Reading the key and parsing it is blocking file IO.
            tokio::task::spawn_blocking(move || Identity::load_or_create(name))
                .await
                .context("load identity")??
        }
    };
    let (mut ctrl_send, mut ctrl_recv) = conn.open_bi().await.context("open control stream")?;
    proto::write_frame(
        &mut ctrl_send,
        &Frame::Hello {
            version: proto::PROTO_VERSION,
            device_id: identity.device_id.clone(),
            name: identity.name(),
            platform: Identity::platform().to_string(),
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

    proto::write_msg_limit(
        &mut ctrl_send,
        &Frame::Offer {
            session,
            files: metas.clone(),
        },
        proto::MAX_OFFER_FRAME,
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
        .filter_map(|accepted| {
            metas
                .get(accepted.id as usize)
                .map(|meta| meta.size.saturating_sub(accepted.offset))
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
    let file_verdict = opts.file_verdict.clone();

    // A one-chunk file costs three round trips whatever its size, so files that
    // fit share a stream. Anything larger keeps its own chunk workers.
    const BATCH_BYTES: u64 = 8 * 1024 * 1024;
    const BATCH_FILES: usize = 64;
    enum Group {
        File(PathBuf, FileMeta, u64),
        Batch(Vec<(PathBuf, FileMeta, u64)>),
    }

    let mut groups: Vec<Group> = Vec::new();
    let mut batch: Vec<(PathBuf, FileMeta, u64)> = Vec::new();
    let mut batch_bytes = 0u64;
    for item in accepted {
        let Some(path) = paths.get(item.id as usize).cloned() else {
            continue;
        };
        let meta = metas[item.id as usize].clone();
        if proto::chunk_count(item.offset, meta.size) > 1 {
            groups.push(Group::File(path, meta, item.offset));
            continue;
        }
        let span = meta.size.saturating_sub(item.offset);
        if !batch.is_empty() && (batch_bytes + span > BATCH_BYTES || batch.len() >= BATCH_FILES) {
            groups.push(Group::Batch(std::mem::take(&mut batch)));
            batch_bytes = 0;
        }
        batch_bytes += span;
        batch.push((path, meta, item.offset));
    }
    if !batch.is_empty() {
        groups.push(Group::Batch(batch));
    }

    let mut tasks = Vec::with_capacity(groups.len());
    for group in groups {
        let conn = conn.clone();
        let progress = progress.clone();
        let file_progress = file_progress.clone();
        let file_verdict = file_verdict.clone();
        let permit = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit.acquire_owned().await;
            match group {
                Group::File(path, meta, offset) => {
                    let result = send_one(
                        &conn,
                        session,
                        &path,
                        &meta,
                        offset,
                        progress.clone(),
                        file_progress.clone(),
                        file_verdict.clone(),
                    )
                    .await;
                    vec![(meta, offset, result)]
                }
                Group::Batch(items) => {
                    send_batch(
                        &conn,
                        session,
                        &items,
                        progress,
                        file_progress,
                        file_verdict,
                    )
                    .await
                }
            }
        }));
    }

    let mut summary = Summary::default();
    for task in tasks {
        let results = match task.await {
            Ok(results) => results,
            Err(e) => {
                summary.files += 1;
                summary.failed += 1;
                println!("  failed  task join error: {e}");
                continue;
            }
        };
        for (meta, offset, result) in results {
            match result {
                Ok(bytes) => {
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
                Err(e) => {
                    summary.files += 1;
                    summary.failed += 1;
                    println!("  failed  {}: {e:#}", meta.name);
                }
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

#[allow(clippy::too_many_arguments)]
async fn send_one(
    conn: &quinn::Connection,
    session: [u8; 16],
    path: &Path,
    meta: &FileMeta,
    offset: u64,
    progress: Arc<Progress>,
    file_progress: Option<FileSink>,
    file_verdict: Option<FileVerdictSink>,
) -> Result<u64> {
    let started = Instant::now();
    // Every chunk carries the same whole-file digest, so the hash is only needed
    // once the first trailer is written. Start it now and overlap it with the payload.
    let hash_rx = start_hash(path);
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
            hash_rx.clone(),
            progress.clone(),
            file_progress.clone(),
            file_verdict.clone(),
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
    hash_rx: watch::Receiver<Option<HashState>>,
    progress: Arc<Progress>,
    file_progress: Option<FileSink>,
    file_verdict: Option<FileVerdictSink>,
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
            &hash_rx,
            &progress,
            &file_progress,
            &file_done,
            &file_verdict,
        )
        .await?;
    }
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
    hash_rx: &watch::Receiver<Option<HashState>>,
    progress: &Progress,
    file_progress: &Option<FileSink>,
    file_done: &AtomicU64,
    file_verdict: &Option<FileVerdictSink>,
) -> Result<u64> {
    let (mut send, mut recv) = conn.open_bi().await.context("open file stream")?;
    let (written, hash) = write_unit(
        &mut send,
        path,
        meta,
        start,
        len,
        index,
        count,
        session,
        hash_rx,
        progress,
        file_progress,
        file_done,
    )
    .await?;
    send.finish().context("finish file stream")?;
    if let Ack::Verdict { ok: false, message } =
        check_ack(&mut recv, meta.id, &hash, file_verdict).await?
    {
        bail!("receiver rejected file: {message}");
    }
    Ok(written)
}

/// Start hashing a file in the background. Every chunk of that file shares the result.
fn start_hash(path: &Path) -> watch::Receiver<Option<HashState>> {
    let (hash_tx, hash_rx) = watch::channel::<Option<HashState>>(None);
    let path = path.to_path_buf();
    tokio::spawn(async move {
        let result = hash::hash_file(&path).await.map_err(|e| format!("{e:#}"));
        let _ = hash_tx.send(Some(result));
    });
    hash_rx
}

/// Write one chunk unit on an existing stream: header, payload pieces, trailer.
#[allow(clippy::too_many_arguments)]
async fn write_unit(
    send: &mut quinn::SendStream,
    path: &Path,
    meta: &FileMeta,
    start: u64,
    len: u64,
    index: u16,
    count: u16,
    session: [u8; 16],
    hash_rx: &watch::Receiver<Option<HashState>>,
    progress: &Progress,
    file_progress: &Option<FileSink>,
    file_done: &AtomicU64,
) -> Result<(u64, String)> {
    proto::write_msg(
        send,
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

    // Disk reads block, so one task reads the chunk and hands pieces to the async writer.
    let (tx, mut rx) = mpsc::channel::<std::io::Result<Bytes>>(READ_AHEAD);
    let reader = {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut file = std::fs::File::open(&path)?;
            file.seek(SeekFrom::Start(start))?;
            let mut left = len;
            while left > 0 {
                let want = left.min(proto::CHUNK_SIZE as u64) as usize;
                let mut buf = BytesMut::zeroed(want);
                file.read_exact(&mut buf)?;
                left -= want as u64;
                // A closed receiver means the chunk is already lost; stop reading.
                if tx.blocking_send(Ok(buf.freeze())).is_err() {
                    break;
                }
            }
            Ok(())
        })
    };

    let mut written = 0u64;
    while let Some(piece) = rx.recv().await {
        let piece = piece.with_context(|| format!("read {}", path.display()))?;
        let n = piece.len() as u64;
        // `Bytes` ownership lets quinn take the buffer without another copy.
        send.write_chunk(piece).await.context("write file chunk")?;
        written += n;
        progress.add(n);
        if let Some(sink) = file_progress {
            let done = file_done.fetch_add(n, Ordering::Relaxed) + n;
            sink(meta.id, done, meta.size);
        }
    }
    match reader.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => bail!("{}: {e}", path.display()),
        Err(e) => bail!("source reader: {e}"),
    }
    if written != len {
        bail!(
            "{} ended early, {} bytes missing",
            path.display(),
            len - written
        );
    }

    let hash = await_hash(hash_rx).await.map_err(anyhow::Error::msg)?;
    proto::write_msg(
        send,
        &FileTrailer {
            id: meta.id,
            hash: hash.clone(),
        },
    )
    .await?;
    Ok((written, hash))
}

/// What the receiver reported for one stream unit.
enum Ack {
    /// The bytes reached disk; the file verdict is still pending.
    Segment,
    /// The final answer for the whole file.
    Verdict { ok: bool, message: String },
}

/// Read the receiver's answer for the chunk just written.
async fn check_ack(
    recv: &mut quinn::RecvStream,
    id: u32,
    hash: &str,
    verdicts: &Option<FileVerdictSink>,
) -> Result<Ack> {
    let ack: FileAck = proto::read_msg(recv).await.context("read file ack")?;
    match ack {
        FileAck::Segment { id: ack_id } => {
            if ack_id != id {
                bail!("ack for file {ack_id} on the stream for {id}");
            }
            Ok(Ack::Segment)
        }
        FileAck::Verdict {
            id: ack_id,
            ok,
            hash: ack_hash,
            message,
        } => {
            if ack_id != id {
                bail!("verdict for file {ack_id} on the stream for {id}");
            }
            if ok && !ack_hash.is_empty() && ack_hash != hash {
                bail!("peer computed a different digest");
            }
            if let Some(sink) = verdicts {
                sink(id, ok);
            }
            Ok(Ack::Verdict { ok, message })
        }
    }
}

/// Send several single-chunk files over one stream, one verdict each.
async fn send_batch(
    conn: &quinn::Connection,
    session: [u8; 16],
    items: &[(PathBuf, FileMeta, u64)],
    progress: Arc<Progress>,
    file_progress: Option<FileSink>,
    file_verdict: Option<FileVerdictSink>,
) -> Vec<(FileMeta, u64, Result<u64>)> {
    let mut results: Vec<(FileMeta, u64, Result<u64>)> = Vec::with_capacity(items.len());
    let (mut send, mut recv) = match conn.open_bi().await {
        Ok(pair) => pair,
        Err(e) => {
            let message = format!("open file stream: {e}");
            for (_, meta, offset) in items {
                results.push((meta.clone(), *offset, Err(anyhow!(message.clone()))));
            }
            return results;
        }
    };

    let mut hashes: Vec<Option<String>> = Vec::with_capacity(items.len());
    for (path, meta, offset) in items {
        let (start, len) = proto::chunk_range(*offset, meta.size, 0);
        let done = AtomicU64::new(0);
        let hash_rx = start_hash(path);
        let written = write_unit(
            &mut send,
            path,
            meta,
            start,
            len,
            0,
            1,
            session,
            &hash_rx,
            &progress,
            &file_progress,
            &done,
        )
        .await;
        match written {
            Ok((bytes, hash)) => {
                results.push((meta.clone(), *offset, Ok(bytes)));
                hashes.push(Some(hash));
            }
            Err(e) => {
                results.push((meta.clone(), *offset, Err(e)));
                break;
            }
        }
    }
    // Bodies are written before any answer is read; a failure leaves the rest unsent.
    let _ = send.finish();
    for (index, hash) in hashes.iter().enumerate() {
        let Some(hash) = hash else { break };
        let id = items[index].1.id;
        let result = check_ack(&mut recv, id, hash, &file_verdict).await;
        match result {
            Ok(Ack::Verdict { ok: true, .. }) => {}
            Ok(Ack::Verdict { ok: false, message }) => {
                // Every body was already written, so the remaining verdicts still
                // arrive on this stream: a bad file must not fail the others.
                if let Some((_, _, result)) = results.get_mut(index) {
                    *result = Err(anyhow!("receiver rejected file: {message}"));
                }
            }
            Ok(Ack::Segment) => {
                if let Some((_, _, result)) = results.get_mut(index) {
                    *result = Err(anyhow!("missing file verdict"));
                }
                break;
            }
            Err(e) => {
                if let Some((_, _, result)) = results.get_mut(index) {
                    if result.is_ok() {
                        *result = Err(e);
                    }
                }
                break;
            }
        }
    }
    for (_, meta, offset) in items.iter().skip(results.len()) {
        results.push((meta.clone(), *offset, Err(anyhow!("stream ended early"))));
    }
    results
}

/// Wait for the background hash. Every chunk of a file shares one digest.
async fn await_hash(rx: &watch::Receiver<Option<HashState>>) -> HashState {
    let mut rx = rx.clone();
    loop {
        if let Some(state) = rx.borrow_and_update().clone() {
            return state;
        }
        if rx.changed().await.is_err() {
            return Err("hashing stopped before it finished".to_string());
        }
    }
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
