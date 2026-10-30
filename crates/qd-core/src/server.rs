//! Receiver side: advertise, accept QUIC connections, write files to disk.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::approval::{ApprovalRequest, ApprovalSender, Decision, APPROVAL_TIMEOUT};
use crate::discovery::{self, Discovery};
use crate::identity::Identity;
use crate::progress::{FileThrottle, Progress};
use crate::proto::{self, AcceptFile, FileAck, FileHeader, FileMeta, FileTrailer, Frame};
use crate::trust::TrustStore;
use crate::util::{human_bytes, human_rate, sanitize_rel_path, unique_path, wire_path};

/// What to do with a transfer from a device that is not in the trust store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    /// Ask the user (or the embedding UI) for every unknown device.
    #[default]
    Ask,
    /// Accept everything without asking (useful for tests and trusted networks).
    Auto,
    /// Reject everything, trusted or not.
    Deny,
}

#[derive(Clone)]
pub struct ServerOpts {
    pub quic_port: u16,
    pub discovery_port: u16,
    pub out_dir: PathBuf,
    pub name: Option<String>,
    /// Suppress the receiver's progress meter.
    pub quiet: bool,
    /// Policy for devices that are not in the trust store.
    pub policy: Policy,
    /// Approved devices. Defaults to the store in the config dir.
    pub trust: TrustStore,
    /// Where approval requests go. `None` means unknown devices are rejected.
    pub approver: Option<ApprovalSender>,
    /// Structured session events for embedders (daemon, GUI).
    pub events: Option<ServerEventSink>,
    /// Optional live overrides for settings a long-running host changes at runtime.
    pub runtime: Option<Arc<RuntimeSettings>>,
}

/// Settings that can be changed after the server started (used by the daemon).
#[derive(Debug)]
pub struct RuntimeSettings {
    out_dir: std::sync::RwLock<PathBuf>,
    policy: std::sync::RwLock<Policy>,
    concurrency: std::sync::RwLock<usize>,
}

impl RuntimeSettings {
    pub fn new(out_dir: PathBuf, policy: Policy) -> Self {
        Self {
            out_dir: std::sync::RwLock::new(out_dir),
            policy: std::sync::RwLock::new(policy),
            concurrency: std::sync::RwLock::new(8),
        }
    }

    pub fn out_dir(&self) -> PathBuf {
        self.out_dir.read().map(|p| p.clone()).unwrap_or_default()
    }

    pub fn set_out_dir(&self, dir: PathBuf) {
        if let Ok(mut slot) = self.out_dir.write() {
            *slot = dir;
        }
    }

    pub fn policy(&self) -> Policy {
        self.policy.read().map(|p| *p).unwrap_or_default()
    }

    /// Parallel file streams used by outbound transfers.
    pub fn concurrency(&self) -> usize {
        self.concurrency
            .read()
            .map(|c| *c)
            .unwrap_or(8)
            .clamp(1, 16)
    }

    pub fn set_concurrency(&self, value: usize) {
        if let Ok(mut slot) = self.concurrency.write() {
            *slot = value.clamp(1, 16);
        }
    }

    pub fn set_policy(&self, policy: Policy) {
        if let Ok(mut slot) = self.policy.write() {
            *slot = policy;
        }
    }
}

impl std::fmt::Debug for ServerOpts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerOpts")
            .field("quic_port", &self.quic_port)
            .field("discovery_port", &self.discovery_port)
            .field("out_dir", &self.out_dir)
            .field("name", &self.name)
            .field("policy", &self.policy)
            .field("approver", &self.approver.is_some())
            .field("events", &self.events.is_some())
            .field("runtime", &self.runtime.is_some())
            .finish()
    }
}

/// Structured lifecycle events emitted by the receiver.
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A sender was accepted and the transfer is about to start.
    Session {
        peer_id: String,
        peer_name: String,
        files: Vec<FileMeta>,
        total_bytes: u64,
        /// Held so a host can abort this session; cheap to clone.
        conn: quinn::Connection,
    },
    Progress {
        peer_id: String,
        done: u64,
        total: u64,
    },
    /// A file stream was planned; `size` is the full file size.
    FileStart {
        peer_id: String,
        id: u32,
        name: String,
        size: u64,
    },
    /// Bytes written for one file, including a resumed prefix.
    FileProgress {
        peer_id: String,
        id: u32,
        name: String,
        done: u64,
        size: u64,
    },
    File {
        peer_id: String,
        id: u32,
        name: String,
        bytes: u64,
        ok: bool,
    },
    Done {
        peer_id: String,
        bytes: u64,
        failures: usize,
    },
    Declined {
        peer_id: String,
        peer_name: String,
        reason: String,
    },
}

pub type ServerEventSink = Arc<dyn Fn(ServerEvent) + Send + Sync>;

fn emit(opts: &ServerOpts, event: ServerEvent) {
    if let Some(sink) = &opts.events {
        sink(event);
    }
}

/// Throttle file progress; lifecycle events pass through unchanged.
fn throttle_events(sink: ServerEventSink) -> ServerEventSink {
    let throttle = Arc::new(FileThrottle::default());
    Arc::new(move |event| {
        if let ServerEvent::FileProgress { id, done, size, .. } = &event {
            if !throttle.should_emit(*id, *done, *size) {
                return;
            }
        }
        sink(event);
    })
}

impl ServerOpts {
    pub fn new(quic_port: u16, discovery_port: u16, out_dir: PathBuf) -> Self {
        Self {
            quic_port,
            discovery_port,
            out_dir,
            name: None,
            quiet: false,
            policy: Policy::default(),
            trust: TrustStore::default(),
            approver: None,
            events: None,
            runtime: None,
        }
    }

    /// Output directory, including a runtime override if one is set.
    pub fn effective_out_dir(&self) -> PathBuf {
        match &self.runtime {
            Some(runtime) => runtime.out_dir(),
            None => self.out_dir.clone(),
        }
    }

    /// Policy for unknown devices, including a runtime override if one is set.
    pub fn effective_policy(&self) -> Policy {
        match &self.runtime {
            Some(runtime) => runtime.policy(),
            None => self.policy,
        }
    }
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

    accept_loop(server.endpoint.clone(), server.identity.clone(), opts).await
}

/// Accept connections on `endpoint` until it is closed.
pub async fn accept_loop(
    endpoint: quinn::Endpoint,
    identity: Arc<Identity>,
    opts: ServerOpts,
) -> Result<()> {
    let opts = Arc::new(opts);
    while let Some(incoming) = endpoint.accept().await {
        let opts = opts.clone();
        let identity = identity.clone();
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
    /// Safe relative name shown to the user and in events.
    label: String,
    offset: u64,
    flight: Arc<Flight>,
}

/// (file id, label, bytes, ok, completes the file)
type SegmentOutcome = (u32, String, u64, bool, bool);

/// Segments of one file share this: how many are still writing, how many bytes have
/// arrived, and the single verdict every chunk must report.
struct Flight {
    remaining: AtomicU16,
    written: AtomicU64,
    expected_hash: Mutex<Option<String>>,
    verdict: watch::Sender<Option<Verdict>>,
}

#[derive(Clone)]
struct Verdict {
    ok: bool,
    hash: String,
    message: String,
}

impl Flight {
    fn new(count: u16) -> Self {
        let (verdict, _) = watch::channel(None);
        Self {
            remaining: AtomicU16::new(count.max(1)),
            written: AtomicU64::new(0),
            expected_hash: Mutex::new(None),
            verdict,
        }
    }
}

/// Account for a chunk that will never complete, so the others stop waiting.
fn fail_flight(planned: &PlannedFile, message: String) {
    if planned.flight.remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
        let _ = planned.flight.verdict.send(Some(Verdict {
            ok: false,
            hash: String::new(),
            message,
        }));
    }
}

async fn handle_connection(
    conn: quinn::Connection,
    identity: Arc<Identity>,
    opts: Arc<ServerOpts>,
) -> Result<()> {
    let opts = {
        let mut opts = (*opts).clone();
        opts.events = opts.events.clone().map(throttle_events);
        Arc::new(opts)
    };
    let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.context("accept control stream")?;

    let hello: Frame = proto::read_msg(&mut ctrl_recv).await?;
    let (sender_id, sender_name, sender_platform) = match hello {
        Frame::Hello {
            version,
            device_id,
            name,
            platform,
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
            (device_id, name, platform)
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

    // Ask before touching the disk: a declined transfer must not create files.
    let decision = decide(
        opts.effective_policy(),
        &opts,
        &sender_id,
        &sender_name,
        &sender_platform,
        &files,
    )
    .await;
    if let Decision::Reject { reason } = decision {
        info!("declined transfer from \"{sender_name}\": {reason}");
        emit(
            &opts,
            ServerEvent::Declined {
                peer_id: sender_id.clone(),
                peer_name: sender_name.clone(),
                reason: reason.clone(),
            },
        );
        let _ = proto::write_frame(
            &mut ctrl_send,
            &Frame::Reject {
                session,
                reason: reason.clone(),
            },
        )
        .await;
        let _ = ctrl_send.finish();
        conn.close(0u32.into(), b"rejected");
        return Ok(());
    }

    let out_dir = opts.effective_out_dir();

    // Decide destination paths before accepting: the offsets we report are the ones we
    // will append to.
    let mut plan: Vec<PlannedFile> = Vec::with_capacity(files.len());
    let mut accepted: Vec<AcceptFile> = Vec::with_capacity(files.len());
    let mut total: u64 = 0;
    for meta in files {
        // Senders may announce `folder/sub/file.txt`; folders are recreated below out_dir.
        let rel = sanitize_rel_path(&meta.name);
        let label = wire_path(&rel);
        let file_name = rel
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unnamed".to_string());
        let parent = match rel.parent() {
            Some(p) if !p.as_os_str().is_empty() => out_dir.join(p),
            _ => out_dir.clone(),
        };
        tokio::fs::create_dir_all(&parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
        // Resume: continue from a leftover partial file instead of creating "name (1)".
        let (dest, offset) = match same_partial(&parent, &file_name, meta.size).await? {
            Some(existing) => {
                let len = tokio::fs::metadata(&existing)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);
                (existing, len)
            }
            None => (unique_path(&parent, &file_name), 0),
        };
        // A split transfer may leave holes when it is interrupted; only resume from
        // the contiguous prefix the receiver can prove it wrote.
        let offset = trusted_prefix(&dest, offset).await;
        total += meta.size.saturating_sub(offset);
        accepted.push(AcceptFile {
            id: meta.id,
            offset,
        });
        let count = proto::chunk_count(offset, meta.size);
        plan.push(PlannedFile {
            meta,
            dest,
            label,
            offset,
            flight: Arc::new(Flight::new(count)),
        });
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
                        label: p.label.clone(),
                        offset: p.offset,
                        flight: p.flight.clone(),
                    }),
                )
            })
            .collect(),
    );

    let progress = Arc::new(match &opts.events {
        Some(sink) => {
            let sink = sink.clone();
            let peer_id = sender_id.clone();
            Progress::with_sink(
                total,
                Arc::new(move |done, total| {
                    sink(ServerEvent::Progress {
                        peer_id: peer_id.clone(),
                        done,
                        total,
                    })
                }),
            )
        }
        None => Progress::new(total),
    });
    emit(
        &opts,
        ServerEvent::Session {
            peer_id: sender_id.clone(),
            peer_name: sender_name.clone(),
            files: plan.iter().map(|p| p.meta.clone()).collect(),
            total_bytes: total,
            conn: conn.clone(),
        },
    );
    for planned in plan.iter() {
        emit(
            &opts,
            ServerEvent::FileStart {
                peer_id: sender_id.clone(),
                id: planned.meta.id,
                name: planned.label.clone(),
                size: planned.meta.size,
            },
        );
    }
    let reporter = progress.clone();
    let quiet = opts.quiet;
    let reporter_handle = tokio::spawn(async move { reporter.run(quiet).await });

    let expected = plan.len();
    let mut finished = 0usize;
    let mut transferred = 0u64;
    let mut failures = 0usize;
    let mut set: JoinSet<Result<SegmentOutcome>> = JoinSet::new();

    loop {
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() && finished < expected => {
                match joined {
                    Some(Ok(Ok((id, name, bytes, ok, complete)))) => {
                        if !complete {
                            continue;
                        }
                        finished += 1;
                        transferred += bytes;
                        if !ok { failures += 1; }
                        let status = if ok { "ok" } else { "FAILED" };
                        println!("  {status:<7} {name} ({})", human_bytes(bytes));
                        emit(
                            &opts,
                            ServerEvent::File {
                                peer_id: sender_id.clone(),
                                id,
                                name,
                                bytes,
                                ok,
                            },
                        );
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
                        let task_opts = (*opts).clone();
                        let peer_id = sender_id.clone();
                        set.spawn(async move {
                            receive_segment(send, recv, index, progress, task_opts, peer_id).await
                        });
                    }
                    Err(e) => {
                        debug!("no more streams: {e}");
                        break;
                    }
                }
            }
        }
    }

    // Streams that never finished are failures, not a clean end.
    if finished < expected {
        failures += expected - finished;
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

    // Wait for `Bye` so file acks still in flight are not dropped by the close.
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctrl_recv.read_to_end(64 * 1024),
    )
    .await;

    let _ = ctrl_send.finish();
    conn.close(0u32.into(), b"done");
    emit(
        &opts,
        ServerEvent::Done {
            peer_id: sender_id,
            bytes: transferred,
            failures,
        },
    );
    Ok(())
}

/// Trusted devices are accepted silently, unknown ones follow `opts.policy`, and anything
/// we cannot answer fails closed with a reason the sender can show.
async fn decide(
    policy: Policy,
    opts: &ServerOpts,
    peer_id: &str,
    peer_name: &str,
    peer_platform: &str,
    files: &[FileMeta],
) -> Decision {
    match policy {
        Policy::Deny => {
            return Decision::reject("receiver is not accepting transfers (policy: deny)")
        }
        Policy::Auto => return Decision::accept(),
        Policy::Ask => {}
    }

    if opts.trust.is_trusted(peer_id) {
        opts.trust.touch(peer_id);
        info!("accepting transfer from trusted device \"{peer_name}\"");
        return Decision::accept();
    }

    let Some(approver) = &opts.approver else {
        return Decision::reject(
            "receiver needs approval for new devices but has no UI; ask the owner to trust this device",
        );
    };

    let (reply, rx) = tokio::sync::oneshot::channel();
    let request = ApprovalRequest {
        peer_id: peer_id.to_string(),
        peer_name: peer_name.to_string(),
        peer_platform: peer_platform.to_string(),
        files: files.to_vec(),
        total_bytes: files.iter().map(|f| f.size).sum(),
        reply,
    };
    if approver.send(request).is_err() {
        return Decision::reject("receiver approval channel is closed");
    }

    match tokio::time::timeout(APPROVAL_TIMEOUT, rx).await {
        Ok(Ok(decision)) if decision.is_accept() => {
            if decision.should_remember() {
                if let Err(e) = opts.trust.remember(peer_id, peer_name, peer_platform) {
                    warn!("could not save trust decision for \"{peer_name}\": {e:#}");
                } else {
                    info!("trusted \"{peer_name}\" ({peer_id})");
                }
            }
            decision
        }
        Ok(Ok(decision)) => decision,
        Ok(Err(_)) => Decision::reject("receiver did not answer"),
        Err(_) => Decision::reject(format!("no answer within {}s", APPROVAL_TIMEOUT.as_secs())),
    }
}

/// Find an existing partial file with the expected final size.
async fn same_partial(dir: &std::path::Path, name: &str, size: u64) -> Result<Option<PathBuf>> {
    let candidate = dir.join(name);
    match tokio::fs::metadata(&candidate).await {
        Ok(meta) if meta.is_file() && meta.len() < size => Ok(Some(candidate)),
        _ => Ok(None),
    }
}

async fn receive_segment(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    index: Arc<HashMap<u32, Arc<PlannedFile>>>,
    progress: Arc<Progress>,
    opts: ServerOpts,
    peer_id: String,
) -> Result<SegmentOutcome> {
    let header: FileHeader = proto::read_msg(&mut recv)
        .await
        .context("read file header")?;
    let planned = index.get(&header.id).context("unknown file id")?.clone();

    let outcome = write_segment(
        &mut send, recv, &planned, &header, progress, &opts, &peer_id,
    )
    .await;
    match outcome {
        Ok(value) => Ok(value),
        Err(e) => {
            fail_flight(&planned, format!("{e:#}"));
            Err(e)
        }
    }
}

async fn write_segment(
    send: &mut quinn::SendStream,
    mut recv: quinn::RecvStream,
    planned: &PlannedFile,
    header: &FileHeader,
    progress: Arc<Progress>,
    opts: &ServerOpts,
    peer_id: &str,
) -> Result<SegmentOutcome> {
    let expected = planned.meta.size;
    let offset = planned.offset.min(expected);
    let count = proto::chunk_count(offset, expected);
    if header.size != expected || header.count != count || header.index >= count {
        bail!(
            "chunk {}/{} does not match the accepted plan ({count} expected)",
            header.index,
            header.count
        );
    }
    let (start, len) = proto::chunk_range(offset, expected, header.index);
    if header.offset != start {
        bail!(
            "chunk {} starts at {} instead of {start}",
            header.index,
            header.offset
        );
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&planned.dest)
        .await
        .with_context(|| format!("open {}", planned.dest.display()))?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut written = 0u64;
    let mut buf = vec![0u8; proto::CHUNK_SIZE];
    while written < len {
        let want = ((len - written) as usize).min(buf.len());
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
        written += n as u64;
        progress.add(n as u64);
        let done = planned
            .flight
            .written
            .fetch_add(n as u64, Ordering::Relaxed)
            + n as u64;
        if let Some(sink) = &opts.events {
            sink(ServerEvent::FileProgress {
                peer_id: peer_id.to_string(),
                id: planned.meta.id,
                name: planned.label.clone(),
                done: offset + done,
                size: expected,
            });
        }
    }
    file.flush().await?;
    if written != len {
        bail!("{} chunk incomplete: {written}/{len}", planned.label);
    }
    if count > 1 {
        remember_range(&planned.dest, start, len).await;
    }

    if let Ok(trailer) = proto::read_msg::<FileTrailer>(&mut recv).await {
        if let Ok(mut slot) = planned.flight.expected_hash.lock() {
            slot.get_or_insert(trailer.hash);
        }
    }

    let left = planned.flight.remaining.fetch_sub(1, Ordering::SeqCst);
    if left == 1 {
        let verdict = verify_file(planned, expected).await;
        if !verdict.ok {
            error!("{}: {}", planned.dest.display(), verdict.message);
        }
        let _ = planned.flight.verdict.send(Some(verdict.clone()));
        if verdict.ok {
            let _ = tokio::fs::remove_file(sidecar_path(&planned.dest)).await;
        }
        send_ack(send, planned.meta.id, &verdict).await;
        if let Some(sink) = &opts.events {
            sink(ServerEvent::File {
                peer_id: peer_id.to_string(),
                id: planned.meta.id,
                name: planned.label.clone(),
                bytes: expected.saturating_sub(offset),
                ok: verdict.ok,
            });
        }
        return Ok((
            planned.meta.id,
            planned.label.clone(),
            expected.saturating_sub(offset),
            verdict.ok,
            true,
        ));
    }

    // A written chunk is acknowledged right away; the whole-file verdict arrives on
    // the ack of whichever chunk finishes last. Waiting here would stall the sender,
    // because it only asks for the next chunk after this ack.
    send_ack(
        send,
        planned.meta.id,
        &Verdict {
            ok: true,
            hash: String::new(),
            message: String::new(),
        },
    )
    .await;
    Ok((planned.meta.id, planned.label.clone(), 0, true, false))
}

fn sidecar_path(dest: &std::path::Path) -> PathBuf {
    let mut name = dest.as_os_str().to_os_string();
    name.push(".qdp");
    PathBuf::from(name)
}

/// Contiguous prefix proven by the ranges recorded next to a split partial file.
async fn trusted_prefix(dest: &std::path::Path, len: u64) -> u64 {
    if len <= proto::CHUNK_STREAM_BYTES {
        return len;
    }
    let Ok(text) = tokio::fs::read_to_string(sidecar_path(dest)).await else {
        return 0;
    };
    let mut ranges: Vec<(u64, u64)> = text
        .lines()
        .filter_map(|line| {
            let (start, len) = line.split_once(' ')?;
            Some((start.parse().ok()?, len.parse().ok()?))
        })
        .collect();
    ranges.sort_unstable();
    let mut cursor = 0u64;
    for (start, len) in ranges {
        if start > cursor {
            break;
        }
        cursor = cursor.max(start.saturating_add(len));
    }
    cursor.min(len)
}

async fn remember_range(dest: &std::path::Path, start: u64, len: u64) {
    use tokio::io::AsyncWriteExt as _;
    let path = sidecar_path(dest);
    if let Ok(mut file) = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
    {
        let _ = file.write_all(format!("{start} {len}\n").as_bytes()).await;
        let _ = file.flush().await;
    }
}

async fn send_ack(send: &mut quinn::SendStream, id: u32, verdict: &Verdict) {
    let ack = FileAck {
        id,
        ok: verdict.ok,
        hash: verdict.hash.clone(),
        message: verdict.message.clone(),
    };
    let _ = proto::write_msg(send, &ack).await;
    let _ = send.finish();
}

async fn verify_file(planned: &PlannedFile, expected: u64) -> Verdict {
    let wanted = planned.meta.hash.clone().or_else(|| {
        planned
            .flight
            .expected_hash
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    });
    match hash_path(&planned.dest).await {
        Ok((size, hash)) => {
            if size != expected {
                Verdict {
                    ok: false,
                    hash,
                    message: format!("size {size} != {expected}"),
                }
            } else if wanted.as_deref() != Some(hash.as_str()) {
                Verdict {
                    ok: false,
                    hash,
                    message: "checksum mismatch".into(),
                }
            } else {
                Verdict {
                    ok: true,
                    hash,
                    message: String::new(),
                }
            }
        }
        Err(e) => Verdict {
            ok: false,
            hash: String::new(),
            message: format!("cannot verify: {e:#}"),
        },
    }
}

async fn hash_path(path: &std::path::Path) -> Result<(u64, String)> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, hasher.finalize().to_hex().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_partials_are_trusted_as_is() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("small.bin");
        std::fs::write(&dest, vec![0u8; 1024]).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(trusted_prefix(&dest, 1024)), 1024);
    }

    #[test]
    fn split_partials_resume_from_the_proven_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("big.bin");
        let len = proto::CHUNK_STREAM_BYTES * 2;
        std::fs::write(&dest, b"").unwrap();
        let big = proto::CHUNK_STREAM_BYTES;
        std::fs::write(
            sidecar_path(&dest),
            format!("0 {big}\n{big} {big}\n{} 5\n", len),
        )
        .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(trusted_prefix(&dest, len)), big * 2);
    }

    #[test]
    fn holes_are_not_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("big.bin");
        let big = proto::CHUNK_STREAM_BYTES;
        std::fs::write(&dest, b"").unwrap();
        std::fs::write(sidecar_path(&dest), format!("{big} {big}\n")).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(trusted_prefix(&dest, big * 2)), 0);
    }
}
