//! Receiver side: advertise, accept QUIC connections, write files to disk.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};

use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::approval::{ApprovalRequest, ApprovalSender, Decision, APPROVAL_TIMEOUT};
use crate::discovery::{self, Discovery};
use crate::hash;
use crate::identity::Identity;
use crate::progress::Progress;
use crate::proto::{self, AcceptFile, FileAck, FileHeader, FileMeta, FileTrailer, Frame};
use crate::trust::TrustStore;
use crate::util::{human_bytes, human_rate, sanitize_rel_path, unique_path, wire_path};

/// Chunks buffered between the network and the disk writer for one segment.
const WRITE_AHEAD: usize = 3;
/// Minimum interval between per-file progress events.
const FILE_PROGRESS_INTERVAL_MS: u64 = 100;
/// Upper bound for the resume sidecar read from disk.
const MAX_SIDECAR_BYTES: u64 = 1024 * 1024;

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
    /// Prebuilt identity; the daemon shares one so renames reach the API and the advertised
    /// session.
    pub identity: Option<Arc<Identity>>,
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
            .field("identity", &self.identity.as_ref().map(|i| i.short_id()))
            .finish()
    }
}

/// Structured lifecycle events emitted by the receiver.
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A sender was accepted and the transfer is about to start.
    Session {
        peer_id: String,
        /// Sender's offer id; a peer can have several in flight.
        session: [u8; 16],
        peer_name: String,
        files: Vec<FileMeta>,
        total_bytes: u64,
        /// Held so a host can abort this session; cheap to clone.
        conn: quinn::Connection,
    },
    Progress {
        peer_id: String,
        session: [u8; 16],
        done: u64,
        total: u64,
    },
    /// A file stream was planned; `size` is the full file size.
    FileStart {
        peer_id: String,
        session: [u8; 16],
        id: u32,
        name: String,
        size: u64,
    },
    /// Bytes written for one file, including a resumed prefix.
    FileProgress {
        peer_id: String,
        session: [u8; 16],
        id: u32,
        name: String,
        done: u64,
        size: u64,
    },
    File {
        peer_id: String,
        session: [u8; 16],
        id: u32,
        name: String,
        bytes: u64,
        ok: bool,
    },
    Done {
        peer_id: String,
        session: [u8; 16],
        bytes: u64,
        duration: std::time::Duration,
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

/// Emit per-file progress, at most once per [`FILE_PROGRESS_INTERVAL_MS`]. Deciding
/// before the event is built keeps the skipped steps free.
fn emit_file_progress(planned: &PlannedFile, peer_id: &str, done: u64, size: u64) {
    let Some(sink) = &planned.events else {
        return;
    };
    if !planned.flight.should_emit(done, size) {
        return;
    }
    sink(ServerEvent::FileProgress {
        peer_id: peer_id.to_string(),
        session: planned.session,
        id: planned.meta.id,
        name: planned.label.clone(),
        done,
        size,
    });
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
            identity: None,
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
    let identity = match opts.identity.clone() {
        Some(identity) => identity,
        None => Arc::new(Identity::load_or_create(opts.name.clone())?),
    };
    tokio::fs::create_dir_all(&opts.out_dir)
        .await
        .with_context(|| format!("create output dir {}", opts.out_dir.display()))?;

    let endpoint = crate::net::server_endpoint(&identity, opts.quic_port)?;
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
        let mut events = std::mem::replace(
            &mut server.discovery.events,
            tokio::sync::mpsc::unbounded_channel().1,
        );
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                match event {
                    discovery::DiscoveryEvent::Seen(peer) => debug!("peer seen: {peer}"),
                    discovery::DiscoveryEvent::Lost { device_id } => {
                        debug!("peer lost: {device_id}")
                    }
                }
            }
        });
    }

    info!(
        "receiving as \"{}\" ({}) on {}",
        server.identity.name(),
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
    session: [u8; 16],
    meta: FileMeta,
    dest: PathBuf,
    /// Safe relative name shown to the user and in events.
    label: String,
    offset: u64,
    flight: Arc<Flight>,
    /// Event sink for this segment, if the host asked for one.
    events: Option<ServerEventSink>,
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
    started: Instant,
    progress_at_ms: AtomicU64,
    progress_done: AtomicU64,
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
            started: Instant::now(),
            progress_at_ms: AtomicU64::new(0),
            progress_done: AtomicU64::new(0),
        }
    }

    /// Gate for [`ServerEvent::FileProgress`].
    fn should_emit(&self, done: u64, size: u64) -> bool {
        let complete = size == 0 || done >= size;
        if !complete && done <= self.progress_done.load(Ordering::Relaxed) {
            return false;
        }
        let now = self.started.elapsed().as_millis() as u64;
        if !complete
            && now.saturating_sub(self.progress_at_ms.load(Ordering::Relaxed))
                < FILE_PROGRESS_INTERVAL_MS
        {
            return false;
        }
        self.progress_at_ms.store(now, Ordering::Relaxed);
        self.progress_done.store(done, Ordering::Relaxed);
        true
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
            name: identity.name(),
            platform: Identity::platform().to_string(),
        },
    )
    .await?;

    let offer: Frame = proto::read_msg_limit(&mut ctrl_recv, proto::MAX_OFFER_FRAME).await?;
    let (session, files) = match offer {
        Frame::Offer { session, files } => (session, files),
        Frame::Bye => {
            info!("sender disconnected before offering files");
            return Ok(());
        }
        other => bail!("expected offer, got {other:?}"),
    };
    if let Err(e) = proto::validate_offer(&files) {
        let _ = proto::write_frame(
            &mut ctrl_send,
            &Frame::Reject {
                session,
                reason: format!("invalid offer: {e}"),
            },
        )
        .await;
        bail!("invalid offer: {e}");
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
        // Resume: continue from a leftover QuicDrop partial instead of creating "name (1)".
        let (dest, len) = match same_partial(&parent, &file_name, meta.size).await? {
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
        let offset = trusted_prefix(&dest, len).await;
        let count = proto::chunk_count(offset, meta.size);
        // Only split files need a marker; adding one to every small file would double
        // the filesystem traffic of a many-file transfer.
        if count > 1 && (len == 0 || offset == 0) {
            // The marker proves to a later run that the leftover file belongs to us.
            let _ = tokio::fs::write(sidecar_path(&dest), b"").await;
        }
        total += meta.size.saturating_sub(offset);
        accepted.push(AcceptFile {
            id: meta.id,
            offset,
        });
        plan.push(PlannedFile {
            session,
            meta,
            dest,
            label,
            offset,
            flight: Arc::new(Flight::new(count)),
            events: opts.events.clone(),
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

    let progress = Arc::new(match &opts.events {
        Some(sink) => {
            let sink = sink.clone();
            let peer_id = sender_id.clone();
            Progress::with_sink(
                total,
                Arc::new(move |done, total| {
                    sink(ServerEvent::Progress {
                        peer_id: peer_id.clone(),
                        session,
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
            session,
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
                session,
                id: planned.meta.id,
                name: planned.label.clone(),
                size: planned.meta.size,
            },
        );
    }
    let expected = plan.len();
    let index: Arc<HashMap<u32, Arc<PlannedFile>>> = Arc::new(
        plan.into_iter()
            .map(|planned| (planned.meta.id, Arc::new(planned)))
            .collect(),
    );
    let reporter = progress.clone();
    let quiet = opts.quiet;
    let reporter_handle = tokio::spawn(async move { reporter.run(quiet).await });
    let mut finished = 0usize;
    let mut transferred = 0u64;
    let mut failures = 0usize;
    let mut set: JoinSet<Vec<SegmentOutcome>> = JoinSet::new();

    loop {
        tokio::select! {
            joined = set.join_next(), if !set.is_empty() && finished < expected => {
                match joined {
                    Some(Ok(outcomes)) => {
                        for (id, name, bytes, ok, complete) in outcomes {
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
                                    session,
                                    id,
                                    name,
                                    bytes,
                                    ok,
                                },
                            );
                        }
                    }
                    Some(Err(e)) => {
                        finished += 1;
                        failures += 1;
                        warn!("file task panicked: {e}");
                    }
                    None => break,
                }
                if finished >= expected {
                    break;
                }
            }
            accepted = conn.accept_bi() => {
                match accepted {
                    Ok((send, recv)) => {
                        let index = index.clone();
                        let progress = progress.clone();
                        let task_opts = opts.clone();
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

    let elapsed = progress.elapsed();
    let secs = elapsed.as_secs_f64().max(0.001);
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
            session,
            bytes: transferred,
            duration: elapsed.max(std::time::Duration::from_millis(1)),
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
        opts.trust.touch_async(peer_id).await;
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
                if let Err(e) = opts
                    .trust
                    .remember_async(peer_id, peer_name, peer_platform)
                    .await
                {
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

/// Find a leftover QuicDrop partial that can still take more bytes.
async fn same_partial(dir: &std::path::Path, name: &str, size: u64) -> Result<Option<PathBuf>> {
    let candidate = dir.join(name);
    let marker = sidecar_path(&candidate);
    match tokio::fs::metadata(&candidate).await {
        Ok(meta)
            if meta.is_file()
                && meta.len() <= size
                && tokio::fs::metadata(&marker)
                    .await
                    .map(|marker| marker.is_file())
                    .unwrap_or(false) =>
        {
            Ok(Some(candidate))
        }
        _ => Ok(None),
    }
}

async fn receive_segment(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    index: Arc<HashMap<u32, Arc<PlannedFile>>>,
    progress: Arc<Progress>,
    opts: Arc<ServerOpts>,
    peer_id: String,
) -> Vec<SegmentOutcome> {
    // One stream carries one or more chunk units, so a batch of small files needs a
    // single stream. Each unit is answered on the reply direction as it completes.
    let mut outcomes = Vec::new();
    loop {
        let header: FileHeader = match proto::read_msg_opt(&mut recv).await {
            Ok(Some(header)) => header,
            Ok(None) => break,
            Err(e) => {
                debug!("stream ended mid-header: {e:#}");
                break;
            }
        };
        let Some(planned) = index.get(&header.id).cloned() else {
            outcomes.push((header.id, String::from("?"), 0, false, true));
            break;
        };
        match write_segment(
            &mut send,
            &mut recv,
            &planned,
            &header,
            progress.clone(),
            &opts,
            &peer_id,
        )
        .await
        {
            Ok(outcome) => outcomes.push(outcome),
            Err(e) => {
                fail_flight(&planned, format!("{e:#}"));
                outcomes.push((planned.meta.id, planned.label.clone(), 0, false, true));
                break;
            }
        }
    }
    let _ = send.finish();
    outcomes
}

async fn write_segment(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    planned: &PlannedFile,
    header: &FileHeader,
    progress: Arc<Progress>,
    opts: &ServerOpts,
    peer_id: &str,
) -> Result<SegmentOutcome> {
    let expected = planned.meta.size;
    let offset = planned.offset.min(expected);
    let count = proto::chunk_count(offset, expected);
    if header.session != planned.session {
        bail!("file header session does not match the accepted session");
    }
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

    // One blocking writer per segment owns the file handle and its offset;
    // `read_chunk` hands over quinn's buffers without copying.
    let (tx, rx) = mpsc::channel::<Bytes>(WRITE_AHEAD);
    let writer = {
        let dest = planned.dest.clone();
        tokio::task::spawn_blocking(move || write_to_disk(&dest, start, rx))
    };

    let mut written = 0u64;
    while written < len {
        let want = ((len - written) as usize).min(proto::CHUNK_SIZE);
        let chunk = match recv
            .read_chunk(want, true)
            .await
            .context("read file chunk")?
        {
            Some(chunk) if !chunk.bytes.is_empty() => chunk,
            _ => break,
        };
        let n = chunk.bytes.len() as u64;
        if tx.send(chunk.bytes).await.is_err() {
            break;
        }
        written += n;
        progress.add(n);
        let done = planned.flight.written.fetch_add(n, Ordering::Relaxed) + n;
        emit_file_progress(planned, peer_id, offset + done, expected);
    }
    drop(tx);

    let on_disk = match writer.await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(e)) => bail!("write {}: {e}", planned.dest.display()),
        Err(e) => bail!("disk writer: {e}"),
    };
    if written != len || on_disk != len {
        bail!(
            "{} chunk incomplete: {on_disk}/{len} written, {written}/{len} received",
            planned.label
        );
    }
    if count > 1 {
        remember_range(&planned.dest, start, len).await;
    }

    if let Ok(trailer) = proto::read_msg::<FileTrailer>(recv).await {
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
        send_verdict(send, planned.meta.id, &verdict).await;
        if let Some(sink) = &opts.events {
            sink(ServerEvent::File {
                peer_id: peer_id.to_string(),
                session: planned.session,
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
    send_segment_ack(send, planned.meta.id).await;
    Ok((planned.meta.id, planned.label.clone(), 0, true, false))
}

fn sidecar_path(dest: &std::path::Path) -> PathBuf {
    let mut name = dest.as_os_str().to_os_string();
    name.push(".qdp");
    PathBuf::from(name)
}

/// Sequential write of one segment's pieces, on the blocking pool with its own handle.
fn write_to_disk(
    dest: &std::path::Path,
    start: u64,
    mut rx: mpsc::Receiver<Bytes>,
) -> std::io::Result<u64> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(dest)?;
    file.seek(SeekFrom::Start(start))?;
    let mut written = 0u64;
    while let Some(chunk) = rx.blocking_recv() {
        file.write_all(&chunk)?;
        written += chunk.len() as u64;
    }
    Ok(written)
}

/// Contiguous prefix proven by the ranges recorded next to a split partial file.
async fn trusted_prefix(dest: &std::path::Path, len: u64) -> u64 {
    if len == 0 {
        return 0;
    }
    let sidecar = sidecar_path(dest);
    let Ok(meta) = tokio::fs::metadata(&sidecar).await else {
        return 0;
    };
    if !meta.is_file() || meta.len() > MAX_SIDECAR_BYTES {
        return 0;
    }
    // A single chunk is written sequentially from zero, so its length is the prefix.
    if len <= proto::CHUNK_STREAM_BYTES {
        return len;
    }
    let Ok(text) = tokio::fs::read_to_string(&sidecar).await else {
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

async fn send_segment_ack(send: &mut quinn::SendStream, id: u32) {
    let _ = proto::write_msg(send, &FileAck::Segment { id }).await;
}

async fn send_verdict(send: &mut quinn::SendStream, id: u32, verdict: &Verdict) {
    let ack = FileAck::Verdict {
        id,
        ok: verdict.ok,
        hash: verdict.hash.clone(),
        message: verdict.message.clone(),
    };
    let _ = proto::write_msg(send, &ack).await;
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
    match hash::hash_file_with_size(&planned.dest).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marked_single_chunk_partials_are_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("small.bin");
        std::fs::write(&dest, vec![0u8; 1024]).unwrap();
        std::fs::write(sidecar_path(&dest), b"").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(trusted_prefix(&dest, 1024)), 1024);
    }

    #[test]
    fn unmarked_files_are_not_resumed() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("user.bin");
        std::fs::write(&dest, vec![0u8; 1024]).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(runtime.block_on(trusted_prefix(&dest, 1024)), 0);
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
