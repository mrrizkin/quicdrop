//! Receiver side: advertise, accept QUIC connections, write files to disk.

use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::approval::{ApprovalRequest, ApprovalSender, Decision, APPROVAL_TIMEOUT};
use crate::discovery::{self, Discovery};
use crate::identity::Identity;
use crate::progress::Progress;
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
    /// Answer discovery probes with a summary of what is on disk (unused placeholder).
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
}

impl RuntimeSettings {
    pub fn new(out_dir: PathBuf, policy: Policy) -> Self {
        Self {
            out_dir: std::sync::RwLock::new(out_dir),
            policy: std::sync::RwLock::new(policy),
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
    },
    Progress {
        peer_id: String,
        done: u64,
        total: u64,
    },
    File {
        peer_id: String,
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

    // Decide destination paths *before* accepting so the offsets we report are the
    // offsets we will actually append to.
    let mut plan: Vec<PlannedFile> = Vec::with_capacity(files.len());
    let mut accepted: Vec<AcceptFile> = Vec::with_capacity(files.len());
    let mut total: u64 = 0;
    for meta in files {
        // Senders may announce `folder/sub/file.txt`; sanitising strips traversal and
        // we recreate the folders below the receive directory.
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
        total += meta.size.saturating_sub(offset);
        accepted.push(AcceptFile {
            id: meta.id,
            offset,
        });
        plan.push(PlannedFile {
            meta,
            dest,
            label,
            offset,
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
        },
    );
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
                        emit(
                            &opts,
                            ServerEvent::File {
                                peer_id: sender_id.clone(),
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

    // Wait for the sender to close the control stream (it sends Bye after it has
    // read every file ack). Closing the connection earlier can drop those acks.
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

/// Decide whether to accept a transfer from `peer_id`.
///
/// Trusted devices are accepted silently, unknown ones follow `opts.policy`, and
/// anything we cannot answer fails closed with a reason the sender can show.
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
        Err(_) => Decision::reject(format!(
            "no answer within {}s",
            APPROVAL_TIMEOUT.as_secs()
        )),
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
    Ok((planned.label.clone(), written, ok))
}
