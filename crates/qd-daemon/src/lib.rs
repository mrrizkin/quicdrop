//! Daemon: the transfer engine behind a loopback JSON/SSE API, embedded by `quicdropd` and
//! the desktop app. State changes require the bearer token written to `daemon.json`.

use anyhow::{bail, Context, Result};
use qd_core::approval::{self, ApprovalRequest, Decision, APPROVAL_TIMEOUT};
use qd_core::client::{self, SendOpts};
use qd_core::proto::FileMeta;
use qd_core::server::{self, Policy, RunningServer, RuntimeSettings, ServerEvent, ServerEventSink};
use qd_core::{
    discovery, net, Identity, Peer, TrustStore, DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{debug, info, warn};

mod api;

pub use api::router;

pub const DEFAULT_API_PORT: u16 = 53319;
pub const API_VERSION: &str = env!("CARGO_PKG_VERSION");

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn new_token() -> String {
    let mut buf = [0u8; 32];
    // OsRng-backed; failure here should not silently produce a weak token.
    getrandom::fill(&mut buf).expect("system random source");
    hex::encode(buf)
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// HTTP listen address. Defaults to `127.0.0.1:53319`.
    pub listen: SocketAddr,
    pub quic_port: u16,
    pub discovery_port: u16,
    pub out_dir: PathBuf,
    pub name: Option<String>,
    pub policy: Policy,
    /// Fixed API token. `None` generates one on startup.
    pub token: Option<String>,
    pub quiet: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], DEFAULT_API_PORT)),
            quic_port: DEFAULT_QUIC_PORT,
            discovery_port: DEFAULT_DISCOVERY_PORT,
            out_dir: qd_core::util::default_download_dir(),
            name: None,
            policy: Policy::Ask,
            token: None,
            quiet: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    In,
    Out,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferStatus {
    Active,
    Done,
    Failed,
    Declined,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileView {
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub done: u64,
    pub ok: Option<bool>,
}

/// Counts reported by `/api/status`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    pub peers: usize,
    pub trusted: usize,
    pub approvals: usize,
    pub active: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerView {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub addr: String,
    pub source: String,
    pub trusted: bool,
    pub last_seen: u64,
}

impl PeerView {
    fn from_peer(peer: &Peer, trusted: bool) -> Self {
        Self {
            device_id: peer.device_id.clone(),
            name: peer.name.clone(),
            platform: peer.platform.clone(),
            addr: peer.addr.to_string(),
            source: peer.source.to_string(),
            trusted,
            last_seen: now_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferView {
    pub id: String,
    pub direction: Direction,
    pub peer_id: String,
    pub peer_name: String,
    pub files: Vec<FileView>,
    pub total_bytes: u64,
    pub transferred: u64,
    pub status: TransferStatus,
    pub error: Option<String>,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApprovalView {
    pub id: String,
    pub peer_id: String,
    pub peer_name: String,
    pub peer_platform: String,
    pub files: Vec<FileView>,
    pub total_bytes: u64,
    pub requested_at: u64,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DaemonEvent {
    /// Snapshot sent once when an event stream connects.
    Hello {
        name: String,
        device_id: String,
        peers: Vec<PeerView>,
        approvals: Vec<ApprovalView>,
        active: Vec<TransferView>,
    },
    Peer {
        peer: PeerView,
    },
    Session {
        transfer: TransferView,
    },
    Progress {
        id: String,
        transferred: u64,
        total: u64,
    },
    File {
        id: String,
        index: usize,
        file: FileView,
    },
    Approval {
        request: ApprovalView,
    },
    ApprovalResolved {
        id: String,
        accepted: bool,
    },
    Trust {
        peers: Vec<qd_core::trust::TrustedPeer>,
    },
    Config {
        out_dir: String,
        policy: Policy,
        name: String,
    },
}

/// Shared daemon state. Everything here is cheap to clone via the `Arc`.
pub struct Inner {
    pub identity: Arc<Identity>,
    pub token: String,
    pub listen: SocketAddr,
    pub quic_port: u16,
    pub discovery_port: u16,
    pub version: String,
    pub started_at: u64,
    pub local_addrs: Vec<std::net::IpAddr>,
    pub runtime: Arc<RuntimeSettings>,
    pub trust: TrustStore,
    peers: Mutex<HashMap<String, PeerView>>,
    transfers: Mutex<Vec<TransferView>>,
    /// `(peer, session)` to transfer id; a peer can run several transfers.
    inbound: Mutex<HashMap<(String, [u8; 16]), String>>,
    cancels: Mutex<HashMap<String, qd_core::Cancel>>,
    conns: Mutex<HashMap<String, qd_core::quinn::Connection>>,
    approvals: Mutex<HashMap<String, (ApprovalView, oneshot::Sender<Decision>)>>,
    events: broadcast::Sender<DaemonEvent>,
    next_id: AtomicU64,
    shutdown: watch::Sender<bool>,
}

impl Inner {
    fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}{}", self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn broadcast(&self, event: DaemonEvent) {
        let _ = self.events.send(event);
    }

    /// Rename and re-announce. The device id, and any trust pinned to it, does not change.
    pub fn rename(&self, name: &str) -> Result<()> {
        self.identity.set_name(name)?;
        self.broadcast(self.config_event());
        Ok(())
    }

    fn config_event(&self) -> DaemonEvent {
        DaemonEvent::Config {
            out_dir: self.runtime.out_dir().display().to_string(),
            policy: self.runtime.policy(),
            name: self.identity.name(),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DaemonEvent> {
        self.events.subscribe()
    }

    pub fn event_capacity(&self) -> usize {
        self.events.receiver_count()
    }

    pub fn peers(&self) -> Vec<PeerView> {
        let mut peers: Vec<_> = self
            .peers
            .lock()
            .map(|p| p.values().cloned().collect())
            .unwrap_or_default();
        peers.sort_by(|a, b| a.name.cmp(&b.name));
        peers
    }

    /// Counts for `/api/status`, taken under the lock so nothing is cloned.
    pub fn counts(&self) -> Counts {
        Counts {
            peers: self.peers.lock().map(|p| p.len()).unwrap_or(0),
            trusted: self.trust.len(),
            approvals: self.approvals.lock().map(|a| a.len()).unwrap_or(0),
            active: self
                .transfers
                .lock()
                .map(|t| {
                    t.iter()
                        .filter(|t| t.status == TransferStatus::Active)
                        .count()
                })
                .unwrap_or(0),
        }
    }

    pub fn trusted_peers(&self) -> Vec<qd_core::trust::TrustedPeer> {
        self.trust.list()
    }

    pub fn transfers(&self, limit: usize) -> Vec<TransferView> {
        let transfers = self.transfers.lock().map(|t| t.clone()).unwrap_or_default();
        transfers.iter().rev().take(limit).cloned().collect()
    }

    /// Drop finished history entries, keep transfers still in flight.
    pub fn clear_transfers(&self) {
        if let Ok(mut transfers) = self.transfers.lock() {
            transfers.retain(|t| t.status == TransferStatus::Active);
        }
    }

    pub fn pending_approvals(&self) -> Vec<ApprovalView> {
        self.approvals
            .lock()
            .map(|a| a.values().map(|(view, _)| view.clone()).collect())
            .unwrap_or_default()
    }

    fn remember_peer(&self, peer: &Peer) {
        let view = PeerView::from_peer(peer, self.trust.is_trusted(&peer.device_id));
        let changed = {
            let Ok(mut peers) = self.peers.lock() else {
                return;
            };
            match peers.get(&peer.device_id) {
                Some(existing)
                    if existing.addr == view.addr
                        && existing.name == view.name
                        && existing.trusted == view.trusted =>
                {
                    false
                }
                _ => {
                    peers.insert(peer.device_id.clone(), view.clone());
                    true
                }
            }
        };
        if changed {
            self.broadcast(DaemonEvent::Peer { peer: view });
        }
    }

    fn start_transfer(
        &self,
        direction: Direction,
        peer_id: &str,
        peer_name: &str,
        files: Vec<FileView>,
        total_bytes: u64,
    ) -> String {
        let view = TransferView {
            id: self.next_id("t"),
            direction,
            peer_id: peer_id.to_string(),
            peer_name: peer_name.to_string(),
            files,
            total_bytes,
            transferred: 0,
            status: TransferStatus::Active,
            error: None,
            started_at: now_secs(),
            finished_at: None,
        };
        if let Ok(mut transfers) = self.transfers.lock() {
            transfers.push(view.clone());
            // Keep the history bounded; old entries are not interesting.
            if transfers.len() > 200 {
                let excess = transfers.len() - 200;
                transfers.drain(0..excess);
            }
        }
        self.broadcast(DaemonEvent::Session {
            transfer: view.clone(),
        });
        view.id
    }

    fn progress(&self, id: &str, transferred: u64, total: u64) {
        let mut found = false;
        if let Ok(mut transfers) = self.transfers.lock() {
            if let Some(t) = transfers.iter_mut().find(|t| t.id == id) {
                // Resume can lower the accepted total; it is authoritative once sending starts.
                if total > 0 {
                    t.total_bytes = total;
                }
                t.transferred = transferred.max(t.transferred);
                found = true;
            }
        }
        if found {
            self.broadcast(DaemonEvent::Progress {
                id: id.to_string(),
                transferred,
                total,
            });
        }
    }

    fn finish_transfer(&self, id: &str, status: TransferStatus, error: Option<String>) {
        let view = {
            let Ok(mut transfers) = self.transfers.lock() else {
                return;
            };
            let Some(transfer) = transfers.iter_mut().find(|t| t.id == id) else {
                return;
            };
            // A cancelled transfer stays cancelled; late events must not revive it.
            if transfer.status != TransferStatus::Cancelled {
                transfer.status = status;
                if error.is_some() {
                    transfer.error = error;
                }
            }
            transfer.finished_at = Some(now_secs());
            transfer.clone()
        };
        self.broadcast(DaemonEvent::Session { transfer: view });
    }

    pub fn shutdown_signal(&self) -> bool {
        self.shutdown.send(true).is_ok()
    }

    fn transfer_id(&self, peer_id: &str, session: [u8; 16]) -> Option<String> {
        self.inbound
            .lock()
            .ok()
            .and_then(|map| map.get(&(peer_id.to_string(), session)).cloned())
    }

    /// Update one file inside a transfer and publish it.
    pub fn set_file(
        &self,
        transfer_id: &str,
        index: usize,
        done: Option<u64>,
        ok: Option<bool>,
    ) -> Option<FileView> {
        let file = {
            let mut transfers = self.transfers.lock().ok()?;
            let transfer = transfers.iter_mut().find(|t| t.id == transfer_id)?;
            let file = transfer.files.get_mut(index)?;
            if let Some(done) = done {
                file.done = done.min(file.size);
            }
            if ok.is_some() {
                file.ok = ok;
            }
            file.clone()
        };
        self.broadcast(DaemonEvent::File {
            id: transfer_id.to_string(),
            index,
            file: file.clone(),
        });
        Some(file)
    }

    /// Mark every file complete after a successful transfer.
    fn complete_files(&self, transfer_id: &str) {
        if let Ok(mut transfers) = self.transfers.lock() {
            if let Some(transfer) = transfers.iter_mut().find(|t| t.id == transfer_id) {
                for file in &mut transfer.files {
                    file.done = file.size;
                    if file.ok.is_none() {
                        file.ok = Some(true);
                    }
                }
            }
        }
    }

    fn resolve_approval(&self, id: &str, decision: Decision) -> bool {
        let entry = self
            .approvals
            .lock()
            .ok()
            .and_then(|mut map| map.remove(id));
        match entry {
            Some((_, reply)) => reply.send(decision).is_ok(),
            None => false,
        }
    }
}

/// Handle to a running daemon. Drop without `shutdown` leaves the tasks running
/// until the process exits.
pub struct Daemon {
    pub addr: SocketAddr,
    pub token: String,
    inner: Arc<Inner>,
    endpoint: qd_core::quinn::Endpoint,
    info_path: PathBuf,
}

impl Daemon {
    /// Start the QUIC server, discovery, and the local API.
    pub async fn start(cfg: DaemonConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&cfg.out_dir)
            .await
            .with_context(|| format!("create output dir {}", cfg.out_dir.display()))?;

        let identity = Arc::new(Identity::load_or_create(cfg.name.clone())?);
        let token = cfg.token.clone().unwrap_or_else(new_token);

        // Bind the HTTP socket first so the reported addr is the real one.
        let listener = tokio::net::TcpListener::bind(cfg.listen)
            .await
            .with_context(|| format!("bind API on {}", cfg.listen))?;
        let addr = listener.local_addr()?;
        if !addr.ip().is_loopback() {
            warn!("daemon API is listening on non-loopback address {addr}");
        }

        let runtime = Arc::new(RuntimeSettings::new(cfg.out_dir.clone(), cfg.policy));
        let local_addrs = discovery::local_ipv4_addrs_async().await;
        let (events, _) = broadcast::channel(512);
        let (shutdown, _) = watch::channel(false);

        let inner = Arc::new(Inner {
            identity: identity.clone(),
            token: token.clone(),
            listen: addr,
            quic_port: cfg.quic_port,
            discovery_port: cfg.discovery_port,
            version: API_VERSION.to_string(),
            started_at: now_secs(),
            local_addrs,
            runtime: runtime.clone(),
            trust: TrustStore::default(),
            peers: Mutex::new(HashMap::new()),
            transfers: Mutex::new(Vec::new()),
            inbound: Mutex::new(HashMap::new()),
            cancels: Mutex::new(HashMap::new()),
            conns: Mutex::new(HashMap::new()),
            approvals: Mutex::new(HashMap::new()),
            events,
            next_id: AtomicU64::new(1),
            shutdown,
        });

        // Approval requests are turned into events and parked until the UI answers.
        let (approver, approval_rx) = approval::channel();
        tokio::spawn(approval_forwarder(inner.clone(), approval_rx));

        let mut opts =
            server::ServerOpts::new(cfg.quic_port, cfg.discovery_port, cfg.out_dir.clone());
        opts.name = cfg.name.clone();
        opts.identity = Some(identity.clone());
        opts.quiet = cfg.quiet;
        opts.policy = cfg.policy;
        opts.trust = inner.trust.clone();
        opts.approver = Some(approver);
        opts.events = Some(server_event_sink(inner.clone()));
        opts.runtime = Some(runtime);

        let mut running: RunningServer = server::start(opts.clone()).await?;
        let endpoint = running.endpoint.clone();
        info!(
            "quicdropd {} as \"{}\" ({}) — api {addr}, quic udp/{}",
            API_VERSION,
            identity.name(),
            identity.short_id(),
            cfg.quic_port
        );

        // Peer discovery updates feed the peer list.
        let mut peers_rx = std::mem::replace(
            &mut running.discovery.peers,
            tokio::sync::mpsc::unbounded_channel().1,
        );
        {
            let inner = inner.clone();
            tokio::spawn(async move {
                while let Some(peer) = peers_rx.recv().await {
                    inner.remember_peer(&peer);
                }
            });
        }

        // QUIC accept loop; holds the server (and its discovery) alive.
        {
            let identity = running.identity.clone();
            let endpoint = running.endpoint.clone();
            tokio::spawn(async move {
                let _ = server::accept_loop(endpoint, identity, opts).await;
                drop(running);
            });
        }

        let app = api::router(inner.clone());
        {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut rx = inner.shutdown.subscribe();
                let shutdown = async move {
                    let _ = rx.wait_for(|stop| *stop).await;
                };
                if let Err(e) = axum::serve(listener, app)
                    .with_graceful_shutdown(shutdown)
                    .await
                {
                    warn!("api server stopped: {e}");
                }
            })
        };

        let info_path = Identity::config_dir().join("daemon.json");
        write_daemon_info(&info_path, addr, &token)?;

        Ok(Self {
            addr,
            token,
            inner,
            endpoint,
            info_path,
        })
    }

    pub fn state(&self) -> &Arc<Inner> {
        &self.inner
    }

    pub fn config(&self) -> DaemonConfigView {
        DaemonConfigView {
            out_dir: self.inner.runtime.out_dir(),
            policy: self.inner.runtime.policy(),
        }
    }

    /// Wait until [`Daemon::shutdown`] (or the API `POST /api/shutdown`).
    pub async fn wait(&self) {
        let mut rx = self.inner.shutdown.subscribe();
        let _ = rx.wait_for(|stop| *stop).await;
    }

    /// Stop accepting work and remove `daemon.json` (only if it is still ours).
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
        self.endpoint.close(0u32.into(), b"shutdown");
        let ours = std::fs::read(&self.info_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|value| {
                value
                    .get("token")
                    .and_then(|t| t.as_str())
                    .map(String::from)
            })
            .map(|token| token == self.token)
            .unwrap_or(false);
        if ours {
            let _ = std::fs::remove_file(&self.info_path);
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DaemonConfigView {
    pub out_dir: PathBuf,
    pub policy: Policy,
}

fn write_daemon_info(path: &Path, addr: SocketAddr, token: &str) -> Result<()> {
    #[derive(Serialize)]
    struct Info<'a> {
        pid: u32,
        addr: SocketAddr,
        token: &'a str,
        started_at: u64,
        version: &'a str,
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let info = Info {
        pid: std::process::id(),
        addr,
        token,
        started_at: now_secs(),
        version: API_VERSION,
    };
    let json = serde_json::to_vec_pretty(&info)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(&json)?;
    Ok(())
}

/// Map receiver events onto transfer records and daemon events.
fn server_event_sink(inner: Arc<Inner>) -> ServerEventSink {
    Arc::new(move |event| match event {
        ServerEvent::Session {
            peer_id,
            session,
            peer_name,
            files,
            total_bytes,
            conn,
        } => {
            let files = files
                .into_iter()
                .map(|f| FileView {
                    name: f.name,
                    size: f.size,
                    done: 0,
                    ok: None,
                })
                .collect();
            let id = inner.start_transfer(Direction::In, &peer_id, &peer_name, files, total_bytes);
            if let Ok(mut inbound) = inner.inbound.lock() {
                inbound.insert((peer_id.clone(), session), id.clone());
            }
            if let Ok(mut conns) = inner.conns.lock() {
                conns.insert(id, conn);
            }
        }
        ServerEvent::Progress {
            peer_id,
            session,
            done,
            total,
        } => {
            if let Some(id) = inner.transfer_id(&peer_id, session) {
                inner.progress(&id, done, total);
            }
        }
        ServerEvent::FileStart {
            peer_id,
            session,
            id,
            name,
            size,
        } => {
            debug!("receiving {name} ({size} bytes) from {peer_id}");
            if let Some(transfer) = inner.transfer_id(&peer_id, session) {
                inner.set_file(&transfer, id as usize, Some(0), None);
            }
        }
        ServerEvent::FileProgress {
            peer_id,
            session,
            id,
            name,
            done,
            size,
        } => {
            let _ = (name, size);
            if let Some(transfer) = inner.transfer_id(&peer_id, session) {
                inner.set_file(&transfer, id as usize, Some(done), None);
            }
        }
        ServerEvent::File {
            peer_id,
            session,
            name,
            id,
            bytes,
            ok,
        } => {
            debug!(
                "received {name} ({bytes} bytes) from {peer_id}: {}",
                if ok { "ok" } else { "failed" }
            );
            if let Some(transfer) = inner.transfer_id(&peer_id, session) {
                inner.set_file(&transfer, id as usize, None, Some(ok));
            }
        }
        ServerEvent::Done {
            peer_id,
            session,
            bytes,
            failures,
        } => {
            let id = inner
                .inbound
                .lock()
                .ok()
                .and_then(|mut map| map.remove(&(peer_id.clone(), session)));
            if let Some(id) = id {
                if let Ok(mut conns) = inner.conns.lock() {
                    conns.remove(&id);
                }
                let status = if failures > 0 {
                    TransferStatus::Failed
                } else {
                    TransferStatus::Done
                };
                inner.progress(&id, bytes, bytes);
                if failures == 0 {
                    inner.complete_files(&id);
                }
                inner.finish_transfer(
                    &id,
                    status,
                    if failures > 0 {
                        Some(format!("{failures} file(s) failed"))
                    } else {
                        None
                    },
                );
            }
        }
        ServerEvent::Declined {
            peer_id,
            peer_name,
            reason,
        } => {
            let id = inner.start_transfer(Direction::In, &peer_id, &peer_name, Vec::new(), 0);
            inner.finish_transfer(&id, TransferStatus::Declined, Some(reason));
        }
    })
}

async fn approval_forwarder(
    inner: Arc<Inner>,
    mut requests: tokio::sync::mpsc::UnboundedReceiver<ApprovalRequest>,
) {
    while let Some(request) = requests.recv().await {
        let id = inner.next_id("a");
        let view = ApprovalView {
            id: id.clone(),
            peer_id: request.peer_id.clone(),
            peer_name: request.peer_name.clone(),
            peer_platform: request.peer_platform.clone(),
            files: request
                .files
                .iter()
                .map(|f| FileView {
                    name: f.name.clone(),
                    size: f.size,
                    done: 0,
                    ok: None,
                })
                .collect(),
            total_bytes: request.total_bytes,
            requested_at: now_secs(),
            timeout_secs: APPROVAL_TIMEOUT.as_secs(),
        };
        let (reply, decision_rx) = oneshot::channel();
        if let Ok(mut pending) = inner.approvals.lock() {
            pending.insert(id.clone(), (view.clone(), reply));
        }
        inner.broadcast(DaemonEvent::Approval { request: view });

        let inner = inner.clone();
        tokio::spawn(async move {
            let decision = match tokio::time::timeout(APPROVAL_TIMEOUT, decision_rx).await {
                Ok(Ok(decision)) => decision,
                Ok(Err(_)) => Decision::reject("approval cancelled"),
                Err(_) => Decision::reject("no answer in time"),
            };
            if let Ok(mut pending) = inner.approvals.lock() {
                pending.remove(&id);
            }
            inner.broadcast(DaemonEvent::ApprovalResolved {
                id: id.clone(),
                accepted: decision.is_accept(),
            });
            let _ = request.reply.send(decision);
        });
    }
}

/// Best match for `target` among the peers discovery has already reported. A device id
/// prefix wins; a name that matches several devices is an error, not a guess.
fn known_peer(inner: &Arc<Inner>, target: &str, needle: &str) -> Result<Option<PeerView>> {
    let peers = inner.peers();
    let by_id: Vec<PeerView> = peers
        .iter()
        .filter(|p| p.device_id.starts_with(target))
        .cloned()
        .collect();
    if by_id.len() == 1 {
        return Ok(by_id.into_iter().next());
    }
    if by_id.len() > 1 {
        bail!(
            "device id prefix '{target}' matches {} devices: {}",
            by_id.len(),
            peer_choices(&by_id)
        );
    }

    let by_name: Vec<PeerView> = peers
        .into_iter()
        .filter(|p| p.name.to_lowercase().contains(needle))
        .collect();
    match by_name.len() {
        0 => Ok(None),
        1 => Ok(by_name.into_iter().next()),
        _ => bail!(
            "'{target}' matches {} devices: {}. Send to one of the 8-character device ids instead.",
            by_name.len(),
            peer_choices(&by_name)
        ),
    }
}

fn peer_choices(peers: &[PeerView]) -> String {
    peers
        .iter()
        .map(|p| {
            format!(
                "{} ({}) at {}",
                p.name,
                &p.device_id[..8.min(p.device_id.len())],
                p.addr
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve a send target: `ip:port`, a hostname, a known peer, or a discovery lookup.
pub async fn resolve_target(
    inner: &Arc<Inner>,
    target: &str,
    timeout: Duration,
) -> Result<(SocketAddr, Option<String>, String)> {
    if let Ok(addr) = target.parse::<SocketAddr>() {
        return Ok((addr, None, target.to_string()));
    }
    if net::is_address_like(target) {
        if let Some(addr) = net::resolve_target_addr(target, inner.quic_port).await {
            return Ok((addr, None, target.to_string()));
        }
    }

    let needle = target.to_lowercase();
    // The running discovery feeds `peers`; wait for it before starting another one.
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(peer) = known_peer(inner, target, &needle)? {
            let addr: SocketAddr = peer
                .addr
                .parse()
                .with_context(|| format!("invalid stored address {}", peer.addr))?;
            return Ok((
                addr,
                Some(peer.device_id.clone()),
                format!(
                    "{} ({})",
                    peer.name,
                    &peer.device_id[..8.min(peer.device_id.len())]
                ),
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Last resort: a short-lived lookup for peers the running instance cannot see.
    let mut disc = discovery::start(
        inner.identity.clone(),
        inner.quic_port,
        inner.discovery_port,
        false,
    )
    .await?;
    let found = discovery::find_peer(&mut disc, target, timeout).await;
    disc.stop()?;
    let peer = match found {
        Ok(peer) => peer,
        Err(e) => {
            // A bare hostname may still resolve through the DNS search domain.
            return match net::resolve_target_addr(target, inner.quic_port).await {
                Some(addr) => Ok((addr, None, target.to_string())),
                None => Err(e),
            };
        }
    };
    Ok((
        peer.addr,
        Some(peer.device_id.clone()),
        format!("{} ({})", peer.name, peer.short_id()),
    ))
}

/// Files offered by a sender, as transfer history entries.
pub fn file_views(files: &[FileMeta]) -> Vec<FileView> {
    files
        .iter()
        .map(|f| FileView {
            name: f.name.clone(),
            size: f.size,
            done: 0,
            ok: None,
        })
        .collect()
}

/// Helper for the API: start an outbound transfer and return its id.
pub async fn spawn_send(
    inner: Arc<Inner>,
    to: String,
    peer_name: Option<String>,
    paths: Vec<PathBuf>,
    concurrency: usize,
    timeout: Duration,
) -> String {
    // Expand once so the transfer list shows real files, with real sizes. The walk
    // blocks, so it runs off the runtime and its result is handed to `client::send`.
    let walk = {
        let paths = paths.clone();
        tokio::task::spawn_blocking(move || qd_core::source::expand(&paths)).await
    };
    let (sources, walk_error) = match walk {
        Ok(Ok(sources)) => (sources, None),
        Ok(Err(e)) => (Vec::new(), Some(format!("{e:#}"))),
        Err(e) => (Vec::new(), Some(format!("scan task: {e}"))),
    };
    let files: Vec<FileView> = sources
        .iter()
        .map(|f| FileView {
            name: f.name.clone(),
            size: f.size,
            done: 0,
            ok: None,
        })
        .collect();
    let total: u64 = files.iter().map(|f| f.size).sum();
    // `to` may be a device id; keep the human name for the transfer list.
    let label = peer_name.clone().unwrap_or_else(|| to.clone());
    let id = inner.start_transfer(Direction::Out, &to, &label, files, total);

    let state = inner.clone();
    let transfer_id = id.clone();
    tokio::spawn(async move {
        if let Some(error) = walk_error {
            state.finish_transfer(
                &transfer_id,
                TransferStatus::Failed,
                Some(format!("cannot read input: {error}")),
            );
            return;
        }
        let result = resolve_target(&state, &to, timeout).await;
        let (addr, device_id, label) = match result {
            Ok(target) => target,
            Err(e) => {
                state.finish_transfer(
                    &transfer_id,
                    TransferStatus::Failed,
                    Some(format!("cannot find peer: {e:#}")),
                );
                return;
            }
        };

        let sink_state = state.clone();
        let sink_id = transfer_id.clone();
        let progress: qd_core::progress::ProgressSink = Arc::new(move |done, total| {
            sink_state.progress(&sink_id, done, total);
        });

        let cancel = qd_core::Cancel::new();
        if let Ok(mut cancels) = state.cancels.lock() {
            cancels.insert(transfer_id.clone(), cancel.clone());
        }
        let file_state = state.clone();
        let file_id = transfer_id.clone();
        let file_progress: qd_core::FileSink = Arc::new(move |id, done, _| {
            file_state.set_file(&file_id, id as usize, Some(done), None);
        });
        let verdict_state = state.clone();
        let verdict_id = transfer_id.clone();
        let file_verdict: qd_core::FileVerdictSink = Arc::new(move |id, ok| {
            verdict_state.set_file(&verdict_id, id as usize, None, Some(ok));
        });

        let opts = SendOpts {
            addr,
            device_id,
            files: paths,
            concurrency: concurrency.max(1),
            quiet: true,
            name: Some(state.identity.name()),
            progress: Some(progress),
            file_progress: Some(file_progress),
            file_verdict: Some(file_verdict),
            cancel: Some(cancel.clone()),
            identity: Some(state.identity.clone()),
            sources: Some(sources),
        };
        debug!("sending to {label} at {addr}");
        match client::send(opts).await {
            Ok(summary) => {
                let status = if cancel.is_cancelled() {
                    TransferStatus::Cancelled
                } else if summary.failed > 0 {
                    TransferStatus::Failed
                } else {
                    TransferStatus::Done
                };
                state.progress(&transfer_id, summary.bytes, summary.bytes);
                if summary.failed == 0 {
                    state.complete_files(&transfer_id);
                }
                state.finish_transfer(
                    &transfer_id,
                    status,
                    if summary.failed > 0 {
                        Some(format!(
                            "{} of {} files failed",
                            summary.failed, summary.files
                        ))
                    } else {
                        None
                    },
                );
            }
            Err(e) => {
                let status = if cancel.is_cancelled() {
                    TransferStatus::Cancelled
                } else {
                    TransferStatus::Failed
                };
                state.finish_transfer(&transfer_id, status, Some(format!("{e:#}")));
            }
        }
        if let Ok(mut cancels) = state.cancels.lock() {
            cancels.remove(&transfer_id);
        }
    });

    id
}
