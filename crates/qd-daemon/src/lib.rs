//! Daemon: the transfer engine behind a loopback JSON/SSE API, embedded by `quicdropd` and
//! the desktop app. State changes require the bearer token written to `daemon.json`.

use anyhow::{bail, Context, Result};
use qd_core::approval::{self, ApprovalRequest, Decision, APPROVAL_TIMEOUT};
use qd_core::client::{self, SendOpts};
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot, watch};
use tracing::{debug, info, warn};

mod api;

pub use api::router;

pub const DEFAULT_API_PORT: u16 = 53319;
pub const API_VERSION: &str = env!("CARGO_PKG_VERSION");

/// How often stale peers are swept; well under `PEER_TTL` so removal stays prompt.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) fn now_secs() -> u64 {
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
    pub duration_ms: Option<u64>,
    pub avg_bps: Option<u64>,
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
    PeerLost {
        device_id: String,
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
    transfer_times: Mutex<HashMap<String, Instant>>,
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

    pub(crate) fn remember_peer(&self, peer: &Peer) {
        self.remember_peer_at(peer, now_secs());
    }

    pub(crate) fn remember_peer_at(&self, peer: &Peer, seen_at_secs: u64) {
        let mut view = PeerView::from_peer(peer, self.trust.is_trusted(&peer.device_id));
        view.last_seen = seen_at_secs;
        let mut broadcast_view = None;
        if let Ok(mut peers) = self.peers.lock() {
            if let Some(existing) = peers.get_mut(&peer.device_id) {
                existing.last_seen = seen_at_secs;
                let changed = existing.addr != view.addr
                    || existing.name != view.name
                    || existing.trusted != view.trusted;
                if changed {
                    *existing = view.clone();
                    broadcast_view = Some(view);
                } else if existing.source == "mdns" && view.source == "beacon" {
                    existing.source = view.source;
                }
            } else {
                peers.insert(peer.device_id.clone(), view.clone());
                broadcast_view = Some(view);
            }
        }
        if let Some(view) = broadcast_view {
            self.broadcast(DaemonEvent::Peer { peer: view });
        }
    }

    pub(crate) fn forget_peer(&self, device_id: &str) -> bool {
        let removed = {
            let Ok(mut peers) = self.peers.lock() else {
                return false;
            };
            peers.remove(device_id).is_some()
        };
        if removed {
            self.broadcast(DaemonEvent::PeerLost {
                device_id: device_id.to_string(),
            });
        }
        removed
    }

    pub(crate) fn sweep_at(&self, now_secs: u64, ttl_secs: u64) -> Vec<String> {
        let lost = {
            let Ok(mut peers) = self.peers.lock() else {
                return Vec::new();
            };
            sweep_stale_peers(&mut peers, now_secs, ttl_secs)
        };
        for id in &lost {
            self.broadcast(DaemonEvent::PeerLost {
                device_id: id.clone(),
            });
        }
        lost
    }

    pub(crate) fn sweep(&self) -> Vec<String> {
        self.sweep_at(now_secs(), discovery::PEER_TTL.as_secs())
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
            duration_ms: None,
            avg_bps: None,
        };
        if let Ok(mut transfers) = self.transfers.lock() {
            transfers.push(view.clone());
            // Bound history so a long-lived daemon does not grow without limit.
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

    fn mark_transfer_started(&self, id: &str) {
        if let Ok(mut times) = self.transfer_times.lock() {
            times.entry(id.to_string()).or_insert_with(Instant::now);
        }
    }

    fn progress(&self, id: &str, transferred: u64, total: u64) {
        let live_ms = if transferred > 0 {
            self.transfer_times.lock().ok().map(|mut times| {
                let start = times.entry(id.to_string()).or_insert_with(Instant::now);
                start.elapsed().as_millis() as u64
            })
        } else {
            None
        };
        let mut found = false;
        if let Ok(mut transfers) = self.transfers.lock() {
            if let Some(t) = transfers.iter_mut().find(|t| t.id == id) {
                // Resume can lower the accepted total; it is authoritative once sending starts.
                if total > 0 {
                    t.total_bytes = total;
                }
                t.transferred = transferred.max(t.transferred);
                if let Some(ms) = live_ms {
                    t.duration_ms = Some(ms);
                }
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

    fn finish_transfer(
        &self,
        id: &str,
        status: TransferStatus,
        error: Option<String>,
        explicit_duration: Option<Duration>,
    ) {
        let elapsed = self
            .transfer_times
            .lock()
            .ok()
            .and_then(|mut times| times.remove(id))
            .map(|start| start.elapsed());
        let dur = explicit_duration.or(elapsed);
        let view = {
            let Ok(mut transfers) = self.transfers.lock() else {
                return;
            };
            let Some(transfer) = transfers.iter_mut().find(|t| t.id == id) else {
                return;
            };
            if let Some(dur) = dur {
                let (ms, avg) = qd_core::util::calc_transfer_speed(transfer.transferred, dur);
                transfer.duration_ms = Some(ms);
                transfer.avg_bps = avg;
            }
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
            transfer_times: Mutex::new(HashMap::new()),
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

        let mut events_rx = std::mem::replace(
            &mut running.discovery.events,
            tokio::sync::mpsc::unbounded_channel().1,
        );
        {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut shutdown = inner.shutdown.subscribe();
                loop {
                    tokio::select! {
                        _ = shutdown.wait_for(|stop| *stop) => break,
                        event = events_rx.recv() => {
                            match event {
                                Some(discovery::DiscoveryEvent::Seen(peer)) => {
                                    inner.remember_peer(&peer);
                                }
                                Some(discovery::DiscoveryEvent::Lost { device_id }) => {
                                    inner.forget_peer(&device_id);
                                }
                                None => break,
                            }
                        }
                    }
                }
            });
        }

        // Periodic sweep task removes peers not seen within PEER_TTL.
        {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut shutdown = inner.shutdown.subscribe();
                let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
                ticker.tick().await;
                loop {
                    tokio::select! {
                        _ = shutdown.wait_for(|stop| *stop) => break,
                        _ = ticker.tick() => {
                            inner.sweep();
                        }
                    }
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
            inner.mark_transfer_started(&id);
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
            done,
            ..
        } => {
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
            duration,
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
                    Some(duration),
                );
            }
        }
        ServerEvent::Declined {
            peer_id,
            peer_name,
            reason,
        } => {
            let id = inner.start_transfer(Direction::In, &peer_id, &peer_name, Vec::new(), 0);
            inner.finish_transfer(&id, TransferStatus::Declined, Some(reason), None);
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

    // A bare hostname may still resolve through the DNS search domain.
    if let Some(addr) = net::resolve_target_addr(target, inner.quic_port).await {
        return Ok((addr, None, target.to_string()));
    }
    let seen = inner.peers();
    let seen = if seen.is_empty() {
        "nothing".to_string()
    } else {
        peer_choices(&seen)
    };
    bail!("peer '{target}' not found (saw: {seen})")
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
    let walk = tokio::task::spawn_blocking(move || qd_core::source::expand(&paths)).await;
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
                None,
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
                    None,
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

        let start_state = state.clone();
        let start_id = transfer_id.clone();
        let on_start: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            start_state.mark_transfer_started(&start_id);
        });

        let opts = SendOpts {
            addr,
            device_id,
            files: Vec::new(),
            concurrency: concurrency.max(1),
            quiet: true,
            name: Some(state.identity.name()),
            progress: Some(progress),
            file_progress: Some(file_progress),
            file_verdict: Some(file_verdict),
            cancel: Some(cancel.clone()),
            identity: Some(state.identity.clone()),
            sources: Some(sources),
            on_start: Some(on_start),
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
                    Some(summary.duration),
                );
            }
            Err(e) => {
                let status = if cancel.is_cancelled() {
                    TransferStatus::Cancelled
                } else {
                    TransferStatus::Failed
                };
                state.finish_transfer(&transfer_id, status, Some(format!("{e:#}")), None);
            }
        }
        if let Ok(mut cancels) = state.cancels.lock() {
            cancels.remove(&transfer_id);
        }
    });

    id
}

/// Sweep stale peers from the peer map. Returns the device IDs of removed peers.
pub(crate) fn sweep_stale_peers(
    peers: &mut HashMap<String, PeerView>,
    now_secs: u64,
    ttl_secs: u64,
) -> Vec<String> {
    let mut lost = Vec::new();
    peers.retain(|id, peer| {
        if discovery::is_stale(&peer.source, peer.last_seen, now_secs, ttl_secs) {
            lost.push(id.clone());
            false
        } else {
            true
        }
    });
    lost
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn test_peer(id: &str, name: &str, source: &'static str) -> Peer {
        Peer {
            device_id: id.to_string(),
            name: name.to_string(),
            platform: "linux".to_string(),
            quic_port: 53320,
            addr: "10.0.0.2:53320".parse().expect("addr"),
            source,
            last_seen: Instant::now(),
        }
    }

    fn test_inner() -> (Arc<Inner>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (events, _) = broadcast::channel(512);
        let (shutdown, _) = watch::channel(false);
        let identity =
            Arc::new(Identity::load_or_create_in(dir.path().join("cfg"), None).expect("identity"));
        let trust = TrustStore::at(dir.path().join("trusted.json"));
        let inner = Arc::new(Inner {
            identity,
            token: "test".into(),
            listen: "127.0.0.1:0".parse().expect("addr"),
            quic_port: 53320,
            discovery_port: 53321,
            version: API_VERSION.to_string(),
            started_at: 1000,
            local_addrs: vec![],
            runtime: Arc::new(RuntimeSettings::new(dir.path().join("out"), Policy::Ask)),
            trust,
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
        (inner, dir)
    }

    #[test]
    fn pure_sweep_removes_expired_beacon_peer() {
        let mut peers = HashMap::new();
        peers.insert(
            "dev1".into(),
            PeerView {
                device_id: "dev1".into(),
                name: "Peer 1".into(),
                platform: "linux".into(),
                addr: "10.0.0.1:53320".into(),
                source: "beacon".into(),
                trusted: false,
                last_seen: 100,
            },
        );
        let lost = sweep_stale_peers(&mut peers, 113, 12);
        assert_eq!(lost, vec!["dev1".to_string()]);
        assert!(peers.is_empty());
    }

    #[test]
    fn pure_sweep_retains_fresh_beacon_peer() {
        let mut peers = HashMap::new();
        peers.insert(
            "dev1".into(),
            PeerView {
                device_id: "dev1".into(),
                name: "Peer 1".into(),
                platform: "linux".into(),
                addr: "10.0.0.1:53320".into(),
                source: "beacon".into(),
                trusted: false,
                last_seen: 105,
            },
        );
        let lost = sweep_stale_peers(&mut peers, 113, 12);
        assert!(lost.is_empty());
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn pure_sweep_ignores_mdns_only_peer() {
        let mut peers = HashMap::new();
        peers.insert(
            "dev1".into(),
            PeerView {
                device_id: "dev1".into(),
                name: "Peer 1".into(),
                platform: "linux".into(),
                addr: "10.0.0.1:53320".into(),
                source: "mdns".into(),
                trusted: false,
                last_seen: 100,
            },
        );
        let lost = sweep_stale_peers(&mut peers, 1000, 12);
        assert!(lost.is_empty());
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn refreshed_peer_stays() {
        let (inner, _dir) = test_inner();
        let peer = test_peer("dev1", "Peer 1", "beacon");

        inner.remember_peer_at(&peer, 100);
        assert_eq!(inner.peers().len(), 1);

        // Sweep before TTL -> stays
        let lost = inner.sweep_at(108, 12);
        assert!(lost.is_empty());
        assert_eq!(inner.peers().len(), 1);

        // Refreshed sighting at t=108
        inner.remember_peer_at(&peer, 108);

        // Sweep at t=115 (15s from initial, but only 7s from refresh) -> stays
        let lost = inner.sweep_at(115, 12);
        assert!(lost.is_empty());
        assert_eq!(inner.peers().len(), 1);
        assert_eq!(inner.counts().peers, 1);
    }

    #[tokio::test]
    async fn stale_peer_removed_and_emits_peer_lost() {
        let (inner, _dir) = test_inner();
        let mut rx = inner.subscribe();
        let peer = test_peer("dev1", "Peer 1", "beacon");

        inner.remember_peer_at(&peer, 100);
        // First sighting emits Peer event
        let event = rx.recv().await.expect("peer event");
        assert!(matches!(event, DaemonEvent::Peer { .. }));

        // Sweep after TTL
        let lost = inner.sweep_at(113, 12);
        assert_eq!(lost, vec!["dev1".to_string()]);
        assert_eq!(inner.peers().len(), 0);
        assert_eq!(inner.counts().peers, 0);

        // Broadcast emits PeerLost event
        let event = rx.recv().await.expect("peer lost event");
        match event {
            DaemonEvent::PeerLost { device_id } => assert_eq!(device_id, "dev1"),
            other => panic!("expected PeerLost event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn lost_event_removes_peer_and_emits_peer_lost() {
        let (inner, _dir) = test_inner();
        let mut rx = inner.subscribe();
        let peer = test_peer("dev1", "Peer 1", "mdns");

        inner.remember_peer_at(&peer, 100);
        let _ = rx.recv().await.expect("peer event");
        assert_eq!(inner.peers().len(), 1);

        // forget_peer (called by DiscoveryEvent::Lost) promptly removes it
        let removed = inner.forget_peer("dev1");
        assert!(removed);
        assert_eq!(inner.peers().len(), 0);
        assert_eq!(inner.counts().peers, 0);

        let event = rx.recv().await.expect("peer lost event");
        match event {
            DaemonEvent::PeerLost { device_id } => assert_eq!(device_id, "dev1"),
            other => panic!("expected PeerLost event, got {other:?}"),
        }
    }

    #[test]
    fn mdns_peer_not_expired_by_ttl() {
        let (inner, _dir) = test_inner();
        let peer = test_peer("dev1", "Peer 1", "mdns");

        inner.remember_peer_at(&peer, 100);
        let lost = inner.sweep_at(1000, 12);
        assert!(lost.is_empty());
        assert_eq!(inner.peers().len(), 1);
    }

    #[tokio::test]
    async fn sighting_refreshes_last_seen_without_duplicate_broadcast() {
        let (inner, _dir) = test_inner();
        let mut rx = inner.subscribe();
        let peer = test_peer("dev1", "Peer 1", "beacon");

        inner.remember_peer_at(&peer, 100);
        let _ = rx.recv().await.expect("peer event");
        assert_eq!(inner.peers()[0].last_seen, 100);

        // Re-sighting with same address/name/trusted
        inner.remember_peer_at(&peer, 105);
        assert_eq!(inner.peers()[0].last_seen, 105);

        // No new event broadcast
        assert!(rx.try_recv().is_err());
    }
}
