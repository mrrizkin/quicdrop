//! Peer discovery.
//!
//! Two independent mechanisms run at the same time, because neither works everywhere:
//!
//! * **mDNS** (`_quicdrop._udp.local.`) — zero config, resolves names, but dies on networks
//!   that filter multicast or on APs with client isolation.
//! * **UDP broadcast/multicast beacon** — dumb and predictable, survives most campus/office
//!   LANs, carries the device id in the payload so no name resolution is required.
//!
//! Duplicate sightings of the same device are merged by `device_id`.

use anyhow::{Context, Result};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::identity::Identity;
use crate::proto::{BROADCAST_MAGIC, MDNS_SERVICE, MULTICAST_GROUP, PROTO_VERSION};

const BEACON_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone)]
pub struct Peer {
    pub device_id: String,
    pub name: String,
    pub platform: String,
    pub quic_port: u16,
    pub addr: SocketAddr,
    /// `mdns` or `beacon`
    pub source: &'static str,
    pub last_seen: Instant,
}

impl Peer {
    pub fn short_id(&self) -> &str {
        &self.device_id[..8.min(self.device_id.len())]
    }
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}] {} ({})",
            self.name,
            self.short_id(),
            self.addr,
            self.source
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Beacon {
    v: u16,
    device_id: String,
    name: String,
    platform: String,
    quic_port: u16,
}

pub struct Discovery {
    pub peers: mpsc::UnboundedReceiver<Peer>,
    pub local_addrs: Vec<IpAddr>,
    daemon: ServiceDaemon,
}

impl Discovery {
    pub fn stop(&self) -> Result<()> {
        let _ = self.daemon.shutdown();
        Ok(())
    }
}

/// Start advertising ourselves and listening for peers.
pub async fn start(
    identity: Arc<Identity>,
    quic_port: u16,
    disc_port: u16,
    advertise: bool,
) -> Result<Discovery> {
    let (tx, rx) = mpsc::unbounded_channel::<Peer>();
    let local_addrs = local_ipv4_addrs();
    let primary = local_addrs
        .iter()
        .find(|ip| matches!(ip, IpAddr::V4(v4) if !v4.is_loopback()))
        .copied();

    // ---- mDNS -------------------------------------------------------------
    let daemon = ServiceDaemon::new().context("create mDNS daemon")?;
    let instance = format!("{}-{}", identity.name, identity.short_id());
    let host = format!("{}.local.", sanitize_host(&identity.name));
    let props = HashMap::from([
        ("id".to_string(), identity.device_id.clone()),
        ("name".to_string(), identity.name.clone()),
        ("platform".to_string(), Identity::platform().to_string()),
        ("v".to_string(), PROTO_VERSION.to_string()),
    ]);
    let ip = primary.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    if advertise {
        match ServiceInfo::new(MDNS_SERVICE, &instance, &host, ip, quic_port, props) {
            Ok(info) => {
                if let Err(e) = daemon.register(info) {
                    warn!("mDNS register failed: {e}");
                }
            }
            Err(e) => warn!("mDNS service info failed: {e}"),
        }
    }

    let browse = daemon.browse(MDNS_SERVICE).context("mDNS browse")?;
    {
        let tx = tx.clone();
        let me = identity.device_id.clone();
        tokio::task::spawn_blocking(move || {
            while let Ok(event) = browse.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let device_id = info
                        .txt_properties
                        .get_property_val_str("id")
                        .unwrap_or_default()
                        .to_string();
                    if device_id.is_empty() || device_id == me {
                        continue;
                    }
                    let name = info
                        .txt_properties
                        .get_property_val_str("name")
                        .unwrap_or("unknown")
                        .to_string();
                    let platform = info
                        .txt_properties
                        .get_property_val_str("platform")
                        .unwrap_or("unknown")
                        .to_string();
                    let addrs: Vec<SocketAddr> = info
                        .addresses
                        .iter()
                        .map(|a| SocketAddr::new(a.to_ip_addr(), info.port))
                        .collect();
                    for addr in addrs {
                        let peer = Peer {
                            device_id: device_id.clone(),
                            name: name.clone(),
                            platform: platform.clone(),
                            quic_port: info.port,
                            addr,
                            source: "mdns",
                            last_seen: Instant::now(),
                        };
                        trace!("mdns peer {peer}");
                        if tx.send(peer).is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }

    // ---- UDP beacon -------------------------------------------------------
    let socket = bind_discovery_socket(disc_port, primary)?;
    let socket = Arc::new(socket);

    if advertise {
        let socket = socket.clone();
        let identity = identity.clone();
        tokio::spawn(async move {
            let beacon = Beacon {
                v: PROTO_VERSION,
                device_id: identity.device_id.clone(),
                name: identity.name.clone(),
                platform: Identity::platform().to_string(),
                quic_port,
            };
            let mut payload = BROADCAST_MAGIC.to_vec();
            match postcard::to_stdvec(&beacon) {
                Ok(body) => payload.extend_from_slice(&body[..]),
                Err(e) => {
                    warn!("beacon encode failed: {e}");
                    return;
                }
            }
            let broadcast = SocketAddrV4::new(Ipv4Addr::BROADCAST, disc_port);
            let multicast = SocketAddrV4::new(Ipv4Addr::from(MULTICAST_GROUP), disc_port);
            loop {
                for target in [SocketAddr::V4(broadcast), SocketAddr::V4(multicast)] {
                    if let Err(e) = socket.send_to(&payload, target).await {
                        trace!("beacon send to {target} failed: {e}");
                    }
                }
                tokio::time::sleep(BEACON_INTERVAL).await;
            }
        });
    }

    {
        let socket = socket.clone();
        let me = identity.device_id.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                match socket.recv_from(&mut buf).await {
                    Ok((n, src)) => {
                        if n < BROADCAST_MAGIC.len() + 1 || &buf[..4] != BROADCAST_MAGIC {
                            continue;
                        }
                        let beacon: Beacon = match postcard::from_bytes(&buf[4..n]) {
                            Ok(b) => b,
                            Err(e) => {
                                trace!("bad beacon from {src}: {e}");
                                continue;
                            }
                        };
                        if beacon.device_id == me || beacon.v != PROTO_VERSION {
                            continue;
                        }
                        let mut addr = src;
                        addr.set_port(beacon.quic_port);
                        let peer = Peer {
                            device_id: beacon.device_id,
                            name: beacon.name,
                            platform: beacon.platform,
                            quic_port: beacon.quic_port,
                            addr,
                            source: "beacon",
                            last_seen: Instant::now(),
                        };
                        trace!("beacon peer {peer}");
                        if tx.send(peer).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        debug!("discovery recv error: {e}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        });
    }

    info!("discovery listening on udp/{disc_port}, quic on udp/{quic_port}");
    Ok(Discovery {
        peers: rx,
        local_addrs,
        daemon,
    })
}

/// Collect peers until `timeout` elapses, deduplicated by device id.
pub async fn collect_peers(disc: &mut Discovery, timeout: Duration) -> Vec<Peer> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen: HashMap<String, Peer> = HashMap::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, disc.peers.recv()).await {
            Ok(Some(peer)) => {
                // Prefer entries with a routable, non loopback address.
                let better = match seen.get(&peer.device_id) {
                    None => true,
                    Some(existing) => rank(&peer) > rank(existing),
                };
                if better {
                    seen.insert(peer.device_id.clone(), peer);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let mut peers: Vec<Peer> = seen.into_values().collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name));
    peers
}

/// Resolve a peer by exact address, device id prefix, or name substring.
pub async fn find_peer(disc: &mut Discovery, needle: &str, timeout: Duration) -> Result<Peer> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut seen: HashMap<String, Peer> = HashMap::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining.min(Duration::from_millis(500)), disc.peers.recv())
            .await
        {
            Ok(Some(peer)) => {
                if matches(&peer, needle) {
                    return Ok(peer);
                }
                let better = match seen.get(&peer.device_id) {
                    None => true,
                    Some(existing) => rank(&peer) > rank(existing),
                };
                if better {
                    seen.insert(peer.device_id.clone(), peer);
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    let known: Vec<String> = seen.values().map(|p| p.to_string()).collect();
    anyhow::bail!(
        "peer '{needle}' not found (saw: {})",
        if known.is_empty() {
            "nothing".to_string()
        } else {
            known.join(", ")
        }
    )
}

fn matches(peer: &Peer, needle: &str) -> bool {
    peer.device_id.starts_with(needle)
        || peer.name.to_lowercase().contains(&needle.to_lowercase())
        || peer.addr.ip().to_string() == needle
}

fn rank(peer: &Peer) -> u8 {
    match peer.addr.ip() {
        IpAddr::V4(v4) if v4.is_loopback() => 0,
        IpAddr::V4(_) => 2,
        IpAddr::V6(v6) if v6.is_loopback() => 0,
        IpAddr::V6(_) => 1,
    }
}

fn sanitize_host(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('-').to_lowercase();
    if cleaned.is_empty() {
        "quicdrop".to_string()
    } else {
        cleaned
    }
}

fn bind_discovery_socket(port: u16, primary: Option<IpAddr>) -> Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};

    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("create discovery socket")?;
    sock.set_reuse_address(true).ok();
    #[cfg(all(unix, not(target_os = "linux")))]
    sock.set_reuse_port(true).ok();
    sock.set_broadcast(true).ok();
    sock.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)).into())
        .with_context(|| format!("bind udp/{port} for discovery"))?;

    let iface = match primary {
        Some(IpAddr::V4(v4)) => v4,
        _ => Ipv4Addr::UNSPECIFIED,
    };
    if let Err(e) = sock.join_multicast_v4(&Ipv4Addr::from(MULTICAST_GROUP), &iface) {
        debug!("join multicast failed: {e}");
    }

    sock.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(sock.into())?)
}

/// Local IPv4 addresses, primary first.
pub fn local_ipv4_addrs() -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    if let Some(ip) = primary_ip() {
        out.push(ip);
    }
    for ip in extra_local_ips() {
        if !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

fn primary_ip() -> Option<IpAddr> {
    // No packet is sent; this only asks the routing table which source address it would use.
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("1.1.1.1:80").ok()?;
    let ip = sock.local_addr().ok()?.ip();
    if ip.is_unspecified() {
        None
    } else {
        Some(ip)
    }
}

fn extra_local_ips() -> HashSet<IpAddr> {
    // Cheap best-effort: hostname lookup covers most single-homed desktops.
    let mut set = HashSet::new();
    if let Ok(name) = std::env::var("HOSTNAME") {
        if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&(name.as_str(), 0)) {
            for a in addrs {
                set.insert(a.ip());
            }
        }
    }
    set
}
