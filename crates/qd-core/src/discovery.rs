//! Peer discovery. mDNS and a UDP beacon run together because networks block one or the
//! other; sightings are merged by `device_id`.

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

pub const BEACON_INTERVAL: Duration = Duration::from_secs(3);
pub const PEER_TTL: Duration = Duration::from_secs(12);

/// Peers discovered only via mDNS are not expired by TTL alone, as mDNS re-announces
/// rarely; they rely on mDNS `ServiceRemoved` instead.
pub fn is_stale(source: &str, last_seen_secs: u64, now_secs: u64, ttl_secs: u64) -> bool {
    if source == "mdns" {
        false
    } else {
        now_secs.saturating_sub(last_seen_secs) >= ttl_secs
    }
}

#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    Seen(Peer),
    Lost { device_id: String },
}

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
    pub events: mpsc::UnboundedReceiver<DiscoveryEvent>,
    pub local_addrs: Vec<IpAddr>,
    daemon: ServiceDaemon,
}

impl Drop for Discovery {
    fn drop(&mut self) {
        // Closing the browse channel also ends the mDNS reader thread.
        let _ = self.daemon.shutdown();
    }
}

/// Start advertising ourselves and listening for peers.
pub async fn start(
    identity: Arc<Identity>,
    quic_port: u16,
    disc_port: u16,
    advertise: bool,
) -> Result<Discovery> {
    let (tx, rx) = mpsc::unbounded_channel::<DiscoveryEvent>();
    let local_addrs = local_ipv4_addrs_async().await;
    let primary = local_addrs
        .iter()
        .find(|ip| matches!(ip, IpAddr::V4(v4) if !v4.is_loopback()))
        .copied();

    let daemon = ServiceDaemon::new().context("create mDNS daemon")?;
    let ip = primary.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    if advertise {
        let mut current = register_service(&daemon, &identity, ip, quic_port);
        // Two records for one device would let peers resolve a stale address.
        let mut renamed = identity.subscribe_name();
        let daemon = daemon.clone();
        let identity = identity.clone();
        tokio::spawn(async move {
            while renamed.changed().await.is_ok() {
                if let Some(fullname) = current.take() {
                    if let Err(e) = daemon.unregister(&fullname) {
                        debug!("mDNS unregister failed: {e}");
                    }
                }
                current = register_service(&daemon, &identity, ip, quic_port);
            }
        });
    }

    let browse = daemon.browse(MDNS_SERVICE).context("mDNS browse")?;
    {
        let tx = tx.clone();
        let me = identity.device_id.clone();
        // Own thread: this loop outlives the runtime and must not stall shutdown.
        std::thread::Builder::new()
            .name("qd-mdns".into())
            .spawn(move || {
                let mut fullname_to_device: HashMap<String, String> = HashMap::new();
                while let Ok(event) = browse.recv() {
                    match event {
                        ServiceEvent::ServiceResolved(info) => {
                            let device_id = info
                                .txt_properties
                                .get_property_val_str("id")
                                .unwrap_or_default()
                                .to_string();
                            if device_id.is_empty() || device_id == me {
                                continue;
                            }
                            let fullname = info.get_fullname().to_string();
                            fullname_to_device.insert(fullname, device_id.clone());

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
                                if tx.send(DiscoveryEvent::Seen(peer)).is_err() {
                                    return;
                                }
                            }
                        }
                        ServiceEvent::ServiceRemoved(_service_type, fullname) => {
                            if let Some(device_id) = fullname_to_device.remove(&fullname) {
                                if !fullname_to_device.values().any(|id| id == &device_id) {
                                    trace!("mdns service removed for device {device_id}");
                                    if tx.send(DiscoveryEvent::Lost { device_id }).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            })
            .context("spawn mDNS reader")?;
    }

    let socket = bind_discovery_socket(disc_port, primary)?;
    let socket = Arc::new(socket);

    if advertise {
        let socket = socket.clone();
        let identity = identity.clone();
        tokio::spawn(async move {
            let broadcast = SocketAddrV4::new(Ipv4Addr::BROADCAST, disc_port);
            let multicast = SocketAddrV4::new(Ipv4Addr::from(MULTICAST_GROUP), disc_port);
            loop {
                // Rebuilt each tick so a rename applies without restarting discovery.
                let beacon = Beacon {
                    v: PROTO_VERSION,
                    device_id: identity.device_id.clone(),
                    name: identity.name(),
                    platform: Identity::platform().to_string(),
                    quic_port,
                };
                match postcard::to_stdvec(&beacon) {
                    Ok(body) => {
                        let mut payload = BROADCAST_MAGIC.to_vec();
                        payload.extend_from_slice(&body);
                        for target in [SocketAddr::V4(broadcast), SocketAddr::V4(multicast)] {
                            if let Err(e) = socket.send_to(&payload, target).await {
                                trace!("beacon send to {target} failed: {e}");
                            }
                        }
                    }
                    Err(e) => warn!("beacon encode failed: {e}"),
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
                        if tx.send(DiscoveryEvent::Seen(peer)).is_err() {
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
        events: rx,
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
        match tokio::time::timeout(remaining, disc.events.recv()).await {
            Ok(Some(DiscoveryEvent::Seen(peer))) => {
                // Prefer entries with a routable, non loopback address.
                let better = match seen.get(&peer.device_id) {
                    None => true,
                    Some(existing) => rank(&peer) > rank(existing),
                };
                if better {
                    seen.insert(peer.device_id.clone(), peer);
                }
            }
            Ok(Some(DiscoveryEvent::Lost { device_id })) => {
                seen.remove(&device_id);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    let mut peers: Vec<Peer> = seen.into_values().collect();
    peers.sort_by(|a, b| a.name.cmp(&b.name));
    peers
}

/// Names are labels and can repeat; wait briefly after the first match in case another
/// device answers with the same name.
const AMBIGUITY_SETTLE: Duration = Duration::from_millis(500);

/// Resolve by exact address, device id prefix, or name substring. A name that matches
/// several devices is an error listing their short ids.
pub async fn find_peer(disc: &mut Discovery, needle: &str, timeout: Duration) -> Result<Peer> {
    find_peer_rx(&mut disc.events, needle, timeout).await
}

async fn find_peer_rx(
    events: &mut mpsc::UnboundedReceiver<DiscoveryEvent>,
    needle: &str,
    timeout: Duration,
) -> Result<Peer> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut matched: HashMap<String, Peer> = HashMap::new();
    let mut seen: HashMap<String, Peer> = HashMap::new();
    let mut settle_until: Option<tokio::time::Instant> = None;

    loop {
        let now = tokio::time::Instant::now();
        if settle_until.is_some_and(|until| now >= until) || now >= deadline {
            break;
        }
        let wait = settle_until
            .map(|until| until.saturating_duration_since(now))
            .unwrap_or_else(|| Duration::from_millis(500))
            .min(deadline.saturating_duration_since(now));
        match tokio::time::timeout(wait, events.recv()).await {
            Ok(Some(DiscoveryEvent::Seen(peer))) => {
                if peer.device_id == needle {
                    return Ok(peer);
                }
                if matches(&peer, needle) {
                    if settle_until.is_none() {
                        settle_until = Some(now + AMBIGUITY_SETTLE);
                    }
                    let better = match matched.get(&peer.device_id) {
                        None => true,
                        Some(existing) => rank(&peer) > rank(existing),
                    };
                    if better {
                        matched.insert(peer.device_id.clone(), peer);
                    }
                } else {
                    let better = match seen.get(&peer.device_id) {
                        None => true,
                        Some(existing) => rank(&peer) > rank(existing),
                    };
                    if better {
                        seen.insert(peer.device_id.clone(), peer);
                    }
                }
            }
            Ok(Some(DiscoveryEvent::Lost { device_id })) => {
                matched.remove(&device_id);
                seen.remove(&device_id);
                if matched.is_empty() {
                    settle_until = None;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    match matched.len() {
        0 => {
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
        1 => Ok(matched.into_values().next().expect("one match")),
        _ => {
            let mut choices: Vec<&Peer> = matched.values().collect();
            choices.sort_by(|a, b| a.name.cmp(&b.name));
            anyhow::bail!(
                "'{needle}' matches {} devices: {}. Send to one of the 8-character device ids instead.",
                choices.len(),
                choices
                    .iter()
                    .map(|p| format!("{} ({}) at {}", p.name, p.short_id(), p.addr))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        }
    }
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

/// mDNS labels stay under 64 bytes (RFC 6763).
const MAX_LABEL: usize = 63;

fn label(name: &str, suffix: &str) -> String {
    let base = sanitize_host(name);
    let room = MAX_LABEL.saturating_sub(suffix.len() + 1);
    let base: String = base.chars().take(room).collect();
    let base = base.trim_end_matches('-');
    let base = if base.is_empty() { "quicdrop" } else { base };
    format!("{base}-{suffix}")
}

fn service_info(identity: &Identity, ip: IpAddr, quic_port: u16) -> Result<ServiceInfo> {
    let name = identity.name();
    // The short id keeps instance and host unique when two devices pick the same name.
    let instance = label(&name, identity.short_id());
    let host = format!("{}.local.", label(&name, identity.short_id()));
    let props = HashMap::from([
        ("id".to_string(), identity.device_id.clone()),
        ("name".to_string(), name),
        ("platform".to_string(), Identity::platform().to_string()),
        ("v".to_string(), PROTO_VERSION.to_string()),
    ]);
    ServiceInfo::new(MDNS_SERVICE, &instance, &host, ip, quic_port, props)
        .context("mDNS service info")
}

/// Returns the fullname needed to unregister the service.
fn register_service(
    daemon: &ServiceDaemon,
    identity: &Identity,
    ip: IpAddr,
    quic_port: u16,
) -> Option<String> {
    let info = match service_info(identity, ip, quic_port) {
        Ok(info) => info,
        Err(e) => {
            warn!("mDNS service info failed: {e:#}");
            return None;
        }
    };
    let fullname = info.get_fullname().to_string();
    match daemon.register(info) {
        Ok(()) => Some(fullname),
        Err(e) => {
            warn!("mDNS register failed: {e}");
            None
        }
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
    // Beacons arrive in bursts; the default buffer drops them on a busy LAN.
    sock.set_recv_buffer_size(1024 * 1024).ok();
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

/// Local IPv4 addresses, primary first, then every interface. Blocks on a hostname lookup,
/// so async callers use [`local_ipv4_addrs_async`].
pub fn local_ipv4_addrs() -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = Vec::new();
    if let Some(ip) = primary_ip() {
        out.push(ip);
    }
    // Covers interfaces the routing table does not pick, such as a Tailscale 100.x address.
    for interface in if_addrs::get_if_addrs().unwrap_or_default() {
        let ip = interface.ip();
        if interface.is_loopback() || !ip.is_ipv4() || out.contains(&ip) {
            continue;
        }
        out.push(ip);
    }
    for ip in extra_local_ips() {
        if !out.contains(&ip) {
            out.push(ip);
        }
    }
    out
}

/// [`local_ipv4_addrs`] off the async runtime: resolving the hostname can hit DNS.
pub async fn local_ipv4_addrs_async() -> Vec<IpAddr> {
    tokio::task::spawn_blocking(local_ipv4_addrs)
        .await
        .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, name: &str, addr: &str) -> Peer {
        Peer {
            device_id: id.to_string(),
            name: name.to_string(),
            platform: "linux".to_string(),
            quic_port: 53320,
            addr: addr.parse().expect("test address"),
            source: "beacon",
            last_seen: Instant::now(),
        }
    }

    #[tokio::test]
    async fn duplicate_names_are_rejected_with_short_ids() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(DiscoveryEvent::Seen(peer(
            "aaaaaaaaaaaaaaaa",
            "Brave Otter",
            "10.0.0.2:53320",
        )))
        .expect("send");
        tx.send(DiscoveryEvent::Seen(peer(
            "bbbbbbbbbbbbbbbb",
            "Brave Otter",
            "10.0.0.3:53320",
        )))
        .expect("send");
        drop(tx);

        let err = find_peer_rx(&mut rx, "brave otter", Duration::from_secs(1))
            .await
            .expect_err("two devices share the name");
        let message = err.to_string();
        assert!(message.contains("2 devices"), "{message}");
        assert!(message.contains("aaaaaaaa"), "{message}");
        assert!(message.contains("bbbbbbbb"), "{message}");
    }

    #[tokio::test]
    async fn unique_name_resolves_after_settle() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(DiscoveryEvent::Seen(peer(
            "aaaaaaaaaaaaaaaa",
            "Brave Otter",
            "10.0.0.2:53320",
        )))
        .expect("send");

        let peer = find_peer_rx(&mut rx, "brave otter", Duration::from_secs(2))
            .await
            .expect("one match");
        assert_eq!(peer.device_id, "aaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn full_device_id_skips_the_settle_window() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(DiscoveryEvent::Seen(peer(
            "aaaaaaaaaaaaaaaa",
            "Brave Otter",
            "10.0.0.2:53320",
        )))
        .expect("send");
        tx.send(DiscoveryEvent::Seen(peer(
            "bbbbbbbbbbbbbbbb",
            "Brave Otter",
            "10.0.0.3:53320",
        )))
        .expect("send");

        let peer = tokio::time::timeout(
            Duration::from_millis(200),
            find_peer_rx(&mut rx, "aaaaaaaaaaaaaaaa", Duration::from_secs(2)),
        )
        .await
        .expect("full id is unambiguous")
        .expect("one match");
        assert_eq!(peer.device_id, "aaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn collect_peers_drops_lost_peers() {
        let (tx, rx) = mpsc::unbounded_channel();
        let daemon = ServiceDaemon::new().expect("daemon");
        let mut disc = Discovery {
            events: rx,
            local_addrs: vec![],
            daemon,
        };

        tx.send(DiscoveryEvent::Seen(peer(
            "aaaaaaaaaaaaaaaa",
            "Device A",
            "10.0.0.2:53320",
        )))
        .expect("send");
        tx.send(DiscoveryEvent::Seen(peer(
            "bbbbbbbbbbbbbbbb",
            "Device B",
            "10.0.0.3:53320",
        )))
        .expect("send");
        tx.send(DiscoveryEvent::Lost {
            device_id: "aaaaaaaaaaaaaaaa".into(),
        })
        .expect("send");

        let peers = collect_peers(&mut disc, Duration::from_millis(100)).await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].device_id, "bbbbbbbbbbbbbbbb");
    }

    #[tokio::test]
    async fn find_peer_rx_drops_lost_peer() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tx.send(DiscoveryEvent::Seen(peer(
            "aaaaaaaaaaaaaaaa",
            "Device A",
            "10.0.0.2:53320",
        )))
        .expect("send");
        tx.send(DiscoveryEvent::Lost {
            device_id: "aaaaaaaaaaaaaaaa".into(),
        })
        .expect("send");
        tx.send(DiscoveryEvent::Seen(peer(
            "bbbbbbbbbbbbbbbb",
            "Device B",
            "10.0.0.3:53320",
        )))
        .expect("send");
        drop(tx);

        let peer = find_peer_rx(&mut rx, "Device B", Duration::from_secs(1))
            .await
            .expect("find Device B");
        assert_eq!(peer.device_id, "bbbbbbbbbbbbbbbb");
    }

    #[test]
    fn stale_peer_detection() {
        // Beacon peer expires after TTL
        assert!(!is_stale("beacon", 100, 105, 12));
        assert!(is_stale("beacon", 100, 112, 12));
        assert!(is_stale("beacon", 100, 120, 12));

        // mDNS peer does not expire by TTL alone
        assert!(!is_stale("mdns", 100, 105, 12));
        assert!(!is_stale("mdns", 100, 1000, 12));
    }
}
