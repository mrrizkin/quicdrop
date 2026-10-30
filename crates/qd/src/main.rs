use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use qd_core::approval;
use qd_core::client::SendOpts;
use qd_core::server::{serve, start, Policy, ServerOpts};
use qd_core::trust::TrustStore;
use qd_core::{Identity, DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "qd",
    version,
    about = "Fast peer-to-peer file transfer over your local network (QUIC)",
    long_about = None
)]
struct Cli {
    /// UDP port for QUIC transfers
    #[arg(long, global = true, default_value_t = DEFAULT_QUIC_PORT)]
    port: u16,

    /// UDP port for peer discovery beacons
    #[arg(long, global = true, default_value_t = DEFAULT_DISCOVERY_PORT)]
    discovery_port: u16,

    /// Override the device name shown to peers
    #[arg(long, global = true)]
    name: Option<String>,

    /// Suppress the progress meter
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Increase log verbosity (-v, -vv)
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Wait for incoming transfers (also advertises this device)
    Receive {
        /// Directory to write files into
        #[arg(short, long)]
        out: Option<PathBuf>,

        /// What to do with senders that are not trusted yet
        #[arg(long, value_enum, default_value_t = PolicyArg::Ask)]
        policy: PolicyArg,
    },
    /// Send one or more files to a peer
    Send {
        /// Files to send
        #[arg(required = true)]
        paths: Vec<PathBuf>,

        /// Peer name, device id prefix, IP, or ip:port
        #[arg(short, long)]
        to: String,

        /// Parallel file streams
        #[arg(short = 'j', long, default_value_t = 4)]
        concurrency: usize,

        /// How long to wait for the peer to show up in discovery
        #[arg(long, default_value = "5s")]
        timeout: String,
    },
    /// List peers visible on the local network
    Peers {
        /// How long to listen for discovery traffic
        #[arg(long, default_value = "4s")]
        timeout: String,
    },
    /// Print this device's identity
    Id,
    /// Manage devices that may send without asking
    Trust {
        #[command(subcommand)]
        action: Option<TrustAction>,
    },
}

#[derive(Subcommand)]
enum TrustAction {
    /// Trust a device by (a prefix of) its device id
    Add {
        /// Device id or unique prefix
        id: String,
        /// Friendly name to show in prompts
        #[arg(long)]
        name: Option<String>,
    },
    /// Stop trusting a device
    Rm {
        /// Device id or unique prefix
        id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PolicyArg {
    /// Prompt before accepting a device that is not trusted
    Ask,
    /// Accept every transfer
    Auto,
    /// Reject every transfer
    Deny,
}

impl From<PolicyArg> for Policy {
    fn from(value: PolicyArg) -> Self {
        match value {
            PolicyArg::Ask => Policy::Ask,
            PolicyArg::Auto => Policy::Auto,
            PolicyArg::Deny => Policy::Deny,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match cli.command {
        Command::Receive { out, policy } => {
            let out_dir = out.unwrap_or_else(default_out_dir);
            let mut opts = ServerOpts::new(cli.port, cli.discovery_port, out_dir);
            opts.name = cli.name;
            opts.quiet = cli.quiet;
            opts.policy = policy.into();
            if opts.policy == Policy::Ask {
                opts.approver = Some(approval::spawn_tty_approver());
            }
            let server = start(opts.clone()).await?;
            println!(
                "qd receiving as \"{}\" ({}) on {}",
                server.identity.name,
                server.identity.short_id(),
                server.addr_hint(cli.port)
            );
            println!("saving to {}", opts.out_dir.display());
            println!("press ctrl+c to stop\n");
            serve(server, opts).await
        }
        Command::Send {
            paths,
            to,
            concurrency,
            timeout,
        } => {
            let timeout = parse_duration(&timeout)?;
            let (addr, device_id, label) = resolve_peer(
                &to,
                cli.name.clone(),
                cli.port,
                cli.discovery_port,
                timeout,
                cli.quiet,
            )
            .await?;
            println!("connecting to {label} at {addr}");
            let summary = SendOpts {
                addr,
                device_id,
                files: paths,
                concurrency,
                quiet: cli.quiet,
                name: cli.name,
                progress: None,
            };
            let summary = qd_core::client::send(summary).await?;
            if summary.failed > 0 {
                anyhow::bail!("{} of {} files failed", summary.failed, summary.files);
            }
            Ok(())
        }
        Command::Peers { timeout } => {
            let timeout = parse_duration(&timeout)?;
            let identity = Arc::new(Identity::load_or_create(cli.name)?);
            let mut disc =
                qd_core::discovery::start(identity, cli.port, cli.discovery_port, false).await?;
            println!("listening for peers for {timeout:?}...");
            let peers = qd_core::discovery::collect_peers(&mut disc, timeout).await;
            if peers.is_empty() {
                println!("no peers found");
            } else {
                println!("{:<24} {:<18} {:<12} ADDRESS", "NAME", "ID", "PLATFORM");
                for peer in peers {
                    println!(
                        "{:<24} {:<18} {:<12} {} [{}]",
                        peer.name,
                        peer.short_id(),
                        peer.platform,
                        peer.addr,
                        peer.source
                    );
                }
            }
            disc.stop()
        }
        Command::Id => {
            let identity = Identity::load_or_create(cli.name)?;
            println!("name:      {}", identity.name);
            println!("device id: {}", identity.device_id);
            println!("config:    {}", identity.dir.display());
            Ok(())
        }
        Command::Trust { action } => match action {
            None => list_trusted(),
            Some(TrustAction::Add { id, name }) => {
                let store = TrustStore::default();
                let name = name.unwrap_or_else(|| format!("device {}", &id[..id.len().min(8)]));
                store.remember(&id, &name, "")?;
                println!("trusted {name} ({id})");
                println!("store: {}", store.path().display());
                Ok(())
            }
            Some(TrustAction::Rm { id }) => {
                let store = TrustStore::default();
                match store.forget_prefix(&id)? {
                    Some(peer) => {
                        println!("forgot {} ({})", peer.name, peer.device_id);
                        Ok(())
                    }
                    None => anyhow::bail!("no trusted device matches \"{id}\""),
                }
            }
        },
    }
}

fn list_trusted() -> Result<()> {
    let store = TrustStore::default();
    let peers = store.list();
    if peers.is_empty() {
        println!("no trusted devices yet");
        println!("incoming transfers will ask for approval (policy: ask)");
        return Ok(());
    }
    println!("{:<18} {:<22} {:<10} LAST SEEN", "ID", "NAME", "PLATFORM");
    for peer in peers {
        println!(
            "{:<18} {:<22} {:<10} {}",
            peer.device_id,
            peer.name,
            if peer.platform.is_empty() {
                "-"
            } else {
                &peer.platform
            },
            qd_core::util::human_age(peer.last_seen)
        );
    }
    println!("\nstore: {}", store.path().display());
    Ok(())
}

async fn resolve_peer(
    target: &str,
    name: Option<String>,
    quic_port: u16,
    discovery_port: u16,
    timeout: Duration,
    quiet: bool,
) -> Result<(SocketAddr, Option<String>, String)> {
    if let Ok(addr) = target.parse::<SocketAddr>() {
        if !quiet {
            eprintln!("note: connecting by address skips peer identity verification");
        }
        return Ok((addr, None, target.to_string()));
    }

    let identity = Arc::new(Identity::load_or_create(name)?);
    let mut disc = qd_core::discovery::start(identity, quic_port, discovery_port, false).await?;
    if !quiet {
        eprintln!("looking for \"{target}\" (up to {timeout:?})...");
    }
    let peer = qd_core::discovery::find_peer(&mut disc, target, timeout).await?;
    disc.stop()?;
    let label = format!("{} ({})", peer.name, peer.short_id());
    Ok((peer.addr, Some(peer.device_id), label))
}

fn parse_duration(s: &str) -> Result<Duration> {
    humantime::parse_duration(s).with_context(|| format!("invalid duration: {s}"))
}

fn default_out_dir() -> PathBuf {
    qd_core::util::default_download_dir()
}

fn init_tracing(verbose: u8) {
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("qd={level},qd_core={level}")));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();
}
