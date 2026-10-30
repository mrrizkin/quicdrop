use anyhow::Result;
use clap::{Parser, ValueEnum};
use qd_core::server::Policy;
use qd_core::{DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT};
use qd_daemon::{Daemon, DaemonConfig};
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "quicdropd",
    version,
    about = "quicdrop daemon: local API for the desktop app"
)]
struct Cli {
    /// HTTP API address (loopback by default)
    #[arg(long, default_value = "127.0.0.1:53319")]
    listen: SocketAddr,

    /// UDP port for QUIC transfers
    #[arg(long, default_value_t = DEFAULT_QUIC_PORT)]
    port: u16,

    /// UDP port for discovery beacons
    #[arg(long, default_value_t = DEFAULT_DISCOVERY_PORT)]
    discovery_port: u16,

    /// Directory incoming files are written to
    #[arg(short, long)]
    out: Option<PathBuf>,

    /// Override the device name
    #[arg(long)]
    name: Option<String>,

    /// What to do with senders that are not trusted yet
    #[arg(long, value_enum, default_value_t = PolicyArg::Ask)]
    policy: PolicyArg,

    /// Fixed API token (otherwise generated and written to daemon.json)
    #[arg(long, env = "QD_API_TOKEN", hide_env_values = true)]
    token: Option<String>,

    /// Suppress the progress meter
    #[arg(short, long)]
    quiet: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PolicyArg {
    Ask,
    Auto,
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
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("quicdropd=info,qd_daemon=info,qd_core=warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .try_init();

    let cfg = DaemonConfig {
        listen: cli.listen,
        quic_port: cli.port,
        discovery_port: cli.discovery_port,
        out_dir: cli.out.unwrap_or_else(qd_core::util::default_download_dir),
        name: cli.name,
        policy: cli.policy.into(),
        token: cli.token,
        quiet: cli.quiet,
    };

    let daemon = Daemon::start(cfg).await?;
    println!("quicdropd api: http://{}", daemon.addr);
    println!("api token:     {}", daemon.token);
    println!(
        "daemon info:   {}",
        qd_core::Identity::config_dir()
            .join("daemon.json")
            .display()
    );
    println!("press ctrl+c to stop\n");

    tokio::select! {
        _ = daemon.wait() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
    daemon.shutdown();
    Ok(())
}
