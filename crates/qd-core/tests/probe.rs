//! Tests for client::probe.

use anyhow::Result;
use qd_core::client::{self, SendOpts};
use qd_core::server::{self, Policy, ServerOpts};
use std::time::Duration;

fn free_ports() -> (u16, u16) {
    let base = 42_000 + (std::process::id() % 800) as u16 * 4;
    (base, base + 1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_live_peer_and_mismatch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let prev_cfg = std::env::var("QD_CONFIG_DIR").ok();
    std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg"));

    let (quic_port, discovery_port) = free_ports();
    let out = root.path().join("out");
    let mut opts = ServerOpts::new(quic_port, discovery_port, out);
    opts.policy = Policy::Auto;
    opts.quiet = true;

    let running = server::start(opts.clone()).await?;
    let target_addr = format!("127.0.0.1:{quic_port}").parse()?;
    let server_task = tokio::spawn(server::serve(running, opts));

    // 1. Probe without expected device_id -> should succeed and capture peer info
    let captured = client::probe(SendOpts {
        addr: target_addr,
        ..Default::default()
    })
    .await?;
    assert!(!captured.device_id.is_empty());
    assert!(!captured.name.is_empty());

    // 2. Probe with matching device_id -> should succeed
    let captured2 = client::probe(SendOpts {
        addr: target_addr,
        device_id: Some(captured.device_id.clone()),
        ..Default::default()
    })
    .await?;
    assert_eq!(captured2.device_id, captured.device_id);

    // 3. Probe with mismatched device_id -> should fail with mismatch error
    let err = client::probe(SendOpts {
        addr: target_addr,
        device_id: Some("0000000000000000".to_string()),
        ..Default::default()
    })
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("device id mismatch"),
        "error should contain mismatch: {err}"
    );

    // 4. Probe dead port -> should fail / timeout
    let dead_addr = format!("127.0.0.1:{}", quic_port + 50).parse()?;
    let dead_res = client::probe_timeout(
        SendOpts {
            addr: dead_addr,
            ..Default::default()
        },
        Duration::from_millis(500),
    )
    .await;
    assert!(dead_res.is_err());

    server_task.abort();
    if let Some(prev) = prev_cfg {
        std::env::set_var("QD_CONFIG_DIR", prev);
    } else {
        std::env::remove_var("QD_CONFIG_DIR");
    }
    Ok(())
}
