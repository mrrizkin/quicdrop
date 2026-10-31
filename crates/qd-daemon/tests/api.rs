//! Daemon API: auth, config, trust and an outbound transfer driven over HTTP.

use anyhow::Result;
use qd_core::server::{self, Policy, ServerOpts};
use qd_daemon::{Daemon, DaemonConfig};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;

fn ports() -> (u16, u16, u16, u16, u16) {
    let base = 44_000 + (std::process::id() % 400) as u16 * 5;
    (base, base + 1, base + 2, base + 3, base + 4)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_roundtrip() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg"));

    let (api_port, quic_port, discovery_port, recv_port, recv_disc) = ports();
    let out_dir = root.path().join("out");
    let cfg = DaemonConfig {
        listen: ([127, 0, 0, 1], api_port).into(),
        quic_port,
        discovery_port,
        out_dir: out_dir.clone(),
        policy: Policy::Ask,
        token: Some("test-token".into()),
        ..Default::default()
    };
    let daemon = Daemon::start(cfg).await?;
    let base = format!("http://{}", daemon.addr);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // --- auth -------------------------------------------------------------
    let unauthorized = http.get(format!("{base}/api/status")).send().await?;
    assert_eq!(unauthorized.status(), 401);

    let status: Value = http
        .get(format!("{base}/api/status"))
        .bearer_auth("test-token")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(status["policy"], "ask");
    assert_eq!(status["quic_port"], quic_port);
    assert_eq!(status["pending_approvals"], 0);

    // --- config -----------------------------------------------------------
    let new_out = root.path().join("other-out");
    let patched: Value = http
        .patch(format!("{base}/api/config"))
        .bearer_auth("test-token")
        .json(&json!({ "out_dir": new_out, "policy": "auto" }))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(patched["policy"], "auto");
    assert!(new_out.exists());
    // Put it back so the receive test writes where we expect.
    http.patch(format!("{base}/api/config"))
        .bearer_auth("test-token")
        .json(&json!({ "out_dir": out_dir, "policy": "ask" }))
        .send()
        .await?;

    // --- rename -----------------------------------------------------------
    let device_id = status["device_id"].clone();
    let short_id = status["short_id"].clone();
    let renamed: Value = http
        .patch(format!("{base}/api/config"))
        .bearer_auth("test-token")
        .json(&json!({ "name": "  Quiet Falcon  " }))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(renamed["name"], "Quiet Falcon");
    assert_eq!(renamed["short_id"], short_id);
    let after: Value = http
        .get(format!("{base}/api/status"))
        .bearer_auth("test-token")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(after["name"], "Quiet Falcon");
    // The certificate fingerprint, and so the trust anchor, is unchanged.
    assert_eq!(after["device_id"], device_id);
    assert_eq!(after["short_id"], short_id);

    for bad in ["", "   ", "line\nbreak"] {
        let response = http
            .patch(format!("{base}/api/config"))
            .bearer_auth("test-token")
            .json(&json!({ "name": bad }))
            .send()
            .await?;
        assert_eq!(response.status(), 400, "name {bad:?} must be rejected");
    }
    let too_long = "x".repeat(qd_core::identity::MAX_NAME_LEN + 1);
    let response = http
        .patch(format!("{base}/api/config"))
        .bearer_auth("test-token")
        .json(&json!({ "name": too_long }))
        .send()
        .await?;
    assert_eq!(response.status(), 400);

    // --- trust ------------------------------------------------------------
    http.post(format!("{base}/api/trust"))
        .bearer_auth("test-token")
        .json(&json!({ "device_id": "abcdef0123456789", "name": "dad's phone" }))
        .send()
        .await?;
    let trusted: Vec<Value> = http
        .get(format!("{base}/api/trust"))
        .bearer_auth("test-token")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(trusted.len(), 1);
    assert_eq!(trusted[0]["name"], "dad's phone");

    let removed: Value = http
        .delete(format!("{base}/api/trust/abcdef"))
        .bearer_auth("test-token")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(removed["device_id"], "abcdef0123456789");

    // --- outbound transfer over the API ----------------------------------
    let recv_out = root.path().join("recv");
    let mut opts = ServerOpts::new(recv_port, recv_disc, recv_out.clone());
    opts.policy = Policy::Auto;
    opts.quiet = true;
    let running = server::start(opts.clone()).await?;
    let recv_task = tokio::spawn(server::serve(running, opts));

    let src = root.path().join("hello.txt");
    std::fs::write(&src, b"driven over http\n")?;
    let job: Value = http
        .post(format!("{base}/api/send"))
        .bearer_auth("test-token")
        .json(&json!({
            "to": format!("127.0.0.1:{recv_port}"),
            "paths": [src],
        }))
        .send()
        .await?
        .json()
        .await?;
    let id = job["id"].as_str().expect("job id").to_string();

    let mut transfer = Value::Null;
    for _ in 0..100 {
        let list: Vec<Value> = http
            .get(format!("{base}/api/transfers?limit=10"))
            .bearer_auth("test-token")
            .send()
            .await?
            .json()
            .await?;
        if let Some(found) = list.into_iter().find(|t| t["id"] == id.as_str()) {
            let status = found["status"].as_str().unwrap_or("").to_string();
            transfer = found;
            if status != "active" {
                break;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(transfer["status"], "done", "transfer: {transfer}");
    assert_eq!(transfer["direction"], "out");
    let transferred = transfer["transferred"].as_u64().expect("transferred");
    assert!(transferred > 0);
    let duration_ms = transfer["duration_ms"].as_u64().expect("duration_ms");
    let avg_bps = transfer["avg_bps"].as_u64().expect("avg_bps");
    assert!(duration_ms >= 1);
    let expected_bps = transferred * 1000 / duration_ms;
    assert_eq!(avg_bps, expected_bps);
    assert_eq!(
        std::fs::read(recv_out.join("hello.txt"))?,
        b"driven over http\n"
    );

    // --- 0-byte outbound transfer -----------------------------------------
    let empty_src = root.path().join("empty.txt");
    std::fs::write(&empty_src, b"")?;
    let job: Value = http
        .post(format!("{base}/api/send"))
        .bearer_auth("test-token")
        .json(&json!({
            "to": format!("127.0.0.1:{recv_port}"),
            "paths": [empty_src],
        }))
        .send()
        .await?
        .json()
        .await?;
    let empty_id = job["id"].as_str().expect("job id").to_string();

    let mut empty_transfer = Value::Null;
    for _ in 0..100 {
        let list: Vec<Value> = http
            .get(format!("{base}/api/transfers?limit=10"))
            .bearer_auth("test-token")
            .send()
            .await?
            .json()
            .await?;
        if let Some(found) = list.into_iter().find(|t| t["id"] == empty_id.as_str()) {
            let status = found["status"].as_str().unwrap_or("").to_string();
            empty_transfer = found;
            if status != "active" {
                break;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        empty_transfer["status"], "done",
        "transfer: {empty_transfer}"
    );
    assert_eq!(empty_transfer["transferred"], 0);
    assert!(empty_transfer["duration_ms"].is_number());
    assert!(empty_transfer["avg_bps"].is_null());

    // --- shutdown ---------------------------------------------------------
    http.post(format!("{base}/api/shutdown"))
        .bearer_auth("test-token")
        .send()
        .await?;
    tokio::time::timeout(Duration::from_secs(5), daemon.wait())
        .await
        .expect("daemon should stop");
    recv_task.abort();
    Ok(())
}
