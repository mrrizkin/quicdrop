//! Two inbound transfers from the same peer must stay separate.

use anyhow::Result;
use qd_core::proto::{self, FileAck, FileHeader, FileMeta, FileTrailer, Frame, PROTO_VERSION};
use qd_core::quinn;
use qd_core::server::Policy;
use qd_daemon::{Daemon, DaemonConfig};
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

const MIB: u64 = 1024 * 1024;

fn ports() -> (u16, u16, u16) {
    let base = 55_000 + (std::process::id() % 400) as u16 * 3;
    (base, base + 1, base + 2)
}

/// Open the control stream for one offer and return it once the receiver accepted.
async fn offer(
    conn: &quinn::Connection,
    session: [u8; 16],
    file: &FileMeta,
) -> Result<(quinn::SendStream, quinn::RecvStream)> {
    let (mut ctrl_send, mut ctrl_recv) = conn.open_bi().await?;
    proto::write_frame(
        &mut ctrl_send,
        &Frame::Hello {
            version: PROTO_VERSION,
            // Both connections announce the same device id.
            device_id: "same-peer".into(),
            name: "same peer".into(),
            platform: "test".into(),
        },
    )
    .await?;
    match proto::read_frame(&mut ctrl_recv).await? {
        Frame::Hello { .. } => {}
        other => panic!("expected hello, got {other:?}"),
    }
    proto::write_msg_limit(
        &mut ctrl_send,
        &Frame::Offer {
            session,
            files: vec![file.clone()],
        },
        proto::MAX_OFFER_FRAME,
    )
    .await?;
    match proto::read_frame(&mut ctrl_recv).await? {
        Frame::Accept { .. } => {}
        other => panic!("expected accept, got {other:?}"),
    }
    Ok((ctrl_send, ctrl_recv))
}

async fn write_header(
    send: &mut quinn::SendStream,
    session: [u8; 16],
    id: u32,
    size: u64,
) -> Result<()> {
    proto::write_msg(
        send,
        &FileHeader {
            session,
            id,
            offset: 0,
            size,
            index: 0,
            count: 1,
        },
    )
    .await?;
    Ok(())
}

async fn write_trailer(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    id: u32,
    payload: &[u8],
) -> Result<()> {
    proto::write_msg(
        send,
        &FileTrailer {
            id,
            hash: blake3::hash(payload).to_hex().to_string(),
        },
    )
    .await?;
    send.finish()?;
    match proto::read_msg::<FileAck>(recv).await? {
        FileAck::Verdict { ok: true, .. } => Ok(()),
        other => panic!("expected a passing verdict, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inbound_sessions_stay_separate() -> Result<()> {
    let root = tempfile::tempdir()?;
    std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg"));

    let (api_port, quic_port, discovery_port) = ports();
    let cfg = DaemonConfig {
        listen: ([127, 0, 0, 1], api_port).into(),
        quic_port,
        discovery_port,
        out_dir: root.path().join("out"),
        policy: Policy::Auto,
        token: Some("test-token".into()),
        ..Default::default()
    };
    let daemon = Daemon::start(cfg).await?;
    let base = format!("http://{}", daemon.addr);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let client_cfg = qd_core::net::client_config(None)?;
    let endpoint = qd_core::net::client_endpoint(client_cfg)?;
    let addr = SocketAddr::from(([127, 0, 0, 1], quic_port));
    // Two sessions are two connections; both announce the same device id.
    let conn_a = endpoint.connect(addr, "quicdrop.local")?.await?;
    let conn_b = endpoint.connect(addr, "quicdrop.local")?.await?;

    let a_session = [1u8; 16];
    let b_session = [2u8; 16];
    let a_size = 8 * MIB;
    let b_size = MIB;
    let a_meta = FileMeta {
        id: 0,
        name: "large.bin".into(),
        size: a_size,
        hash: None,
    };
    let b_meta = FileMeta {
        id: 0,
        name: "small.bin".into(),
        size: b_size,
        hash: None,
    };
    let a_payload = vec![1u8; a_size as usize];
    let b_payload = vec![2u8; b_size as usize];

    // A is already transferring when B arrives.
    let (mut a_ctrl_send, _a_ctrl_recv) = offer(&conn_a, a_session, &a_meta).await?;
    let (mut a_send, mut a_recv) = conn_a.open_bi().await?;
    let first = 4 * MIB as usize;
    let after_first = first + 1;
    let after_second = after_first + MIB as usize + 1;
    write_header(&mut a_send, a_session, 0, a_size).await?;
    a_send.write_all(&a_payload[..first]).await?;
    sleep(Duration::from_millis(150)).await;
    // Progress events are throttled to 100 ms.
    a_send.write_all(&a_payload[first..after_first]).await?;

    // B starts while A is still running.
    let (mut b_ctrl_send, _b_ctrl_recv) = offer(&conn_b, b_session, &b_meta).await?;
    let (mut b_send, mut b_recv) = conn_b.open_bi().await?;

    // A progresses while B is active.
    a_send
        .write_all(&a_payload[after_first..after_first + MIB as usize])
        .await?;
    sleep(Duration::from_millis(150)).await;
    a_send
        .write_all(&a_payload[after_first + MIB as usize..after_second])
        .await?;

    write_header(&mut b_send, b_session, 0, b_size).await?;
    b_send.write_all(&b_payload).await?;
    write_trailer(&mut b_send, &mut b_recv, 0, &b_payload).await?;
    proto::write_frame(&mut b_ctrl_send, &Frame::Bye).await?;
    let _ = b_ctrl_send.finish();

    // A finishes after B.
    a_send.write_all(&a_payload[after_second..]).await?;
    write_trailer(&mut a_send, &mut a_recv, 0, &a_payload).await?;
    proto::write_frame(&mut a_ctrl_send, &Frame::Bye).await?;
    let _ = a_ctrl_send.finish();

    let mut inbound: Vec<Value> = Vec::new();
    for _ in 0..100 {
        let list: Vec<Value> = http
            .get(format!("{base}/api/transfers?limit=10"))
            .bearer_auth("test-token")
            .send()
            .await?
            .json()
            .await?;
        inbound = list
            .into_iter()
            .filter(|t| t["direction"] == "in")
            .collect();
        if inbound.len() == 2
            && inbound
                .iter()
                .all(|t| t["status"].as_str() != Some("active"))
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(inbound.len(), 2, "transfers: {inbound:?}");
    for transfer in &inbound {
        assert_eq!(
            transfer["status"], "done",
            "transfer did not finish: {transfer}"
        );
        assert_eq!(
            transfer["transferred"], transfer["total_bytes"],
            "progress leaked across sessions: {transfer}"
        );
    }
    let total_for = |name: &str| {
        inbound
            .iter()
            .find(|t| t["files"][0]["name"] == name)
            .map(|t| t["total_bytes"].as_u64().unwrap_or(0))
            .unwrap_or(0)
    };
    assert_eq!(total_for("large.bin"), a_size, "transfers: {inbound:?}");
    assert_eq!(total_for("small.bin"), b_size, "transfers: {inbound:?}");

    daemon.shutdown();
    Ok(())
}
