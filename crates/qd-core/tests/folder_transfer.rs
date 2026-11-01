#![allow(clippy::await_holding_lock)] // the env/port lock must live for the whole test
//! Folder transfers: nested paths survive the roundtrip, traversal is contained.

use anyhow::Result;
use qd_core::client::{self, SendOpts};
use qd_core::proto::{self, FileHeader, FileMeta, FileTrailer, Frame, PROTO_VERSION};
use qd_core::server::{serve, start, Policy, ServerOpts};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::time::timeout;

/// `QD_CONFIG_DIR` is process-global, so these tests must not overlap.
static ENV_LOCK: Mutex<()> = Mutex::new(());
static NEXT_PORT: AtomicU16 = AtomicU16::new(45_000);

fn free_ports() -> (u16, u16) {
    let base = NEXT_PORT.fetch_add(3, Ordering::SeqCst);
    (base, base + 1)
}

struct Harness {
    _env: MutexGuard<'static, ()>,
    root: tempfile::TempDir,
    addr: SocketAddr,
    out: PathBuf,
}

impl Harness {
    async fn new(policy: Policy) -> Result<Self> {
        let _env = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let root = tempfile::tempdir()?;
        std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg"));

        let out = root.path().join("received");
        let (quic_port, discovery_port) = free_ports();
        let mut opts = ServerOpts::new(quic_port, discovery_port, out.clone());
        opts.policy = policy;
        opts.quiet = true;

        let server = start(opts.clone()).await?;
        tokio::spawn(serve(server, opts));

        Ok(Self {
            _env,
            root,
            addr: SocketAddr::from(([127, 0, 0, 1], quic_port)),
            out,
        })
    }

    async fn send_paths(&self, paths: Vec<PathBuf>) -> Result<client::Summary> {
        let opts = SendOpts {
            addr: self.addr,
            files: paths,
            concurrency: 2,
            ..SendOpts::default()
        };
        timeout(Duration::from_secs(20), client::send(opts)).await?
    }
}

#[tokio::test]
async fn folder_tree_roundtrip() -> Result<()> {
    let h = Harness::new(Policy::Auto).await?;
    let tree = h.root.path().join("src/photos");
    std::fs::create_dir_all(tree.join("2024/raw"))?;
    std::fs::write(tree.join("a.jpg"), b"alpha")?;
    std::fs::write(tree.join("2024/b.jpg"), b"bravo")?;
    std::fs::write(tree.join("2024/raw/c.jpg"), b"charlie")?;

    let summary = h.send_paths(vec![tree]).await?;
    assert_eq!(summary.ok, 3, "{summary:?}");

    let base = h.out.join("photos");
    assert_eq!(std::fs::read(base.join("a.jpg"))?, b"alpha");
    assert_eq!(std::fs::read(base.join("2024/b.jpg"))?, b"bravo");
    assert_eq!(std::fs::read(base.join("2024/raw/c.jpg"))?, b"charlie");
    Ok(())
}

#[tokio::test]
async fn sender_gets_one_verdict_per_file() -> Result<()> {
    let h = Harness::new(Policy::Auto).await?;
    let tree = h.root.path().join("src/mixed");
    std::fs::create_dir_all(&tree)?;
    std::fs::write(tree.join("a.txt"), b"alpha")?;
    std::fs::write(tree.join("b.txt"), b"bravo")?;
    // More than one stream chunk, so this file takes the per-chunk path while the
    // two small files share a batch stream.
    std::fs::write(tree.join("big.bin"), vec![7u8; 9 * 1024 * 1024])?;

    let seen: Arc<Mutex<Vec<(u32, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let opts = SendOpts {
        addr: h.addr,
        files: vec![tree],
        concurrency: 2,
        file_verdict: Some(Arc::new(move |id, ok| {
            sink.lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((id, ok));
        })),
        ..SendOpts::default()
    };
    let summary = timeout(Duration::from_secs(20), client::send(opts)).await??;
    assert_eq!(summary.ok, 3, "{summary:?}");

    let mut seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
    seen.sort_unstable();
    assert_eq!(seen.len(), 3, "exactly one verdict per file: {seen:?}");
    assert_eq!(
        seen.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(seen.iter().all(|(_, ok)| *ok), "{seen:?}");
    Ok(())
}

#[tokio::test]
async fn crafted_traversal_names_stay_inside_out_dir() -> Result<()> {
    let h = Harness::new(Policy::Auto).await?;

    // Bypass the normal client so the receiver sees hostile names verbatim.
    let cfg = qd_core::net::client_config(None)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(cfg);
    let conn = endpoint.connect(h.addr, "quicdrop.local")?.await?;
    let (mut ctrl_send, mut ctrl_recv) = conn.open_bi().await?;
    proto::write_frame(
        &mut ctrl_send,
        &Frame::Hello {
            version: PROTO_VERSION,
            device_id: "evil".into(),
            name: "evil".into(),
            platform: "test".into(),
        },
    )
    .await?;
    let _: Frame = proto::read_msg::<Frame>(&mut ctrl_recv).await?;

    let session = [7u8; 16];
    let files = vec![
        FileMeta {
            id: 0,
            name: "../../evil.txt".into(),
            size: 3,
            hash: None,
        },
        FileMeta {
            id: 1,
            name: "nested/../../../../etc/x.txt".into(),
            size: 3,
            hash: None,
        },
    ];
    proto::write_frame(&mut ctrl_send, &Frame::Offer { session, files }).await?;
    match proto::read_msg::<Frame>(&mut ctrl_recv).await? {
        Frame::Accept { files, .. } => assert_eq!(files.len(), 2),
        other => panic!("expected accept, got {other:?}"),
    }

    for (id, payload) in [(0u32, b"one"), (1u32, b"two")] {
        let (mut send, mut recv) = conn.open_bi().await?;
        proto::write_msg(
            &mut send,
            &FileHeader {
                session,
                id,
                offset: 0,
                size: payload.len() as u64,
                index: 0,
                count: 1,
            },
        )
        .await?;
        send.write_all(payload).await?;
        let hash = blake3::hash(payload).to_hex().to_string();
        proto::write_msg(&mut send, &FileTrailer { id, hash }).await?;
        send.finish()?;
        match proto::read_msg::<proto::FileAck>(&mut recv).await? {
            proto::FileAck::Verdict { ok: true, .. } => {}
            other => panic!("expected a passing verdict, got {other:?}"),
        }
    }

    let _ = proto::read_msg::<Frame>(&mut ctrl_recv).await?;
    let _ = proto::write_frame(&mut ctrl_send, &Frame::Bye).await;
    let _ = ctrl_send.finish();
    conn.close(0u32.into(), b"done");

    assert_eq!(std::fs::read(h.out.join("evil.txt"))?, b"one");
    assert_eq!(std::fs::read(h.out.join("nested/etc/x.txt"))?, b"two");

    // Nothing may appear beside (or above) the receive directory.
    assert!(!h.root.path().join("evil.txt").exists());
    assert!(!h.root.path().join("etc").exists());
    assert!(!h.root.path().join("nested").exists());
    Ok(())
}

#[tokio::test]
async fn resume_works_for_nested_paths() -> Result<()> {
    let h = Harness::new(Policy::Auto).await?;
    let tree = h.root.path().join("src/docs");
    std::fs::create_dir_all(tree.join("sub"))?;
    let body = vec![42u8; 512 * 1024];
    std::fs::write(tree.join("sub/big.bin"), &body)?;

    // Pre-seed a truncated partial where the receiver will look for it, including the
    // sidecar marker a previous interrupted transfer would have left.
    let partial = h.out.join("docs/sub/big.bin");
    std::fs::create_dir_all(partial.parent().unwrap())?;
    std::fs::write(&partial, &body[..200 * 1024])?;
    std::fs::write(partial.with_extension("bin.qdp"), b"")?;

    let summary = h.send_paths(vec![tree]).await?;
    assert_eq!(summary.ok, 1, "{summary:?}");
    assert_eq!(summary.bytes, (body.len() - 200 * 1024) as u64);
    assert_eq!(std::fs::read(&partial)?, body);
    // No duplicate "big (1).bin" was created next to it.
    let count = std::fs::read_dir(partial.parent().unwrap())?.count();
    assert_eq!(count, 1);
    Ok(())
}

#[tokio::test]
async fn send_captures_peer_identity_and_detects_mismatch() -> Result<()> {
    let h = Harness::new(Policy::Auto).await?;
    let file = h.root.path().join("test.txt");
    std::fs::write(&file, b"hello identity")?;

    // 1. Normal send without expected device_id captures peer
    let summary = h.send_paths(vec![file.clone()]).await?;
    assert!(
        summary.peer.is_some(),
        "captured peer info must be populated"
    );
    let peer = summary.peer.unwrap();
    assert!(!peer.device_id.is_empty());
    assert!(!peer.name.is_empty());
    assert!(!peer.platform.is_empty());

    // 2. Send with correct pinned device_id succeeds
    let opts = SendOpts {
        addr: h.addr,
        device_id: Some(peer.device_id.clone()),
        files: vec![file.clone()],
        ..SendOpts::default()
    };
    let summary2 = timeout(Duration::from_secs(20), client::send(opts)).await??;
    assert_eq!(summary2.ok, 1);
    assert_eq!(summary2.peer.unwrap().device_id, peer.device_id);

    // 3. Send with mismatched device_id fails with distinct mismatch error
    let bad_opts = SendOpts {
        addr: h.addr,
        device_id: Some("ffffffffffffffff".to_string()),
        files: vec![file],
        ..SendOpts::default()
    };
    let err = match timeout(Duration::from_secs(20), client::send(bad_opts)).await? {
        Ok(_) => panic!("should fail with mismatch"),
        Err(e) => format!("{e:#}"),
    };
    assert!(
        err.contains("device id mismatch"),
        "error must contain 'device id mismatch', got: {err}"
    );
    assert!(
        err.contains("expected ffffffffffffffff"),
        "error must mention expected pin, got: {err}"
    );
    assert!(
        err.contains(&peer.device_id),
        "error must mention actual fingerprint, got: {err}"
    );

    Ok(())
}
