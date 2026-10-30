//! End-to-end approval flow: accept, reject, and "always" (trust store).

use anyhow::Result;
use qd_core::approval::{self, Decision};
use qd_core::client::{self, SendOpts};
use qd_core::server::{serve, start, Policy, ServerOpts};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

fn free_ports() -> (u16, u16) {
    // Derive from the pid so parallel or leftover runs do not collide.
    let base = 40_000 + (std::process::id() % 1000) as u16 * 3;
    (base, base + 1)
}

/// `QD_CONFIG_DIR` is process-global, so tests that change it must not overlap.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

struct Harness {
    root: tempfile::TempDir,
    addr: SocketAddr,
    out: PathBuf,
    decisions: mpsc::UnboundedSender<Decision>,
    _serve: tokio::task::JoinHandle<Result<()>>,
}

impl Harness {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg"));

        let out = root.path().join("received");
        let (quic_port, discovery_port) = free_ports();
        let mut opts = ServerOpts::new(quic_port, discovery_port, out.clone());
        opts.policy = Policy::Ask;
        opts.quiet = true;

        let (req_tx, mut req_rx) = approval::channel();
        opts.approver = Some(req_tx);

        let (dec_tx, mut dec_rx) = mpsc::unbounded_channel::<Decision>();
        tokio::spawn(async move {
            while let Some(request) = req_rx.recv().await {
                let decision = dec_rx
                    .recv()
                    .await
                    .unwrap_or_else(|| Decision::reject("test"));
                let _ = request.reply.send(decision);
            }
        });

        let server = start(opts.clone()).await?;
        let serve_task = tokio::spawn(serve(server, opts));

        Ok(Self {
            root,
            addr: SocketAddr::from(([127, 0, 0, 1], quic_port)),
            out,
            decisions: dec_tx,
            _serve: serve_task,
        })
    }

    fn source(&self, name: &str, contents: &[u8]) -> PathBuf {
        let dir = self.root.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    async fn send(&self, path: &Path) -> Result<client::Summary> {
        let opts = SendOpts {
            addr: self.addr,
            device_id: None,
            files: vec![path.to_path_buf()],
            concurrency: 2,
            quiet: true,
            name: None,
            progress: None,
        };
        timeout(Duration::from_secs(20), client::send(opts)).await?
    }

    fn received(&self, name: &str) -> Option<Vec<u8>> {
        std::fs::read(self.out.join(name)).ok()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)] // the lock serializes env mutation on purpose
async fn approval_accept_reject_and_remember() -> Result<()> {
    let _guard = env_guard();
    let harness = Harness::new().await?;

    // 1. Accept once: the file lands, the device is not remembered.
    let hello = harness.source("hello.txt", b"hello over quic\n");
    harness.decisions.send(Decision::accept())?;
    let summary = harness.send(&hello).await?;
    assert_eq!(summary.ok, 1, "expected one accepted file");
    assert_eq!(
        harness.received("hello.txt").as_deref(),
        Some(b"hello over quic\n".as_slice())
    );

    let trust = qd_core::TrustStore::default();
    assert!(
        trust.list().is_empty(),
        "accept-once must not persist trust"
    );

    // 2. Reject: the sender sees an error and nothing is written.
    let nope = harness.source("nope.bin", &[7u8; 4096]);
    harness.decisions.send(Decision::reject("not now"))?;
    let rejected = harness.send(&nope).await;
    assert!(
        rejected.is_err(),
        "sender should learn the transfer was rejected"
    );
    assert!(harness.received("nope.bin").is_none());

    // 3. Always: remembered, then a second transfer skips the prompt entirely.
    let again = harness.source("again.txt", b"remembered device\n");
    harness.decisions.send(Decision::accept_always())?;
    let summary = harness.send(&again).await?;
    assert_eq!(summary.ok, 1);
    assert_eq!(
        harness.received("again.txt").as_deref(),
        Some(b"remembered device\n".as_slice())
    );

    let trust = qd_core::TrustStore::default();
    let trusted = trust.list();
    assert_eq!(trusted.len(), 1, "one device should now be trusted");

    let silent = harness.source("silent.txt", b"no prompt this time\n");
    let summary = timeout(Duration::from_secs(10), harness.send(&silent)).await??;
    assert_eq!(summary.ok, 1, "trusted device must not need a prompt");
    assert_eq!(
        harness.received("silent.txt").as_deref(),
        Some(b"no prompt this time\n".as_slice())
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::await_holding_lock)] // the lock serializes env mutation on purpose
async fn deny_policy_rejects_everything() -> Result<()> {
    let _guard = env_guard();
    let root = tempfile::tempdir()?;
    std::env::set_var("QD_CONFIG_DIR", root.path().join("cfg-deny"));

    let out = root.path().join("received");
    let (quic_port, discovery_port) = free_ports();
    let mut opts = ServerOpts::new(quic_port, discovery_port, out);
    opts.policy = Policy::Deny;
    opts.quiet = true;
    let server = start(opts.clone()).await?;
    let serve_task = tokio::spawn(serve(server, opts));

    let src = root.path().join("x.txt");
    std::fs::write(&src, b"x").unwrap();
    let result = timeout(
        Duration::from_secs(20),
        client::send(SendOpts {
            addr: SocketAddr::from(([127, 0, 0, 1], quic_port)),
            device_id: None,
            files: vec![src],
            concurrency: 1,
            quiet: true,
            name: None,
            progress: None,
        }),
    )
    .await?;
    assert!(result.is_err(), "policy deny must reject the offer");
    assert!(!root.path().join("received").join("x.txt").exists());
    serve_task.abort();
    Ok(())
}
