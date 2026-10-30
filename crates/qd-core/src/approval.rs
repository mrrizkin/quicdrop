//! Receiver-side approval: the server hands an [`ApprovalRequest`] to whoever owns the
//! [`ApprovalSender`] and waits for a [`Decision`]; silence past [`APPROVAL_TIMEOUT`] rejects.

use std::io::{BufRead, IsTerminal, Write};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::debug;

use crate::proto::FileMeta;
use crate::util::human_bytes;

/// How long the server waits for a decision before giving up.
pub const APPROVAL_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Accept { remember: bool },
    Reject { reason: String },
}

impl Decision {
    pub fn accept() -> Self {
        Self::Accept { remember: false }
    }

    pub fn accept_always() -> Self {
        Self::Accept { remember: true }
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    pub fn is_accept(&self) -> bool {
        matches!(self, Self::Accept { .. })
    }

    pub fn should_remember(&self) -> bool {
        matches!(self, Self::Accept { remember: true })
    }
}

/// One pending transfer waiting for a human (or an automation policy).
#[derive(Debug)]
pub struct ApprovalRequest {
    pub peer_id: String,
    pub peer_name: String,
    pub peer_platform: String,
    pub files: Vec<FileMeta>,
    pub total_bytes: u64,
    pub reply: oneshot::Sender<Decision>,
}

impl ApprovalRequest {
    /// Human readable multi-line summary used by the TTY prompt and the GUI.
    pub fn summary(&self) -> String {
        let mut out = format!(
            "\"{}\" ({}{}) wants to send {} file(s), {}",
            self.peer_name,
            &self.peer_id[..self.peer_id.len().min(8)],
            if self.peer_platform.is_empty() {
                String::new()
            } else {
                format!(", {}", self.peer_platform)
            },
            self.files.len(),
            human_bytes(self.total_bytes),
        );
        for file in self.files.iter().take(20) {
            out.push_str(&format!("\n    {} ({})", file.name, human_bytes(file.size)));
        }
        if self.files.len() > 20 {
            out.push_str(&format!("\n    ... and {} more", self.files.len() - 20));
        }
        out
    }
}

pub type ApprovalSender = mpsc::UnboundedSender<ApprovalRequest>;

pub fn channel() -> (ApprovalSender, mpsc::UnboundedReceiver<ApprovalRequest>) {
    mpsc::unbounded_channel()
}

/// Spawn the stdin prompt. Without a TTY every request is rejected, so headless receivers
/// fail closed.
pub fn spawn_tty_approver() -> ApprovalSender {
    let (tx, rx) = channel();
    let interactive = std::io::stdin().is_terminal();
    tokio::spawn(run_tty_approver(rx, interactive));
    tx
}

async fn run_tty_approver(mut rx: mpsc::UnboundedReceiver<ApprovalRequest>, interactive: bool) {
    // Prompts are serialized: two transfers arriving together queue up.
    let gate = Arc::new(Mutex::new(()));
    while let Some(req) = rx.recv().await {
        let _guard = gate.lock().await;
        let decision = if interactive {
            ask_on_tty(&req).await
        } else {
            Decision::reject("receiver has no interactive terminal (start it with --policy auto or approve this device)")
        };
        if req.reply.send(decision).is_err() {
            debug!("approval for {} expired before we answered", req.peer_name);
        }
    }
}

async fn ask_on_tty(req: &ApprovalRequest) -> Decision {
    {
        let mut stderr = std::io::stderr();
        let _ = writeln!(stderr, "\nIncoming transfer: {}", req.summary());
        let _ = write!(stderr, "Accept? [y]es / [n]o / [a]lways: ");
        let _ = stderr.flush();
    }

    for attempt in 0..3 {
        let read = tokio::task::spawn_blocking(|| {
            let mut line = String::new();
            std::io::stdin().lock().read_line(&mut line).map(|_| line)
        })
        .await;

        let line = match read {
            Ok(Ok(line)) => line,
            Ok(Err(e)) => return Decision::reject(format!("cannot read answer: {e}")),
            Err(e) => return Decision::reject(format!("prompt cancelled: {e}")),
        };
        let answer = line.trim().to_ascii_lowercase();
        match answer.as_str() {
            "y" | "yes" | "" => return Decision::accept(),
            "a" | "always" => return Decision::accept_always(),
            "n" | "no" | "q" | "quit" => {
                return Decision::reject(format!(
                    "\"{}\" was declined by the receiver",
                    req.peer_name
                ))
            }
            _ if attempt == 2 => break,
            _ => {
                let mut stderr = std::io::stderr();
                let _ = write!(stderr, "Please answer y, n or a: ");
                let _ = stderr.flush();
            }
        }
    }
    Decision::reject("receiver did not give a valid answer")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(name: &str, size: u64) -> FileMeta {
        FileMeta {
            id: 1,
            name: name.to_string(),
            size,
            hash: None,
        }
    }

    #[test]
    fn summary_mentions_peer_and_files() {
        let (tx, _rx) = oneshot::channel();
        let req = ApprovalRequest {
            peer_id: "deadbeefdeadbeef".into(),
            peer_name: "laptop".into(),
            peer_platform: "linux".into(),
            files: vec![meta("a.txt", 10), meta("b.bin", 2048)],
            total_bytes: 2058,
            reply: tx,
        };
        let text = req.summary();
        assert!(text.contains("laptop"));
        assert!(text.contains("deadbeef"));
        assert!(text.contains("a.txt"));
        assert!(text.contains("2 file(s)"));
    }

    #[test]
    fn decision_helpers() {
        assert!(Decision::accept().is_accept());
        assert!(!Decision::accept().should_remember());
        assert!(Decision::accept_always().should_remember());
        assert!(!Decision::reject("no").is_accept());
    }
}
