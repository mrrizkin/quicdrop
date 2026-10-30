//! Cancellation handle shared by a host and an in-flight transfer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Set by a host to abort a transfer. Dropping a connection keeps whatever the
/// receiver already wrote, so a later send of the same files resumes.
#[derive(Clone, Default)]
pub struct Cancel {
    flag: Arc<AtomicBool>,
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }
}
