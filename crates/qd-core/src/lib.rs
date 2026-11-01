//! quicdrop core: QUIC peer-to-peer file transfer for the local network.
//!
//! [`identity`] and [`net`] cover who a peer is, [`discovery`] how to find one,
//! [`proto`] the wire format, and [`client`]/[`server`] the transfer itself.

pub mod approval;
pub mod cancel;
pub mod client;
pub mod discovery;
pub mod hash;
pub mod identity;
pub mod net;
pub mod progress;
pub mod proto;
pub mod server;
pub mod source;
pub mod trust;
pub mod util;

pub use approval::{ApprovalRequest, ApprovalSender, Decision};
pub use cancel::Cancel;
pub use discovery::Peer;
pub use identity::Identity;
pub use progress::{FileSink, FileVerdictSink};
pub use proto::{
    FileMeta, Frame, ALPN, CHUNK_SIZE, DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT, PROTO_VERSION,
};
pub use quinn;
pub use server::{Policy, ServerEvent, ServerEventSink};
pub use source::{expand as expand_sources, SourceFile};
pub use trust::TrustStore;
