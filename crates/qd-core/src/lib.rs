//! quicdrop core: QUIC-based peer-to-peer file transfer for the local network.
//!
//! Layout:
//! - [`identity`]  : long-lived device identity (self-signed cert + fingerprint)
//! - [`proto`]     : wire protocol (frames, framing helpers, file stream framing)
//! - [`discovery`] : mDNS + UDP broadcast peer discovery
//! - [`server`]    : receiver side
//! - [`client`]    : sender side
//! - [`trust`]     : persistent list of devices allowed to send without asking
//! - [`approval`]  : receiver-side accept/decline flow

pub mod approval;
pub mod client;
pub mod discovery;
pub mod identity;
pub mod net;
pub mod progress;
pub mod proto;
pub mod server;
pub mod trust;
pub mod util;

pub use approval::{ApprovalRequest, ApprovalSender, Decision};
pub use discovery::Peer;
pub use identity::Identity;
pub use server::{Policy, ServerEvent, ServerEventSink};
pub use trust::TrustStore;
pub use proto::{
    FileMeta, Frame, ALPN, CHUNK_SIZE, DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT, PROTO_VERSION,
};
