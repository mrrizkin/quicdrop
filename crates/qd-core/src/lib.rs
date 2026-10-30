//! quicdrop core: QUIC-based peer-to-peer file transfer for the local network.
//!
//! Layout:
//! - [`identity`]  : long-lived device identity (self-signed cert + fingerprint)
//! - [`proto`]     : wire protocol (frames, framing helpers, file stream framing)
//! - [`discovery`] : mDNS + UDP broadcast peer discovery
//! - [`server`]    : receiver side
//! - [`client`]    : sender side

pub mod client;
pub mod discovery;
pub mod identity;
pub mod net;
pub mod progress;
pub mod proto;
pub mod server;
pub mod util;

pub use discovery::Peer;
pub use identity::Identity;
pub use proto::{
    FileMeta, Frame, ALPN, CHUNK_SIZE, DEFAULT_DISCOVERY_PORT, DEFAULT_QUIC_PORT, PROTO_VERSION,
};
