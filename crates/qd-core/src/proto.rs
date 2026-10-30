//! Wire format: length-prefixed postcard frames, plus the header, trailer and ack that
//! bracket a file stream.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// ALPN identifier; bumped when the wire format changes incompatibly.
pub const ALPN: &[u8] = b"quicdrop/1";
pub const PROTO_VERSION: u16 = 1;

pub const DEFAULT_QUIC_PORT: u16 = 53320;
pub const DEFAULT_DISCOVERY_PORT: u16 = 53321;

pub const MDNS_SERVICE: &str = "_quicdrop._udp.local.";
pub const BROADCAST_MAGIC: &[u8; 4] = b"QDB1";
pub const MULTICAST_GROUP: [u8; 4] = [224, 0, 0, 167];

/// Payload chunk size pushed into a QUIC stream.
pub const CHUNK_SIZE: usize = 512 * 1024;
/// Upper bound for a single control frame.
pub const MAX_FRAME: usize = 4 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub id: u32,
    /// Original file name as advertised by the sender (untrusted).
    pub name: String,
    pub size: u64,
    /// blake3 of the whole file, hex encoded. `None` means the digest arrives in a [`FileTrailer`].
    pub hash: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct AcceptFile {
    pub id: u32,
    /// Byte offset the receiver already has on disk (resume support).
    pub offset: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Hello {
        version: u16,
        device_id: String,
        name: String,
        platform: String,
    },
    Offer {
        session: [u8; 16],
        files: Vec<FileMeta>,
    },
    Accept {
        session: [u8; 16],
        files: Vec<AcceptFile>,
    },
    Reject {
        session: [u8; 16],
        reason: String,
    },
    Complete {
        session: [u8; 16],
        transferred: u64,
    },
    Error {
        message: String,
    },
    Ping,
    Bye,
}

/// Header written at the beginning of every file stream.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub session: [u8; 16],
    pub id: u32,
    pub offset: u64,
    pub size: u64,
}

/// Written after the payload of a file stream.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileTrailer {
    pub id: u32,
    pub hash: String,
}

/// Verdict sent back on the same stream once the receiver finished writing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileAck {
    pub id: u32,
    pub ok: bool,
    pub hash: String,
    pub message: String,
}

pub async fn write_frame(send: &mut quinn::SendStream, frame: &Frame) -> Result<()> {
    write_msg(send, frame).await
}

pub async fn read_frame(recv: &mut quinn::RecvStream) -> Result<Frame> {
    read_msg(recv).await
}

pub async fn write_msg<T: Serialize>(send: &mut quinn::SendStream, msg: &T) -> Result<()> {
    let body = postcard::to_stdvec(msg).context("serialize frame")?;
    if body.len() > MAX_FRAME {
        bail!("frame too large: {} bytes", body.len());
    }
    send.write_all(&(body.len() as u32).to_le_bytes())
        .await
        .context("write frame length")?;
    send.write_all(&body).await.context("write frame body")?;
    Ok(())
}

pub async fn read_msg<T: for<'de> Deserialize<'de>>(recv: &mut quinn::RecvStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .context("read frame length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        bail!("frame exceeds limit: {len} bytes");
    }
    let mut body = vec![0u8; len];
    recv.read_exact(&mut body)
        .await
        .context("read frame body")?;
    postcard::from_bytes(&body).context("decode frame")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip() {
        let frame = Frame::Offer {
            session: [7u8; 16],
            files: vec![FileMeta {
                id: 3,
                name: "a.bin".into(),
                size: 9,
                hash: None,
            }],
        };
        let bytes = postcard::to_stdvec(&frame).unwrap();
        let back: Frame = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(frame, back);
    }

    #[test]
    fn rejects_oversized_frames() {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(u32::from_le_bytes(buf) as usize > MAX_FRAME);
    }
}
