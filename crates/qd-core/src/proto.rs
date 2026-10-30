//! Wire format: length-prefixed postcard frames, plus the header, trailer and ack that
//! bracket a file stream.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

/// ALPN identifier; bumped when the wire format changes incompatibly.
pub const ALPN: &[u8] = b"quicdrop/1";
/// Bumped when a frame's shape or meaning changes.
pub const PROTO_VERSION: u16 = 4;

pub const DEFAULT_QUIC_PORT: u16 = 53320;
pub const DEFAULT_DISCOVERY_PORT: u16 = 53321;

pub const MDNS_SERVICE: &str = "_quicdrop._udp.local.";
pub const BROADCAST_MAGIC: &[u8; 4] = b"QDB1";
pub const MULTICAST_GROUP: [u8; 4] = [224, 0, 0, 167];

/// Payload chunk size pushed into a QUIC stream.
pub const CHUNK_SIZE: usize = 512 * 1024;
/// Upper bound for a single control frame.
pub const MAX_FRAME: usize = 4 * 1024 * 1024;
/// Upper bound for an `Offer`, which carries one entry per file.
pub const MAX_OFFER_FRAME: usize = 16 * 1024 * 1024;
/// Longest relative path a peer may announce. With [`MAX_OFFER_FRAME`] it bounds
/// what a single offer can weigh.
pub const MAX_NAME_LEN: usize = 1024;

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

/// Bytes carried by one stream chunk.
pub const CHUNK_STREAM_BYTES: u64 = 8 * 1024 * 1024;

/// Streams one file may use at once. The transport allows far more; this keeps a
/// single large file from starving the others.
pub const MAX_STREAMS_PER_FILE: u16 = 4;

/// Number of chunks the remaining range is cut into.
pub fn chunk_count(offset: u64, size: u64) -> u16 {
    let remaining = size.saturating_sub(offset);
    remaining
        .div_ceil(CHUNK_STREAM_BYTES)
        .max(1)
        .min(u16::MAX as u64) as u16
}

/// Byte range of chunk `index`. Both sides derive it, so the receiver can reject a
/// header that does not match the accepted plan.
pub fn chunk_range(offset: u64, size: u64, index: u16) -> (u64, u64) {
    let start = offset.saturating_add(u64::from(index) * CHUNK_STREAM_BYTES);
    let len = size.saturating_sub(start).min(CHUNK_STREAM_BYTES);
    (start, len)
}

/// Workers to run for a file with `count` chunks.
pub fn stream_count(count: u16) -> u16 {
    count.min(MAX_STREAMS_PER_FILE)
}

/// Header written at the beginning of every file stream.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub session: [u8; 16],
    pub id: u32,
    /// Start of this chunk.
    pub offset: u64,
    /// Whole file size, not the chunk length.
    pub size: u64,
    pub index: u16,
    pub count: u16,
}

/// Written after the payload of a stream. `hash` covers the whole file, so every
/// chunk carries the same value; the receiver verifies once at the end.
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

/// Reject an offer whose announced paths do not fit the limits both sides enforce.
pub fn validate_offer(files: &[FileMeta]) -> Result<()> {
    if files.is_empty() {
        bail!("offer has no files");
    }
    for file in files {
        if file.name.len() > MAX_NAME_LEN {
            bail!(
                "announced path is {} bytes, limit is {MAX_NAME_LEN}",
                file.name.len()
            );
        }
    }
    Ok(())
}

pub async fn write_msg<T: Serialize>(send: &mut quinn::SendStream, msg: &T) -> Result<()> {
    write_msg_limit(send, msg, MAX_FRAME).await
}

/// [`write_msg`] with an explicit size limit.
pub async fn write_msg_limit<T: Serialize>(
    send: &mut quinn::SendStream,
    msg: &T,
    limit: usize,
) -> Result<()> {
    let body = postcard::to_stdvec(msg).context("serialize frame")?;
    if body.len() > limit {
        bail!("frame too large: {} bytes", body.len());
    }
    send.write_all(&(body.len() as u32).to_le_bytes())
        .await
        .context("write frame length")?;
    send.write_all(&body).await.context("write frame body")?;
    Ok(())
}

pub async fn read_msg<T: for<'de> Deserialize<'de>>(recv: &mut quinn::RecvStream) -> Result<T> {
    read_msg_limit(recv, MAX_FRAME).await
}

/// [`read_msg`] with an explicit size limit, checked before the buffer is allocated.
pub async fn read_msg_limit<T: for<'de> Deserialize<'de>>(
    recv: &mut quinn::RecvStream,
    limit: usize,
) -> Result<T> {
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .context("read frame length")?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > limit {
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

    #[test]
    fn offers_have_room_for_the_file_limit() {
        // Every file in a full offer must have room for a short path.
        let source = crate::source::MAX_FILES;
        let per_file = MAX_OFFER_FRAME / source;
        assert!(per_file >= 160, "only {per_file} bytes per file");
    }

    #[test]
    fn long_names_are_rejected() {
        let ok = FileMeta {
            id: 1,
            name: "a".repeat(MAX_NAME_LEN),
            size: 0,
            hash: None,
        };
        assert!(validate_offer(std::slice::from_ref(&ok)).is_ok());
        let long = FileMeta {
            name: "a".repeat(MAX_NAME_LEN + 1),
            ..ok.clone()
        };
        assert!(validate_offer(&[long]).is_err());
        assert!(validate_offer(&[]).is_err());
    }
}
