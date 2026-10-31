//! Whole-file hashing: one blocking task per file, reading straight from disk.

use anyhow::{Context, Result};
use std::io::Read;
use std::path::Path;

/// Upper bound for each read; small files use their own size instead.
const HASH_BUFFER: usize = 8 * 1024 * 1024;

/// Hash a file on the blocking pool.
pub async fn hash_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || hash_file_sync(&path))
        .await
        .context("hash task")?
}

/// Hash a file on the calling thread.
pub fn hash_file_sync(path: &Path) -> Result<String> {
    Ok(hash_file_with_size_sync(path)?.1)
}

/// Hash a file and return its size in the same pass.
pub async fn hash_file_with_size(path: &Path) -> Result<(u64, String)> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || hash_file_with_size_sync(&path))
        .await
        .context("hash task")?
}

fn hash_file_with_size_sync(path: &Path) -> Result<(u64, String)> {
    let mut file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    // Size the buffer to the file so a small file does not pay for a full one. A short
    // read just means another pass.
    let hint = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let mut buf = vec![0u8; (hint as usize).clamp(1, HASH_BUFFER)];
    let mut size = 0u64;
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, hasher.finalize().to_hex().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_a_file_across_buffer_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin");
        // Larger than HASH_BUFFER so the loop runs more than once.
        let data = vec![7u8; HASH_BUFFER + 4096];
        std::fs::write(&path, &data).unwrap();

        let (size, hash) = hash_file_with_size_sync(&path).unwrap();
        assert_eq!(size, data.len() as u64);
        assert_eq!(hash, blake3::hash(&data).to_hex().to_string());
        assert_eq!(hash_file_sync(&path).unwrap(), hash);
    }
}
