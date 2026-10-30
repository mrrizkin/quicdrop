//! Trust store: `trusted.json` keyed by device id. No secrets, no expiry, easy to delete.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::identity::Identity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrustedPeer {
    pub device_id: String,
    pub name: String,
    #[serde(default)]
    pub platform: String,
    /// Unix seconds when the device was first approved.
    #[serde(default)]
    pub first_seen: u64,
    /// Unix seconds of the last accepted transfer.
    #[serde(default)]
    pub last_seen: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StoreFile {
    #[serde(default)]
    peers: BTreeMap<String, TrustedPeer>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// How stale `last_seen` may get on disk before a transfer rewrites the store.
pub const TOUCH_SAVE_INTERVAL: Duration = Duration::from_secs(60);

/// Cheap-to-clone handle around the on-disk trust list.
#[derive(Debug, Clone)]
pub struct TrustStore {
    path: PathBuf,
    peers: Arc<Mutex<BTreeMap<String, TrustedPeer>>>,
}

impl Default for TrustStore {
    fn default() -> Self {
        Self::at(Identity::config_dir().join("trusted.json"))
    }
}

impl TrustStore {
    /// Open (or create in memory) the store at `path`.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let peers = load_file(&path).unwrap_or_default();
        Self {
            path,
            peers: Arc::new(Mutex::new(peers)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of trusted devices.
    pub fn len(&self) -> usize {
        self.peers.lock().map(|p| p.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_trusted(&self, device_id: &str) -> bool {
        self.peers
            .lock()
            .map(|p| p.contains_key(device_id))
            .unwrap_or(false)
    }

    pub fn get(&self, device_id: &str) -> Option<TrustedPeer> {
        self.peers.lock().ok()?.get(device_id).cloned()
    }

    /// Approve a device, or refresh its name/`last_seen` if it is already trusted.
    pub fn remember(&self, device_id: &str, name: &str, platform: &str) -> Result<()> {
        let now = now_secs();
        {
            let mut peers = self.peers.lock().expect("trust store poisoned");
            let entry = peers
                .entry(device_id.to_string())
                .or_insert_with(|| TrustedPeer {
                    device_id: device_id.to_string(),
                    name: name.to_string(),
                    platform: platform.to_string(),
                    first_seen: now,
                    last_seen: now,
                });
            entry.name = name.to_string();
            if !platform.is_empty() {
                entry.platform = platform.to_string();
            }
            entry.last_seen = now;
        }
        self.save()
    }

    /// Record an accepted transfer from an already trusted device. The store is
    /// written at most once per [`TOUCH_SAVE_INTERVAL`].
    pub fn touch(&self, device_id: &str) {
        let now = now_secs();
        let stale = {
            let Ok(mut peers) = self.peers.lock() else {
                return;
            };
            match peers.get_mut(device_id) {
                Some(peer) => {
                    let stale = now.saturating_sub(peer.last_seen) >= TOUCH_SAVE_INTERVAL.as_secs();
                    peer.last_seen = now;
                    stale
                }
                None => false,
            }
        };
        if stale {
            let _ = self.save();
        }
    }

    /// [`Self::touch`] for callers on the async runtime: saving is blocking file IO.
    pub async fn touch_async(&self, device_id: &str) {
        let store = self.clone();
        let device_id = device_id.to_string();
        let _ = tokio::task::spawn_blocking(move || store.touch(&device_id)).await;
    }

    /// [`Self::remember`] for callers on the async runtime.
    pub async fn remember_async(&self, device_id: &str, name: &str, platform: &str) -> Result<()> {
        let store = self.clone();
        let device_id = device_id.to_string();
        let name = name.to_string();
        let platform = platform.to_string();
        tokio::task::spawn_blocking(move || store.remember(&device_id, &name, &platform))
            .await
            .context("trust store task")?
    }

    /// Remove an exact device id. Returns whether anything was removed.
    pub fn forget(&self, device_id: &str) -> Result<bool> {
        let removed = {
            let mut peers = self.peers.lock().expect("trust store poisoned");
            peers.remove(device_id).is_some()
        };
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    /// Remove by unique device-id prefix. Ambiguous prefixes are an error.
    pub fn forget_prefix(&self, prefix: &str) -> Result<Option<TrustedPeer>> {
        let target = {
            let peers = self.peers.lock().expect("trust store poisoned");
            let mut hits = peers
                .keys()
                .filter(|id| id.starts_with(prefix))
                .cloned()
                .collect::<Vec<_>>();
            match hits.len() {
                0 => None,
                1 => hits.pop(),
                _ => anyhow::bail!(
                    "device id prefix \"{prefix}\" is ambiguous (matches {})",
                    hits.join(", ")
                ),
            }
        };
        match target {
            Some(id) => {
                let peer = self.get(&id);
                self.forget(&id)?;
                Ok(peer)
            }
            None => Ok(None),
        }
    }

    /// Snapshot of all trusted peers, newest first.
    pub fn list(&self) -> Vec<TrustedPeer> {
        let mut peers: Vec<_> = self
            .peers
            .lock()
            .map(|p| p.values().cloned().collect())
            .unwrap_or_default();
        peers.sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then(a.name.cmp(&b.name)));
        peers
    }

    fn save(&self) -> Result<()> {
        let snapshot = {
            let peers = self.peers.lock().expect("trust store poisoned");
            StoreFile {
                peers: peers.clone(),
            }
        };
        save_file(&self.path, &snapshot)
    }
}

fn load_file(path: &Path) -> Result<BTreeMap<String, TrustedPeer>> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let parsed: StoreFile = serde_json::from_slice(&bytes)
                .with_context(|| format!("parse trust store {}", path.display()))?;
            Ok(parsed.peers)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e).with_context(|| format!("read trust store {}", path.display())),
    }
}

fn save_file(path: &Path, store: &StoreFile) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config dir {}", parent.display()))?;
    }
    let json = serde_json::to_vec_pretty(store).context("serialize trust store")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, TrustStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = TrustStore::at(dir.path().join("trusted.json"));
        (dir, store)
    }

    #[test]
    fn remembers_and_reloads() {
        let (dir, store) = store();
        assert!(!store.is_trusted("abc"));
        store.remember("abc", "laptop", "linux").unwrap();
        assert!(store.is_trusted("abc"));

        let reopened = TrustStore::at(dir.path().join("trusted.json"));
        assert!(reopened.is_trusted("abc"));
        let peer = reopened.get("abc").unwrap();
        assert_eq!(peer.name, "laptop");
        assert_eq!(peer.platform, "linux");
        assert!(peer.first_seen > 0);
    }

    #[test]
    fn forget_by_prefix() {
        let (_dir, store) = store();
        store.remember("deadbeef0000", "phone", "android").unwrap();
        store.remember("cafebabe1111", "laptop", "linux").unwrap();

        let gone = store.forget_prefix("dead").unwrap().unwrap();
        assert_eq!(gone.name, "phone");
        assert!(!store.is_trusted("deadbeef0000"));
        assert!(store.forget_prefix("zzz").unwrap().is_none());
    }

    #[test]
    fn ambiguous_prefix_is_rejected() {
        let (_dir, store) = store();
        store.remember("aaaa1111", "a", "linux").unwrap();
        store.remember("aaaa2222", "b", "linux").unwrap();
        assert!(store.forget_prefix("aaaa").is_err());
        assert!(store.is_trusted("aaaa1111"));
    }

    #[test]
    fn repeated_touches_do_not_rewrite_the_file() {
        let (dir, store) = store();
        store.remember("abc", "laptop", "linux").unwrap();
        let path = dir.path().join("trusted.json");
        let before = std::fs::metadata(&path).unwrap().modified().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        for _ in 0..20 {
            store.touch("abc");
        }
        let after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(before, after);
        // The in-memory view still moved.
        assert!(store.get("abc").unwrap().last_seen > 0);
    }
}
