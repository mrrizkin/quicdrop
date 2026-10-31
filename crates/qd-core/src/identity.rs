use anyhow::{bail, Context, Result};
use rcgen::{date_time_ymd, CertificateParams, DnType, KeyPair};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

/// Longest display name accepted from a user or host.
pub const MAX_NAME_LEN: usize = 48;

/// Shared by every clone of an [`Identity`]; the device id does not follow the name.
#[derive(Debug, Clone)]
struct NameCell {
    value: Arc<RwLock<String>>,
    changed: watch::Sender<u64>,
}

impl NameCell {
    fn new(name: String) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            value: Arc::new(RwLock::new(name)),
            changed,
        }
    }

    fn get(&self) -> String {
        self.value
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn set(&self, name: String) {
        *self
            .value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = name;
        self.changed.send_modify(|generation| *generation += 1);
    }
}

/// Long-lived identity. The device id is the certificate fingerprint, so peers can pin it.
#[derive(Debug, Clone)]
pub struct Identity {
    pub device_id: String,
    name: NameCell,
    pub cert_der: Vec<u8>,
    pub key_der: Vec<u8>,
    pub dir: PathBuf,
}

impl Identity {
    pub fn config_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("QD_CONFIG_DIR") {
            return PathBuf::from(dir);
        }
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("quicdrop")
    }

    /// Derive a short, stable, human shareable device id from a certificate.
    pub fn fingerprint(cert_der: &[u8]) -> String {
        let hash = blake3::hash(cert_der);
        hex::encode(&hash.as_bytes()[..16])
    }

    pub fn load_or_create(name: Option<String>) -> Result<Self> {
        Self::load_or_create_in(Self::config_dir(), name)
    }

    pub fn load_or_create_in(dir: impl Into<PathBuf>, name: Option<String>) -> Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).with_context(|| format!("create config dir {}", dir.display()))?;

        let key_path = dir.join("identity.key");
        let cert_path = dir.join("identity.crt");
        let name_path = dir.join("name.txt");

        let display_name = match name {
            Some(name) => Self::validate_name(&name)?,
            None => fs::read_to_string(&name_path)
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(random_name),
        };

        let key = match fs::read_to_string(&key_path) {
            Ok(pem) => KeyPair::from_pem(&pem).context("load identity key")?,
            Err(_) => {
                let key = KeyPair::generate().context("generate identity key")?;
                fs::write(&key_path, key.serialize_pem()).context("persist identity key")?;
                restrict_permissions(&key_path);
                key
            }
        };

        let cert_der = match fs::read(&cert_path) {
            Ok(der) if !der.is_empty() => der,
            _ => {
                let der = build_cert(&display_name, &key)?;
                fs::write(&cert_path, &der).context("persist identity cert")?;
                der
            }
        };

        if fs::read_to_string(&name_path)
            .ok()
            .as_deref()
            .map(str::trim)
            != Some(display_name.as_str())
        {
            let _ = fs::write(&name_path, &display_name);
        }

        let key_der = key.serialize_der();
        Ok(Self {
            device_id: Self::fingerprint(&cert_der),
            name: NameCell::new(display_name),
            cert_der,
            key_der,
            dir,
        })
    }

    /// Current display name.
    pub fn name(&self) -> String {
        self.name.get()
    }

    /// Rename notifications; the payload is a generation counter.
    pub fn subscribe_name(&self) -> watch::Receiver<u64> {
        self.name.changed.subscribe()
    }

    /// Persist a new name and notify holders. The certificate, and therefore the device id,
    /// stays untouched.
    pub fn set_name(&self, name: &str) -> Result<()> {
        let name = Self::validate_name(name)?;
        let name_path = self.dir.join("name.txt");
        fs::write(&name_path, &name)
            .with_context(|| format!("persist name to {}", name_path.display()))?;
        self.name.set(name);
        Ok(())
    }

    /// Trim and bound a display name. Control characters are rejected: names appear in
    /// terminals, desktop rows and mDNS labels.
    pub fn validate_name(raw: &str) -> Result<String> {
        let name = raw.trim();
        if name.is_empty() {
            bail!("device name cannot be empty");
        }
        let len = name.chars().count();
        if len > MAX_NAME_LEN {
            bail!("device name is {len} characters; the limit is {MAX_NAME_LEN}");
        }
        if name.chars().any(char::is_control) {
            bail!("device name cannot contain control characters");
        }
        Ok(name.to_string())
    }

    pub fn cert_chain(&self) -> Vec<quinn::rustls::pki_types::CertificateDer<'static>> {
        vec![quinn::rustls::pki_types::CertificateDer::from(
            self.cert_der.clone(),
        )]
    }

    pub fn private_key(&self) -> quinn::rustls::pki_types::PrivateKeyDer<'static> {
        quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
            quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(self.key_der.clone()),
        )
    }

    /// Short id used in the UI/logs.
    pub fn short_id(&self) -> &str {
        &self.device_id[..8.min(self.device_id.len())]
    }

    pub fn platform() -> &'static str {
        std::env::consts::OS
    }
}

fn build_cert(name: &str, key: &KeyPair) -> Result<Vec<u8>> {
    let mut params = CertificateParams::new(vec![
        "localhost".to_string(),
        "quicdrop.local".to_string(),
        name.to_string(),
    ])
    .context("certificate params")?;
    params
        .distinguished_name
        .push(DnType::CommonName, format!("quicdrop/{name}"));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "quicdrop");
    // Fixed validity keeps the DER, and therefore the device id, stable when the
    // certificate is regenerated from the same key.
    params.not_before = date_time_ymd(2020, 1, 1);
    params.not_after = date_time_ymd(9999, 12, 31);
    let cert = params.self_signed(key).context("self-sign certificate")?;
    Ok(cert.der().to_vec())
}

/// LocalSend-style default: "Brave Otter". Not a secret, only a label.
fn random_name() -> String {
    const ADJECTIVES: [&str; 24] = [
        "Brave", "Calm", "Clever", "Cosmic", "Dusty", "Eager", "Fuzzy", "Gentle", "Golden",
        "Hidden", "Jolly", "Lucky", "Mellow", "Nimble", "Quiet", "Rapid", "Shiny", "Silent",
        "Sleepy", "Solar", "Swift", "Tidy", "Witty", "Young",
    ];
    const NOUNS: [&str; 24] = [
        "Badger", "Comet", "Dolphin", "Ember", "Falcon", "Fern", "Gecko", "Heron", "Ibis", "Koala",
        "Lantern", "Lynx", "Meadow", "Otter", "Owl", "Panda", "Pebble", "Raven", "River",
        "Sparrow", "Tiger", "Willow", "Wolf", "Wren",
    ];
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&(std::process::id() as u64).to_le_bytes());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    seed[8..].copy_from_slice(&(nanos as u64).to_le_bytes());
    let hash = blake3::hash(&seed);
    let bytes = hash.as_bytes();
    format!(
        "{} {}",
        ADJECTIVES[bytes[0] as usize % ADJECTIVES.len()],
        NOUNS[bytes[1] as usize % NOUNS.len()]
    )
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) {}
