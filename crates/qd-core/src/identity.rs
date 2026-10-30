use anyhow::{Context, Result};
use rcgen::{date_time_ymd, CertificateParams, DnType, KeyPair};
use std::fs;
use std::path::PathBuf;

use crate::util::hostname;

/// Long-lived identity. The device id is the certificate fingerprint, so peers can pin it.
#[derive(Debug, Clone)]
pub struct Identity {
    pub device_id: String,
    pub name: String,
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
        let dir = Self::config_dir();
        fs::create_dir_all(&dir).with_context(|| format!("create config dir {}", dir.display()))?;

        let key_path = dir.join("identity.key");
        let cert_path = dir.join("identity.crt");
        let name_path = dir.join("name.txt");

        let display_name = name
            .or_else(|| {
                fs::read_to_string(&name_path)
                    .ok()
                    .map(|s| s.trim().to_string())
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(hostname);

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
                let (params, der) = build_cert(&display_name, &key)?;
                let _ = params;
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
            name: display_name,
            cert_der,
            key_der,
            dir,
        })
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

fn build_cert(name: &str, key: &KeyPair) -> Result<(CertificateParams, Vec<u8>)> {
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
    Ok((params, cert.der().to_vec()))
}

#[cfg(unix)]
fn restrict_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &std::path::Path) {}
