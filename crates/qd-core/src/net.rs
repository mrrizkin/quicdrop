//! QUIC endpoints and TLS pinning: the advertised device id is the blake3 fingerprint of the
//! peer certificate, so a peer that presents a different cert fails the handshake.

use anyhow::{Context, Result};
use quinn::rustls;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
use std::sync::Arc;
use std::time::Duration;

use crate::identity::Identity;
use crate::proto::ALPN;

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

pub fn transport_config() -> quinn::TransportConfig {
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(quinn::VarInt::from_u32(128));
    t.max_concurrent_uni_streams(quinn::VarInt::from_u32(32));
    // Per-stream window must exceed the chunk size or every chunk stalls on flow control.
    t.stream_receive_window(quinn::VarInt::from_u32(8 * 1024 * 1024));
    t.receive_window(quinn::VarInt::from_u32(64 * 1024 * 1024));
    t.send_window(32 * 1024 * 1024);
    t.keep_alive_interval(Some(Duration::from_secs(5)));
    if let Ok(idle) = quinn::IdleTimeout::try_from(Duration::from_secs(60)) {
        t.max_idle_timeout(Some(idle));
    }
    t
}

pub fn server_config(identity: &Identity) -> Result<quinn::ServerConfig> {
    let mut crypto = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("rustls server builder")?
        .with_no_client_auth()
        .with_single_cert(identity.cert_chain(), identity.private_key())
        .context("rustls server cert")?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];
    // 0-RTT lets a peer that already completed a handshake send immediately.
    crypto.max_early_data_size = u32::MAX;

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .context("build QUIC server crypto")?;
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    cfg.transport_config(Arc::new(transport_config()));
    Ok(cfg)
}

/// Client config that only accepts a certificate matching `expected_device_id` (prefix
/// allowed so users can pass a short id).
pub fn client_config(expected_device_id: Option<String>) -> Result<quinn::ClientConfig> {
    let provider = provider();
    let verifier = Arc::new(PinnedVerifier {
        expected: expected_device_id,
        provider: provider.clone(),
    });

    let mut crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .context("rustls client builder")?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .context("build QUIC client crypto")?;
    let mut cfg = quinn::ClientConfig::new(Arc::new(quic_crypto));
    cfg.transport_config(Arc::new(transport_config()));
    Ok(cfg)
}

#[derive(Debug)]
struct PinnedVerifier {
    expected: Option<String>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let fingerprint = Identity::fingerprint(end_entity.as_ref());
        if let Some(expected) = self.expected.as_deref().filter(|e| !e.is_empty()) {
            if !fingerprint.starts_with(expected) {
                return Err(TlsError::General(format!(
                    "device id mismatch: peer certificate is {fingerprint}, expected {expected}"
                )));
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
