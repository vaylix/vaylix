use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::sync::RwLock;
use x509_parser::prelude::parse_x509_certificate;

use crate::error::{Result, ServerError};

const MILLIS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Default)]
pub struct TlsMetadata {
    pub cert_not_after_ms: Option<u64>,
    pub cert_days_remaining: Option<i64>,
    pub last_reload_success_at_ms: Option<u64>,
    pub last_reload_failure_at_ms: Option<u64>,
}

pub struct TlsState {
    cert_path: PathBuf,
    key_path: PathBuf,
    client_ca_path: Option<PathBuf>,
    config: RwLock<Arc<ServerConfig>>,
    metadata: RwLock<TlsMetadata>,
}

impl TlsState {
    pub fn from_server_config(server_config: Arc<ServerConfig>) -> Arc<Self> {
        Arc::new(Self {
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            client_ca_path: None,
            config: RwLock::new(server_config),
            metadata: RwLock::new(TlsMetadata::default()),
        })
    }

    pub fn load(
        cert_path: &Path,
        key_path: &Path,
        client_ca_path: Option<&Path>,
    ) -> Result<Arc<Self>> {
        let loaded = load_tls_config(cert_path, key_path, client_ca_path)?;
        Ok(Arc::new(Self {
            cert_path: cert_path.to_path_buf(),
            key_path: key_path.to_path_buf(),
            client_ca_path: client_ca_path.map(Path::to_path_buf),
            config: RwLock::new(loaded.server_config),
            metadata: RwLock::new(TlsMetadata {
                cert_not_after_ms: loaded.cert_not_after_ms,
                cert_days_remaining: loaded.cert_days_remaining,
                last_reload_success_at_ms: None,
                last_reload_failure_at_ms: None,
            }),
        }))
    }

    pub async fn server_config(&self) -> Arc<ServerConfig> {
        self.config.read().await.clone()
    }

    pub async fn metadata_snapshot(&self) -> TlsMetadata {
        self.metadata.read().await.clone()
    }

    pub async fn reload(&self) -> Result<()> {
        match load_tls_config(
            &self.cert_path,
            &self.key_path,
            self.client_ca_path.as_deref(),
        ) {
            Ok(loaded) => {
                *self.config.write().await = loaded.server_config;
                let mut metadata = self.metadata.write().await;
                metadata.cert_not_after_ms = loaded.cert_not_after_ms;
                metadata.cert_days_remaining = loaded.cert_days_remaining;
                metadata.last_reload_success_at_ms = Some(now_millis());
                Ok(())
            }
            Err(err) => {
                self.metadata.write().await.last_reload_failure_at_ms = Some(now_millis());
                Err(err)
            }
        }
    }
}

struct LoadedTlsConfig {
    server_config: Arc<ServerConfig>,
    cert_not_after_ms: Option<u64>,
    cert_days_remaining: Option<i64>,
}

pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<Arc<ServerConfig>> {
    Ok(load_tls_config(cert_path, key_path, client_ca_path)?.server_config)
}

/// Builds a Rustls server configuration from PEM-encoded certificate and private key files.
///
/// When `client_ca_path` is provided, the server requires clients to present a certificate
/// chaining to one of the CA certificates in that PEM file.
fn load_tls_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<LoadedTlsConfig> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|_| ServerError::TlsConfiguration)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| ServerError::TlsConfiguration)?;
    if certs.is_empty() {
        return Err(ServerError::TlsConfiguration);
    }

    let key_bytes = fs::read(key_path)?;
    let private_key = PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(|_| ServerError::TlsConfiguration)?
        .clone_key();

    let builder = ServerConfig::builder();
    let builder = if let Some(client_ca_path) = client_ca_path {
        let roots = load_root_store(client_ca_path)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|_| ServerError::TlsConfiguration)?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };

    let config = builder
        .with_single_cert(certs.clone(), private_key)
        .map_err(|_| ServerError::TlsConfiguration)?;

    let (cert_not_after_ms, cert_days_remaining) = inspect_certificate_expiry(&certs[0])?;
    if let Some(days_remaining) = cert_days_remaining
        && days_remaining < 0
    {
        return Err(ServerError::TlsConfiguration);
    }

    Ok(LoadedTlsConfig {
        server_config: Arc::new(config),
        cert_not_after_ms,
        cert_days_remaining,
    })
}

fn inspect_certificate_expiry(cert: &CertificateDer<'_>) -> Result<(Option<u64>, Option<i64>)> {
    let (_, parsed) =
        parse_x509_certificate(cert.as_ref()).map_err(|_| ServerError::TlsConfiguration)?;
    let not_after = parsed.validity().not_after.timestamp();
    let not_after_ms = u64::try_from(not_after)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000));
    let days_remaining = not_after_ms.map(|value| {
        let remaining_ms = i64::try_from(value).unwrap_or(i64::MAX) - now_millis() as i64;
        remaining_ms / MILLIS_PER_DAY
    });
    Ok((not_after_ms, days_remaining))
}

fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path).map_err(|_| ServerError::TlsConfiguration)? {
        roots
            .add(cert.map_err(|_| ServerError::TlsConfiguration)?)
            .map_err(|_| ServerError::TlsConfiguration)?;
    }
    if roots.is_empty() {
        return Err(ServerError::TlsConfiguration);
    }

    Ok(roots)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as u64
}
