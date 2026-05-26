use std::fs;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{Result, ServerError};

/// Builds a Rustls server configuration from PEM-encoded certificate and private key files.
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>> {
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

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|_| ServerError::TlsConfiguration)?;

    Ok(Arc::new(config))
}
