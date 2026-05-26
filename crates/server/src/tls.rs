use std::fs;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;

use crate::error::{Result, ServerError};

/// Builds a Rustls server configuration from PEM-encoded certificate and private key files.
pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>> {
    let cert_bytes = fs::read(cert_path)?;
    let key_bytes = fs::read(key_path)?;

    let mut cert_reader = cert_bytes.as_slice();
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| ServerError::TlsConfiguration)?;
    if certs.is_empty() {
        return Err(ServerError::TlsConfiguration);
    }

    let mut key_reader = key_bytes.as_slice();
    let private_key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|_| ServerError::TlsConfiguration)?
        .ok_or(ServerError::TlsConfiguration)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|_| ServerError::TlsConfiguration)?;

    Ok(Arc::new(config))
}
