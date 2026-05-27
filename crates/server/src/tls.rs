use std::fs;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

use crate::error::{Result, ServerError};

/// Builds a Rustls server configuration from PEM-encoded certificate and private key files.
///
/// When `client_ca_path` is provided, the server requires clients to present a certificate
/// chaining to one of the CA certificates in that PEM file.
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca_path: Option<&Path>,
) -> Result<Arc<ServerConfig>> {
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
        .with_single_cert(certs, private_key)
        .map_err(|_| ServerError::TlsConfiguration)?;

    Ok(Arc::new(config))
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
