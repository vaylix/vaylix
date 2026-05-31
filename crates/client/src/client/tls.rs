use std::net::TcpStream;
use std::sync::Arc;

use rustls::pki_types::ServerName;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::{ClientConfig as TlsClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::error::Result;

pub(super) fn connect_tls(
    stream: TcpStream,
    host: &str,
    ca_cert: Option<&std::path::Path>,
    client_cert: Option<&std::path::Path>,
    client_key: Option<&std::path::Path>,
) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let tls_config = build_tls_client_config(ca_cert, client_cert, client_key)?;
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|_| std::io::Error::other("invalid TLS server name"))?;
    let connection =
        ClientConnection::new(tls_config, server_name).map_err(std::io::Error::other)?;
    Ok(StreamOwned::new(connection, stream))
}

fn build_tls_client_config(
    ca_cert: Option<&std::path::Path>,
    client_cert: Option<&std::path::Path>,
    client_key: Option<&std::path::Path>,
) -> Result<Arc<TlsClientConfig>> {
    let mut roots = RootCertStore::empty();

    if let Some(ca_cert) = ca_cert {
        for cert in CertificateDer::pem_file_iter(ca_cert).map_err(std::io::Error::other)? {
            roots
                .add(cert.map_err(std::io::Error::other)?)
                .map_err(std::io::Error::other)?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            roots.add(cert).map_err(std::io::Error::other)?;
        }
        if !native.errors.is_empty() && roots.is_empty() {
            return Err(std::io::Error::other("no native root certificates available").into());
        }
    }

    let builder = TlsClientConfig::builder().with_root_certificates(roots);
    let config = match (client_cert, client_key) {
        (Some(client_cert), Some(client_key)) => {
            let certs = load_cert_chain(client_cert)?;
            let key = load_private_key(client_key)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(std::io::Error::other)?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(std::io::Error::other(
                "tls_client_cert and tls_client_key must be provided together",
            )
            .into());
        }
    };

    Ok(Arc::new(config))
}

fn load_cert_chain(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(std::io::Error::other)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;
    if certs.is_empty() {
        return Err(std::io::Error::other("client certificate file is empty").into());
    }

    Ok(certs)
}

fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    let key_bytes = std::fs::read(path)?;
    Ok(PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(std::io::Error::other)?
        .clone_key())
}
