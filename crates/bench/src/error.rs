use thiserror::Error;

pub type Result<T> = std::result::Result<T, BenchError>;

#[derive(Debug, Error)]
pub enum BenchError {
    #[error("invalid benchmark configuration: {0}")]
    InvalidConfiguration(String),
    #[error("i/o failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("transport failure: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("command parse failure: {0}")]
    Command(#[from] command::CommandError),
    #[error("json failure: {0}")]
    Json(#[from] serde_json::Error),
    #[error("task join failure: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("tls failure: {0}")]
    Tls(#[from] rustls::Error),
    #[error("tls handshake failure: {0}")]
    TlsHandshake(#[from] tokio_rustls::rustls::pki_types::InvalidDnsNameError),
    #[error("certificate generation failure: {0}")]
    Rcgen(#[from] rcgen::Error),
}
