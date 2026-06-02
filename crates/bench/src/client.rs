use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use command::Command;
use pin_project_lite::pin_project;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;
use transport::{
    ClientHello, CodecOptions, Request, Response, client_options_from_server_hello,
    encode_request_with_options, read_response_from_async_with_options,
    read_server_hello_from_async, write_client_hello_to_async, write_request_to_async_with_options,
};
use uuid::Uuid;

use crate::error::{BenchError, Result};

#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_cert: Option<PathBuf>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub addr: String,
    pub host_for_tls: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: TlsConfig,
}

pin_project! {
    #[project = BenchmarkStreamProj]
    enum BenchmarkStream {
        Tcp { #[pin] stream: TcpStream },
        Tls { #[pin] stream: tokio_rustls::client::TlsStream<TcpStream> },
    }
}

impl AsyncRead for BenchmarkStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            BenchmarkStreamProj::Tcp { stream } => stream.poll_read(cx, buf),
            BenchmarkStreamProj::Tls { stream } => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BenchmarkStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.project() {
            BenchmarkStreamProj::Tcp { stream } => stream.poll_write(cx, buf),
            BenchmarkStreamProj::Tls { stream } => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            BenchmarkStreamProj::Tcp { stream } => stream.poll_flush(cx),
            BenchmarkStreamProj::Tls { stream } => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.project() {
            BenchmarkStreamProj::Tcp { stream } => stream.poll_shutdown(cx),
            BenchmarkStreamProj::Tls { stream } => stream.poll_shutdown(cx),
        }
    }
}

#[derive(Clone)]
pub struct BenchmarkClient {
    inner: Arc<Mutex<ClientState>>,
}

struct ClientState {
    stream: BenchmarkStream,
    transport: CodecOptions,
}

impl BenchmarkClient {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let tcp = TcpStream::connect(&config.addr).await?;
        tcp.set_nodelay(true)?;
        let mut stream = if config.tls.enabled {
            BenchmarkStream::Tls {
                stream: connect_tls(tcp, config).await?,
            }
        } else {
            BenchmarkStream::Tcp { stream: tcp }
        };
        let hello = ClientHello {
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            auth_intent: config.username.is_some(),
            ..ClientHello::new("vaylix-bench", env!("CARGO_PKG_VERSION"))
        };
        write_client_hello_to_async(&mut stream, &hello).await?;
        let server_hello = read_server_hello_from_async(&mut stream).await?;
        let transport = client_options_from_server_hello(&server_hello)?;
        let client = Self {
            inner: Arc::new(Mutex::new(ClientState { stream, transport })),
        };

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            let response = client
                .send(Command::Auth {
                    username: username.clone(),
                    password: password.clone(),
                })
                .await?;
            if response.status != transport::Status::Ok {
                return Err(BenchError::InvalidConfiguration(format!(
                    "authentication failed against {}",
                    config.addr
                )));
            }
        }

        Ok(client)
    }

    pub async fn send(&self, command: Command) -> Result<Response> {
        let request_id = Uuid::now_v7();
        let request = Request::from_command(request_id, command)?;
        let mut state = self.inner.lock().await;
        let transport = state.transport;
        write_request_to_async_with_options(&mut state.stream, &request, transport).await?;
        let response = read_response_from_async_with_options(&mut state.stream, transport).await?;
        if response.request_id != request_id {
            return Err(BenchError::InvalidConfiguration(format!(
                "mismatched response id: expected {request_id}, got {}",
                response.request_id
            )));
        }
        Ok(response)
    }

    /// Sends several independent commands over one connection before reading
    /// responses. This exercises VTP2 request-id correlation and removes
    /// client-side request/response round trips from load-generator profiles.
    pub async fn send_pipeline(&self, commands: Vec<Command>) -> Result<Vec<Response>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        let mut requests = Vec::with_capacity(commands.len());
        for command in commands {
            let request_id = Uuid::now_v7();
            requests.push(Request::from_command(request_id, command)?);
        }

        let mut state = self.inner.lock().await;
        let transport = state.transport;
        for request in &requests {
            let encoded = encode_request_with_options(request, transport)?;
            state.stream.write_all(&encoded).await?;
        }
        state.stream.flush().await?;

        let mut responses = HashMap::with_capacity(requests.len());
        for _ in 0..requests.len() {
            let response =
                read_response_from_async_with_options(&mut state.stream, transport).await?;
            responses.insert(response.request_id, response);
        }

        let mut ordered = Vec::with_capacity(requests.len());
        for request in requests {
            let response = responses.remove(&request.request_id).ok_or_else(|| {
                BenchError::InvalidConfiguration(format!(
                    "missing pipelined response id {}",
                    request.request_id
                ))
            })?;
            ordered.push(response);
        }

        Ok(ordered)
    }

    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let started = tokio::time::Instant::now();
        loop {
            let response = self.send(Command::Ping { message: None }).await?;
            if response.status == transport::Status::Ok {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err(BenchError::InvalidConfiguration(
                    "server did not become ready before timeout".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn connect_tls(
    tcp: TcpStream,
    config: &ConnectionConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let client_config = Arc::new(build_tls_client_config(&config.tls)?);
    let server_name = ServerName::try_from(config.host_for_tls.clone())
        .map_err(|_| BenchError::InvalidConfiguration("invalid TLS server name".to_string()))?;
    let connector = TlsConnector::from(client_config);
    Ok(connector.connect(server_name, tcp).await?)
}

fn build_tls_client_config(config: &TlsConfig) -> Result<RustlsClientConfig> {
    let mut roots = RootCertStore::empty();
    let ca_cert = config.ca_cert.as_ref().ok_or_else(|| {
        BenchError::InvalidConfiguration("tls requires --tls-ca-cert".to_string())
    })?;
    for cert in CertificateDer::pem_file_iter(ca_cert).map_err(std::io::Error::other)? {
        roots
            .add(cert.map_err(std::io::Error::other)?)
            .map_err(std::io::Error::other)?;
    }

    let builder = RustlsClientConfig::builder().with_root_certificates(roots);
    match (&config.client_cert, &config.client_key) {
        (Some(client_cert), Some(client_key)) => {
            let certs = load_cert_chain(client_cert)?;
            let key = load_private_key(client_key)?;
            Ok(builder.with_client_auth_cert(certs, key)?)
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        _ => Err(BenchError::InvalidConfiguration(
            "tls client auth requires both --tls-client-cert and --tls-client-key".to_string(),
        )),
    }
}

fn load_cert_chain(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(std::io::Error::other)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;
    if certs.is_empty() {
        return Err(BenchError::InvalidConfiguration(
            "client certificate file is empty".to_string(),
        ));
    }
    Ok(certs)
}

fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>> {
    let key_bytes = std::fs::read(path)?;
    Ok(PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(std::io::Error::other)?
        .clone_key())
}
