use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use command::{Command, Expiration, SetCondition, SetOptions};
use engine::{Engine, EngineOptions, Paths, WalSyncPolicy};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use server::Server;
use server::audit::AuditLogger;
use server::auth::AuthConfig;
use server::server::{ServerGuards, ServerRuntimeConfig};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use transport::{CodecOptions, Request, Status, read_response_from_async, write_request_to_async};
use uuid::Uuid;

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("veyra-server-test-{name}-{unique}"))
}

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn test_keyring(secret: &str) -> engine::StorageKeyring {
    engine::StorageKeyring {
        active: engine::StorageKey {
            id: Uuid::from_u128(1),
            secret: secret.to_string(),
            created_at_ms: now_ms(),
        },
        previous: Vec::new(),
    }
}

async fn authenticate<S>(stream: &mut S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let auth = Request::from_command(
        id(0),
        Command::Auth {
            username: "vaylix".to_string(),
            password: "vaylix".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(stream, &auth).await.unwrap();
    let response = read_response_from_async(stream).await.unwrap();
    assert_eq!(response.status, Status::Ok);
}

fn tls_config_for(root: &Path) -> (Arc<rustls::ServerConfig>, String) {
    fs::create_dir_all(root).unwrap();
    let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let cert_path = root.join("server.crt");
    let key_path = root.join("server.key");
    fs::write(&cert_path, cert_pem.as_bytes()).unwrap();
    fs::write(&key_path, key_pem.as_bytes()).unwrap();

    (
        server::tls::load_server_config(&cert_path, &key_path).unwrap(),
        cert_pem,
    )
}

async fn connect_tls(addr: SocketAddr, cert_pem: &str) -> TlsStream<TcpStream> {
    let mut roots = RootCertStore::empty();
    let cert_der = rustls::pki_types::CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .next()
        .unwrap()
        .unwrap();
    roots.add(cert_der).unwrap();
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let connector = TlsConnector::from(tls_config);
    let tcp_stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    let server_name = ServerName::try_from("localhost".to_string()).unwrap();

    connector.connect(server_name, tcp_stream).await.unwrap()
}

async fn connect_tcp(addr: SocketAddr) -> TcpStream {
    timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap()
}

fn runtime(snapshot_interval: Option<Duration>) -> ServerRuntimeConfig {
    runtime_with_tls(snapshot_interval, None)
}

fn runtime_with_tls(
    snapshot_interval: Option<Duration>,
    tls_config: Option<Arc<rustls::ServerConfig>>,
) -> ServerRuntimeConfig {
    let audit_path = temp_dir("audit").join("audit.log");
    ServerRuntimeConfig {
        snapshot_interval,
        expiration_sweep_interval: None,
        idle_timeout: None,
        auth_config: Some(AuthConfig::new("vaylix".to_string(), "vaylix".to_string()).unwrap()),
        guards: ServerGuards {
            max_request_payload_bytes: 1_048_576,
            max_key_bytes: 1_024,
            max_value_bytes: 262_144,
            max_keys_per_batch: 256,
            max_transaction_queue_len: 128,
            requests_per_second: 200,
            request_burst: 400,
        },
        tls_config,
        transport: CodecOptions::default(),
        audit_logger: std::sync::Arc::new(AuditLogger::open(&audit_path).unwrap()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_unauthenticated_requests() {
    let root = temp_dir("tcp-auth-required");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime(None))
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = connect_tcp(addr).await;

    let request = Request::from_command(
        id(1),
        Command::Get {
            key: "missing".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &request).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Error);
    let error = response.decode_error().unwrap();
    assert_eq!(error.code, "SRV-007");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allows_unauthenticated_requests_when_auth_is_disabled() {
    let root = temp_dir("auth-disabled");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let mut runtime = runtime(None);
    runtime.auth_config = None;
    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime)
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = connect_tcp(addr).await;
    let request = Request::from_command(
        id(1),
        Command::Get {
            key: "missing".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &request).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::NotFound);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_real_tcp_round_trip_for_extended_commands() {
    let root = temp_dir("tcp-round-trip");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime(None))
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = connect_tcp(addr).await;
    authenticate(&mut stream).await;

    let set = Request::from_command(
        id(1),
        Command::Set {
            key: "user:1".to_string(),
            value: "alice".to_string(),
            options: SetOptions {
                condition: Some(SetCondition::Nx),
                expiration: Some(Expiration::Px(500)),
                keep_ttl: false,
                return_previous: false,
            },
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &set).await.unwrap();
    let set_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(set_response.status, Status::Ok);
    assert!(set_response.decode_bool().unwrap());

    let getex = Request::from_command(
        id(2),
        Command::GetEx {
            key: "user:1".to_string(),
            expiration: Some(Expiration::Ex(1)),
            persist: false,
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &getex).await.unwrap();
    let getex_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(getex_response.decode_value().unwrap(), "alice");

    let scan = Request::from_command(
        id(3),
        Command::Scan {
            cursor: 0,
            pattern: Some("user:*".to_string()),
            count: Some(10),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &scan).await.unwrap();
    let scan_response = read_response_from_async(&mut stream).await.unwrap();
    let scan_payload = scan_response.decode_scan().unwrap();
    assert_eq!(scan_payload.keys, vec!["user:1".to_string()]);

    let getdel = Request::from_command(
        id(4),
        Command::GetDel {
            key: "user:1".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &getdel).await.unwrap();
    let getdel_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(getdel_response.decode_value().unwrap(), "alice");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_snapshotter_writes_snapshot_and_flushes_wal() {
    let root = temp_dir("snapshotter");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        runtime(Some(Duration::from_millis(50))),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = connect_tcp(addr).await;
    authenticate(&mut stream).await;

    let set = Request::from_command(
        id(1),
        Command::Set {
            key: "snapshot:key".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &set).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Ok);

    let snapshot_path = root.join("snapshot.bin");
    let manifest_path = root.join("manifest.bin");
    for _ in 0..20 {
        if snapshot_path.exists() && manifest_path.exists() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(snapshot_path.exists());
    assert!(manifest_path.exists());

    let wal_path = root.join("wal.log");
    let mut wal_len = fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    for _ in 0..20 {
        if wal_len == 0 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
        wal_len = fs::metadata(&wal_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
    }
    assert_eq!(wal_len, 0);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepts_tls_connections_when_enabled() {
    let root = temp_dir("tls");
    let (tls_config, cert_pem) = tls_config_for(&root);

    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        runtime_with_tls(None, Some(tls_config)),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut tls_stream = connect_tls(addr, &cert_pem).await;

    let auth = Request::from_command(
        id(1),
        Command::Auth {
            username: "vaylix".to_string(),
            password: "vaylix".to_string(),
        },
    )
    .unwrap();
    let encoded = transport::encode_request(&auth).unwrap();
    tls_stream.write_all(&encoded).await.unwrap();
    tls_stream.flush().await.unwrap();
    let response = transport::read_response_from_async(&mut tls_stream)
        .await
        .unwrap();
    assert_eq!(response.status, Status::Ok);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_plain_tcp_frames_when_tls_is_required() {
    let root = temp_dir("plain-tcp-rejected");
    let (tls_config, _cert_pem) = tls_config_for(&root);
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        runtime_with_tls(None, Some(tls_config)),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    let auth = Request::from_command(
        id(1),
        Command::Auth {
            username: "vaylix".to_string(),
            password: "vaylix".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &auth).await.unwrap();

    let response = timeout(
        Duration::from_secs(2),
        transport::read_response_from_async(&mut stream),
    )
    .await;
    if let Ok(result) = response {
        assert!(result.is_err());
    }

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enforces_rate_limits_over_the_network() {
    let root = temp_dir("rate-limit");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let mut runtime = runtime(None);
    runtime.guards.requests_per_second = 1;
    runtime.guards.request_burst = 1;
    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime)
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = connect_tcp(addr).await;
    authenticate(&mut stream).await;

    let request = Request::from_command(
        id(2),
        Command::Get {
            key: "missing".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &request).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Error);
    let error = response.decode_error().unwrap();
    assert_eq!(error.code, "SRV-012");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn handles_concurrent_clients_against_serialized_engine() {
    let root = temp_dir("concurrent-clients");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
        },
    )
    .unwrap();

    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime(None))
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut workers = Vec::new();
    for index in 0..8 {
        workers.push(tokio::spawn(async move {
            let mut stream = connect_tcp(addr).await;
            authenticate(&mut stream).await;
            let request = Request::from_command(
                Uuid::now_v7(),
                Command::Set {
                    key: format!("client:{index}"),
                    value: format!("value:{index}"),
                    options: SetOptions::default(),
                },
            )
            .unwrap();
            write_request_to_async(&mut stream, &request).await.unwrap();
            let response = read_response_from_async(&mut stream).await.unwrap();
            assert_eq!(response.status, Status::Ok);
        }));
    }

    for worker in workers {
        worker.await.unwrap();
    }

    let mut stream = connect_tcp(addr).await;
    authenticate(&mut stream).await;
    let count = Request::from_command(id(99), Command::Count).unwrap();
    write_request_to_async(&mut stream, &count).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.decode_count().unwrap(), 8);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}
