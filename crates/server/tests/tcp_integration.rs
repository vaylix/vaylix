use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{Arc as StdArc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use command::{Command, Expiration, SetCondition, SetOptions};
use engine::{Engine, EngineOptions, Paths, WalSyncPolicy, inspect_wal};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose, generate_simple_self_signed,
};
use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use server::Server;
use server::audit::AuditLogger;
use server::auth::AuthConfig;
use server::replication::{
    ClusterMember, ReplicationConfig, ReplicationRole, ReplicationRuntime, WriteAckMode,
};
use server::server::{
    CommittedReadIndex, ReplicationClientPool, ServerGuards, ServerRuntimeConfig,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, OwnedMutexGuard};
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use transport::{
    ClientHello, CodecOptions, CompressionMode, Request, RequestMetadata, Status,
    read_response_from_async, read_response_from_async_with_options, read_server_hello_from_async,
    write_client_hello_to_async, write_request_to_async,
};
use uuid::Uuid;

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("vaylix-server-test-{name}-{unique}"))
}

async fn acquire_ha_test_lock() -> OwnedMutexGuard<()> {
    static LOCK: OnceLock<StdArc<TokioMutex<()>>> = OnceLock::new();
    LOCK.get_or_init(|| StdArc::new(TokioMutex::new(())))
        .clone()
        .lock_owned()
        .await
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
    let certified = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = certified.cert.pem();
    let key_pem = certified.signing_key.serialize_pem();
    let cert_path = root.join("server.crt");
    let key_path = root.join("server.key");
    fs::write(&cert_path, cert_pem.as_bytes()).unwrap();
    fs::write(&key_path, key_pem.as_bytes()).unwrap();

    (
        server::tls::load_server_config(&cert_path, &key_path, None).unwrap(),
        cert_pem,
    )
}

struct MutualTlsMaterial {
    server_config: Arc<rustls::ServerConfig>,
    ca_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn mutual_tls_config_for(root: &Path) -> MutualTlsMaterial {
    fs::create_dir_all(root).unwrap();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_cert = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_cert = server_params.signed_by(&server_key, &ca_cert).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::default();
    client_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params.signed_by(&client_key, &ca_cert).unwrap();

    let server_cert_path = root.join("server.crt");
    let server_key_path = root.join("server.key");
    let client_ca_path = root.join("client-ca.crt");
    fs::write(&server_cert_path, server_cert.pem().as_bytes()).unwrap();
    fs::write(&server_key_path, server_key.serialize_pem().as_bytes()).unwrap();
    fs::write(&client_ca_path, ca_cert.pem().as_bytes()).unwrap();

    MutualTlsMaterial {
        server_config: server::tls::load_server_config(
            &server_cert_path,
            &server_key_path,
            Some(&client_ca_path),
        )
        .unwrap(),
        ca_pem: ca_cert.pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

fn root_store_from_pem(cert_pem: &str) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    let cert_der = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .next()
        .unwrap()
        .unwrap();
    roots.add(cert_der).unwrap();

    roots
}

async fn connect_tls(addr: SocketAddr, cert_pem: &str) -> TlsStream<TcpStream> {
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(root_store_from_pem(cert_pem))
            .with_no_client_auth(),
    );
    let mut stream = connect_tls_with_config(addr, tls_config).await.unwrap();
    negotiate(&mut stream).await;
    stream
}

async fn connect_mutual_tls(
    addr: SocketAddr,
    ca_pem: &str,
    client_cert_pem: &str,
    client_key_pem: &str,
) -> TlsStream<TcpStream> {
    let client_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(client_cert_pem.as_bytes())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
    let client_key = PrivateKeyDer::from_pem_slice(client_key_pem.as_bytes())
        .unwrap()
        .clone_key();
    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(root_store_from_pem(ca_pem))
            .with_client_auth_cert(client_certs, client_key)
            .unwrap(),
    );
    let mut stream = connect_tls_with_config(addr, tls_config).await.unwrap();
    negotiate(&mut stream).await;
    stream
}

async fn connect_tls_with_config(
    addr: SocketAddr,
    tls_config: Arc<ClientConfig>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let connector = TlsConnector::from(tls_config);
    let tcp_stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    let server_name = ServerName::try_from("localhost".to_string()).unwrap();

    connector.connect(server_name, tcp_stream).await
}

async fn connect_tcp(addr: SocketAddr) -> TcpStream {
    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    negotiate(&mut stream).await;
    stream
}

async fn negotiate<S>(stream: &mut S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let hello = ClientHello::new("tcp-integration-test", "0.3.0");
    write_client_hello_to_async(stream, &hello).await.unwrap();
    let response = read_server_hello_from_async(stream).await.unwrap();
    assert_eq!(response.status, Status::Ok);
}

fn runtime(snapshot_interval: Option<Duration>) -> ServerRuntimeConfig {
    runtime_with_tls(snapshot_interval, None)
}

fn runtime_without_auth(snapshot_interval: Option<Duration>) -> ServerRuntimeConfig {
    let mut runtime = runtime(snapshot_interval);
    runtime.auth_config = None;
    runtime
}

fn standalone_replication(node_id: &str) -> Arc<ReplicationRuntime> {
    let state_dir = temp_dir(&format!("cluster-state-{node_id}"));
    Arc::new(
        ReplicationRuntime::new(ReplicationConfig {
            node_id: node_id.to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: None,
            role: ReplicationRole::Standalone,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::Local,
            ack_timeout: Duration::from_millis(100),
            poll_interval: Duration::from_millis(100),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(250),
            election_timeout_max: Duration::from_millis(500),
            state_path: state_dir.join("cluster-state.json"),
            state_tmp_path: state_dir.join("cluster-state.json.tmp"),
            initial_members: Vec::new(),
        })
        .unwrap(),
    )
}

fn reserve_local_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn clustered_replication(
    node_id: &str,
    role: ReplicationRole,
    advertise_addr: SocketAddr,
    members: &[ClusterMember],
) -> Arc<ReplicationRuntime> {
    let state_dir = temp_dir(&format!("cluster-state-{node_id}"));
    clustered_replication_with_state_dir(node_id, role, advertise_addr, members, &state_dir)
}

fn clustered_replication_with_state_dir(
    node_id: &str,
    role: ReplicationRole,
    advertise_addr: SocketAddr,
    members: &[ClusterMember],
    state_dir: &Path,
) -> Arc<ReplicationRuntime> {
    Arc::new(
        ReplicationRuntime::new(ReplicationConfig {
            node_id: node_id.to_string(),
            group_id: "ha-test-group".to_string(),
            advertise_addr: Some(advertise_addr.to_string()),
            role,
            upstream: None,
            upstream_username: Some("vaylix".to_string()),
            upstream_password: Some("vaylix".to_string()),
            write_ack_mode: WriteAckMode::Replica,
            ack_timeout: Duration::from_secs(3),
            poll_interval: Duration::from_millis(100),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(3),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(900),
            election_timeout_max: Duration::from_millis(1_500),
            state_path: state_dir.join("cluster-state.json"),
            state_tmp_path: state_dir.join("cluster-state.json.tmp"),
            initial_members: members.to_vec(),
        })
        .unwrap(),
    )
}

fn runtime_with_tls(
    snapshot_interval: Option<Duration>,
    tls_config: Option<Arc<rustls::ServerConfig>>,
) -> ServerRuntimeConfig {
    let audit_path = temp_dir("audit").join("audit.log");
    let backup_dir = temp_dir("tcp-backups");
    let maintenance_path = temp_dir("tcp-maintenance").join("maintenance.mode");
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
        tls_state: tls_config.map(server::tls::TlsState::from_server_config),
        transport: CodecOptions::default(),
        log_requests: false,
        audit_commands: false,
        backup_dir,
        mtls_enabled: false,
        slow_command_threshold: Some(Duration::from_millis(100)),
        audit_logger: std::sync::Arc::new(AuditLogger::open(&audit_path).unwrap()),
        wal_segment_size_bytes: engine::DEFAULT_WAL_SEGMENT_SIZE_BYTES,
        wal_retain_segments: engine::DEFAULT_WAL_RETAIN_SEGMENTS,
        auth_failure_window: Duration::from_secs(300),
        auth_failure_limit: 5,
        auth_lockout: Duration::from_secs(900),
        transaction_max_duration: Duration::from_secs(30),
        maintenance: std::sync::Arc::new(
            server::server::MaintenanceMode::load(maintenance_path).unwrap(),
        ),
        auth_lockouts: std::sync::Arc::new(tokio::sync::Mutex::new(
            server::server::AuthLockoutState::default(),
        )),
        insecure_auth_disabled: false,
        insecure_default_credentials: true,
        replication: standalone_replication("test-node"),
        read_index: Arc::new(CommittedReadIndex::default()),
        replication_clients: Arc::new(ReplicationClientPool::default()),
        ha_write_coordinator: None,
        replication_fanout_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        replication_apply_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    }
}

async fn issue_command<S>(stream: &mut S, request_id: u128, command: Command) -> transport::Response
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = Request::from_command(id(request_id), command).unwrap();
    write_request_to_async(stream, &request).await.unwrap();
    timeout(Duration::from_secs(20), read_response_from_async(stream))
        .await
        .unwrap()
        .unwrap()
}

async fn wait_for_writable_leader(
    addrs: &[SocketAddr],
    excluded: Option<usize>,
    deadline: Instant,
    request_id_base: u128,
    probe_key: &str,
) -> usize {
    let mut last_probe_results = vec![String::from("unprobed"); addrs.len()];
    let mut previous_candidate: Option<(usize, u64)> = None;
    loop {
        let mut leader_candidates = Vec::new();
        for (idx, addr) in addrs.iter().enumerate() {
            if excluded == Some(idx) {
                last_probe_results[idx] = "excluded".to_string();
                continue;
            }
            let mut stream = connect_tcp(*addr).await;
            authenticate(&mut stream).await;
            let cluster = issue_command(
                &mut stream,
                request_id_base + idx as u128,
                Command::ShowCluster,
            )
            .await;
            if cluster.status == Status::Ok {
                let entries = cluster.decode_entries().unwrap_or_default();
                let lookup = |key: &str| {
                    entries
                        .iter()
                        .find_map(|(entry_key, value)| (entry_key == key).then(|| value.clone()))
                        .unwrap_or_else(|| "unknown".to_string())
                };
                let role = lookup("role");
                let leader = lookup("leader_node_id");
                let term = lookup("current_term");
                let parsed_term = term.parse::<u64>().unwrap_or(0);
                let commit = lookup("commit_index");
                let applied = lookup("last_applied_index");
                let health = lookup("health");
                last_probe_results[idx] = format!(
                    "cluster_status=Ok role={role} term={term} leader={leader} commit={commit} applied={applied} health={health}"
                );
                if role == "leader" {
                    leader_candidates.push((idx, *addr, parsed_term));
                }
            } else {
                last_probe_results[idx] = format!("cluster_status={:?}", cluster.status);
            }
        }

        leader_candidates.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        if let Some((idx, addr, term)) = leader_candidates.into_iter().next() {
            if previous_candidate != Some((idx, term)) {
                previous_candidate = Some((idx, term));
                sleep(Duration::from_millis(150)).await;
                continue;
            }
            let mut stream = connect_tcp(addr).await;
            authenticate(&mut stream).await;
            let response = issue_command(
                &mut stream,
                request_id_base + 1_000 + idx as u128,
                Command::Set {
                    key: probe_key.to_string(),
                    value: format!("leader-{idx}"),
                    options: SetOptions::default(),
                },
            )
            .await;
            let response_error = response
                .decode_error()
                .ok()
                .map(|payload| format!("{}:{}", payload.code, payload.name));
            last_probe_results[idx] = format!(
                "{} probe_set={:?}{}",
                last_probe_results[idx],
                response.status,
                response_error
                    .as_ref()
                    .map(|value| format!(" error={value}"))
                    .unwrap_or_default()
            );
            if response.status == Status::Ok {
                return idx;
            }
            previous_candidate = None;
        }

        if Instant::now() >= deadline {
            let mut cluster_views = Vec::new();
            for (idx, addr) in addrs.iter().enumerate() {
                if excluded == Some(idx) {
                    continue;
                }
                let mut stream = connect_tcp(*addr).await;
                authenticate(&mut stream).await;
                let cluster = issue_command(
                    &mut stream,
                    request_id_base + 10_000 + idx as u128,
                    Command::ShowCluster,
                )
                .await;
                let summary = if cluster.status == Status::Ok {
                    let entries = cluster.decode_entries().unwrap_or_default();
                    let lookup = |key: &str| {
                        entries
                            .iter()
                            .find_map(|(entry_key, value)| {
                                (entry_key == key).then(|| value.clone())
                            })
                            .unwrap_or_else(|| "unknown".to_string())
                    };
                    format!(
                        "idx={idx} probe={} role={} term={} leader={} commit={} applied={} health={}",
                        last_probe_results[idx],
                        lookup("role"),
                        lookup("current_term"),
                        lookup("leader_node_id"),
                        lookup("commit_index"),
                        lookup("last_applied_index"),
                        lookup("health"),
                    )
                } else {
                    format!(
                        "idx={idx} probe={} show_cluster_status={:?}",
                        last_probe_results[idx], cluster.status
                    )
                };
                cluster_views.push(summary);
            }
            panic!(
                "no writable leader became available before timeout: {}",
                cluster_views.join(" | ")
            );
        }
        sleep(Duration::from_millis(150)).await;
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
            ..EngineOptions::default()
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
async fn rejects_old_v1_protocol_frames_before_handshake() {
    let root = temp_dir("old-protocol-rejected");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime(None))
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    let mut v1_frame = transport::encode_request(
        &Request::from_command(
            id(1),
            Command::Ping {
                message: Some("old".to_string()),
            },
        )
        .unwrap(),
    )
    .unwrap();
    v1_frame[..4].copy_from_slice(b"VTP1");
    stream.write_all(&v1_frame).await.unwrap();
    let response = timeout(
        Duration::from_secs(2),
        read_response_from_async(&mut stream),
    )
    .await;
    assert!(response.is_err() || response.unwrap().is_err());

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negotiates_compression_none_when_client_requests_it() {
    let root = temp_dir("compression-negotiation");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime(None))
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();
    let mut hello = ClientHello::new("tcp-integration-test", "0.3.0");
    hello.desired_compression = CompressionMode::None;
    write_client_hello_to_async(&mut stream, &hello)
        .await
        .unwrap();
    let server_hello = read_server_hello_from_async(&mut stream).await.unwrap();
    assert_eq!(server_hello.status, Status::Ok);
    assert_eq!(server_hello.compression, CompressionMode::None);

    let options = CodecOptions {
        compression: CompressionMode::None,
        compression_threshold_bytes: 0,
        max_frame_len: server_hello.max_frame_len as usize,
        max_decompressed_frame_len: server_hello.max_frame_len as usize,
    };
    let ping = Request::from_command(
        id(2),
        Command::Ping {
            message: Some("hello".to_string()),
        },
    )
    .unwrap();
    transport::write_request_to_async_with_options(&mut stream, &ping, options)
        .await
        .unwrap();
    let response = read_response_from_async_with_options(&mut stream, options)
        .await
        .unwrap();
    assert_eq!(response.decode_value().unwrap(), "hello");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_expired_request_deadline_metadata() {
    let root = temp_dir("deadline-rejected");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
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
        id(7),
        Command::Ping {
            message: Some("late".to_string()),
        },
    )
    .unwrap()
    .with_metadata(RequestMetadata {
        deadline_ms: Some(0),
        trace_id: None,
        sequence: Some(1),
    });
    write_request_to_async(&mut stream, &request).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Error);
    assert_eq!(response.decode_error().unwrap().code, "TRN-016");

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
            ..EngineOptions::default()
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
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let runtime = runtime(None);
    let backup_dir = runtime.backup_dir.clone();
    let server = Server::with_engine("127.0.0.1".to_string(), 0, 16, engine, runtime)
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
                expiration: Some(Expiration::Ex(60)),
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
            expiration: Some(Expiration::Ex(60)),
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

    let info = Request::from_command(id(31), Command::Info).unwrap();
    write_request_to_async(&mut stream, &info).await.unwrap();
    let info_response = read_response_from_async(&mut stream).await.unwrap();
    let info_entries = info_response.decode_entries().unwrap();
    assert!(
        info_entries
            .iter()
            .any(|(key, value)| key == "transport.protocol_version" && value == "2")
    );
    assert!(
        info_entries
            .iter()
            .any(|(key, _)| key == "persistence.wal_size_bytes")
    );

    let backup = Request::from_command(id(32), Command::Backup).unwrap();
    write_request_to_async(&mut stream, &backup).await.unwrap();
    let backup_dump = read_response_from_async(&mut stream)
        .await
        .unwrap()
        .decode_value()
        .unwrap();
    assert!(backup_dump.contains("user:1"));

    let backup_to = Request::from_command(
        id(321),
        Command::BackupTo {
            path: "tcp-backup.json".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &backup_to)
        .await
        .unwrap();
    let backup_to_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(backup_to_response.status, Status::Ok);
    assert!(backup_dir.join("tcp-backup.json").exists());
    assert!(backup_dir.join("tcp-backup.json.manifest.json").exists());

    let backup_verify = Request::from_command(
        id(323),
        Command::BackupVerifyFrom {
            path: "tcp-backup.json".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &backup_verify)
        .await
        .unwrap();
    let backup_verify_response = read_response_from_async(&mut stream).await.unwrap();
    let backup_verify_entries = backup_verify_response.decode_entries().unwrap();
    assert!(
        backup_verify_entries
            .iter()
            .any(|(key, value)| key == "status" && value == "ok")
    );
    assert!(
        backup_verify_entries
            .iter()
            .any(|(key, value)| key == "entries" && value == "1")
    );

    let restore_check = Request::from_command(
        id(322),
        Command::RestoreCheckFrom {
            path: "tcp-backup.json".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &restore_check)
        .await
        .unwrap();
    let restore_check_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(restore_check_response.decode_count().unwrap(), 1);

    let metrics_prom = Request::from_command(id(324), Command::MetricsProm).unwrap();
    write_request_to_async(&mut stream, &metrics_prom)
        .await
        .unwrap();
    let metrics_prom_response = read_response_from_async(&mut stream).await.unwrap();
    let metrics_body = metrics_prom_response.decode_value().unwrap();
    assert!(metrics_body.contains("# HELP vaylix_server_request_count"));
    assert!(metrics_body.contains("# TYPE vaylix_server_connection_active gauge"));

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

    let restore = Request::from_command(id(33), Command::Restore { dump: backup_dump }).unwrap();
    write_request_to_async(&mut stream, &restore).await.unwrap();
    let restore_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(restore_response.decode_count().unwrap(), 1);

    let restored_get = Request::from_command(
        id(34),
        Command::Get {
            key: "user:1".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &restored_get)
        .await
        .unwrap();
    let restored_get_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(restored_get_response.decode_value().unwrap(), "alice");

    server_task.abort();
    fs::remove_dir_all(root).ok();
    fs::remove_dir_all(backup_dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enforces_rbac_over_tcp() {
    let root = temp_dir("tcp-rbac");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
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

    for (request_id, command) in [
        (
            id(41),
            Command::CreateUser {
                username: "alice".to_string(),
                password: "password1234".to_string(),
            },
        ),
        (
            id(42),
            Command::CreateRole {
                role: "readonly".to_string(),
            },
        ),
        (
            id(43),
            Command::GrantPermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string(),
            },
        ),
        (
            id(44),
            Command::GrantRole {
                role: "readonly".to_string(),
                username: "alice".to_string(),
            },
        ),
    ] {
        let request = Request::from_command(request_id, command).unwrap();
        write_request_to_async(&mut stream, &request).await.unwrap();
        let response = read_response_from_async(&mut stream).await.unwrap();
        assert_eq!(response.status, Status::Ok);
    }

    let auth_alice = Request::from_command(
        id(45),
        Command::Auth {
            username: "alice".to_string(),
            password: "password1234".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &auth_alice)
        .await
        .unwrap();
    let auth_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(auth_response.status, Status::Ok);

    let read = Request::from_command(
        id(46),
        Command::Get {
            key: "app:missing".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &read).await.unwrap();
    let read_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(read_response.status, Status::NotFound);

    let show_own_grants = Request::from_command(id(460), Command::ShowGrants).unwrap();
    write_request_to_async(&mut stream, &show_own_grants)
        .await
        .unwrap();
    let show_own_grants_response = read_response_from_async(&mut stream).await.unwrap();
    let own_grants = show_own_grants_response.decode_entries().unwrap();
    assert!(
        own_grants
            .iter()
            .any(|(_, value)| value == "role=readonly read on app:*")
    );

    let out_of_pattern_read = Request::from_command(
        id(461),
        Command::Get {
            key: "other:missing".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &out_of_pattern_read)
        .await
        .unwrap();
    let out_of_pattern_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(out_of_pattern_response.status, Status::Error);
    assert_eq!(
        out_of_pattern_response.decode_error().unwrap().code,
        "SRV-017"
    );

    let write = Request::from_command(
        id(47),
        Command::Set {
            key: "locked".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &write).await.unwrap();
    let write_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(write_response.status, Status::Error);
    assert_eq!(write_response.decode_error().unwrap().code, "SRV-017");

    let mut admin_stream = connect_tcp(addr).await;
    authenticate(&mut admin_stream).await;
    let rotate = Request::from_command(
        id(48),
        Command::AlterUserPassword {
            username: "alice".to_string(),
            password: "newpassword1234".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut admin_stream, &rotate)
        .await
        .unwrap();
    let rotate_response = read_response_from_async(&mut admin_stream).await.unwrap();
    assert_eq!(rotate_response.status, Status::Ok);

    let show_role_grants = Request::from_command(
        id(481),
        Command::ShowGrantsForRole {
            role: "readonly".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut admin_stream, &show_role_grants)
        .await
        .unwrap();
    let show_role_grants_response = read_response_from_async(&mut admin_stream).await.unwrap();
    let role_grants = show_role_grants_response.decode_entries().unwrap();
    assert!(
        role_grants
            .iter()
            .any(|(_, value)| value == "read on app:*")
    );

    let existing_session_read = Request::from_command(
        id(49),
        Command::Get {
            key: "app:still-authenticated".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &existing_session_read)
        .await
        .unwrap();
    let existing_session_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(existing_session_response.status, Status::NotFound);

    let mut old_password_stream = connect_tcp(addr).await;
    let old_auth = Request::from_command(
        id(50),
        Command::Auth {
            username: "alice".to_string(),
            password: "password1234".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut old_password_stream, &old_auth)
        .await
        .unwrap();
    let old_auth_response = read_response_from_async(&mut old_password_stream)
        .await
        .unwrap();
    assert_eq!(old_auth_response.status, Status::Error);

    let mut new_password_stream = connect_tcp(addr).await;
    let new_auth = Request::from_command(
        id(51),
        Command::Auth {
            username: "alice".to_string(),
            password: "newpassword1234".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut new_password_stream, &new_auth)
        .await
        .unwrap();
    let new_auth_response = read_response_from_async(&mut new_password_stream)
        .await
        .unwrap();
    assert_eq!(new_auth_response.status, Status::Ok);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserves_request_ids_for_pipelined_commands() {
    let root = temp_dir("pipelined");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
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

    let set_id = id(11);
    let get_id = id(12);
    let set = Request::from_command(
        set_id,
        Command::Set {
            key: "pipe:key".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .unwrap()
    .with_metadata(RequestMetadata {
        deadline_ms: Some(5_000),
        trace_id: Some(id(99)),
        sequence: Some(1),
    });
    let get = Request::from_command(
        get_id,
        Command::Get {
            key: "pipe:key".to_string(),
        },
    )
    .unwrap()
    .with_metadata(RequestMetadata {
        deadline_ms: Some(5_000),
        trace_id: Some(id(99)),
        sequence: Some(2),
    });

    let mut bytes = transport::encode_request(&set).unwrap();
    bytes.extend_from_slice(&transport::encode_request(&get).unwrap());
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();

    let set_response = read_response_from_async(&mut stream).await.unwrap();
    let get_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(set_response.request_id, set_id);
    assert_eq!(get_response.request_id, get_id);
    assert_eq!(get_response.decode_value().unwrap(), "value");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_sequence_marked_requests_inside_transactions() {
    let root = temp_dir("transaction-pipeline-rejected");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
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

    let multi = Request::from_command(id(21), Command::Multi).unwrap();
    write_request_to_async(&mut stream, &multi).await.unwrap();
    assert_eq!(
        read_response_from_async(&mut stream).await.unwrap().status,
        Status::Ok
    );

    let queued = Request::from_command(
        id(22),
        Command::Set {
            key: "tx:key".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .unwrap()
    .with_metadata(RequestMetadata {
        deadline_ms: None,
        trace_id: None,
        sequence: Some(2),
    });
    write_request_to_async(&mut stream, &queued).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Error);
    assert_eq!(response.decode_error().unwrap().code, "TRN-018");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_snapshotter_writes_snapshot_and_flushes_wal() {
    let root = temp_dir("snapshotter");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths.clone(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
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

    let snapshot_path = paths.snapshot_path.clone();
    let manifest_path = paths.manifest_path.clone();
    for _ in 0..100 {
        if snapshot_path.exists() && manifest_path.exists() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(
        snapshot_path.exists(),
        "expected snapshot at {}",
        snapshot_path.display()
    );
    assert!(
        manifest_path.exists(),
        "expected manifest at {}",
        manifest_path.display()
    );

    let wal_dir = paths.wal_dir.clone();
    let mut wal_report = inspect_wal(&wal_dir).unwrap();
    for _ in 0..100 {
        if wal_report.sealed_segment_count >= 1 && wal_report.active_segment_count == 1 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
        wal_report = inspect_wal(&wal_dir).unwrap();
    }
    assert!(
        wal_dir.exists(),
        "expected wal dir at {}",
        wal_dir.display()
    );
    assert!(
        wal_report.sealed_segment_count >= 1,
        "expected at least one sealed segment, got {:?}",
        wal_report
    );
    assert_eq!(wal_report.active_segment_count, 1, "{wal_report:?}");

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
            ..EngineOptions::default()
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
async fn accepts_mutual_tls_connections_with_valid_client_certificate() {
    let root = temp_dir("mtls-valid");
    let mtls = mutual_tls_config_for(&root);

    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        runtime_with_tls(None, Some(mtls.server_config)),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut tls_stream = connect_mutual_tls(
        addr,
        &mtls.ca_pem,
        &mtls.client_cert_pem,
        &mtls.client_key_pem,
    )
    .await;

    authenticate(&mut tls_stream).await;

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_tls_clients_without_certificate_when_mutual_tls_is_required() {
    let root = temp_dir("mtls-missing-client-cert");
    let mtls = mutual_tls_config_for(&root);

    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("tcp-test-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        runtime_with_tls(None, Some(mtls.server_config)),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let tls_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(root_store_from_pem(&mtls.ca_pem))
            .with_no_client_auth(),
    );
    let mut tls_stream = connect_tls_with_config(addr, tls_config).await.unwrap();
    let auth = Request::from_command(
        id(1),
        Command::Auth {
            username: "vaylix".to_string(),
            password: "vaylix".to_string(),
        },
    )
    .unwrap();
    let write_result = write_request_to_async(&mut tls_stream, &auth).await;
    if write_result.is_ok() {
        let read_result = timeout(
            Duration::from_secs(2),
            transport::read_response_from_async(&mut tls_stream),
        )
        .await;
        assert!(read_result.is_err() || read_result.unwrap().is_err());
    }

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
            ..EngineOptions::default()
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
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let mut runtime = runtime(None);
    // AUTH consumes the only burst token. Disable refill so the next request
    // deterministically exercises the network rate-limit error path.
    runtime.guards.requests_per_second = 0;
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
            ..EngineOptions::default()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicates_wal_to_follower_and_requires_replica_ack() {
    let leader_root = temp_dir("leader-replication");
    let follower_root = temp_dir("follower-replication");
    let leader_engine = Engine::from_paths_with_options(
        Paths::from_data_dir(&leader_root).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("leader-repl-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let follower_engine = Engine::from_paths_with_options(
        Paths::from_data_dir(&follower_root).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("follower-repl-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let mut leader_runtime = runtime_without_auth(None);
    let leader_cluster_dir = temp_dir("leader-cluster-state");
    leader_runtime.replication = Arc::new(
        ReplicationRuntime::new(ReplicationConfig {
            node_id: "leader-node".to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: None,
            role: ReplicationRole::Leader,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::Replica,
            ack_timeout: Duration::from_secs(3),
            poll_interval: Duration::from_millis(100),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(250),
            election_timeout_max: Duration::from_millis(500),
            state_path: leader_cluster_dir.join("cluster-state.json"),
            state_tmp_path: leader_cluster_dir.join("cluster-state.json.tmp"),
            initial_members: Vec::new(),
        })
        .unwrap(),
    );
    let leader = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        leader_engine,
        leader_runtime,
    )
    .await
    .unwrap();
    let leader_addr = leader.local_addr().unwrap();
    let leader_task = tokio::spawn(async move { leader.start().await });

    let mut follower_runtime = runtime_without_auth(None);
    let follower_cluster_dir = temp_dir("follower-cluster-state");
    follower_runtime.replication = Arc::new(
        ReplicationRuntime::new(ReplicationConfig {
            node_id: "follower-node".to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: None,
            role: ReplicationRole::Follower,
            upstream: Some(leader_addr.to_string()),
            upstream_username: Some("vaylix".to_string()),
            upstream_password: Some("vaylix".to_string()),
            write_ack_mode: WriteAckMode::Local,
            ack_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(100),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(5),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(250),
            election_timeout_max: Duration::from_millis(500),
            state_path: follower_cluster_dir.join("cluster-state.json"),
            state_tmp_path: follower_cluster_dir.join("cluster-state.json.tmp"),
            initial_members: Vec::new(),
        })
        .unwrap(),
    );
    let follower = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        follower_engine,
        follower_runtime,
    )
    .await
    .unwrap();
    let follower_addr = follower.local_addr().unwrap();
    let follower_task = tokio::spawn(async move { follower.start().await });

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut stream = connect_tcp(leader_addr).await;
        authenticate(&mut stream).await;
        let response = issue_command(&mut stream, 40_000, Command::ShowReplication).await;
        let entries = response.decode_entries().unwrap();
        let known_followers = entries
            .iter()
            .find_map(|(key, value)| (key == "known_followers").then(|| value.clone()))
            .unwrap();
        if known_followers == "1" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "follower never registered with leader"
        );
        sleep(Duration::from_millis(100)).await;
    }

    let mut leader_stream = connect_tcp(leader_addr).await;
    authenticate(&mut leader_stream).await;
    let set = issue_command(
        &mut leader_stream,
        40_001,
        Command::Set {
            key: "repl:key".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .await;
    assert_eq!(
        set.status,
        Status::Ok,
        "initial leader write failed: {:?}",
        String::from_utf8_lossy(&set.payload)
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut follower_stream = connect_tcp(follower_addr).await;
        authenticate(&mut follower_stream).await;
        let response = issue_command(
            &mut follower_stream,
            40_002,
            Command::Get {
                key: "repl:key".to_string(),
            },
        )
        .await;
        if response.status == Status::Ok && response.decode_value().unwrap() == "value" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "follower never applied replicated write"
        );
        sleep(Duration::from_millis(100)).await;
    }

    follower_task.abort();
    leader_task.abort();
    fs::remove_dir_all(leader_root).ok();
    fs::remove_dir_all(follower_root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_leader_fails_over_and_keeps_replicating() {
    let _ha_test_lock = acquire_ha_test_lock().await;
    let node1_addr = reserve_local_addr();
    let node2_addr = reserve_local_addr();
    let node3_addr = reserve_local_addr();
    let members = vec![
        ClusterMember {
            node_id: "node-1".to_string(),
            advertise_addr: node1_addr.to_string(),
            voter: true,
        },
        ClusterMember {
            node_id: "node-2".to_string(),
            advertise_addr: node2_addr.to_string(),
            voter: true,
        },
        ClusterMember {
            node_id: "node-3".to_string(),
            advertise_addr: node3_addr.to_string(),
            voter: true,
        },
    ];

    let root1 = temp_dir("ha-node-1");
    let root2 = temp_dir("ha-node-2");
    let root3 = temp_dir("ha-node-3");

    let engine1 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root1).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-node-1-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let engine2 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root2).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-node-2-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let engine3 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root3).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-node-3-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let mut runtime1 = runtime_without_auth(None);
    runtime1.replication =
        clustered_replication("node-1", ReplicationRole::Leader, node1_addr, &members);
    let mut runtime2 = runtime_without_auth(None);
    runtime2.replication =
        clustered_replication("node-2", ReplicationRole::Follower, node2_addr, &members);
    let mut runtime3 = runtime_without_auth(None);
    runtime3.replication =
        clustered_replication("node-3", ReplicationRole::Follower, node3_addr, &members);

    let server1 = Server::with_engine(
        "127.0.0.1".to_string(),
        node1_addr.port(),
        16,
        engine1,
        runtime1,
    )
    .await
    .unwrap();
    let server2 = Server::with_engine(
        "127.0.0.1".to_string(),
        node2_addr.port(),
        16,
        engine2,
        runtime2,
    )
    .await
    .unwrap();
    let server3 = Server::with_engine(
        "127.0.0.1".to_string(),
        node3_addr.port(),
        16,
        engine3,
        runtime3,
    )
    .await
    .unwrap();

    let task1 = tokio::spawn(async move { server1.start().await });
    let task2 = tokio::spawn(async move { server2.start().await });
    let task3 = tokio::spawn(async move { server3.start().await });
    let tasks = [task1, task2, task3];
    let addrs = [node1_addr, node2_addr, node3_addr];

    let initial_leader = wait_for_writable_leader(
        &addrs,
        None,
        Instant::now() + Duration::from_secs(10),
        49_900,
        "ha:key:probe",
    )
    .await;

    let mut leader_stream = connect_tcp(addrs[initial_leader]).await;
    authenticate(&mut leader_stream).await;
    let set = issue_command(
        &mut leader_stream,
        50_001,
        Command::Set {
            key: "ha:key:1".to_string(),
            value: "value-1".to_string(),
            options: SetOptions::default(),
        },
    )
    .await;
    assert_eq!(
        set.status,
        Status::Ok,
        "initial HA leader write failed: {:?}",
        String::from_utf8_lossy(&set.payload)
    );

    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut replicated = 0usize;
        for (idx, addr) in addrs.iter().enumerate() {
            if idx == initial_leader {
                continue;
            }
            let mut stream = connect_tcp(*addr).await;
            authenticate(&mut stream).await;
            let response = issue_command(
                &mut stream,
                50_002 + idx as u128,
                Command::Get {
                    key: "ha:key:1".to_string(),
                },
            )
            .await;
            if response.status == Status::Ok && response.decode_value().unwrap() == "value-1" {
                replicated += 1;
            }
        }
        if replicated == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "initial leader write did not replicate to both followers"
        );
        sleep(Duration::from_millis(100)).await;
    }

    tasks[initial_leader].abort();

    let survivor_indices = [0usize, 1, 2]
        .into_iter()
        .filter(|idx| *idx != initial_leader)
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(12);
    let new_leader = loop {
        let mut elected = None;
        for idx in &survivor_indices {
            let mut stream = connect_tcp(addrs[*idx]).await;
            authenticate(&mut stream).await;
            let response = issue_command(
                &mut stream,
                50_100 + *idx as u128,
                Command::Set {
                    key: "ha:key:2".to_string(),
                    value: "value-2".to_string(),
                    options: SetOptions::default(),
                },
            )
            .await;
            if response.status == Status::Ok {
                elected = Some(*idx);
                break;
            }
        }
        if let Some(idx) = elected {
            break idx;
        }
        assert!(
            Instant::now() < deadline,
            "no surviving node accepted writes after leader failure"
        );
        sleep(Duration::from_millis(200)).await;
    };

    let follower_idx = [0usize, 1, 2]
        .into_iter()
        .find(|idx| *idx != initial_leader && *idx != new_leader)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut follower_stream = connect_tcp(addrs[follower_idx]).await;
        authenticate(&mut follower_stream).await;
        let response = issue_command(
            &mut follower_stream,
            50_101,
            Command::Get {
                key: "ha:key:2".to_string(),
            },
        )
        .await;
        if response.status == Status::Ok && response.decode_value().unwrap() == "value-2" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "post-failover leader write did not replicate to surviving follower"
        );
        sleep(Duration::from_millis(100)).await;
    }

    for (idx, task) in tasks.into_iter().enumerate() {
        if idx != initial_leader {
            task.abort();
        }
    }
    fs::remove_dir_all(root1).ok();
    fs::remove_dir_all(root2).ok();
    fs::remove_dir_all(root3).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_leader_rejoins_as_follower_and_catches_up() {
    let _ha_test_lock = acquire_ha_test_lock().await;
    let node1_addr = reserve_local_addr();
    let node2_addr = reserve_local_addr();
    let node3_addr = reserve_local_addr();
    let members = vec![
        ClusterMember {
            node_id: "node-1".to_string(),
            advertise_addr: node1_addr.to_string(),
            voter: true,
        },
        ClusterMember {
            node_id: "node-2".to_string(),
            advertise_addr: node2_addr.to_string(),
            voter: true,
        },
        ClusterMember {
            node_id: "node-3".to_string(),
            advertise_addr: node3_addr.to_string(),
            voter: true,
        },
    ];

    let root1 = temp_dir("ha-rejoin-node-1");
    let root2 = temp_dir("ha-rejoin-node-2");
    let root3 = temp_dir("ha-rejoin-node-3");
    let state1 = temp_dir("ha-rejoin-state-1");
    let state2 = temp_dir("ha-rejoin-state-2");
    let state3 = temp_dir("ha-rejoin-state-3");
    let roots = [root1.clone(), root2.clone(), root3.clone()];
    let state_dirs = [state1.clone(), state2.clone(), state3.clone()];
    let node_ids = ["node-1", "node-2", "node-3"];

    let engine1 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root1).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-rejoin-node-1-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let engine2 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root2).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-rejoin-node-2-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();
    let engine3 = Engine::from_paths_with_options(
        Paths::from_data_dir(&root3).unwrap(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            keyring: Some(test_keyring("ha-rejoin-node-3-key")),
            ..EngineOptions::default()
        },
    )
    .unwrap();

    let mut runtime1 = runtime_without_auth(None);
    runtime1.replication = clustered_replication_with_state_dir(
        "node-1",
        ReplicationRole::Leader,
        node1_addr,
        &members,
        &state1,
    );
    let mut runtime2 = runtime_without_auth(None);
    runtime2.replication = clustered_replication_with_state_dir(
        "node-2",
        ReplicationRole::Follower,
        node2_addr,
        &members,
        &state2,
    );
    let mut runtime3 = runtime_without_auth(None);
    runtime3.replication = clustered_replication_with_state_dir(
        "node-3",
        ReplicationRole::Follower,
        node3_addr,
        &members,
        &state3,
    );

    let server1 = Server::with_engine(
        "127.0.0.1".to_string(),
        node1_addr.port(),
        16,
        engine1,
        runtime1,
    )
    .await
    .unwrap();
    let server2 = Server::with_engine(
        "127.0.0.1".to_string(),
        node2_addr.port(),
        16,
        engine2,
        runtime2,
    )
    .await
    .unwrap();
    let server3 = Server::with_engine(
        "127.0.0.1".to_string(),
        node3_addr.port(),
        16,
        engine3,
        runtime3,
    )
    .await
    .unwrap();

    let mut tasks = vec![
        tokio::spawn(async move { server1.start().await }),
        tokio::spawn(async move { server2.start().await }),
        tokio::spawn(async move { server3.start().await }),
    ];
    let addrs = [node1_addr, node2_addr, node3_addr];

    let _initial_probe_leader = wait_for_writable_leader(
        &addrs,
        None,
        Instant::now() + Duration::from_secs(10),
        60_000,
        "ha:rejoin:probe",
    )
    .await;

    let mut initial_leader = wait_for_writable_leader(
        &addrs,
        None,
        Instant::now() + Duration::from_secs(5),
        61_000,
        "ha:rejoin:stable-probe",
    )
    .await;

    loop {
        let mut leader_stream = connect_tcp(addrs[initial_leader]).await;
        authenticate(&mut leader_stream).await;
        let set = issue_command(
            &mut leader_stream,
            60_010,
            Command::Set {
                key: "ha:rejoin:key:1".to_string(),
                value: "value-1".to_string(),
                options: SetOptions::default(),
            },
        )
        .await;
        if set.status == Status::Ok {
            break;
        }
        initial_leader = wait_for_writable_leader(
            &addrs,
            None,
            Instant::now() + Duration::from_secs(5),
            62_000,
            "ha:rejoin:retry-probe",
        )
        .await;
    }

    tasks[initial_leader].abort();
    let _ = (&mut tasks[initial_leader]).await;
    sleep(Duration::from_millis(300)).await;

    let new_leader = wait_for_writable_leader(
        &addrs,
        Some(initial_leader),
        Instant::now() + Duration::from_secs(12),
        60_100,
        "ha:rejoin:key:2",
    )
    .await;

    let restarted_server = {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let restarted_engine = Engine::from_paths_with_options(
                Paths::from_data_dir(&roots[initial_leader]).unwrap(),
                EngineOptions {
                    wal_sync: WalSyncPolicy::Flush,
                    keyring: Some(test_keyring(&format!(
                        "ha-rejoin-{}-key",
                        node_ids[initial_leader]
                    ))),
                    ..EngineOptions::default()
                },
            )
            .unwrap();
            let mut restarted_runtime = runtime_without_auth(None);
            restarted_runtime.replication = clustered_replication_with_state_dir(
                node_ids[initial_leader],
                ReplicationRole::Leader,
                addrs[initial_leader],
                &members,
                &state_dirs[initial_leader],
            );
            match Server::with_engine(
                "127.0.0.1".to_string(),
                addrs[initial_leader].port(),
                16,
                restarted_engine,
                restarted_runtime,
            )
            .await
            {
                Ok(server) => break server,
                Err(server::ServerError::Bind(_)) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(150)).await;
                }
                Err(err) => panic!("failed to restart old leader: {err}"),
            }
        }
    };
    let restarted_task = tokio::spawn(async move { restarted_server.start().await });

    let mut restarted_stream = connect_tcp(addrs[initial_leader]).await;
    authenticate(&mut restarted_stream).await;
    let restarted_write = issue_command(
        &mut restarted_stream,
        60_200,
        Command::Set {
            key: "ha:rejoin:key:stale".to_string(),
            value: "bad".to_string(),
            options: SetOptions::default(),
        },
    )
    .await;
    assert_eq!(restarted_write.status, Status::Error);

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut stream = connect_tcp(addrs[initial_leader]).await;
        authenticate(&mut stream).await;
        let response = issue_command(
            &mut stream,
            60_201,
            Command::Get {
                key: "ha:rejoin:key:2".to_string(),
            },
        )
        .await;
        if response.status == Status::Ok
            && response.decode_value().unwrap() == format!("leader-{new_leader}")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted old leader never caught up to the surviving leader"
        );
        sleep(Duration::from_millis(150)).await;
    }

    let final_leader = wait_for_writable_leader(
        &addrs,
        None,
        Instant::now() + Duration::from_secs(10),
        60_300,
        "ha:rejoin:key:3",
    )
    .await;
    let follower_idx = [0usize, 1, 2]
        .into_iter()
        .find(|idx| *idx != final_leader && *idx != initial_leader)
        .unwrap_or(initial_leader);
    let final_write_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut survivor_stream = connect_tcp(addrs[final_leader]).await;
        authenticate(&mut survivor_stream).await;
        let set = issue_command(
            &mut survivor_stream,
            60_300,
            Command::Set {
                key: "ha:rejoin:key:3".to_string(),
                value: "value-3".to_string(),
                options: SetOptions::default(),
            },
        )
        .await;
        if set.status == Status::Ok {
            break;
        }
        assert!(
            Instant::now() < final_write_deadline,
            "post-rejoin leader never accepted the follow-up write"
        );
        sleep(Duration::from_millis(150)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut stream = connect_tcp(addrs[follower_idx]).await;
        authenticate(&mut stream).await;
        let follower_response = issue_command(
            &mut stream,
            60_301,
            Command::Get {
                key: "ha:rejoin:key:3".to_string(),
            },
        )
        .await;

        let mut restarted = connect_tcp(node1_addr).await;
        authenticate(&mut restarted).await;
        let restarted_response = issue_command(
            &mut restarted,
            60_302,
            Command::Get {
                key: "ha:rejoin:key:3".to_string(),
            },
        )
        .await;

        if follower_response.status == Status::Ok
            && restarted_response.status == Status::Ok
            && follower_response.decode_value().unwrap() == "value-3"
            && restarted_response.decode_value().unwrap() == "value-3"
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not converge after old leader rejoin"
        );
        sleep(Duration::from_millis(150)).await;
    }

    restarted_task.abort();
    for task in tasks {
        task.abort();
    }
}
