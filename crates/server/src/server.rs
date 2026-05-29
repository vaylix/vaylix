use std::future::Future;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use command::{
    Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
    SetOptions as CommandSetOptions,
};
use engine::{
    Engine, EngineOptions, Expiration, LogicalBackup, Paths, ScanPage, SetCondition, SetOptions,
    SetOutcome, StorageEngine, TransactionResult,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use transport::{
    CodecOptions, Request, Response, Status, TransportError, negotiate_server_options,
    read_client_hello_from_async, read_request_from_async_with_options,
    write_response_to_async_with_options, write_server_hello_to_async,
};
use uuid::Uuid;

use crate::audit::{AuditEvent, AuditLogger};
use crate::auth::{AuthConfig, Identity, Permission};
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;

const BACKUP_MANIFEST_VERSION: u32 = 1;
const BACKUP_HASH_ALGORITHM: &str = "sha256";

/// Runtime guardrails for request validation, quotas, and abuse controls.
#[derive(Debug, Clone)]
pub struct ServerGuards {
    pub max_request_payload_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_keys_per_batch: usize,
    pub max_transaction_queue_len: usize,
    pub requests_per_second: u32,
    pub request_burst: u32,
}

/// Runtime configuration for the async server.
#[derive(Clone)]
pub struct ServerRuntimeConfig {
    pub snapshot_interval: Option<Duration>,
    pub expiration_sweep_interval: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub auth_config: Option<AuthConfig>,
    pub guards: ServerGuards,
    pub tls_state: Option<Arc<crate::tls::TlsState>>,
    pub transport: CodecOptions,
    pub audit_logger: Arc<AuditLogger>,
    pub backup_dir: PathBuf,
    pub mtls_enabled: bool,
    pub slow_command_threshold: Option<Duration>,
    pub wal_segment_size_bytes: u64,
    pub wal_retain_segments: usize,
    pub auth_failure_window: Duration,
    pub auth_failure_limit: u32,
    pub auth_lockout: Duration,
    pub transaction_max_duration: Duration,
    pub maintenance: Arc<MaintenanceMode>,
    pub auth_lockouts: Arc<Mutex<AuthLockoutState>>,
    pub insecure_auth_disabled: bool,
    pub insecure_default_credentials: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackupManifest {
    manifest_version: u32,
    backup_version: u32,
    created_at_ms: u64,
    source_engine_version: u32,
    source_sequence: u64,
    entry_count: u64,
    byte_len: u64,
    hash_algorithm: String,
    sha256: String,
}

enum EngineRequest {
    Execute {
        request_id: Uuid,
        command: Command,
        respond_to: oneshot::Sender<Result<Response>>,
    },
    ExecuteBatch {
        request_id: Uuid,
        commands: Vec<Command>,
        respond_to: oneshot::Sender<Result<Vec<TransactionResult>>>,
    },
    Info {
        respond_to: oneshot::Sender<Result<Vec<(String, String)>>>,
    },
    Snapshot {
        respond_to: oneshot::Sender<Result<()>>,
    },
    ValidateBackup {
        dump: String,
        respond_to: oneshot::Sender<Result<usize>>,
    },
    SweepExpired {
        respond_to: oneshot::Sender<Result<usize>>,
    },
}

#[derive(Clone)]
struct EngineHandle {
    sender: mpsc::Sender<EngineRequest>,
}

impl EngineHandle {
    fn new(mut engine: Engine) -> Self {
        let (sender, mut receiver) = mpsc::channel(256);
        thread::spawn(move || {
            while let Some(request) = receiver.blocking_recv() {
                match request {
                    EngineRequest::Execute {
                        request_id,
                        command,
                        respond_to,
                    } => {
                        let _ = respond_to.send(execute_command(&mut engine, request_id, command));
                    }
                    EngineRequest::ExecuteBatch {
                        request_id: _request_id,
                        commands,
                        respond_to,
                    } => {
                        let _ = respond_to.send(
                            engine
                                .execute_transaction(&commands)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::Info { respond_to } => {
                        let _ = respond_to.send(engine.info().map_err(ServerError::from));
                    }
                    EngineRequest::Snapshot { respond_to } => {
                        let _ = respond_to.send(engine.snapshot().map_err(ServerError::from));
                    }
                    EngineRequest::ValidateBackup { dump, respond_to } => {
                        let _ = respond_to.send(
                            engine
                                .validate_logical_backup(&dump)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::SweepExpired { respond_to } => {
                        let _ = respond_to.send(engine.sweep_expired().map_err(ServerError::from));
                    }
                }
            }
        });
        Self { sender }
    }

    async fn execute(&self, request_id: Uuid, command: Command) -> Result<Response> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Execute {
                request_id,
                command,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    async fn execute_batch(
        &self,
        request_id: Uuid,
        commands: Vec<Command>,
    ) -> Result<Vec<TransactionResult>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ExecuteBatch {
                request_id,
                commands,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    async fn info(&self) -> Result<Vec<(String, String)>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Info { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    async fn snapshot(&self) -> Result<()> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Snapshot { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    async fn sweep_expired(&self) -> Result<usize> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::SweepExpired { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    async fn validate_backup(&self, dump: String) -> Result<usize> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ValidateBackup {
                dump,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }
}

#[derive(Clone)]
struct RateLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(requests_per_second: u32, burst: u32) -> Self {
        Self {
            capacity: burst as f64,
            tokens: burst as f64,
            refill_per_second: requests_per_second as f64,
            last: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

/// Asynchronous Tokio-based database server with shared engine runtime state.
pub struct Server {
    listener: TcpListener,
    engine: EngineHandle,
    connection_slots: Arc<Semaphore>,
    next_connection_id: AtomicU64,
    runtime: ServerRuntimeConfig,
    metrics: Arc<Metrics>,
}

struct SessionState {
    identity: Option<Identity>,
    transaction_queue: Vec<Command>,
    rate_limiter: RateLimiter,
    transaction_started_at_ms: Option<u64>,
}

impl SessionState {
    fn new(guards: &ServerGuards) -> Self {
        Self {
            identity: None,
            transaction_queue: Vec::new(),
            rate_limiter: RateLimiter::new(guards.requests_per_second, guards.request_burst),
            transaction_started_at_ms: None,
        }
    }

    fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }

    fn in_transaction(&self) -> bool {
        !self.transaction_queue.is_empty()
    }
}

pub struct MaintenanceMode {
    path: PathBuf,
    enabled: std::sync::atomic::AtomicBool,
}

impl MaintenanceMode {
    pub fn load(path: PathBuf) -> Result<Self> {
        Ok(Self {
            enabled: std::sync::atomic::AtomicBool::new(path.exists()),
            path,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set(&self, enabled: bool) -> Result<()> {
        if enabled {
            std::fs::write(&self.path, b"maintenance=on\n")?;
        } else if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        self.enabled.store(enabled, Ordering::Relaxed);
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Default)]
pub struct AuthLockoutState {
    records: std::collections::BTreeMap<String, AuthLockoutRecord>,
}

struct AuthLockoutRecord {
    first_failure_at_ms: u64,
    failure_count: u32,
    locked_until_ms: Option<u64>,
}

impl AuthLockoutState {
    fn active_lockout_count(&mut self, now_ms: u64) -> usize {
        self.records
            .retain(|_, record| record.locked_until_ms.unwrap_or(now_ms) > now_ms);
        self.records
            .values()
            .filter(|record| record.locked_until_ms.unwrap_or(0) > now_ms)
            .count()
    }

    fn remaining_lockout_seconds(&mut self, key: &str, now_ms: u64) -> Option<u64> {
        let remaining_ms = self
            .records
            .get(key)
            .and_then(|record| record.locked_until_ms)
            .and_then(|locked_until_ms| locked_until_ms.checked_sub(now_ms))?;
        Some(remaining_ms.div_ceil(1_000))
    }

    fn clear_success(&mut self, key: &str) {
        self.records.remove(key);
    }

    fn record_failure(
        &mut self,
        key: &str,
        now_ms: u64,
        failure_window: Duration,
        failure_limit: u32,
        lockout: Duration,
    ) -> bool {
        let record = self
            .records
            .entry(key.to_string())
            .or_insert(AuthLockoutRecord {
                first_failure_at_ms: now_ms,
                failure_count: 0,
                locked_until_ms: None,
            });

        if now_ms.saturating_sub(record.first_failure_at_ms) > failure_window.as_millis() as u64 {
            record.first_failure_at_ms = now_ms;
            record.failure_count = 0;
            record.locked_until_ms = None;
        }

        record.failure_count = record.failure_count.saturating_add(1);
        if record.failure_count >= failure_limit {
            record.locked_until_ms = Some(now_ms.saturating_add(lockout.as_millis() as u64));
            true
        } else {
            false
        }
    }
}

struct AuditContext<'a> {
    connection_id: u64,
    peer_addr: Option<SocketAddr>,
    session: &'a SessionState,
    request_id: Uuid,
    opcode: &'a str,
    status: Status,
    error_code: Option<String>,
    latency_ms: u128,
}

impl Server {
    /// Creates a new server instance bound to the given address.
    pub async fn new(
        bind: String,
        port: u16,
        max_connections: usize,
        paths: Paths,
        engine_options: EngineOptions,
        runtime: ServerRuntimeConfig,
    ) -> Result<Self> {
        let engine = Engine::from_paths_with_options(paths, engine_options)?;
        Self::with_engine(bind, port, max_connections, engine, runtime).await
    }

    /// Creates a server around an existing engine instance.
    pub async fn with_engine(
        bind: String,
        port: u16,
        max_connections: usize,
        mut engine: Engine,
        runtime: ServerRuntimeConfig,
    ) -> Result<Self> {
        let recovered_entries = engine
            .info()?
            .into_iter()
            .find_map(|(key, value)| {
                (key == "wal_entries_replayed_total").then(|| value.parse::<u64>().unwrap_or(0))
            })
            .unwrap_or(0);
        let addr = format!("{bind}:{port}");
        log_event(
            "INFO",
            "server.startup",
            &format!(
                "binding listener to {addr} max_connections={max_connections} snapshot_interval={:?} sweep_interval={:?} idle_timeout={:?} tls_enabled={} auth_required={} compression={}",
                runtime.snapshot_interval,
                runtime.expiration_sweep_interval,
                runtime.idle_timeout,
                runtime.tls_state.is_some(),
                runtime.auth_config.is_some(),
                runtime.transport.compression.as_str(),
            ),
        );

        let listener = TcpListener::bind(&addr).await.map_err(ServerError::Bind)?;
        let local_addr = listener.local_addr().map_err(ServerError::Bind)?;
        log_event(
            "INFO",
            "server.startup",
            &format!(
                "listener ready on {local_addr} auth_required={}",
                runtime.auth_config.is_some()
            ),
        );

        if runtime.insecure_auth_disabled {
            log_event(
                "WARN",
                "server.security",
                "authentication is disabled; this is unsafe outside trusted local testing",
            );
        }
        if runtime.insecure_default_credentials {
            log_event(
                "WARN",
                "server.security",
                "default bootstrap credentials are in use; override --user and --password for non-local deployments",
            );
        }
        if let Some(tls_state) = &runtime.tls_state {
            let metadata = tls_state.metadata_snapshot().await;
            if let Some(days_remaining) = metadata.cert_days_remaining
                && days_remaining <= 30
            {
                log_event(
                    "WARN",
                    "server.tls",
                    &format!("server certificate expires in {days_remaining} days"),
                );
            }
        }

        let metrics = Arc::new(Metrics::default());
        metrics
            .wal_entries_replayed_total
            .store(recovered_entries, Ordering::Relaxed);

        Ok(Self {
            listener,
            engine: EngineHandle::new(engine),
            connection_slots: Arc::new(Semaphore::new(max_connections)),
            next_connection_id: AtomicU64::new(1),
            runtime,
            metrics,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    pub async fn start(self) -> Result<()> {
        self.start_with_signal(tokio::signal::ctrl_c()).await
    }

    async fn start_with_signal<S>(self, shutdown_signal: S) -> Result<()>
    where
        S: Future<Output = std::result::Result<(), std::io::Error>>,
    {
        let Self {
            listener,
            engine,
            connection_slots,
            next_connection_id,
            runtime,
            metrics,
        } = self;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        if let Some(snapshot_interval) = runtime.snapshot_interval {
            spawn_snapshotter(
                engine.clone(),
                Arc::clone(&metrics),
                snapshot_interval,
                shutdown_rx.clone(),
            );
        }

        if let Some(sweep_interval) = runtime.expiration_sweep_interval {
            spawn_expiration_sweeper(
                engine.clone(),
                Arc::clone(&metrics),
                sweep_interval,
                shutdown_rx.clone(),
            );
        }

        spawn_tls_reloader(runtime.clone(), shutdown_rx.clone());

        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                signal = &mut shutdown_signal => {
                    match signal {
                        Ok(()) => log_event("INFO", "server.shutdown", "shutdown signal received"),
                        Err(err) => log_event("ERROR", "server.shutdown", &format!("failed to receive shutdown signal: {err}")),
                    }
                    let _ = shutdown_tx.send(true);
                    engine.snapshot().await?;
                    log_event("INFO", "server.shutdown", "final snapshot completed");
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            metrics.accepted_connections.fetch_add(1, Ordering::Relaxed);
                            metrics.active_connections.fetch_add(1, Ordering::Relaxed);
                            let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                            let permit = connection_slots
                                .clone()
                                .acquire_owned()
                                .await
                                .map_err(|_| ServerError::ConnectionPoolClosed)?;
                            let connection_shutdown = shutdown_rx.clone();
                            let engine = engine.clone();
                            let metrics = Arc::clone(&metrics);
                            let runtime = runtime.clone();

                            log_connection_event("INFO", connection_id, Some(peer_addr), "accepted client");

                            tokio::spawn(async move {
                                let _permit = permit;
                                let result = if let Some(tls_state) = runtime.tls_state.clone() {
                                    let acceptor = tokio_rustls::TlsAcceptor::from(
                                        tls_state.server_config().await,
                                    );
                                    match acceptor.accept(stream).await {
                                        Ok(stream) => {
                                            handle_client(
                                                engine,
                                                Arc::clone(&metrics),
                                                runtime,
                                                connection_id,
                                                Some(peer_addr),
                                                stream,
                                                connection_shutdown,
                                            ).await
                                        }
                                        Err(err) => Err(ServerError::TlsHandshake(std::io::Error::other(err))),
                                    }
                                } else {
                                    handle_client(
                                        engine,
                                        Arc::clone(&metrics),
                                        runtime,
                                        connection_id,
                                        Some(peer_addr),
                                        stream,
                                        connection_shutdown,
                                    ).await
                                };

                                match result {
                                    Ok(()) => log_connection_event("INFO", connection_id, Some(peer_addr), "client disconnected"),
                                    Err(err) => log_connection_event("ERROR", connection_id, Some(peer_addr), &format!("[{}] {}: {err}", err.code(), err.name())),
                                }

                                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                                metrics.completed_connections.fetch_add(1, Ordering::Relaxed);
                            });
                        }
                        Err(err) => {
                            let err = ServerError::Accept(err);
                            log_event("ERROR", "server.accept", &format!("[{}] {}: {err}", err.code(), err.name()));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn spawn_snapshotter(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = interval(every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match engine.snapshot().await {
                        Ok(()) => {
                            metrics.snapshots_completed.fetch_add(1, Ordering::Relaxed);
                            log_event("INFO", "server.snapshotter", "periodic snapshot complete");
                        }
                        Err(err) => log_event("ERROR", "server.snapshotter", &format!("[{}] {}: {err}", err.code(), err.name())),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

fn spawn_expiration_sweeper(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut ticker = interval(every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match engine.sweep_expired().await {
                        Ok(removed) => {
                            metrics.expiration_sweeps.fetch_add(1, Ordering::Relaxed);
                            metrics.expired_keys_removed.fetch_add(removed as u64, Ordering::Relaxed);
                        }
                        Err(err) => log_event("ERROR", "server.sweeper", &format!("[{}] {}: {err}", err.code(), err.name())),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

fn spawn_tls_reloader(runtime: ServerRuntimeConfig, mut shutdown: watch::Receiver<bool>) {
    #[cfg(unix)]
    tokio::spawn(async move {
        let Some(tls_state) = runtime.tls_state.clone() else {
            return;
        };
        let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };
        loop {
            tokio::select! {
                _ = signal.recv() => {
                    match tls_state.reload().await {
                        Ok(()) => {
                            log_event("INFO", "server.tls", "reloaded TLS certificates after SIGHUP");
                            record_runtime_event(
                                &runtime.audit_logger,
                                "tls_reload",
                                [("result".to_string(), "ok".to_string())].into_iter().collect(),
                            );
                        }
                        Err(err) => {
                            log_event("ERROR", "server.tls", &format!("[{}] {}: {err}", err.code(), err.name()));
                            record_runtime_event(
                                &runtime.audit_logger,
                                "tls_reload",
                                [
                                    ("result".to_string(), "error".to_string()),
                                    ("error_code".to_string(), err.code().to_string()),
                                ]
                                .into_iter()
                                .collect(),
                            );
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

async fn handle_client<S>(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: ServerRuntimeConfig,
    connection_id: u64,
    peer_addr: Option<SocketAddr>,
    mut stream: S,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut session = SessionState::new(&runtime.guards);
    let client_hello = match read_client_hello_from_async(&mut stream).await {
        Ok(hello) => hello,
        Err(TransportError::UnexpectedEof) => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    let transport = match negotiate_server_options(&client_hello, runtime.transport) {
        Ok((server_hello, transport)) => {
            write_server_hello_to_async(&mut stream, &server_hello).await?;
            log_connection_event(
                "INFO",
                connection_id,
                peer_addr,
                &format!(
                    "negotiated protocol={} compression={} max_frame_len={}",
                    server_hello.protocol_version,
                    server_hello.compression.as_str(),
                    server_hello.max_frame_len
                ),
            );
            transport
        }
        Err(err) => {
            let server_hello =
                transport::ServerHello::error(err.code(), err.name(), &err.to_string());
            write_server_hello_to_async(&mut stream, &server_hello).await?;
            return Err(err.into());
        }
    };

    loop {
        let read_result = if let Some(idle_timeout) = runtime.idle_timeout {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = timeout(idle_timeout, read_request_from_async_with_options(&mut stream, transport)) => {
                    match result {
                        Ok(result) => result,
                        Err(_) => {
                            metrics.idle_disconnects.fetch_add(1, Ordering::Relaxed);
                            log_connection_event("INFO", connection_id, peer_addr, "disconnecting idle client");
                            return Ok(());
                        }
                    }
                }
            }
        } else {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = read_request_from_async_with_options(&mut stream, transport) => result,
            }
        };

        let request = match read_result {
            Ok(request) => request,
            Err(TransportError::UnexpectedEof) => break,
            Err(err) => return Err(err.into()),
        };
        let started_at = Instant::now();

        if request.metadata.deadline_ms == Some(0) {
            let err = TransportError::DeadlineExceeded;
            let response =
                error_response(request.request_id, err.code(), err.name(), &err.to_string());
            write_response_to_async_with_options(&mut stream, &response, transport).await?;
            continue;
        }

        if session.in_transaction() && request.metadata.sequence.is_some() {
            let err = TransportError::ProtocolStateViolation(
                "pipelined requests are not supported inside transactions",
            );
            let response =
                error_response(request.request_id, err.code(), err.name(), &err.to_string());
            write_response_to_async_with_options(&mut stream, &response, transport).await?;
            continue;
        }

        if !session.rate_limiter.allow() {
            let response = error_response(
                request.request_id,
                ServerError::RateLimitExceeded.code(),
                ServerError::RateLimitExceeded.name(),
                &ServerError::RateLimitExceeded.to_string(),
            );
            write_response_to_async_with_options(&mut stream, &response, transport).await?;
            record_audit_event(
                &runtime.audit_logger,
                AuditContext {
                    connection_id,
                    peer_addr,
                    session: &session,
                    request_id: request.request_id,
                    opcode: "RATE_LIMIT",
                    status: response.status,
                    error_code: Some(ServerError::RateLimitExceeded.code().to_string()),
                    latency_ms: started_at.elapsed().as_millis(),
                },
            );
            continue;
        }

        validate_request(&request, &runtime.guards)?;
        metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        log_connection_event(
            "INFO",
            connection_id,
            peer_addr,
            &format!(
                "received request_id={} opcode={:?}",
                request.request_id, request.opcode
            ),
        );

        let request_id = request.request_id;
        let command = match request.into_command() {
            Ok(command) => command,
            Err(err) => {
                let response = error_response(request_id, err.code(), err.name(), &err.to_string());
                write_response_to_async_with_options(&mut stream, &response, transport).await?;
                record_audit_event(
                    &runtime.audit_logger,
                    AuditContext {
                        connection_id,
                        peer_addr,
                        session: &session,
                        request_id,
                        opcode: "DECODE",
                        status: response.status,
                        error_code: Some(err.code().to_string()),
                        latency_ms: started_at.elapsed().as_millis(),
                    },
                );
                continue;
            }
        };
        let opcode = opcode_name(&command);

        if let Err(err) = validate_command(&command, &runtime.guards) {
            let response = error_response(request_id, err.code(), err.name(), &err.to_string());
            write_response_to_async_with_options(&mut stream, &response, transport).await?;
            record_audit_event(
                &runtime.audit_logger,
                AuditContext {
                    connection_id,
                    peer_addr,
                    session: &session,
                    request_id,
                    opcode,
                    status: response.status,
                    error_code: Some(err.code().to_string()),
                    latency_ms: started_at.elapsed().as_millis(),
                },
            );
            continue;
        }

        let response = match process_command(
            engine.clone(),
            Arc::clone(&metrics),
            &runtime,
            &mut session,
            peer_addr,
            request_id,
            command.clone(),
        )
        .await
        {
            Ok(response) => response,
            Err(err) => error_response(request_id, err.code(), err.name(), &err.to_string()),
        };

        let audit_error = if response.status == Status::Error {
            response.decode_error().ok().map(|payload| payload.code)
        } else {
            None
        };
        write_response_to_async_with_options(&mut stream, &response, transport).await?;
        record_audit_event(
            &runtime.audit_logger,
            AuditContext {
                connection_id,
                peer_addr,
                session: &session,
                request_id,
                opcode,
                status: response.status,
                error_code: audit_error.clone(),
                latency_ms: started_at.elapsed().as_millis(),
            },
        );
        record_semantic_audit_event(
            &runtime.audit_logger,
            AuditContext {
                connection_id,
                peer_addr,
                session: &session,
                request_id,
                opcode,
                status: response.status,
                error_code: audit_error,
                latency_ms: started_at.elapsed().as_millis(),
            },
            &command,
        );
        record_slow_command_event(
            &runtime.audit_logger,
            &runtime,
            AuditContext {
                connection_id,
                peer_addr,
                session: &session,
                request_id,
                opcode,
                status: response.status,
                error_code: None,
                latency_ms: started_at.elapsed().as_millis(),
            },
        );
        if let Some(threshold) = runtime.slow_command_threshold
            && started_at.elapsed() >= threshold
        {
            metrics.slow_commands_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    Ok(())
}

fn validate_request(request: &Request, guards: &ServerGuards) -> Result<()> {
    if request.payload.len() > guards.max_request_payload_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_key(key: &str, guards: &ServerGuards) -> Result<()> {
    if key.len() > guards.max_key_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_value(value: &str, guards: &ServerGuards) -> Result<()> {
    if value.len() > guards.max_value_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_command(command: &Command, guards: &ServerGuards) -> Result<()> {
    match command {
        Command::Auth { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::Get { key }
        | Command::GetDel { key }
        | Command::Exists { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::Ttl { key }
        | Command::Persist { key } => validate_key(key, guards)?,
        Command::GetEx { key, .. } => validate_key(key, guards)?,
        Command::Set { key, value, .. } | Command::SetNx { key, value } => {
            validate_key(key, guards)?;
            validate_value(value, guards)?;
        }
        Command::MGet { keys } | Command::Delete { keys } => {
            if keys.len() > guards.max_keys_per_batch {
                return Err(ServerError::QuotaExceeded);
            }
            for key in keys {
                validate_key(key, guards)?;
            }
        }
        Command::MSet { entries } => {
            if entries.len() > guards.max_keys_per_batch {
                return Err(ServerError::QuotaExceeded);
            }
            for (key, value) in entries {
                validate_key(key, guards)?;
                validate_value(value, guards)?;
            }
        }
        Command::Expire { key, .. } => validate_key(key, guards)?,
        Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        } => {
            validate_key(source, guards)?;
            validate_key(destination, guards)?;
        }
        Command::Scan {
            pattern: Some(pattern),
            ..
        } => validate_key(pattern, guards)?,
        Command::Scan { pattern: None, .. } => {}
        Command::BackupVerify { dump } => validate_value(dump, guards)?,
        Command::Restore { dump } => validate_value(dump, guards)?,
        Command::BackupTo { path }
        | Command::BackupVerifyFrom { path }
        | Command::RestoreFrom { path }
        | Command::RestoreCheckFrom { path } => validate_value(path, guards)?,
        Command::RestoreCheck { dump } => validate_value(dump, guards)?,
        Command::CreateUser { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::AlterUserPassword { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::DropUser { username } => validate_key(username, guards)?,
        Command::CreateRole { role } | Command::DropRole { role } => validate_key(role, guards)?,
        Command::ShowGrantsForUser { username } => validate_key(username, guards)?,
        Command::ShowGrantsForRole { role } => validate_key(role, guards)?,
        Command::GrantRole { role, username } | Command::RevokeRole { role, username } => {
            validate_key(role, guards)?;
            validate_key(username, guards)?;
        }
        Command::GrantPermission {
            permission,
            pattern,
            role,
        }
        | Command::RevokePermission {
            permission,
            pattern,
            role,
        } => {
            validate_key(permission, guards)?;
            validate_key(pattern, guards)?;
            validate_key(role, guards)?;
        }
        _ => {}
    }

    Ok(())
}

fn resolve_backup_path(base_dir: &Path, requested: &str, must_exist: bool) -> Result<PathBuf> {
    let requested_path = Path::new(requested);
    if requested_path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }

    std::fs::create_dir_all(base_dir)?;
    let base = base_dir.canonicalize()?;
    let candidate = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        base.join(requested_path)
    };

    if must_exist {
        let canonical = candidate.canonicalize()?;
        if canonical.starts_with(&base) {
            return Ok(canonical);
        }
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }

    let parent = candidate
        .parent()
        .ok_or_else(|| ServerError::BackupPathRejected(requested.to_string()))?;
    std::fs::create_dir_all(parent)?;
    let canonical_parent = parent.canonicalize()?;
    if !canonical_parent.starts_with(&base) {
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }
    Ok(candidate)
}

fn backup_manifest_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("backup");
    path.with_file_name(format!("{file_name}.manifest.json"))
}

fn load_backup_manifest(path: &Path) -> Result<BackupManifest> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|err| ServerError::BackupVerification(err.to_string()))
}

fn build_backup_manifest(dump: &str) -> Result<BackupManifest> {
    let backup = parse_backup_document(dump)?;
    Ok(BackupManifest {
        manifest_version: BACKUP_MANIFEST_VERSION,
        backup_version: backup.version,
        created_at_ms: backup.created_at_ms,
        source_engine_version: backup.source_engine_version,
        source_sequence: backup.source_sequence,
        entry_count: backup.entries.len() as u64,
        byte_len: dump.len() as u64,
        hash_algorithm: BACKUP_HASH_ALGORITHM.to_string(),
        sha256: sha256_hex(dump.as_bytes()),
    })
}

fn verify_backup_manifest(dump: &str, manifest: &BackupManifest) -> Result<()> {
    let expected = build_backup_manifest(dump)?;
    if manifest.manifest_version != BACKUP_MANIFEST_VERSION {
        return Err(ServerError::BackupVerification(format!(
            "unsupported manifest version {}",
            manifest.manifest_version
        )));
    }
    if manifest.hash_algorithm != BACKUP_HASH_ALGORITHM {
        return Err(ServerError::BackupVerification(format!(
            "unsupported hash algorithm {}",
            manifest.hash_algorithm
        )));
    }
    if manifest.backup_version != expected.backup_version
        || manifest.created_at_ms != expected.created_at_ms
        || manifest.source_engine_version != expected.source_engine_version
        || manifest.source_sequence != expected.source_sequence
        || manifest.entry_count != expected.entry_count
        || manifest.byte_len != expected.byte_len
        || manifest.sha256 != expected.sha256
    {
        return Err(ServerError::BackupVerification(
            "backup manifest does not match dump".to_string(),
        ));
    }
    Ok(())
}

async fn verify_backup_dump(dump: &str, engine: EngineHandle) -> Result<Vec<(String, String)>> {
    let manifest = build_backup_manifest(dump)?;
    let live_entries = engine.validate_backup(dump.to_string()).await?;
    Ok(vec![
        ("status".to_string(), "ok".to_string()),
        ("entries".to_string(), live_entries.to_string()),
        ("entry_count".to_string(), manifest.entry_count.to_string()),
        ("sha256".to_string(), manifest.sha256),
    ])
}

fn parse_backup_document(dump: &str) -> Result<LogicalBackup> {
    let backup: LogicalBackup = serde_json::from_str(dump)
        .map_err(|err| ServerError::BackupVerification(err.to_string()))?;
    if backup.version != 1 {
        return Err(ServerError::BackupVerification(format!(
            "unsupported backup version {}",
            backup.version
        )));
    }
    Ok(backup)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes).to_vec();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in &digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

async fn process_command(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    peer_addr: Option<SocketAddr>,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    if matches!(command, Command::Auth { .. }) {
        return handle_auth(metrics, runtime, session, peer_addr, request_id, command).await;
    }

    if runtime.auth_config.is_some()
        && !session.is_authenticated()
        && !matches!(command, Command::Ping { .. })
    {
        metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        return Err(ServerError::AuthenticationRequired);
    }

    expire_transaction_if_needed(metrics.clone(), runtime, session)?;

    if runtime.maintenance.is_enabled() && !is_allowed_during_maintenance(&command) {
        return Err(ServerError::MaintenanceModeEnabled);
    }

    if session.in_transaction() {
        return handle_transaction_command(engine, metrics, runtime, session, request_id, command)
            .await;
    }

    if runtime.auth_config.is_some() {
        authorize_command(&command, session)?;
    }

    match command {
        Command::Multi => {
            metrics.transactions_started.fetch_add(1, Ordering::Relaxed);
            session.transaction_queue.push(Command::Multi);
            session.transaction_started_at_ms = Some(current_time_millis());
            Ok(Response::ok(request_id))
        }
        Command::Exec | Command::Discard => Err(ServerError::NoActiveTransaction),
        Command::MaintenanceOn => {
            runtime.maintenance.set(true)?;
            record_runtime_event(
                &runtime.audit_logger,
                "maintenance_mode",
                [("enabled".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            );
            Ok(Response::ok(request_id))
        }
        Command::MaintenanceOff => {
            runtime.maintenance.set(false)?;
            record_runtime_event(
                &runtime.audit_logger,
                "maintenance_mode",
                [("enabled".to_string(), "false".to_string())]
                    .into_iter()
                    .collect(),
            );
            Ok(Response::ok(request_id))
        }
        Command::MaintenanceStatus => Ok(Response::entries(
            request_id,
            &[
                (
                    "enabled".to_string(),
                    runtime.maintenance.is_enabled().to_string(),
                ),
                (
                    "path".to_string(),
                    runtime.maintenance.path().display().to_string(),
                ),
            ],
        )?),
        Command::Metrics => {
            let entries = metrics.snapshot();
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::MetricsProm => Ok(Response::value(request_id, &metrics.prometheus())?),
        Command::Info => {
            let entries = structured_info(engine, metrics, runtime).await?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::BackupTo { path } => {
            let response = engine.execute(request_id, Command::Backup).await?;
            let dump = response.decode_value()?;
            let path = resolve_backup_path(&runtime.backup_dir, &path, false)?;
            let manifest = build_backup_manifest(&dump)?;
            std::fs::write(&path, dump)?;
            std::fs::write(
                backup_manifest_path(&path),
                serde_json::to_vec_pretty(&manifest).map_err(std::io::Error::other)?,
            )?;
            Ok(Response::ok(request_id))
        }
        Command::BackupVerify { dump } => {
            let entries = verify_backup_dump(&dump, engine.clone()).await?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::BackupVerifyFrom { path } => {
            let path = resolve_backup_path(&runtime.backup_dir, &path, true)?;
            let dump = std::fs::read_to_string(&path)?;
            let manifest = load_backup_manifest(&backup_manifest_path(&path))?;
            verify_backup_manifest(&dump, &manifest)?;
            let entries = verify_backup_dump(&dump, engine.clone()).await?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::RestoreFrom { path } => {
            let path = resolve_backup_path(&runtime.backup_dir, &path, true)?;
            let dump = std::fs::read_to_string(path)?;
            engine.execute(request_id, Command::Restore { dump }).await
        }
        Command::RestoreCheck { dump } => Ok(Response::count(
            request_id,
            engine.validate_backup(dump).await? as u64,
        )),
        Command::RestoreCheckFrom { path } => {
            let path = resolve_backup_path(&runtime.backup_dir, &path, true)?;
            let dump = std::fs::read_to_string(path)?;
            Ok(Response::count(
                request_id,
                engine.validate_backup(dump).await? as u64,
            ))
        }
        Command::CreateUser { username, password } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.create_user(username, password).await?;
            Ok(Response::ok(request_id))
        }
        Command::AlterUserPassword { username, password } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.alter_user_password(&username, password).await?;
            Ok(Response::ok(request_id))
        }
        Command::DropUser { username } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.drop_user(&username).await?;
            Ok(Response::ok(request_id))
        }
        Command::CreateRole { role } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.create_role(role).await?;
            Ok(Response::ok(request_id))
        }
        Command::DropRole { role } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.drop_role(&role).await?;
            Ok(Response::ok(request_id))
        }
        Command::GrantRole { role, username } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.grant_role(&role, &username).await?;
            Ok(Response::ok(request_id))
        }
        Command::RevokeRole { role, username } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.revoke_role(&role, &username).await?;
            Ok(Response::ok(request_id))
        }
        Command::GrantPermission {
            permission,
            pattern,
            role,
        } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config
                .grant_permission(Permission::parse(&permission)?, pattern, &role)
                .await?;
            Ok(Response::ok(request_id))
        }
        Command::RevokePermission {
            permission,
            pattern,
            role,
        } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config
                .revoke_permission(Permission::parse(&permission)?, pattern, &role)
                .await?;
            Ok(Response::ok(request_id))
        }
        Command::ShowUsers => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            Ok(Response::entries(request_id, &auth_config.users().await)?)
        }
        Command::ShowRoles => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            Ok(Response::entries(request_id, &auth_config.roles().await)?)
        }
        Command::ShowGrants => {
            let Some(identity) = &session.identity else {
                return Err(ServerError::AuthenticationRequired);
            };
            if let Some(auth_config) = runtime.auth_config.clone() {
                Ok(Response::entries(
                    request_id,
                    &auth_config.grants_for_user(&identity.username).await?,
                )?)
            } else {
                Ok(Response::entries(
                    request_id,
                    &[("grants".to_string(), identity.grants_csv())],
                )?)
            }
        }
        Command::ShowGrantsForUser { username } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            Ok(Response::entries(
                request_id,
                &auth_config.grants_for_user(&username).await?,
            )?)
        }
        Command::ShowGrantsForRole { role } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            Ok(Response::entries(
                request_id,
                &auth_config.grants_for_role(&role).await?,
            )?)
        }
        Command::WhoAmI => {
            let Some(identity) = &session.identity else {
                return Err(ServerError::AuthenticationRequired);
            };
            Ok(Response::entries(
                request_id,
                &[
                    ("username".to_string(), identity.username.clone()),
                    ("permissions".to_string(), identity.permissions_csv()),
                ],
            )?)
        }
        command => engine.execute(request_id, command).await,
    }
}

async fn structured_info(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
) -> Result<Vec<(String, String)>> {
    let engine_entries = engine.info().await?;
    let locked_auth_records = {
        let mut lockouts = runtime.auth_lockouts.lock().await;
        lockouts.active_lockout_count(current_time_millis())
    };
    let tls_metadata = if let Some(tls_state) = &runtime.tls_state {
        Some(tls_state.metadata_snapshot().await)
    } else {
        None
    };
    let lookup = |key: &str| {
        engine_entries
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then(|| value.clone()))
            .unwrap_or_else(|| "unknown".to_string())
    };
    let mut entries = vec![
        (
            "server.version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ),
        ("server.mode".to_string(), "single-node".to_string()),
        (
            "transport.protocol_magic".to_string(),
            String::from_utf8_lossy(&transport::MAGIC_BYTES).to_string(),
        ),
        (
            "transport.protocol_version".to_string(),
            transport::VERSION.to_string(),
        ),
        (
            "transport.compression".to_string(),
            runtime.transport.compression.as_str().to_string(),
        ),
        (
            "transport.compression_mode".to_string(),
            runtime.transport.compression.as_str().to_string(),
        ),
        (
            "transport.compression_threshold_bytes".to_string(),
            runtime.transport.compression_threshold_bytes.to_string(),
        ),
        (
            "transport.max_frame_len".to_string(),
            runtime.transport.max_frame_len.to_string(),
        ),
        (
            "transport.max_decompressed_frame_len".to_string(),
            runtime.transport.max_decompressed_frame_len.to_string(),
        ),
        (
            "storage.engine_version".to_string(),
            lookup("engine_version"),
        ),
        ("storage.key_count".to_string(), lookup("key_count")),
        (
            "storage.last_applied_sequence".to_string(),
            lookup("last_applied_sequence"),
        ),
        (
            "persistence.wal_size_bytes".to_string(),
            lookup("wal_size_bytes"),
        ),
        (
            "persistence.wal_segment_count".to_string(),
            lookup("wal_segment_count"),
        ),
        (
            "persistence.oldest_retained_sequence".to_string(),
            lookup("oldest_retained_sequence"),
        ),
        (
            "persistence.wal_sync_policy".to_string(),
            lookup("wal_sync_policy"),
        ),
        (
            "persistence.last_recovery_duration_ms".to_string(),
            lookup("recovery_duration_ms"),
        ),
        (
            "persistence.last_snapshot_at_ms".to_string(),
            lookup("last_snapshot_at_ms"),
        ),
        (
            "persistence.last_snapshot_duration_ms".to_string(),
            lookup("last_snapshot_duration_ms"),
        ),
        (
            "security.auth_required".to_string(),
            runtime.auth_config.is_some().to_string(),
        ),
        (
            "security.auth_mode".to_string(),
            if runtime.auth_config.is_some() {
                "password"
            } else {
                "disabled"
            }
            .to_string(),
        ),
        (
            "security.insecure_auth_disabled".to_string(),
            runtime.insecure_auth_disabled.to_string(),
        ),
        (
            "security.insecure_default_credentials".to_string(),
            runtime.insecure_default_credentials.to_string(),
        ),
        (
            "security.rbac_enabled".to_string(),
            runtime.auth_config.is_some().to_string(),
        ),
        (
            "security.tls_enabled".to_string(),
            runtime.tls_state.is_some().to_string(),
        ),
        (
            "security.tls_mode".to_string(),
            if runtime.tls_state.is_some() {
                "tls"
            } else {
                "plaintext"
            }
            .to_string(),
        ),
        (
            "security.mtls_enabled".to_string(),
            runtime.mtls_enabled.to_string(),
        ),
        (
            "security.cert_not_after_ms".to_string(),
            tls_metadata
                .as_ref()
                .and_then(|metadata| metadata.cert_not_after_ms)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "security.cert_days_remaining".to_string(),
            tls_metadata
                .as_ref()
                .and_then(|metadata| metadata.cert_days_remaining)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "security.last_tls_reload_success_at_ms".to_string(),
            tls_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_reload_success_at_ms)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "security.last_tls_reload_failure_at_ms".to_string(),
            tls_metadata
                .as_ref()
                .and_then(|metadata| metadata.last_reload_failure_at_ms)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "security.storage_encryption".to_string(),
            lookup("storage_encryption"),
        ),
        (
            "security.storage_key_id".to_string(),
            lookup("storage_key_id"),
        ),
        (
            "runtime.idle_timeout_seconds".to_string(),
            runtime
                .idle_timeout
                .map(|duration| duration.as_secs().to_string())
                .unwrap_or_else(|| "disabled".to_string()),
        ),
        (
            "runtime.backup_dir".to_string(),
            runtime.backup_dir.display().to_string(),
        ),
        (
            "runtime.maintenance_mode".to_string(),
            runtime.maintenance.is_enabled().to_string(),
        ),
        (
            "runtime.max_request_payload_bytes".to_string(),
            runtime.guards.max_request_payload_bytes.to_string(),
        ),
        (
            "runtime.max_key_bytes".to_string(),
            runtime.guards.max_key_bytes.to_string(),
        ),
        (
            "runtime.max_value_bytes".to_string(),
            runtime.guards.max_value_bytes.to_string(),
        ),
        (
            "runtime.max_keys_per_batch".to_string(),
            runtime.guards.max_keys_per_batch.to_string(),
        ),
        (
            "runtime.max_transaction_queue_len".to_string(),
            runtime.guards.max_transaction_queue_len.to_string(),
        ),
        (
            "runtime.transaction_max_seconds".to_string(),
            runtime.transaction_max_duration.as_secs().to_string(),
        ),
        (
            "runtime.requests_per_second".to_string(),
            runtime.guards.requests_per_second.to_string(),
        ),
        (
            "runtime.request_burst".to_string(),
            runtime.guards.request_burst.to_string(),
        ),
        (
            "runtime.auth_failure_window_seconds".to_string(),
            runtime.auth_failure_window.as_secs().to_string(),
        ),
        (
            "runtime.auth_failure_limit".to_string(),
            runtime.auth_failure_limit.to_string(),
        ),
        (
            "runtime.auth_lockout_seconds".to_string(),
            runtime.auth_lockout.as_secs().to_string(),
        ),
        (
            "runtime.wal_segment_size_bytes".to_string(),
            runtime.wal_segment_size_bytes.to_string(),
        ),
        (
            "runtime.wal_retain_segments".to_string(),
            runtime.wal_retain_segments.to_string(),
        ),
        (
            "runtime.locked_auth_records".to_string(),
            locked_auth_records.to_string(),
        ),
        (
            "runtime.snapshot_interval_seconds".to_string(),
            runtime
                .snapshot_interval
                .map(|duration| duration.as_secs().to_string())
                .unwrap_or_else(|| "disabled".to_string()),
        ),
        (
            "runtime.expiration_sweep_interval_seconds".to_string(),
            runtime
                .expiration_sweep_interval
                .map(|duration| duration.as_secs().to_string())
                .unwrap_or_else(|| "disabled".to_string()),
        ),
        (
            "runtime.slow_command_threshold_ms".to_string(),
            runtime
                .slow_command_threshold
                .map(|duration| duration.as_millis().to_string())
                .unwrap_or_else(|| "disabled".to_string()),
        ),
    ];
    entries.extend(
        metrics
            .snapshot()
            .into_iter()
            .map(|(key, value)| (format!("metrics.{key}"), value)),
    );
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(entries)
}

async fn handle_auth(
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    peer_addr: Option<SocketAddr>,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    let Command::Auth { username, password } = command else {
        unreachable!();
    };

    let Some(auth_config) = runtime.auth_config.clone() else {
        session.identity = Some(Identity {
            username: "anonymous".to_string(),
            permissions: Permission::all(),
            grants: Permission::all()
                .into_iter()
                .map(|permission| crate::auth::PermissionGrant {
                    permission,
                    pattern: "*".to_string(),
                })
                .collect(),
        });
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::ok(request_id));
    };

    let lockout_key = auth_lockout_key(&username, peer_addr);
    {
        let mut lockouts = runtime.auth_lockouts.lock().await;
        if let Some(remaining_seconds) =
            lockouts.remaining_lockout_seconds(&lockout_key, current_time_millis())
        {
            metrics
                .locked_auth_attempts_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(ServerError::AuthenticationLocked {
                username,
                remaining_seconds,
            });
        }
    }

    if let Some(identity) = auth_config.verify(&username, &password).await? {
        session.identity = Some(identity);
        runtime
            .auth_lockouts
            .lock()
            .await
            .clear_success(&lockout_key);
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::ok(request_id));
    }

    let locked = runtime.auth_lockouts.lock().await.record_failure(
        &lockout_key,
        current_time_millis(),
        runtime.auth_failure_window,
        runtime.auth_failure_limit,
        runtime.auth_lockout,
    );
    metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
    if locked {
        metrics
            .locked_auth_attempts_total
            .fetch_add(1, Ordering::Relaxed);
    }
    Err(ServerError::AuthenticationFailed)
}

async fn handle_transaction_command(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    expire_transaction_if_needed(metrics.clone(), runtime, session)?;
    match command {
        Command::Multi => Err(ServerError::TransactionAlreadyActive),
        Command::Discard => {
            clear_transaction(session);
            metrics
                .transactions_discarded
                .fetch_add(1, Ordering::Relaxed);
            Ok(Response::ok(request_id))
        }
        Command::Exec => {
            let mut queued = std::mem::take(&mut session.transaction_queue);
            session.transaction_started_at_ms = None;
            if matches!(queued.first(), Some(Command::Multi)) {
                queued.remove(0);
            }
            for command in &queued {
                validate_transaction_command(command)?;
            }
            let results = engine.execute_batch(request_id, queued.clone()).await?;
            let mut rendered = Vec::with_capacity(results.len());
            for (queued_command, result) in queued.iter().zip(results) {
                rendered.push(Some(render_transaction_result(queued_command, result)?));
            }
            metrics
                .transactions_committed
                .fetch_add(1, Ordering::Relaxed);
            Ok(Response::strings(request_id, &rendered)?)
        }
        Command::Auth { .. } => Err(ServerError::AuthenticationFailed),
        other => {
            if session.transaction_queue.len() >= runtime.guards.max_transaction_queue_len {
                return Err(ServerError::QuotaExceeded);
            }
            validate_command(&other, &runtime.guards)?;
            validate_transaction_command(&other)?;
            if runtime.auth_config.is_some() {
                authorize_command(&other, session)?;
            }
            session.transaction_queue.push(other);
            Ok(Response::value(request_id, "QUEUED")?)
        }
    }
}

fn clear_transaction(session: &mut SessionState) {
    session.transaction_queue.clear();
    session.transaction_started_at_ms = None;
}

fn expire_transaction_if_needed(
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
) -> Result<()> {
    let Some(started_at_ms) = session.transaction_started_at_ms else {
        return Ok(());
    };
    let elapsed_ms = current_time_millis().saturating_sub(started_at_ms);
    if elapsed_ms <= runtime.transaction_max_duration.as_millis() as u64 {
        return Ok(());
    }
    clear_transaction(session);
    metrics
        .transactions_discarded
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .transactions_timed_out
        .fetch_add(1, Ordering::Relaxed);
    Err(ServerError::TransactionExpired {
        seconds: runtime.transaction_max_duration.as_secs(),
    })
}

fn is_allowed_during_maintenance(command: &Command) -> bool {
    matches!(
        command,
        Command::Ping { .. }
            | Command::Get { .. }
            | Command::Exists { .. }
            | Command::GetEx { .. }
            | Command::MGet { .. }
            | Command::Ttl { .. }
            | Command::Scan { .. }
            | Command::DbSize
            | Command::Count
            | Command::List
            | Command::Info
            | Command::Metrics
            | Command::MetricsProm
            | Command::Backup
            | Command::BackupVerify { .. }
            | Command::BackupVerifyFrom { .. }
            | Command::ShowUsers
            | Command::ShowRoles
            | Command::ShowGrants
            | Command::ShowGrantsForUser { .. }
            | Command::ShowGrantsForRole { .. }
            | Command::WhoAmI
            | Command::MaintenanceStatus
            | Command::MaintenanceOff
    )
}

fn authorize_command(command: &Command, session: &SessionState) -> Result<()> {
    let Some(permission) = command_permission(command) else {
        return Ok(());
    };
    let Some(identity) = &session.identity else {
        return Err(ServerError::AuthenticationRequired);
    };
    let keys = command_keys(command);
    if keys.is_empty() {
        if let Some(pattern) = command_pattern(command) {
            if identity.allows_pattern(permission, pattern) {
                return Ok(());
            }
        } else if identity.has(permission) {
            return Ok(());
        }
    } else if keys.iter().all(|key| identity.allows_key(permission, key)) {
        return Ok(());
    }
    Err(ServerError::PermissionDenied)
}

fn command_permission(command: &Command) -> Option<Permission> {
    match command {
        Command::Ping { .. }
        | Command::Auth { .. }
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::MaintenanceStatus
        | Command::WhoAmI => None,
        Command::Get { .. }
        | Command::Exists { .. }
        | Command::MGet { .. }
        | Command::Ttl { .. }
        | Command::Scan { .. }
        | Command::DbSize
        | Command::Count
        | Command::List => Some(Permission::Read),
        Command::Clear => Some(Permission::Clear),
        Command::GetDel { .. }
        | Command::GetEx { .. }
        | Command::Set { .. }
        | Command::SetNx { .. }
        | Command::MSet { .. }
        | Command::Delete { .. }
        | Command::Incr { .. }
        | Command::Decr { .. }
        | Command::Expire { .. }
        | Command::Persist { .. }
        | Command::Rename { .. }
        | Command::RenameNx { .. } => Some(Permission::Write),
        Command::Info | Command::Metrics | Command::MetricsProm => Some(Permission::Metrics),
        Command::Save | Command::Snapshot => Some(Permission::Snapshot),
        Command::Backup
        | Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. } => Some(Permission::Backup),
        Command::Restore { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. } => Some(Permission::Restore),
        Command::CreateUser { .. }
        | Command::AlterUserPassword { .. }
        | Command::DropUser { .. } => Some(Permission::UserAdmin),
        Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowRoles => Some(Permission::RoleAdmin),
        Command::ShowGrants => None,
        Command::ShowGrantsForUser { .. } => Some(Permission::UserAdmin),
        Command::ShowGrantsForRole { .. } => Some(Permission::RoleAdmin),
        Command::ShowUsers => Some(Permission::UserAdmin),
        Command::MaintenanceOn | Command::MaintenanceOff => Some(Permission::Admin),
        Command::Help | Command::Exit => None,
    }
}

fn command_keys(command: &Command) -> Vec<&str> {
    match command {
        Command::Get { key }
        | Command::GetDel { key }
        | Command::GetEx { key, .. }
        | Command::Set { key, .. }
        | Command::SetNx { key, .. }
        | Command::Exists { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::Expire { key, .. }
        | Command::Ttl { key }
        | Command::Persist { key } => vec![key.as_str()],
        Command::MGet { keys } | Command::Delete { keys } => {
            keys.iter().map(String::as_str).collect()
        }
        Command::MSet { entries } => entries.iter().map(|(key, _)| key.as_str()).collect(),
        Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        } => vec![source.as_str(), destination.as_str()],
        _ => Vec::new(),
    }
}

fn command_pattern(command: &Command) -> Option<&str> {
    match command {
        Command::Scan { pattern, .. } => Some(pattern.as_deref().unwrap_or("*")),
        _ => None,
    }
}

fn error_response(request_id: Uuid, code: &str, name: &str, message: &str) -> Response {
    Response::error(request_id, code, name, message).unwrap_or_else(|_| {
        Response::error(
            request_id,
            "TRN-011",
            "Remote Error Encoding Failure",
            "failed to encode structured error payload",
        )
        .expect("static remote error encoding should never fail")
    })
}

fn execute_command<E>(engine: &mut E, request_id: Uuid, command: Command) -> Result<Response>
where
    E: StorageEngine,
{
    match command {
        Command::Auth { .. } => Err(ServerError::UnsupportedRemoteCommand),
        Command::Ping { message } => {
            let payload = message.unwrap_or_else(|| "PONG".to_string());
            Ok(Response::value(request_id, &payload)?)
        }
        Command::Get { key } => value_or_not_found(request_id, engine.get(&key)?),
        Command::GetDel { key } => value_or_not_found(request_id, engine.get_del(&key)?),
        Command::GetEx {
            key,
            expiration,
            persist,
        } => value_or_not_found(
            request_id,
            engine.get_ex(&key, map_expiration(expiration), persist)?,
        ),
        Command::Set {
            key,
            value,
            options,
        } => render_set_response(
            request_id,
            options.return_previous,
            options.condition.is_some(),
            engine.set_with_options(key, value, map_set_options(options))?,
        ),
        Command::SetNx { key, value } => {
            Ok(Response::boolean(request_id, engine.set_nx(key, value)?))
        }
        Command::MGet { keys } => Ok(Response::strings(request_id, &engine.mget(&keys)?)?),
        Command::MSet { entries } => {
            engine.mset(&entries)?;
            Ok(Response::ok(request_id))
        }
        Command::Delete { keys } => Ok(Response::count(
            request_id,
            engine.delete_many(&keys)? as u64,
        )),
        Command::Exists { key } => Ok(Response::boolean(request_id, engine.exists(&key)?)),
        Command::Incr { key } => Ok(Response::integer(request_id, engine.incr(&key)?)),
        Command::Decr { key } => Ok(Response::integer(request_id, engine.decr(&key)?)),
        Command::Expire { key, seconds } => {
            Ok(Response::boolean(request_id, engine.expire(&key, seconds)?))
        }
        Command::Ttl { key } => Ok(Response::integer(request_id, engine.ttl(&key)?)),
        Command::Persist { key } => Ok(Response::boolean(request_id, engine.persist(&key)?)),
        Command::Rename {
            source,
            destination,
        } => Ok(Response::boolean(
            request_id,
            engine.rename(&source, destination)?,
        )),
        Command::RenameNx {
            source,
            destination,
        } => Ok(Response::boolean(
            request_id,
            engine.rename_nx(&source, destination)?,
        )),
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let ScanPage { next_cursor, keys } = engine.scan(cursor, pattern.as_deref(), count)?;
            Ok(Response::scan(request_id, next_cursor, &keys)?)
        }
        Command::DbSize | Command::Count => {
            Ok(Response::count(request_id, engine.db_size()? as u64))
        }
        Command::Info => Ok(Response::entries(request_id, &engine.info()?)?),
        Command::Metrics | Command::MetricsProm => Err(ServerError::UnsupportedRemoteCommand),
        Command::List => Ok(Response::entries(request_id, &engine.list()?)?),
        Command::Clear => {
            engine.clear()?;
            Ok(Response::ok(request_id))
        }
        Command::Save | Command::Snapshot => {
            engine.snapshot()?;
            Ok(Response::ok(request_id))
        }
        Command::Backup => Ok(Response::value(request_id, &engine.logical_backup()?)?),
        Command::Restore { dump } => Ok(Response::count(
            request_id,
            engine.restore_logical_backup(&dump)? as u64,
        )),
        Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. }
        | Command::AlterUserPassword { .. }
        | Command::MaintenanceOn
        | Command::MaintenanceOff
        | Command::MaintenanceStatus => Err(ServerError::UnsupportedRemoteCommand),
        Command::Multi | Command::Exec | Command::Discard => {
            Err(ServerError::UnsupportedRemoteCommand)
        }
        Command::CreateUser { .. }
        | Command::DropUser { .. }
        | Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowUsers
        | Command::ShowRoles
        | Command::ShowGrants
        | Command::ShowGrantsForUser { .. }
        | Command::ShowGrantsForRole { .. }
        | Command::WhoAmI => Err(ServerError::UnsupportedRemoteCommand),
        Command::Help | Command::Exit => Err(ServerError::UnsupportedRemoteCommand),
    }
}

fn render_transaction_result(_command: &Command, result: TransactionResult) -> Result<String> {
    match result {
        TransactionResult::Ok => Ok("OK".to_string()),
        TransactionResult::NotFound => Ok("NOT_FOUND".to_string()),
        TransactionResult::Value(value) => Ok(value),
        TransactionResult::Boolean(value) => Ok(value.to_string()),
        TransactionResult::Count(value) => Ok(value.to_string()),
        TransactionResult::Integer(value) => Ok(value.to_string()),
        TransactionResult::Entries(entries) => Ok(entries
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")),
        TransactionResult::Strings(values) => Ok(values
            .into_iter()
            .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
            .collect::<Vec<_>>()
            .join(", ")),
        TransactionResult::Scan(scan) => Ok(format!(
            "cursor={}, keys=[{}]",
            scan.next_cursor,
            scan.keys.join(", ")
        )),
    }
}

fn validate_transaction_command(command: &Command) -> Result<()> {
    match command {
        Command::Info
        | Command::Metrics
        | Command::MetricsProm
        | Command::Save
        | Command::Snapshot
        | Command::Backup
        | Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. }
        | Command::Restore { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. }
        | Command::AlterUserPassword { .. }
        | Command::CreateUser { .. }
        | Command::DropUser { .. }
        | Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowUsers
        | Command::ShowRoles
        | Command::ShowGrants
        | Command::ShowGrantsForUser { .. }
        | Command::ShowGrantsForRole { .. }
        | Command::WhoAmI
        | Command::MaintenanceOn
        | Command::MaintenanceOff
        | Command::MaintenanceStatus
        | Command::Auth { .. }
        | Command::Help
        | Command::Exit
        | Command::Multi
        | Command::Exec
        | Command::Discard => Err(ServerError::UnsupportedRemoteCommand),
        _ => Ok(()),
    }
}

fn value_or_not_found(request_id: Uuid, value: Option<String>) -> Result<Response> {
    match value {
        Some(value) => Ok(Response::value(request_id, &value)?),
        None => Ok(Response::not_found(request_id)),
    }
}

fn render_set_response(
    request_id: Uuid,
    return_previous: bool,
    conditional_write: bool,
    outcome: SetOutcome,
) -> Result<Response> {
    if return_previous {
        return value_or_not_found(request_id, outcome.previous);
    }
    if conditional_write {
        return Ok(Response::boolean(request_id, outcome.applied));
    }
    Ok(Response::ok(request_id))
}

fn map_expiration(expiration: Option<CommandExpiration>) -> Option<Expiration> {
    expiration.map(|expiration| match expiration {
        CommandExpiration::Ex(value) => Expiration::Seconds(value),
        CommandExpiration::Px(value) => Expiration::Milliseconds(value),
    })
}

fn map_set_options(options: CommandSetOptions) -> SetOptions {
    SetOptions {
        condition: options.condition.map(|condition| match condition {
            CommandSetCondition::Nx => SetCondition::Nx,
            CommandSetCondition::Xx => SetCondition::Xx,
        }),
        expiration: map_expiration(options.expiration),
        keep_ttl: options.keep_ttl,
    }
}

fn log_event(level: &str, component: &str, message: &str) {
    println!("[{level}] [{component}] {message}");
}

fn log_connection_event(
    level: &str,
    connection_id: u64,
    peer_addr: Option<SocketAddr>,
    message: &str,
) {
    match peer_addr {
        Some(peer_addr) => log_event(
            level,
            "server.connection",
            &format!("connection_id={connection_id} peer={peer_addr} {message}"),
        ),
        None => log_event(
            level,
            "server.connection",
            &format!("connection_id={connection_id} peer=unknown {message}"),
        ),
    }
}

fn auth_lockout_key(username: &str, peer_addr: Option<SocketAddr>) -> String {
    format!(
        "{username}|{}",
        peer_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    )
}

fn opcode_name(command: &Command) -> &'static str {
    match command {
        Command::Auth { .. } => "AUTH",
        Command::Ping { .. } => "PING",
        Command::Get { .. } => "GET",
        Command::GetDel { .. } => "GETDEL",
        Command::GetEx { .. } => "GETEX",
        Command::Set { .. } => "SET",
        Command::SetNx { .. } => "SETNX",
        Command::MGet { .. } => "MGET",
        Command::MSet { .. } => "MSET",
        Command::Delete { .. } => "DEL",
        Command::Exists { .. } => "EXISTS",
        Command::Incr { .. } => "INCR",
        Command::Decr { .. } => "DECR",
        Command::Expire { .. } => "EXPIRE",
        Command::Ttl { .. } => "TTL",
        Command::Persist { .. } => "PERSIST",
        Command::Rename { .. } => "RENAME",
        Command::RenameNx { .. } => "RENAMENX",
        Command::Scan { .. } => "SCAN",
        Command::DbSize => "DBSIZE",
        Command::Info => "INFO",
        Command::Metrics => "METRICS",
        Command::MetricsProm => "METRICS_PROM",
        Command::List => "LIST",
        Command::Clear => "CLEAR",
        Command::Count => "COUNT",
        Command::Save => "SAVE",
        Command::Snapshot => "SNAPSHOT",
        Command::Backup => "BACKUP",
        Command::BackupTo { .. } => "BACKUP_TO",
        Command::BackupVerify { .. } => "BACKUP_VERIFY",
        Command::BackupVerifyFrom { .. } => "BACKUP_VERIFY_FROM",
        Command::Restore { .. } => "RESTORE",
        Command::RestoreFrom { .. } => "RESTORE_FROM",
        Command::RestoreCheck { .. } => "RESTORE_CHECK",
        Command::RestoreCheckFrom { .. } => "RESTORE_CHECK_FROM",
        Command::AlterUserPassword { .. } => "ALTER_USER_PASSWORD",
        Command::CreateUser { .. } => "CREATE_USER",
        Command::DropUser { .. } => "DROP_USER",
        Command::CreateRole { .. } => "CREATE_ROLE",
        Command::DropRole { .. } => "DROP_ROLE",
        Command::GrantRole { .. } => "GRANT_ROLE",
        Command::RevokeRole { .. } => "REVOKE_ROLE",
        Command::GrantPermission { .. } => "GRANT_PERMISSION",
        Command::RevokePermission { .. } => "REVOKE_PERMISSION",
        Command::ShowUsers => "SHOW_USERS",
        Command::ShowRoles => "SHOW_ROLES",
        Command::ShowGrants => "SHOW_GRANTS",
        Command::ShowGrantsForUser { .. } => "SHOW_GRANTS_FOR_USER",
        Command::ShowGrantsForRole { .. } => "SHOW_GRANTS_FOR_ROLE",
        Command::WhoAmI => "WHOAMI",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::MaintenanceOn => "MAINTENANCE_ON",
        Command::MaintenanceOff => "MAINTENANCE_OFF",
        Command::MaintenanceStatus => "MAINTENANCE_STATUS",
        Command::Help => "HELP",
        Command::Exit => "EXIT",
    }
}

fn record_audit_event(logger: &AuditLogger, context: AuditContext<'_>) {
    record_audit_event_with(
        logger,
        context,
        "command",
        std::collections::BTreeMap::new(),
    );
}

fn record_runtime_event(
    logger: &AuditLogger,
    event_type: &str,
    details: std::collections::BTreeMap<String, String>,
) {
    let _ = logger.record(&AuditEvent {
        timestamp_ms: current_time_millis(),
        connection_id: 0,
        peer: None,
        username: None,
        request_id: Uuid::nil().to_string(),
        opcode: "RUNTIME".to_string(),
        status: "ok".to_string(),
        error_code: None,
        latency_ms: 0,
        event_type: event_type.to_string(),
        details,
    });
}

fn record_audit_event_with(
    logger: &AuditLogger,
    context: AuditContext<'_>,
    event_type: &str,
    details: std::collections::BTreeMap<String, String>,
) {
    let _ = logger.record(&AuditEvent {
        timestamp_ms: current_time_millis(),
        connection_id: context.connection_id,
        peer: context.peer_addr.map(|addr| addr.to_string()),
        username: context
            .session
            .identity
            .as_ref()
            .map(|identity| identity.username.clone()),
        request_id: context.request_id.to_string(),
        opcode: context.opcode.to_string(),
        status: match context.status {
            Status::Ok => "ok".to_string(),
            Status::Error => "error".to_string(),
            Status::NotFound => "not_found".to_string(),
        },
        error_code: context.error_code,
        latency_ms: context.latency_ms.min(u128::from(u64::MAX)) as u64,
        event_type: event_type.to_string(),
        details,
    });
}

fn record_semantic_audit_event(logger: &AuditLogger, context: AuditContext<'_>, command: &Command) {
    let Some((event_type, mut details)) = semantic_audit_details(command) else {
        return;
    };
    details.insert("result".to_string(), audit_status(context.status));
    record_audit_event_with(logger, context, event_type, details);
}

fn record_slow_command_event(
    logger: &AuditLogger,
    runtime: &ServerRuntimeConfig,
    context: AuditContext<'_>,
) {
    let Some(threshold) = runtime.slow_command_threshold else {
        return;
    };
    if context.latency_ms < threshold.as_millis() {
        return;
    }
    let mut details = std::collections::BTreeMap::new();
    details.insert("opcode".to_string(), context.opcode.to_string());
    details.insert("latency_ms".to_string(), context.latency_ms.to_string());
    details.insert(
        "threshold_ms".to_string(),
        threshold.as_millis().to_string(),
    );
    record_audit_event_with(logger, context, "slow_command", details);
}

fn semantic_audit_details(
    command: &Command,
) -> Option<(&'static str, std::collections::BTreeMap<String, String>)> {
    let mut details = std::collections::BTreeMap::new();
    let event_type = match command {
        Command::Auth { username, .. } => {
            details.insert("username".to_string(), username.clone());
            "auth"
        }
        Command::CreateUser { username, .. } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_create_user"
        }
        Command::AlterUserPassword { username, .. } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_alter_user_password"
        }
        Command::DropUser { username } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_drop_user"
        }
        Command::CreateRole { role } => {
            details.insert("role".to_string(), role.clone());
            "rbac_create_role"
        }
        Command::DropRole { role } => {
            details.insert("role".to_string(), role.clone());
            "rbac_drop_role"
        }
        Command::GrantRole { role, username } => {
            details.insert("role".to_string(), role.clone());
            details.insert("target_user".to_string(), username.clone());
            "rbac_grant_role"
        }
        Command::RevokeRole { role, username } => {
            details.insert("role".to_string(), role.clone());
            details.insert("target_user".to_string(), username.clone());
            "rbac_revoke_role"
        }
        Command::GrantPermission {
            permission,
            pattern,
            role,
        } => {
            details.insert("permission".to_string(), permission.clone());
            details.insert("pattern".to_string(), pattern.clone());
            details.insert("role".to_string(), role.clone());
            "rbac_grant_permission"
        }
        Command::RevokePermission {
            permission,
            pattern,
            role,
        } => {
            details.insert("permission".to_string(), permission.clone());
            details.insert("pattern".to_string(), pattern.clone());
            details.insert("role".to_string(), role.clone());
            "rbac_revoke_permission"
        }
        _ => return None,
    };
    Some((event_type, details))
}

fn audit_status(status: Status) -> String {
    match status {
        Status::Ok => "ok",
        Status::Error => "error",
        Status::NotFound => "not_found",
    }
    .to_string()
}

fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::{
        AuditContext, AuthLockoutState, MaintenanceMode, RateLimiter, ServerGuards, SessionState,
        backup_manifest_path, error_response, execute_command, handle_auth,
        handle_transaction_command, process_command, record_semantic_audit_event,
        record_slow_command_event, structured_info, validate_command,
    };
    use crate::audit::AuditLogger;
    use crate::auth::{AuthConfig, Permission, PermissionGrant};
    use crate::metrics::Metrics;
    use crate::server::{EngineHandle, ServerRuntimeConfig};
    use command::{
        Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
        SetOptions as CommandSetOptions,
    };
    use engine::{
        Engine, Expiration, Paths, Result, ScanPage, SetCondition, SetOptions, SetOutcome,
        StorageEngine, StorageKey, StorageKeyring, WalSyncPolicy,
    };
    use transport::{CodecOptions, Response, Status};
    use uuid::Uuid;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-server-test-{name}-{unique}"))
    }

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn test_keyring(secret: &str) -> StorageKeyring {
        StorageKeyring {
            active: StorageKey {
                id: Uuid::from_u128(1),
                secret: secret.to_string(),
                created_at_ms: now_ms(),
            },
            previous: Vec::new(),
        }
    }

    fn guards() -> ServerGuards {
        ServerGuards {
            max_request_payload_bytes: 1024 * 1024,
            max_key_bytes: 1024,
            max_value_bytes: 1024,
            max_keys_per_batch: 16,
            max_transaction_queue_len: 16,
            requests_per_second: 100,
            request_burst: 100,
        }
    }

    fn runtime() -> ServerRuntimeConfig {
        let audit_path = temp_dir("audit").join("audit.log");
        let backup_dir = temp_dir("backups");
        let maintenance_path = temp_dir("maintenance").join("maintenance.mode");
        ServerRuntimeConfig {
            snapshot_interval: None,
            expiration_sweep_interval: None,
            idle_timeout: None,
            auth_config: Some(AuthConfig::new("dbuser".to_string(), "secret".to_string()).unwrap()),
            guards: guards(),
            tls_state: None,
            transport: CodecOptions::default(),
            backup_dir,
            mtls_enabled: false,
            slow_command_threshold: Some(Duration::from_millis(100)),
            audit_logger: Arc::new(AuditLogger::open(&audit_path).unwrap()),
            wal_segment_size_bytes: engine::DEFAULT_WAL_SEGMENT_SIZE_BYTES,
            wal_retain_segments: engine::DEFAULT_WAL_RETAIN_SEGMENTS,
            auth_failure_window: Duration::from_secs(300),
            auth_failure_limit: 5,
            auth_lockout: Duration::from_secs(900),
            transaction_max_duration: Duration::from_secs(30),
            maintenance: Arc::new(MaintenanceMode::load(maintenance_path).unwrap()),
            auth_lockouts: Arc::new(tokio::sync::Mutex::new(AuthLockoutState::default())),
            insecure_auth_disabled: false,
            insecure_default_credentials: false,
        }
    }

    async fn authenticate(
        metrics: Arc<Metrics>,
        runtime: &ServerRuntimeConfig,
        session: &mut SessionState,
        request_id: Uuid,
        username: &str,
        password: &str,
    ) -> crate::Result<Response> {
        handle_auth(
            metrics,
            runtime,
            session,
            None,
            request_id,
            Command::Auth {
                username: username.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    async fn run_command(
        engine: EngineHandle,
        metrics: Arc<Metrics>,
        runtime: &ServerRuntimeConfig,
        session: &mut SessionState,
        request_id: Uuid,
        command: Command,
    ) -> crate::Result<Response> {
        process_command(engine, metrics, runtime, session, None, request_id, command).await
    }

    #[derive(Default)]
    struct FakeEngine {
        data: BTreeMap<String, String>,
    }

    impl StorageEngine for FakeEngine {
        fn get(&mut self, key: &str) -> Result<Option<String>> {
            Ok(self.data.get(key).cloned())
        }
        fn set_with_options(
            &mut self,
            key: String,
            value: String,
            options: SetOptions,
        ) -> Result<SetOutcome> {
            let previous = self.data.get(&key).cloned();
            let allowed = match options.condition {
                Some(SetCondition::Nx) => previous.is_none(),
                Some(SetCondition::Xx) => previous.is_some(),
                None => true,
            };
            if allowed {
                self.data.insert(key, value);
            }
            Ok(SetOutcome {
                applied: allowed,
                previous,
            })
        }
        fn get_del(&mut self, key: &str) -> Result<Option<String>> {
            Ok(self.data.remove(key))
        }
        fn get_ex(
            &mut self,
            key: &str,
            _expiration: Option<Expiration>,
            _persist: bool,
        ) -> Result<Option<String>> {
            Ok(self.data.get(key).cloned())
        }
        fn mget(&mut self, keys: &[String]) -> Result<Vec<Option<String>>> {
            Ok(keys.iter().map(|key| self.data.get(key).cloned()).collect())
        }
        fn mset(&mut self, entries: &[(String, String)]) -> Result<()> {
            for (key, value) in entries {
                self.data.insert(key.clone(), value.clone());
            }
            Ok(())
        }
        fn delete(&mut self, key: &str) -> Result<bool> {
            Ok(self.data.remove(key).is_some())
        }
        fn delete_many(&mut self, keys: &[String]) -> Result<usize> {
            Ok(keys
                .iter()
                .filter(|key| self.data.remove(key.as_str()).is_some())
                .count())
        }
        fn exists(&mut self, key: &str) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }
        fn incr(&mut self, key: &str) -> Result<i64> {
            let value = self
                .data
                .get(key)
                .cloned()
                .unwrap_or_else(|| "0".to_string())
                .parse::<i64>()
                .unwrap()
                + 1;
            self.data.insert(key.to_string(), value.to_string());
            Ok(value)
        }
        fn decr(&mut self, key: &str) -> Result<i64> {
            let value = self
                .data
                .get(key)
                .cloned()
                .unwrap_or_else(|| "0".to_string())
                .parse::<i64>()
                .unwrap()
                - 1;
            self.data.insert(key.to_string(), value.to_string());
            Ok(value)
        }
        fn expire(&mut self, key: &str, _seconds: u64) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }
        fn ttl(&mut self, key: &str) -> Result<i64> {
            Ok(if self.data.contains_key(key) { -1 } else { -2 })
        }
        fn persist(&mut self, key: &str) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }
        fn rename(&mut self, source: &str, destination: String) -> Result<bool> {
            if let Some(value) = self.data.remove(source) {
                self.data.insert(destination, value);
                Ok(true)
            } else {
                Ok(false)
            }
        }
        fn rename_nx(&mut self, source: &str, destination: String) -> Result<bool> {
            if self.data.contains_key(&destination) {
                Ok(false)
            } else {
                self.rename(source, destination)
            }
        }
        fn db_size(&mut self) -> Result<usize> {
            Ok(self.data.len())
        }
        fn scan(
            &mut self,
            cursor: u64,
            pattern: Option<&str>,
            count: Option<u16>,
        ) -> Result<ScanPage> {
            let mut keys: Vec<String> = self.data.keys().cloned().collect();
            if let Some(pattern) = pattern {
                keys.retain(|key| key.starts_with(pattern.trim_end_matches('*')));
            }
            let start = cursor as usize;
            let limit = usize::from(count.unwrap_or(10));
            let end = (start + limit).min(keys.len());
            Ok(ScanPage {
                next_cursor: if end >= keys.len() { 0 } else { end as u64 },
                keys: keys[start..end].to_vec(),
            })
        }
        fn list(&mut self) -> Result<Vec<(String, String)>> {
            Ok(self
                .data
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
        fn info(&mut self) -> Result<Vec<(String, String)>> {
            Ok(vec![("key_count".to_string(), self.data.len().to_string())])
        }
        fn logical_backup(&mut self) -> Result<String> {
            Ok(serde_json::json!({
                "version": 1,
                "entries": self
                    .data
                    .iter()
                    .map(|(key, value)| serde_json::json!({
                        "key": key,
                        "value": value,
                        "expires_at_ms": null,
                    }))
                    .collect::<Vec<_>>(),
            })
            .to_string())
        }
        fn restore_logical_backup(&mut self, dump: &str) -> Result<usize> {
            let backup: serde_json::Value = serde_json::from_str(dump)
                .map_err(|err| engine::EngineError::SnapshotDeserialize(err.to_string()))?;
            self.data.clear();
            if let Some(entries) = backup.get("entries").and_then(|entries| entries.as_array()) {
                for entry in entries {
                    if let (Some(key), Some(value)) = (
                        entry.get("key").and_then(|key| key.as_str()),
                        entry.get("value").and_then(|value| value.as_str()),
                    ) {
                        self.data.insert(key.to_string(), value.to_string());
                    }
                }
            }
            Ok(self.data.len())
        }
        fn validate_logical_backup(&mut self, dump: &str) -> Result<usize> {
            let backup: serde_json::Value = serde_json::from_str(dump)
                .map_err(|err| engine::EngineError::SnapshotDeserialize(err.to_string()))?;
            Ok(backup
                .get("entries")
                .and_then(|entries| entries.as_array())
                .map(|entries| entries.len())
                .unwrap_or_default())
        }
        fn sweep_expired(&mut self) -> Result<usize> {
            Ok(0)
        }
        fn clear(&mut self) -> Result<()> {
            self.data.clear();
            Ok(())
        }
        fn snapshot(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn routes_value_and_ok_commands() {
        let mut engine = FakeEngine::default();
        engine.set("name".to_string(), "alice".to_string()).unwrap();
        let get = execute_command(
            &mut engine,
            id(41),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        assert_eq!(get.request_id, id(41));
        assert_eq!(get.status, Status::Ok);
        assert_eq!(get.decode_value().unwrap(), "alice");
        let set = execute_command(
            &mut engine,
            id(42),
            Command::Set {
                key: "city".to_string(),
                value: "paris".to_string(),
                options: CommandSetOptions::default(),
            },
        )
        .unwrap();
        assert_eq!(set, Response::ok(id(42)));
    }

    #[test]
    fn routes_set_getdel_getex_and_scan_responses() {
        let mut engine = FakeEngine::default();
        engine
            .mset(&[
                ("user:1".into(), "alice".into()),
                ("other".into(), "x".into()),
            ])
            .unwrap();
        let set = execute_command(
            &mut engine,
            id(1),
            Command::Set {
                key: "user:1".to_string(),
                value: "bob".to_string(),
                options: CommandSetOptions {
                    condition: Some(CommandSetCondition::Xx),
                    expiration: Some(CommandExpiration::Ex(60)),
                    keep_ttl: false,
                    return_previous: true,
                },
            },
        )
        .unwrap();
        assert_eq!(set.decode_value().unwrap(), "alice");
        let getdel = execute_command(
            &mut engine,
            id(2),
            Command::GetDel {
                key: "user:1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(getdel.decode_value().unwrap(), "bob");
        let getex = execute_command(
            &mut engine,
            id(3),
            Command::GetEx {
                key: "other".to_string(),
                expiration: Some(CommandExpiration::Px(1_500)),
                persist: false,
            },
        )
        .unwrap();
        assert_eq!(getex.decode_value().unwrap(), "x");
        let scan = execute_command(
            &mut engine,
            id(4),
            Command::Scan {
                cursor: 0,
                pattern: Some("other*".to_string()),
                count: Some(10),
            },
        )
        .unwrap()
        .decode_scan()
        .unwrap();
        assert_eq!(scan.keys, vec!["other".to_string()]);
    }

    #[test]
    fn handles_auth_and_transaction_queueing() {
        let metrics = Arc::new(Metrics::default());
        let runtime = runtime();
        let mut session = SessionState::new(&guards());
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let denied = runtime_handle.block_on(authenticate(
            Arc::clone(&metrics),
            &runtime,
            &mut session,
            id(1),
            "dbuser",
            "wrong",
        ));
        assert!(denied.is_err());
        let ok = runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(2),
                "dbuser",
                "secret",
            ))
            .unwrap();
        assert_eq!(ok.status, Status::Ok);
        assert!(session.is_authenticated());

        session.transaction_queue.push(Command::Multi);
        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("tx")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("tx-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);
        let queued = runtime_handle
            .block_on(handle_transaction_command(
                handle,
                metrics,
                &runtime,
                &mut session,
                id(3),
                Command::Set {
                    key: "key".to_string(),
                    value: "value".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap();
        assert_eq!(queued.decode_value().unwrap(), "QUEUED");
    }

    #[test]
    fn rbac_allows_admin_management_and_denies_missing_permissions() {
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let metrics = Arc::new(Metrics::default());
        let runtime = runtime();
        let mut admin_session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut admin_session,
                id(20),
                "dbuser",
                "secret",
            ))
            .unwrap();

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("rbac")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("rbac-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);

        for (request_id, command) in [
            (
                id(21),
                Command::CreateUser {
                    username: "alice".to_string(),
                    password: "password1234".to_string(),
                },
            ),
            (
                id(22),
                Command::CreateRole {
                    role: "readonly".to_string(),
                },
            ),
            (
                id(23),
                Command::GrantPermission {
                    permission: "read".to_string(),
                    pattern: "*".to_string(),
                    role: "readonly".to_string(),
                },
            ),
            (
                id(24),
                Command::GrantRole {
                    role: "readonly".to_string(),
                    username: "alice".to_string(),
                },
            ),
        ] {
            let response = runtime_handle
                .block_on(run_command(
                    handle.clone(),
                    Arc::clone(&metrics),
                    &runtime,
                    &mut admin_session,
                    request_id,
                    command,
                ))
                .unwrap();
            assert_eq!(response.status, Status::Ok);
        }

        let user_grants = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut admin_session,
                id(241),
                Command::ShowGrantsForUser {
                    username: "alice".to_string(),
                },
            ))
            .unwrap()
            .decode_entries()
            .unwrap();
        assert!(
            user_grants
                .iter()
                .any(|(key, grant)| key == "user.alice.roles" && grant == "readonly")
        );
        assert!(
            user_grants
                .iter()
                .any(|(_, grant)| grant == "role=readonly read on *")
        );

        let role_grants = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut admin_session,
                id(242),
                Command::ShowGrantsForRole {
                    role: "readonly".to_string(),
                },
            ))
            .unwrap()
            .decode_entries()
            .unwrap();
        assert!(role_grants.iter().any(|(_, grant)| grant == "read on *"));

        let mut readonly_session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut readonly_session,
                id(25),
                "alice",
                "password1234",
            ))
            .unwrap();

        let read_response = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut readonly_session,
                id(26),
                Command::Get {
                    key: "missing".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(read_response.status, Status::NotFound);

        let own_grants = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut readonly_session,
                id(261),
                Command::ShowGrants,
            ))
            .unwrap()
            .decode_entries()
            .unwrap();
        assert!(
            own_grants
                .iter()
                .any(|(_, grant)| grant == "role=readonly read on *")
        );

        let inspect_other_denied = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut readonly_session,
                id(262),
                Command::ShowGrantsForUser {
                    username: "dbuser".to_string(),
                },
            ))
            .unwrap_err();
        assert!(matches!(
            inspect_other_denied,
            crate::ServerError::PermissionDenied
        ));

        let write_denied = runtime_handle
            .block_on(run_command(
                handle,
                metrics,
                &runtime,
                &mut readonly_session,
                id(27),
                Command::Set {
                    key: "key".to_string(),
                    value: "value".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap_err();
        assert!(matches!(write_denied, crate::ServerError::PermissionDenied));
    }

    #[test]
    fn rbac_enforces_key_patterns_and_destructive_permissions() {
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let metrics = Arc::new(Metrics::default());
        let runtime = runtime();
        let mut admin_session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut admin_session,
                id(30),
                "dbuser",
                "secret",
            ))
            .unwrap();

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("rbac-patterns")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("rbac-pattern-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);

        for (request_id, command) in [
            (
                id(31),
                Command::CreateUser {
                    username: "alice".to_string(),
                    password: "password1234".to_string(),
                },
            ),
            (
                id(32),
                Command::CreateRole {
                    role: "app_writer".to_string(),
                },
            ),
            (
                id(33),
                Command::GrantPermission {
                    permission: "write".to_string(),
                    pattern: "app:*".to_string(),
                    role: "app_writer".to_string(),
                },
            ),
            (
                id(34),
                Command::GrantRole {
                    role: "app_writer".to_string(),
                    username: "alice".to_string(),
                },
            ),
        ] {
            let response = runtime_handle
                .block_on(run_command(
                    handle.clone(),
                    Arc::clone(&metrics),
                    &runtime,
                    &mut admin_session,
                    request_id,
                    command,
                ))
                .unwrap();
            assert_eq!(response.status, Status::Ok);
        }

        let mut alice_session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut alice_session,
                id(35),
                "alice",
                "password1234",
            ))
            .unwrap();

        let allowed = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut alice_session,
                id(36),
                Command::Set {
                    key: "app:1".to_string(),
                    value: "ok".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap();
        assert_eq!(allowed.status, Status::Ok);

        let denied = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut alice_session,
                id(37),
                Command::Set {
                    key: "other:1".to_string(),
                    value: "denied".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap_err();
        assert!(matches!(denied, crate::ServerError::PermissionDenied));

        let multi_key_denied = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut alice_session,
                id(38),
                Command::MSet {
                    entries: vec![
                        ("app:2".to_string(), "ok".to_string()),
                        ("other:2".to_string(), "denied".to_string()),
                    ],
                },
            ))
            .unwrap_err();
        assert!(matches!(
            multi_key_denied,
            crate::ServerError::PermissionDenied
        ));

        let clear_denied = runtime_handle
            .block_on(run_command(
                handle,
                metrics,
                &runtime,
                &mut alice_session,
                id(39),
                Command::Clear,
            ))
            .unwrap_err();
        assert!(matches!(clear_denied, crate::ServerError::PermissionDenied));
    }

    #[test]
    fn rejects_local_only_commands_and_builds_error_payloads() {
        let mut engine = FakeEngine::default();
        assert!(execute_command(&mut engine, id(7), Command::Help).is_err());
        assert!(execute_command(&mut engine, id(8), Command::Exit).is_err());
        let response = error_response(id(9), "SRV-400", "Bad Request", "invalid request");
        assert_eq!(response.status, Status::Error);
        let payload = response.decode_error().unwrap();
        assert_eq!(payload.code, "SRV-400");
        assert_eq!(payload.name, "Bad Request");
        assert_eq!(payload.message, "invalid request");
    }

    #[test]
    fn routes_logical_backup_and_restore_commands() {
        let mut engine = FakeEngine::default();
        engine
            .set("app:mode".to_string(), "production".to_string())
            .unwrap();

        let dump = execute_command(&mut engine, id(10), Command::Backup)
            .unwrap()
            .decode_value()
            .unwrap();
        assert!(dump.contains("app:mode"));

        engine.set("old".to_string(), "value".to_string()).unwrap();
        let restored = execute_command(&mut engine, id(11), Command::Restore { dump })
            .unwrap()
            .decode_count()
            .unwrap();
        assert_eq!(restored, 1);
        assert_eq!(
            engine.get("app:mode").unwrap(),
            Some("production".to_string())
        );
        assert_eq!(engine.get("old").unwrap(), None);
    }

    #[test]
    fn routes_sandboxed_backup_files_and_restore_checks() {
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let metrics = Arc::new(Metrics::default());
        let runtime = runtime();
        let mut session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(50),
                "dbuser",
                "secret",
            ))
            .unwrap();

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("backup-files")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("backup-file-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);

        runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(51),
                Command::Set {
                    key: "app:mode".to_string(),
                    value: "production".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap();

        let backup = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(52),
                Command::BackupTo {
                    path: "nightly.json".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(backup.status, Status::Ok);
        let backup_path = runtime.backup_dir.join("nightly.json");
        assert!(backup_path.exists());
        let manifest_path = backup_manifest_path(&backup_path);
        assert!(manifest_path.exists());

        let verified = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(520),
                Command::BackupVerifyFrom {
                    path: "nightly.json".to_string(),
                },
            ))
            .unwrap()
            .decode_entries()
            .unwrap();
        assert!(
            verified
                .iter()
                .any(|(key, value)| key == "status" && value == "ok")
        );
        assert!(
            verified
                .iter()
                .any(|(key, value)| key == "entries" && value == "1")
        );
        assert!(
            verified
                .iter()
                .any(|(key, value)| key == "sha256" && value.len() == 64)
        );

        let dump = fs::read_to_string(&backup_path).unwrap();
        let inline_verified = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(521),
                Command::BackupVerify { dump },
            ))
            .unwrap()
            .decode_entries()
            .unwrap();
        assert!(
            inline_verified
                .iter()
                .any(|(key, value)| key == "status" && value == "ok")
        );

        let rejected = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(53),
                Command::BackupTo {
                    path: "../escape.json".to_string(),
                },
            ))
            .unwrap_err();
        assert!(matches!(
            rejected,
            crate::ServerError::BackupPathRejected(_)
        ));

        runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(54),
                Command::Set {
                    key: "other".to_string(),
                    value: "temporary".to_string(),
                    options: CommandSetOptions::default(),
                },
            ))
            .unwrap();

        let checked = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(55),
                Command::RestoreCheckFrom {
                    path: "nightly.json".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(checked.decode_count().unwrap(), 1);

        let still_present = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(56),
                Command::Get {
                    key: "other".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(still_present.decode_value().unwrap(), "temporary");

        let restored = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(57),
                Command::RestoreFrom {
                    path: "nightly.json".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(restored.decode_count().unwrap(), 1);

        let removed = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(58),
                Command::Get {
                    key: "other".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(removed.status, Status::NotFound);

        fs::write(&manifest_path, br#"{"manifest_version":1}"#).unwrap();
        let corrupt_manifest = runtime_handle
            .block_on(run_command(
                handle.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(59),
                Command::BackupVerifyFrom {
                    path: "nightly.json".to_string(),
                },
            ))
            .unwrap_err();
        assert!(matches!(
            corrupt_manifest,
            crate::ServerError::BackupVerification(_)
        ));

        fs::remove_dir_all(runtime.backup_dir).ok();
    }

    #[test]
    fn renders_prometheus_metrics_text() {
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let runtime = runtime();
        let metrics = Arc::new(Metrics::default());
        metrics.requests_total.store(7, Ordering::Relaxed);
        metrics.active_connections.store(2, Ordering::Relaxed);

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("metrics-prom")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("metrics-prom-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);

        let mut session = SessionState::new(&guards());
        runtime_handle
            .block_on(authenticate(
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                id(70),
                "dbuser",
                "secret",
            ))
            .unwrap();

        let response = runtime_handle
            .block_on(run_command(
                handle,
                metrics,
                &runtime,
                &mut session,
                id(71),
                Command::MetricsProm,
            ))
            .unwrap();
        let body = response.decode_value().unwrap();
        assert!(body.contains("# HELP vaylix_server_request_count"));
        assert!(body.contains("# TYPE vaylix_server_request_count counter"));
        assert!(body.contains("vaylix_server_request_count 7"));
        assert!(body.contains("# TYPE vaylix_server_connection_active gauge"));
        assert!(body.contains("vaylix_server_connection_active 2"));
    }

    #[test]
    fn records_semantic_and_slow_audit_events_without_secrets() {
        let runtime = runtime();
        let mut session = SessionState::new(&guards());
        session.identity = Some(crate::auth::Identity {
            username: "dbuser".to_string(),
            permissions: Permission::all(),
            grants: Permission::all()
                .into_iter()
                .map(|permission| PermissionGrant {
                    permission,
                    pattern: "*".to_string(),
                })
                .collect(),
        });
        let context = AuditContext {
            connection_id: 1,
            peer_addr: None,
            session: &session,
            request_id: id(80),
            opcode: "GRANT_PERMISSION",
            status: Status::Ok,
            error_code: None,
            latency_ms: 2,
        };
        record_semantic_audit_event(
            &runtime.audit_logger,
            context,
            &Command::GrantPermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string(),
            },
        );

        let context = AuditContext {
            connection_id: 1,
            peer_addr: None,
            session: &session,
            request_id: id(81),
            opcode: "GET",
            status: Status::Ok,
            error_code: None,
            latency_ms: 101,
        };
        record_slow_command_event(&runtime.audit_logger, &runtime, context);

        let body = fs::read_to_string(runtime.audit_logger.path()).unwrap();
        assert!(body.contains(r#""event_type":"rbac_grant_permission""#));
        assert!(body.contains(r#""permission":"read""#));
        assert!(body.contains(r#""pattern":"app:*""#));
        assert!(body.contains(r#""role":"readonly""#));
        assert!(body.contains(r#""event_type":"slow_command""#));
        assert!(body.contains(r#""threshold_ms":"100""#));
        assert!(!body.contains("password"));
        assert!(!body.contains("secret"));
    }

    #[test]
    fn structured_info_uses_section_prefixed_keys() {
        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("info")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("info-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);
        let metrics = Arc::new(Metrics::default());
        let entries = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(structured_info(handle, metrics, &runtime()))
            .unwrap();

        assert!(entries.iter().any(|(key, _)| key == "server.version"));
        assert!(
            entries
                .iter()
                .any(|(key, value)| key == "transport.protocol_version" && value == "2")
        );
        assert!(entries.iter().any(|(key, _)| key == "storage.key_count"));
        assert!(
            entries
                .iter()
                .any(|(key, _)| key == "persistence.wal_size_bytes")
        );
        assert!(entries.iter().any(|(key, _)| key == "security.tls_enabled"));
        assert!(
            entries
                .iter()
                .any(|(key, _)| key == "runtime.idle_timeout_seconds")
        );
        assert!(
            entries
                .iter()
                .any(|(key, value)| key == "runtime.slow_command_threshold_ms" && value == "100")
        );
        assert!(
            entries
                .iter()
                .any(|(key, _)| key == "metrics.vaylix.server.request.count")
        );
    }

    #[test]
    fn enforces_command_quotas() {
        let limited = ServerGuards {
            max_request_payload_bytes: 32,
            max_key_bytes: 4,
            max_value_bytes: 5,
            max_keys_per_batch: 1,
            max_transaction_queue_len: 1,
            requests_per_second: 1,
            request_burst: 1,
        };

        assert!(
            validate_command(
                &Command::Get {
                    key: "oversized".to_string()
                },
                &limited
            )
            .is_err()
        );
        assert!(
            validate_command(
                &Command::Set {
                    key: "key".to_string(),
                    value: "oversized".to_string(),
                    options: CommandSetOptions::default()
                },
                &limited
            )
            .is_err()
        );
        assert!(
            validate_command(
                &Command::MGet {
                    keys: vec!["a".to_string(), "b".to_string()]
                },
                &limited
            )
            .is_err()
        );
    }

    #[test]
    fn rate_limiter_blocks_excess_burst() {
        let mut limiter = RateLimiter::new(1, 1);
        assert!(limiter.allow());
        assert!(!limiter.allow());
    }
}
