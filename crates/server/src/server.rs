use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use command::{
    Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
    SetOptions as CommandSetOptions,
};
use engine::{
    Engine, EngineOptions, Expiration, Paths, ScanPage, SetCondition, SetOptions, SetOutcome,
    StorageEngine, TransactionResult,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_rustls::TlsAcceptor;
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
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
    pub transport: CodecOptions,
    pub audit_logger: Arc<AuditLogger>,
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
}

impl SessionState {
    fn new(guards: &ServerGuards) -> Self {
        Self {
            identity: None,
            transaction_queue: Vec::new(),
            rate_limiter: RateLimiter::new(guards.requests_per_second, guards.request_burst),
        }
    }

    fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }

    fn in_transaction(&self) -> bool {
        !self.transaction_queue.is_empty()
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
        engine: Engine,
        runtime: ServerRuntimeConfig,
    ) -> Result<Self> {
        let addr = format!("{bind}:{port}");
        log_event(
            "INFO",
            "server.startup",
            &format!(
                "binding listener to {addr} max_connections={max_connections} snapshot_interval={:?} sweep_interval={:?} idle_timeout={:?} tls_enabled={} auth_required={} compression={}",
                runtime.snapshot_interval,
                runtime.expiration_sweep_interval,
                runtime.idle_timeout,
                runtime.tls_config.is_some(),
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

        Ok(Self {
            listener,
            engine: EngineHandle::new(engine),
            connection_slots: Arc::new(Semaphore::new(max_connections)),
            next_connection_id: AtomicU64::new(1),
            runtime,
            metrics: Arc::new(Metrics::default()),
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
                                let result = if let Some(tls_config) = runtime.tls_config.clone() {
                                    let acceptor = TlsAcceptor::from(tls_config);
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
            request_id,
            command,
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
                error_code: audit_error,
                latency_ms: started_at.elapsed().as_millis(),
            },
        );
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
        Command::Restore { dump } => validate_value(dump, guards)?,
        Command::CreateUser { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::DropUser { username } => validate_key(username, guards)?,
        Command::CreateRole { role } | Command::DropRole { role } => validate_key(role, guards)?,
        Command::GrantRole { role, username } | Command::RevokeRole { role, username } => {
            validate_key(role, guards)?;
            validate_key(username, guards)?;
        }
        Command::GrantPermission { permission, role }
        | Command::RevokePermission { permission, role } => {
            validate_key(permission, guards)?;
            validate_key(role, guards)?;
        }
        _ => {}
    }

    Ok(())
}

async fn process_command(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    if matches!(command, Command::Auth { .. }) {
        return handle_auth(
            metrics,
            runtime.auth_config.clone(),
            session,
            request_id,
            command,
        )
        .await;
    }

    if runtime.auth_config.is_some()
        && !session.is_authenticated()
        && !matches!(command, Command::Ping { .. })
    {
        metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        return Err(ServerError::AuthenticationRequired);
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
            Ok(Response::ok(request_id))
        }
        Command::Exec | Command::Discard => Err(ServerError::NoActiveTransaction),
        Command::Metrics => {
            let entries = metrics.snapshot();
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Info => {
            let entries = structured_info(engine, metrics, runtime).await?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::CreateUser { username, password } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config.create_user(username, password).await?;
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
        Command::GrantPermission { permission, role } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config
                .grant_permission(Permission::parse(&permission)?, &role)
                .await?;
            Ok(Response::ok(request_id))
        }
        Command::RevokePermission { permission, role } => {
            let Some(auth_config) = runtime.auth_config.clone() else {
                return Err(ServerError::UnsupportedRemoteCommand);
            };
            auth_config
                .revoke_permission(Permission::parse(&permission)?, &role)
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
            "transport.max_frame_len".to_string(),
            runtime.transport.max_frame_len.to_string(),
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
            "persistence.wal_sync_policy".to_string(),
            lookup("wal_sync_policy"),
        ),
        (
            "persistence.last_snapshot_at_ms".to_string(),
            lookup("last_snapshot_at_ms"),
        ),
        (
            "security.auth_required".to_string(),
            runtime.auth_config.is_some().to_string(),
        ),
        (
            "security.rbac_enabled".to_string(),
            runtime.auth_config.is_some().to_string(),
        ),
        (
            "security.tls_enabled".to_string(),
            runtime.tls_config.is_some().to_string(),
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
    auth_config: Option<AuthConfig>,
    session: &mut SessionState,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    let Command::Auth { username, password } = command else {
        unreachable!();
    };

    let Some(auth_config) = auth_config else {
        session.identity = Some(Identity {
            username: "anonymous".to_string(),
            permissions: Permission::all(),
        });
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::ok(request_id));
    };

    if let Some(identity) = auth_config.verify(&username, &password).await? {
        session.identity = Some(identity);
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::ok(request_id));
    }

    metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
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
    match command {
        Command::Multi => Err(ServerError::TransactionAlreadyActive),
        Command::Discard => {
            session.transaction_queue.clear();
            metrics
                .transactions_discarded
                .fetch_add(1, Ordering::Relaxed);
            Ok(Response::ok(request_id))
        }
        Command::Exec => {
            let mut queued = std::mem::take(&mut session.transaction_queue);
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

fn authorize_command(command: &Command, session: &SessionState) -> Result<()> {
    let Some(permission) = command_permission(command) else {
        return Ok(());
    };
    let Some(identity) = &session.identity else {
        return Err(ServerError::AuthenticationRequired);
    };
    if identity.has(permission) {
        Ok(())
    } else {
        Err(ServerError::PermissionDenied)
    }
}

fn command_permission(command: &Command) -> Option<Permission> {
    match command {
        Command::Ping { .. }
        | Command::Auth { .. }
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::WhoAmI => None,
        Command::Get { .. }
        | Command::Exists { .. }
        | Command::MGet { .. }
        | Command::Ttl { .. }
        | Command::Scan { .. }
        | Command::DbSize
        | Command::Count
        | Command::List => Some(Permission::Read),
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
        | Command::RenameNx { .. }
        | Command::Clear => Some(Permission::Write),
        Command::Info | Command::Metrics => Some(Permission::Metrics),
        Command::Save | Command::Snapshot => Some(Permission::Snapshot),
        Command::Backup => Some(Permission::Backup),
        Command::Restore { .. } => Some(Permission::Restore),
        Command::CreateUser { .. }
        | Command::DropUser { .. }
        | Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowUsers
        | Command::ShowRoles => Some(Permission::Admin),
        Command::Help | Command::Exit => None,
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
        Command::Metrics => Err(ServerError::UnsupportedRemoteCommand),
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
        | Command::Save
        | Command::Snapshot
        | Command::Backup
        | Command::Restore { .. }
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
        | Command::WhoAmI
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
        Command::List => "LIST",
        Command::Clear => "CLEAR",
        Command::Count => "COUNT",
        Command::Save => "SAVE",
        Command::Snapshot => "SNAPSHOT",
        Command::Backup => "BACKUP",
        Command::Restore { .. } => "RESTORE",
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
        Command::WhoAmI => "WHOAMI",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Help => "HELP",
        Command::Exit => "EXIT",
    }
}

fn record_audit_event(logger: &AuditLogger, context: AuditContext<'_>) {
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
        latency_ms: context.latency_ms,
    });
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
    use std::sync::Arc;

    use super::{
        RateLimiter, ServerGuards, SessionState, error_response, execute_command, handle_auth,
        handle_transaction_command, process_command, structured_info, validate_command,
    };
    use crate::audit::AuditLogger;
    use crate::auth::AuthConfig;
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
        std::env::temp_dir().join(format!("veyra-server-test-{name}-{unique}"))
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
        ServerRuntimeConfig {
            snapshot_interval: None,
            expiration_sweep_interval: None,
            idle_timeout: None,
            auth_config: Some(AuthConfig::new("dbuser".to_string(), "secret".to_string()).unwrap()),
            guards: guards(),
            tls_config: None,
            transport: CodecOptions::default(),
            audit_logger: Arc::new(AuditLogger::open(&audit_path).unwrap()),
        }
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
        let auth = AuthConfig::new("dbuser".to_string(), "secret".to_string()).unwrap();
        let metrics = Arc::new(Metrics::default());
        let mut session = SessionState::new(&guards());
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let denied = runtime_handle.block_on(handle_auth(
            Arc::clone(&metrics),
            Some(auth.clone()),
            &mut session,
            id(1),
            Command::Auth {
                username: "dbuser".to_string(),
                password: "wrong".to_string(),
            },
        ));
        assert!(denied.is_err());
        let ok = runtime_handle
            .block_on(handle_auth(
                Arc::clone(&metrics),
                Some(auth),
                &mut session,
                id(2),
                Command::Auth {
                    username: "dbuser".to_string(),
                    password: "secret".to_string(),
                },
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
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);
        let queued = runtime_handle
            .block_on(handle_transaction_command(
                handle,
                metrics,
                &runtime(),
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
            .block_on(handle_auth(
                Arc::clone(&metrics),
                runtime.auth_config.clone(),
                &mut admin_session,
                id(20),
                Command::Auth {
                    username: "dbuser".to_string(),
                    password: "secret".to_string(),
                },
            ))
            .unwrap();

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("rbac")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("rbac-key")),
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);

        for (request_id, command) in [
            (
                id(21),
                Command::CreateUser {
                    username: "alice".to_string(),
                    password: "pw".to_string(),
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
                .block_on(process_command(
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

        let mut readonly_session = SessionState::new(&guards());
        runtime_handle
            .block_on(handle_auth(
                Arc::clone(&metrics),
                runtime.auth_config.clone(),
                &mut readonly_session,
                id(25),
                Command::Auth {
                    username: "alice".to_string(),
                    password: "pw".to_string(),
                },
            ))
            .unwrap();

        let read_response = runtime_handle
            .block_on(process_command(
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

        let write_denied = runtime_handle
            .block_on(process_command(
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
    fn structured_info_uses_section_prefixed_keys() {
        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("info")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("info-key")),
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
        assert!(entries.iter().any(|(key, _)| key.starts_with("metrics.")));
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
