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
    StorageEngine,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_rustls::TlsAcceptor;
use transport::{
    Request, Response, Status, TransportError, read_request_from_async, write_response_to_async,
};
use uuid::Uuid;

use crate::auth::AuthConfig;
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
    pub auth_config: AuthConfig,
    pub guards: ServerGuards,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
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
        respond_to: oneshot::Sender<Result<Vec<std::result::Result<Response, ServerError>>>>,
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
                        let mut responses = Vec::with_capacity(commands.len());
                        for command in commands {
                            responses.push(execute_command(&mut engine, Uuid::now_v7(), command));
                        }
                        let _ = respond_to.send(Ok(responses));
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
    ) -> Result<Vec<std::result::Result<Response, ServerError>>> {
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
    authenticated_user: Option<String>,
    transaction_queue: Vec<Command>,
    rate_limiter: RateLimiter,
}

impl SessionState {
    fn new(guards: &ServerGuards) -> Self {
        Self {
            authenticated_user: None,
            transaction_queue: Vec::new(),
            rate_limiter: RateLimiter::new(guards.requests_per_second, guards.request_burst),
        }
    }

    fn is_authenticated(&self) -> bool {
        self.authenticated_user.is_some()
    }

    fn in_transaction(&self) -> bool {
        !self.transaction_queue.is_empty()
    }
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
                "binding listener to {addr} max_connections={max_connections} snapshot_interval={:?} sweep_interval={:?} idle_timeout={:?} tls_enabled={}",
                runtime.snapshot_interval,
                runtime.expiration_sweep_interval,
                runtime.idle_timeout,
                runtime.tls_config.is_some(),
            ),
        );

        let listener = TcpListener::bind(&addr).await.map_err(ServerError::Bind)?;
        let local_addr = listener.local_addr().map_err(ServerError::Bind)?;
        log_event(
            "INFO",
            "server.startup",
            &format!("listener ready on {local_addr} auth_required=true"),
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

    loop {
        let read_result = if let Some(idle_timeout) = runtime.idle_timeout {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = timeout(idle_timeout, read_request_from_async(&mut stream)) => {
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
                result = read_request_from_async(&mut stream) => result,
            }
        };

        let request = match read_result {
            Ok(request) => request,
            Err(TransportError::UnexpectedEof) => break,
            Err(err) => return Err(err.into()),
        };

        if !session.rate_limiter.allow() {
            let response = error_response(
                request.request_id,
                ServerError::RateLimitExceeded.code(),
                ServerError::RateLimitExceeded.name(),
                &ServerError::RateLimitExceeded.to_string(),
            );
            write_response_to_async(&mut stream, &response).await?;
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
                write_response_to_async(&mut stream, &response).await?;
                continue;
            }
        };

        if let Err(err) = validate_command(&command, &runtime.guards) {
            let response = error_response(request_id, err.code(), err.name(), &err.to_string());
            write_response_to_async(&mut stream, &response).await?;
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

        write_response_to_async(&mut stream, &response).await?;
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
        return handle_auth(metrics, &runtime.auth_config, session, request_id, command);
    }

    if !session.is_authenticated() && !matches!(command, Command::Ping { .. }) {
        metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        return Err(ServerError::AuthenticationRequired);
    }

    if session.in_transaction() {
        return handle_transaction_command(engine, metrics, runtime, session, request_id, command)
            .await;
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
            let mut entries = engine.info().await?;
            entries.extend(metrics.snapshot());
            entries.push((
                "tls_enabled".to_string(),
                runtime.tls_config.is_some().to_string(),
            ));
            Ok(Response::entries(request_id, &entries)?)
        }
        command => engine.execute(request_id, command).await,
    }
}

fn handle_auth(
    metrics: Arc<Metrics>,
    auth_config: &AuthConfig,
    session: &mut SessionState,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    let Command::Auth { username, password } = command else {
        unreachable!();
    };

    if auth_config.verify(&username, &password) {
        session.authenticated_user = Some(auth_config.username().to_string());
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        Ok(Response::ok(request_id))
    } else {
        metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        Err(ServerError::AuthenticationFailed)
    }
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
            let responses = engine.execute_batch(request_id, queued.clone()).await?;
            let mut rendered = Vec::with_capacity(responses.len());
            for (queued_command, response) in queued.iter().zip(responses) {
                match response {
                    Ok(response) => rendered.push(Some(render_transaction_response(
                        queued_command,
                        &response,
                    )?)),
                    Err(err) => rendered.push(Some(format!(
                        "ERROR [{}] {}: {}",
                        err.code(),
                        err.name(),
                        err
                    ))),
                }
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
            session.transaction_queue.push(other);
            Ok(Response::value(request_id, "QUEUED")?)
        }
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
        Command::Multi | Command::Exec | Command::Discard => {
            Err(ServerError::UnsupportedRemoteCommand)
        }
        Command::Help | Command::Exit => Err(ServerError::UnsupportedRemoteCommand),
    }
}

fn render_transaction_response(command: &Command, response: &Response) -> Result<String> {
    match response.status {
        Status::NotFound => Ok("NOT_FOUND".to_string()),
        Status::Error => {
            let error = response.decode_error()?;
            Ok(format!(
                "ERROR [{}] {}: {}",
                error.code, error.name, error.message
            ))
        }
        Status::Ok => match command {
            Command::Ping { .. }
            | Command::Get { .. }
            | Command::GetDel { .. }
            | Command::GetEx { .. } => response.decode_value().map_err(ServerError::from),
            Command::Set { options, .. } => {
                if options.return_previous {
                    response.decode_value().map_err(ServerError::from)
                } else if options.condition.is_some() {
                    Ok(response.decode_bool()?.to_string())
                } else {
                    Ok("OK".to_string())
                }
            }
            Command::SetNx { .. }
            | Command::Exists { .. }
            | Command::Expire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. } => Ok(response.decode_bool()?.to_string()),
            Command::MSet { .. } | Command::Clear | Command::Save | Command::Snapshot => {
                Ok("OK".to_string())
            }
            Command::MGet { .. } => Ok(response
                .decode_strings()?
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
                .collect::<Vec<_>>()
                .join(", ")),
            Command::Delete { .. } | Command::DbSize | Command::Count => {
                Ok(response.decode_count()?.to_string())
            }
            Command::Incr { .. } | Command::Decr { .. } | Command::Ttl { .. } => {
                Ok(response.decode_integer()?.to_string())
            }
            Command::Scan { .. } => {
                let scan = response.decode_scan()?;
                Ok(format!(
                    "cursor={}, keys=[{}]",
                    scan.next_cursor,
                    scan.keys.join(", ")
                ))
            }
            Command::Info | Command::Metrics | Command::List => Ok(response
                .decode_entries()?
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ")),
            _ => Ok("OK".to_string()),
        },
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::{
        RateLimiter, ServerGuards, SessionState, error_response, execute_command, handle_auth,
        handle_transaction_command, validate_command,
    };
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
    use transport::{Response, Status};
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
        ServerRuntimeConfig {
            snapshot_interval: None,
            expiration_sweep_interval: None,
            idle_timeout: None,
            auth_config: AuthConfig::new("dbuser".to_string(), "secret".to_string()).unwrap(),
            guards: guards(),
            tls_config: None,
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
        let denied = handle_auth(
            Arc::clone(&metrics),
            &auth,
            &mut session,
            id(1),
            Command::Auth {
                username: "dbuser".to_string(),
                password: "wrong".to_string(),
            },
        );
        assert!(denied.is_err());
        let ok = handle_auth(
            Arc::clone(&metrics),
            &auth,
            &mut session,
            id(2),
            Command::Auth {
                username: "dbuser".to_string(),
                password: "secret".to_string(),
            },
        )
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
        let queued = tokio::runtime::Runtime::new()
            .unwrap()
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
