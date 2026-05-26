use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use command::{
    Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
    SetOptions as CommandSetOptions,
};
use engine::{
    Engine, EngineOptions, Expiration, ScanPage, SetCondition, SetOptions, SetOutcome,
    StorageEngine,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task;
use tokio::time::{MissedTickBehavior, interval, timeout};
use transport::{Response, Status, TransportError, read_request_from_async, write_response_to_async};

use crate::auth::AuthConfig;
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;

/// Asynchronous Tokio-based database server with shared engine state.
pub struct Server {
    listener: TcpListener,
    engine: Arc<Mutex<Engine>>,
    connection_slots: Arc<Semaphore>,
    next_connection_id: AtomicU64,
    snapshot_interval: Option<Duration>,
    expiration_sweep_interval: Option<Duration>,
    idle_timeout: Option<Duration>,
    auth_config: Option<AuthConfig>,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct SessionState {
    authenticated_user: Option<String>,
    transaction_queue: Vec<Command>,
}

impl SessionState {
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
        engine_options: EngineOptions,
        snapshot_interval: Option<Duration>,
        expiration_sweep_interval: Option<Duration>,
        idle_timeout: Option<Duration>,
        auth_config: Option<AuthConfig>,
    ) -> Result<Self> {
        let engine = Engine::with_options(engine_options)?;
        Self::with_engine(
            bind,
            port,
            max_connections,
            engine,
            snapshot_interval,
            expiration_sweep_interval,
            idle_timeout,
            auth_config,
        )
        .await
    }

    /// Creates a server around an existing engine instance.
    pub async fn with_engine(
        bind: String,
        port: u16,
        max_connections: usize,
        engine: Engine,
        snapshot_interval: Option<Duration>,
        expiration_sweep_interval: Option<Duration>,
        idle_timeout: Option<Duration>,
        auth_config: Option<AuthConfig>,
    ) -> Result<Self> {
        let addr = format!("{bind}:{port}");
        log_event(
            "INFO",
            "server.startup",
            &format!(
                "binding listener to {addr} max_connections={max_connections} snapshot_interval={snapshot_interval:?} sweep_interval={expiration_sweep_interval:?} idle_timeout={idle_timeout:?}"
            ),
        );

        let listener = TcpListener::bind(&addr).await.map_err(ServerError::Bind)?;
        let local_addr = listener.local_addr().map_err(ServerError::Bind)?;

        log_event(
            "INFO",
            "server.startup",
            &format!(
                "listener ready on {local_addr} auth_required={}",
                auth_config.is_some()
            ),
        );

        Ok(Self {
            listener,
            engine: Arc::new(Mutex::new(engine)),
            connection_slots: Arc::new(Semaphore::new(max_connections)),
            next_connection_id: AtomicU64::new(1),
            snapshot_interval,
            expiration_sweep_interval,
            idle_timeout,
            auth_config,
            metrics: Arc::new(Metrics::default()),
        })
    }

    /// Returns the local socket address for the listener.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(ServerError::Bind)
    }

    /// Starts the accept loop and shuts down gracefully on Ctrl-C.
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
            snapshot_interval,
            expiration_sweep_interval,
            idle_timeout,
            auth_config,
            metrics,
        } = self;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        if let Some(snapshot_interval) = snapshot_interval {
            spawn_snapshotter(
                Arc::clone(&engine),
                Arc::clone(&metrics),
                snapshot_interval,
                shutdown_rx.clone(),
            );
        }

        if let Some(sweep_interval) = expiration_sweep_interval {
            spawn_expiration_sweeper(
                Arc::clone(&engine),
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
                        Ok(()) => {
                            log_event("INFO", "server.shutdown", "shutdown signal received");
                        }
                        Err(err) => {
                            log_event("ERROR", "server.shutdown", &format!("failed to receive shutdown signal: {err}"));
                        }
                    }
                    let _ = shutdown_tx.send(true);
                    let snapshot_result = task::spawn_blocking({
                        let engine = Arc::clone(&engine);
                        move || {
                            let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
                            engine.snapshot()?;
                            Ok::<(), ServerError>(())
                        }
                    }).await.map_err(|_| ServerError::EngineLockPoisoned)?;
                    snapshot_result?;
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
                            let engine = Arc::clone(&engine);
                            let metrics = Arc::clone(&metrics);
                            let auth_config = auth_config.clone();
                            let connection_shutdown = shutdown_rx.clone();

                            log_connection_event("INFO", connection_id, Some(peer_addr), "accepted client");

                            tokio::spawn(async move {
                                let _permit = permit;
                                let session_metrics = Arc::clone(&metrics);
                                let result = handle_client(
                                    engine,
                                    session_metrics,
                                    auth_config,
                                    idle_timeout,
                                    connection_id,
                                    Some(peer_addr),
                                    stream,
                                    connection_shutdown,
                                )
                                .await;

                                match result {
                                    Ok(()) => {
                                        log_connection_event("INFO", connection_id, Some(peer_addr), "client disconnected");
                                    }
                                    Err(err) => {
                                        log_connection_event(
                                            "ERROR",
                                            connection_id,
                                            Some(peer_addr),
                                            &format!("[{}] {}: {err}", err.code(), err.name()),
                                        );
                                    }
                                }

                                metrics.active_connections.fetch_sub(1, Ordering::Relaxed);
                                metrics.completed_connections.fetch_add(1, Ordering::Relaxed);
                            });
                        }
                        Err(err) => {
                            let err = ServerError::Accept(err);
                            log_event(
                                "ERROR",
                                "server.accept",
                                &format!("[{}] {}: {err}", err.code(), err.name()),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn spawn_snapshotter(
    engine: Arc<Mutex<Engine>>,
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
                    let engine = Arc::clone(&engine);
                    let result = task::spawn_blocking(move || {
                        let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
                        engine.snapshot()?;
                        Ok::<(), ServerError>(())
                    }).await;

                    match result {
                        Ok(Ok(())) => {
                            metrics.snapshots_completed.fetch_add(1, Ordering::Relaxed);
                            log_event("INFO", "server.snapshotter", "periodic snapshot complete");
                        }
                        Ok(Err(err)) => log_event("ERROR", "server.snapshotter", &format!("[{}] {}: {err}", err.code(), err.name())),
                        Err(_) => log_event("ERROR", "server.snapshotter", "[SRV-004] Engine Lock Poisoned: snapshot worker join failure"),
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
    engine: Arc<Mutex<Engine>>,
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
                    let engine = Arc::clone(&engine);
                    let result = task::spawn_blocking(move || {
                        let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
                        let removed = engine.sweep_expired()?;
                        Ok::<usize, ServerError>(removed)
                    }).await;

                    match result {
                        Ok(Ok(removed)) => {
                            metrics.expiration_sweeps.fetch_add(1, Ordering::Relaxed);
                            metrics.expired_keys_removed.fetch_add(removed as u64, Ordering::Relaxed);
                        }
                        Ok(Err(err)) => log_event("ERROR", "server.sweeper", &format!("[{}] {}: {err}", err.code(), err.name())),
                        Err(_) => log_event("ERROR", "server.sweeper", "[SRV-004] Engine Lock Poisoned: sweeper join failure"),
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

async fn handle_client(
    engine: Arc<Mutex<Engine>>,
    metrics: Arc<Metrics>,
    auth_config: Option<AuthConfig>,
    idle_timeout: Option<Duration>,
    connection_id: u64,
    peer_addr: Option<SocketAddr>,
    mut stream: TcpStream,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut session = SessionState::default();

    loop {
        let read_result = if let Some(idle_timeout) = idle_timeout {
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

        let response = match process_command(
            Arc::clone(&engine),
            Arc::clone(&metrics),
            auth_config.as_ref(),
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

async fn process_command(
    engine: Arc<Mutex<Engine>>,
    metrics: Arc<Metrics>,
    auth_config: Option<&AuthConfig>,
    session: &mut SessionState,
    request_id: u32,
    command: Command,
) -> Result<Response> {
    if matches!(command, Command::Auth { .. }) {
        return handle_auth(metrics, auth_config, session, request_id, command);
    }

    if auth_config.is_some() && !session.is_authenticated() && !matches!(command, Command::Ping { .. }) {
        metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        return Err(ServerError::AuthenticationRequired);
    }

    if session.in_transaction() {
        return handle_transaction_command(engine, metrics, session, request_id, command).await;
    }

    match command {
        Command::Multi => {
            metrics.transactions_started.fetch_add(1, Ordering::Relaxed);
            session.transaction_queue.push(Command::Multi);
            Ok(Response::ok(request_id))
        }
        Command::Exec => Err(ServerError::NoActiveTransaction),
        Command::Discard => Err(ServerError::NoActiveTransaction),
        Command::Metrics => {
            let entries = metrics.snapshot();
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Info => {
            let engine = Arc::clone(&engine);
            let metrics_snapshot = metrics.snapshot();
            task::spawn_blocking(move || {
                let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
                let mut entries = engine.info()?;
                entries.extend(metrics_snapshot);
                Ok::<Response, ServerError>(Response::entries(request_id, &entries)?)
            })
            .await
            .map_err(|_| ServerError::EngineLockPoisoned)?
        }
        command => execute_command_async(engine, request_id, command).await,
    }
}

fn handle_auth(
    metrics: Arc<Metrics>,
    auth_config: Option<&AuthConfig>,
    session: &mut SessionState,
    request_id: u32,
    command: Command,
) -> Result<Response> {
    let Command::Auth { username, password } = command else {
        unreachable!();
    };

    let Some(auth_config) = auth_config else {
        session.authenticated_user = Some(username);
        metrics.auth_successes.fetch_add(1, Ordering::Relaxed);
        return Ok(Response::ok(request_id));
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
    engine: Arc<Mutex<Engine>>,
    metrics: Arc<Metrics>,
    session: &mut SessionState,
    request_id: u32,
    command: Command,
) -> Result<Response> {
    match command {
        Command::Multi => Err(ServerError::TransactionAlreadyActive),
        Command::Discard => {
            session.transaction_queue.clear();
            metrics.transactions_discarded.fetch_add(1, Ordering::Relaxed);
            Ok(Response::ok(request_id))
        }
        Command::Exec => {
            let mut queued = std::mem::take(&mut session.transaction_queue);
            if matches!(queued.first(), Some(Command::Multi)) {
                queued.remove(0);
            }
            let engine = Arc::clone(&engine);
            let response = task::spawn_blocking(move || {
                let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
                let mut rendered = Vec::with_capacity(queued.len());
                for (index, queued_command) in queued.into_iter().enumerate() {
                    match execute_command(&mut *engine, request_id + index as u32 + 1, queued_command.clone()) {
                        Ok(response) => rendered.push(Some(render_transaction_response(&queued_command, &response)?)),
                        Err(err) => rendered.push(Some(format!("ERROR [{}] {}: {}", err.code(), err.name(), err))),
                    }
                }
                Ok::<Response, ServerError>(Response::strings(request_id, &rendered)?)
            })
            .await
            .map_err(|_| ServerError::EngineLockPoisoned)??;
            metrics.transactions_committed.fetch_add(1, Ordering::Relaxed);
            Ok(response)
        }
        Command::Auth { .. } => Err(ServerError::AuthenticationFailed),
        other => {
            session.transaction_queue.push(other);
            Ok(Response::value(request_id, "QUEUED")?)
        }
    }
}

async fn execute_command_async(
    engine: Arc<Mutex<Engine>>,
    request_id: u32,
    command: Command,
) -> Result<Response> {
    task::spawn_blocking(move || {
        let mut engine = engine.lock().map_err(|_| ServerError::EngineLockPoisoned)?;
        execute_command(&mut *engine, request_id, command)
    })
    .await
    .map_err(|_| ServerError::EngineLockPoisoned)?
}

fn error_response(request_id: u32, code: &str, name: &str, message: &str) -> Response {
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

fn execute_command<E>(engine: &mut E, request_id: u32, command: Command) -> Result<Response>
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
            let inserted = engine.set_nx(key, value)?;
            Ok(Response::boolean(request_id, inserted))
        }
        Command::MGet { keys } => {
            let values = engine.mget(&keys)?;
            Ok(Response::strings(request_id, &values)?)
        }
        Command::MSet { entries } => {
            engine.mset(&entries)?;
            Ok(Response::ok(request_id))
        }
        Command::Delete { keys } => {
            let removed = engine.delete_many(&keys)?;
            Ok(Response::count(request_id, removed as u64))
        }
        Command::Exists { key } => {
            let exists = engine.exists(&key)?;
            Ok(Response::boolean(request_id, exists))
        }
        Command::Incr { key } => {
            let value = engine.incr(&key)?;
            Ok(Response::integer(request_id, value))
        }
        Command::Decr { key } => {
            let value = engine.decr(&key)?;
            Ok(Response::integer(request_id, value))
        }
        Command::Expire { key, seconds } => {
            let changed = engine.expire(&key, seconds)?;
            Ok(Response::boolean(request_id, changed))
        }
        Command::Ttl { key } => {
            let ttl = engine.ttl(&key)?;
            Ok(Response::integer(request_id, ttl))
        }
        Command::Persist { key } => {
            let changed = engine.persist(&key)?;
            Ok(Response::boolean(request_id, changed))
        }
        Command::Rename {
            source,
            destination,
        } => Ok(Response::boolean(request_id, engine.rename(&source, destination)?)),
        Command::RenameNx {
            source,
            destination,
        } => Ok(Response::boolean(request_id, engine.rename_nx(&source, destination)?)),
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let ScanPage { next_cursor, keys } = engine.scan(cursor, pattern.as_deref(), count)?;
            Ok(Response::scan(request_id, next_cursor, &keys)?)
        }
        Command::DbSize | Command::Count => {
            let count = engine.db_size()?;
            Ok(Response::count(request_id, count as u64))
        }
        Command::Info => {
            let entries = engine.info()?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Metrics => Err(ServerError::UnsupportedRemoteCommand),
        Command::List => {
            let entries = engine.list()?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Clear => {
            engine.clear()?;
            Ok(Response::ok(request_id))
        }
        Command::Save | Command::Snapshot => {
            engine.snapshot()?;
            Ok(Response::ok(request_id))
        }
        Command::Multi | Command::Exec | Command::Discard => Err(ServerError::UnsupportedRemoteCommand),
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
            Command::MSet { .. } | Command::Clear | Command::Save | Command::Snapshot => Ok("OK".to_string()),
            Command::MGet { .. } => Ok(response
                .decode_strings()?
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
                .collect::<Vec<_>>()
                .join(", ")),
            Command::Delete { .. } | Command::DbSize | Command::Count => Ok(response.decode_count()?.to_string()),
            Command::Incr { .. } | Command::Decr { .. } | Command::Ttl { .. } => Ok(response.decode_integer()?.to_string()),
            Command::Scan { .. } => {
                let scan = response.decode_scan()?;
                Ok(format!("cursor={}, keys=[{}]", scan.next_cursor, scan.keys.join(", ")))
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

fn value_or_not_found(request_id: u32, value: Option<String>) -> Result<Response> {
    match value {
        Some(value) => Ok(Response::value(request_id, &value)?),
        None => Ok(Response::not_found(request_id)),
    }
}

fn render_set_response(
    request_id: u32,
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
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{SessionState, error_response, execute_command, handle_auth, handle_transaction_command};
    use crate::auth::AuthConfig;
    use crate::metrics::Metrics;
    use command::{
        Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
        SetOptions as CommandSetOptions,
    };
    use engine::{Engine, Expiration, Paths, Result, ScanPage, SetCondition, SetOptions, SetOutcome, StorageEngine};
    use transport::{Response, Status};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("veyra-server-test-{name}-{unique}"))
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
            let mut removed = 0;
            for key in keys {
                if self.data.remove(key).is_some() {
                    removed += 1;
                }
            }
            Ok(removed)
        }

        fn exists(&mut self, key: &str) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }

        fn incr(&mut self, key: &str) -> Result<i64> {
            let current = self
                .data
                .get(key)
                .cloned()
                .unwrap_or_else(|| "0".to_string())
                .parse::<i64>()
                .unwrap();
            let next = current + 1;
            self.data.insert(key.to_string(), next.to_string());
            Ok(next)
        }

        fn decr(&mut self, key: &str) -> Result<i64> {
            let current = self
                .data
                .get(key)
                .cloned()
                .unwrap_or_else(|| "0".to_string())
                .parse::<i64>()
                .unwrap();
            let next = current - 1;
            self.data.insert(key.to_string(), next.to_string());
            Ok(next)
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
            let Some(value) = self.data.remove(source) else {
                return Ok(false);
            };
            self.data.insert(destination, value);
            Ok(true)
        }

        fn rename_nx(&mut self, source: &str, destination: String) -> Result<bool> {
            if self.data.contains_key(&destination) {
                return Ok(false);
            }
            self.rename(source, destination)
        }

        fn db_size(&mut self) -> Result<usize> {
            Ok(self.data.len())
        }

        fn scan(&mut self, cursor: u64, pattern: Option<&str>, count: Option<u16>) -> Result<ScanPage> {
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
            41,
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        assert_eq!(get.request_id, 41);
        assert_eq!(get.status, Status::Ok);
        assert_eq!(get.decode_value().unwrap(), "alice");

        let set = execute_command(
            &mut engine,
            42,
            Command::Set {
                key: "city".to_string(),
                value: "paris".to_string(),
                options: CommandSetOptions::default(),
            },
        )
        .unwrap();
        assert_eq!(set, Response::ok(42));
    }

    #[test]
    fn routes_set_getdel_getex_and_scan_responses() {
        let mut engine = FakeEngine::default();
        engine
            .mset(&[("user:1".into(), "alice".into()), ("other".into(), "x".into())])
            .unwrap();

        let set = execute_command(
            &mut engine,
            1,
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
            2,
            Command::GetDel {
                key: "user:1".to_string(),
            },
        )
        .unwrap();
        assert_eq!(getdel.decode_value().unwrap(), "bob");

        let getex = execute_command(
            &mut engine,
            3,
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
            4,
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
        let mut session = SessionState::default();

        let denied = handle_auth(
            Arc::clone(&metrics),
            Some(&auth),
            &mut session,
            1,
            Command::Auth {
                username: "dbuser".to_string(),
                password: "wrong".to_string(),
            },
        );
        assert!(denied.is_err());

        let ok = handle_auth(
            Arc::clone(&metrics),
            Some(&auth),
            &mut session,
            2,
            Command::Auth {
                username: "dbuser".to_string(),
                password: "secret".to_string(),
            },
        )
        .unwrap();
        assert_eq!(ok.status, Status::Ok);
        assert!(session.is_authenticated());

        session.transaction_queue.push(Command::Multi);
        let queued = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(handle_transaction_command(
                Arc::new(Mutex::new(
                    Engine::from_paths(Paths::from_data_dir(temp_dir("tx")).unwrap()).unwrap(),
                )),
                metrics,
                &mut session,
                3,
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
        assert!(execute_command(&mut engine, 7, Command::Help).is_err());
        assert!(execute_command(&mut engine, 8, Command::Exit).is_err());

        let response = error_response(9, "SRV-400", "Bad Request", "invalid request");
        assert_eq!(response.status, Status::Error);
        let payload = response.decode_error().unwrap();
        assert_eq!(payload.code, "SRV-400");
        assert_eq!(payload.name, "Bad Request");
        assert_eq!(payload.message, "invalid request");
    }
}
