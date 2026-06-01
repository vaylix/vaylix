use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use command::Command;
use engine::{Engine, EngineOptions, Paths, StorageEngine};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{MissedTickBehavior, interval, timeout};
use transport::{
    CodecOptions, Request, Response, Status, TransportError, negotiate_server_options,
    read_client_hello_from_async, read_request_from_async_with_options,
    write_response_to_async_with_options, write_server_hello_to_async,
};
use uuid::Uuid;

use crate::auth::{Identity, Permission};
use crate::backup::{
    backup_manifest_path, build_backup_manifest, load_backup_manifest, resolve_backup_path,
    verify_backup_manifest,
};
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::replication::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterMember, FollowerPhase, HeartbeatRequest,
    ReplicationAckRequest, ReplicationFetchRequest, ReplicationRole, ReplicationStatusSnapshot,
    SnapshotInstallRequest, SnapshotInstallResponse, VoteRequest,
};
pub use crate::runtime_state::{AuthLockoutState, MaintenanceMode};

mod audit_events;
mod authorization;
mod commands;
mod config;
mod engine_worker;
mod lifecycle;
mod session;
mod transactions;
mod validation;

pub(crate) use audit_events::log_event;
use audit_events::{
    auth_lockout_key, current_time_millis, log_connection_event, opcode_name, record_audit_event,
    record_runtime_event, record_semantic_audit_event, record_slow_command_event,
};
use authorization::{authorize_command, is_allowed_during_maintenance};
use commands::{
    error_response, execute_command, map_transaction_result_payload, validate_transaction_command,
};
pub use config::{ServerGuards, ServerRuntimeConfig};
use engine_worker::EngineHandle;
use lifecycle::{spawn_expiration_sweeper, spawn_snapshotter, spawn_tls_reloader};
#[cfg(test)]
use session::RateLimiter;
use session::{AuditContext, SessionState};
use transactions::{expire_transaction_if_needed, handle_transaction_command};
use validation::{is_internal_replication_opcode, validate_command, validate_request};

/// Asynchronous Tokio-based database server with shared engine runtime state.
pub struct Server {
    listener: TcpListener,
    engine: EngineHandle,
    connection_slots: Arc<Semaphore>,
    next_connection_id: AtomicU64,
    runtime: ServerRuntimeConfig,
    metrics: Arc<Metrics>,
}

struct AbortOnDrop {
    handle: JoinHandle<()>,
}

impl AbortOnDrop {
    fn new(handle: JoinHandle<()>) -> Self {
        Self { handle }
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.handle.abort();
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
        runtime
            .replication
            .set_advertise_addr(local_addr.to_string())
            .await?;
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
        let mut background_tasks = Vec::new();

        if let Some(snapshot_interval) = runtime.snapshot_interval {
            background_tasks.push(AbortOnDrop::new(spawn_snapshotter(
                engine.clone(),
                Arc::clone(&metrics),
                snapshot_interval,
                shutdown_rx.clone(),
            )));
        }

        if let Some(sweep_interval) = runtime.expiration_sweep_interval {
            background_tasks.push(AbortOnDrop::new(spawn_expiration_sweeper(
                engine.clone(),
                Arc::clone(&metrics),
                sweep_interval,
                shutdown_rx.clone(),
            )));
        }

        if let Some(handle) = spawn_tls_reloader(runtime.clone(), shutdown_rx.clone()) {
            background_tasks.push(AbortOnDrop::new(handle));
        }

        let initial_info = engine.info().await?;
        let initial_sequence = parse_last_applied_sequence(&initial_info);
        let initial_term = engine.wal_entry_term(initial_sequence).await?;
        let initial_checksum = engine.wal_entry_checksum(initial_sequence).await?;
        runtime
            .replication
            .set_local_last_applied_state(initial_sequence, initial_term, initial_checksum)
            .await;
        if let Some(handle) =
            spawn_consensus_loop(engine.clone(), runtime.clone(), shutdown_rx.clone())
        {
            background_tasks.push(AbortOnDrop::new(handle));
        }
        if let Some(handle) =
            spawn_replication_follower_loop(engine.clone(), runtime.clone(), shutdown_rx.clone())
        {
            background_tasks.push(AbortOnDrop::new(handle));
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

fn spawn_replication_follower_loop(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    if runtime.replication.config().role == ReplicationRole::Standalone {
        return None;
    }

    Some(tokio::spawn(async move {
        let mut ticker = interval(runtime.replication.config().poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !matches!(
                        runtime.replication.role().await,
                        ReplicationRole::Follower
                    ) {
                        continue;
                    }
                    if runtime.replication.is_paused().await {
                        runtime
                            .replication
                            .update_follower_phase(FollowerPhase::Paused, None, None)
                            .await;
                        continue;
                    }
                    let started = Instant::now();
                    match run_replication_round(engine.clone(), runtime.clone()).await {
                        Ok((lag_entries, caught_up)) => {
                            let phase = if caught_up {
                                FollowerPhase::Streaming
                            } else {
                                FollowerPhase::CatchingUp
                            };
                            runtime
                                .replication
                                .update_follower_phase(
                                    phase,
                                    Some(lag_entries),
                                    Some(started.elapsed().as_millis() as u64),
                                )
                                .await;
                        }
                        Err(err) => {
                            log_event(
                                "WARN",
                                "server.replication",
                                &format!("[{}] {}: {err}", err.code(), err.name()),
                            );
                            runtime
                                .replication
                                .update_follower_phase(FollowerPhase::Stale, None, None)
                                .await;
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
    }))
}

async fn run_replication_round(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
) -> Result<(u64, bool)> {
    let sources = replication_sources(&runtime).await;
    if sources.is_empty() {
        return Ok((0, false));
    };

    let mut last_error = None;
    for upstream in sources {
        match run_replication_round_from(engine.clone(), runtime.clone(), &upstream).await {
            Ok(result) => return Ok(result),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        ServerError::InvalidArguments("replication source unavailable".to_string())
    }))
}

async fn replication_sources(runtime: &ServerRuntimeConfig) -> Vec<String> {
    let mut sources = Vec::new();
    if let Some(leader_hint) = runtime.replication.leader_hint().await {
        sources.push(leader_hint);
    }
    if let Some(upstream) = runtime.replication.config().upstream.clone() {
        sources.push(upstream);
    }
    let local_node_id = runtime.replication.config().node_id.as_str();
    for member in runtime.replication.current_members().await {
        if member.node_id != local_node_id {
            sources.push(member.advertise_addr);
        }
    }
    sources.sort();
    sources.dedup();
    sources
}

async fn run_replication_round_from(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
    upstream: &str,
) -> Result<(u64, bool)> {
    let mut stream = TcpStream::connect(upstream)
        .await
        .map_err(ServerError::Accept)?;
    let hello = transport::ClientHello::new("vaylix-replication", env!("CARGO_PKG_VERSION"));
    transport::write_client_hello_to_async(&mut stream, &hello).await?;
    let server_hello = transport::read_server_hello_from_async(&mut stream).await?;
    let transport = transport::client_options_from_server_hello(&server_hello)?;
    if let (Some(username), Some(password)) = (
        runtime.replication.config().upstream_username.clone(),
        runtime.replication.config().upstream_password.clone(),
    ) {
        let auth = Request::from_command(Uuid::now_v7(), Command::Auth { username, password })?;
        transport::write_request_to_async_with_options(&mut stream, &auth, transport).await?;
        let response =
            transport::read_response_from_async_with_options(&mut stream, transport).await?;
        if response.status != Status::Ok {
            return Err(ServerError::AuthenticationFailed);
        }
    }

    let status_request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationStatus,
        Vec::new(),
    );
    transport::write_request_to_async_with_options(&mut stream, &status_request, transport).await?;
    let status_response =
        transport::read_response_from_async_with_options(&mut stream, transport).await?;
    if status_response.status != Status::Ok {
        return Err(ServerError::UnsupportedRemoteCommand);
    }
    let leader_status: ReplicationStatusSnapshot = decode_json_payload(&status_response.payload)?;
    if leader_status.role != "leader" {
        return Err(ServerError::InvalidArguments(format!(
            "replication source {upstream} is not leader"
        )));
    }
    runtime
        .replication
        .observe_leader_status(
            leader_status.node_id.clone(),
            leader_status.advertise_addr.clone(),
            leader_status.current_term,
            leader_status.commit_sequence,
            leader_status
                .local_last_applied_sequence
                .max(leader_status.commit_sequence),
            leader_status.members.clone(),
        )
        .await?;

    let local_sequence = parse_last_applied_sequence(&engine.info().await?);
    if local_sequence > leader_status.commit_sequence
        || local_sequence > leader_status.local_last_applied_sequence
    {
        return Ok((0, false));
    }

    let fetch_request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationFetch,
        serde_json::to_vec(&ReplicationFetchRequest {
            after_sequence: local_sequence,
            limit: runtime.replication.config().fetch_batch_size,
        })
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &fetch_request, transport).await?;
    let fetch_response =
        transport::read_response_from_async_with_options(&mut stream, transport).await?;
    if fetch_response.status != Status::Ok {
        return Err(ServerError::UnsupportedRemoteCommand);
    }
    let entries: Vec<engine::WalEntry> = decode_json_payload(&fetch_response.payload)?;
    let current_sequence = {
        let _apply_guard = runtime.replication_apply_lock.lock().await;
        let mut current_sequence = parse_last_applied_sequence(&engine.info().await?);
        if current_sequence > leader_status.commit_sequence
            || current_sequence > leader_status.local_last_applied_sequence
        {
            return Ok((0, false));
        }
        let entries = entries
            .into_iter()
            .filter(|entry| {
                entry.sequence > current_sequence && entry.sequence <= leader_status.commit_sequence
            })
            .collect::<Vec<_>>();
        if let Some(first) = entries.first()
            && first.sequence != current_sequence.saturating_add(1)
        {
            return Err(ServerError::InvalidArguments(format!(
                "replication fetch gap: expected {}, got {}",
                current_sequence.saturating_add(1),
                first.sequence
            )));
        }
        if !entries.is_empty() {
            match engine.apply_replication_entries(entries).await {
                Ok(applied_sequence) => {
                    current_sequence = applied_sequence;
                    let current_term = engine.wal_entry_term(current_sequence).await?;
                    let current_checksum = engine.wal_entry_checksum(current_sequence).await?;
                    runtime
                        .replication
                        .set_local_last_applied_state(
                            current_sequence,
                            current_term,
                            current_checksum,
                        )
                        .await;
                }
                Err(err) => return Err(err),
            }
        }
        current_sequence
    };

    let ack_request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationAck,
        serde_json::to_vec(&ReplicationAckRequest {
            follower_node_id: runtime.replication.config().node_id.clone(),
            applied_sequence: current_sequence,
            term: leader_status.current_term,
            leader_node_id: leader_status.node_id.clone(),
        })
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &ack_request, transport).await?;
    let ack_response =
        transport::read_response_from_async_with_options(&mut stream, transport).await?;
    if ack_response.status != Status::Ok {
        return Err(ServerError::UnsupportedRemoteCommand);
    }

    let lag_entries = leader_status
        .commit_sequence
        .saturating_sub(current_sequence);
    Ok((lag_entries, lag_entries == 0))
}

fn spawn_consensus_loop(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    if runtime.replication.config().role == ReplicationRole::Standalone {
        return None;
    }

    Some(tokio::spawn(async move {
        let tick_every = runtime
            .replication
            .config()
            .heartbeat_interval
            .min(Duration::from_millis(100));
        let mut ticker = interval(tick_every.max(Duration::from_millis(25)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if runtime.replication.current_members().await.len() <= 1 {
                        continue;
                    }
                    if runtime.replication.heartbeat_due().await
                        && let Err(err) = send_cluster_liveness_heartbeats(engine.clone(), runtime.clone()).await
                    {
                        log_event(
                            "WARN",
                            "server.consensus",
                            &format!("[{}] {}: {err}", err.code(), err.name()),
                        );
                    }
                    if runtime.replication.heartbeat_due().await
                        && let Err(err) = try_send_cluster_heartbeats(engine.clone(), runtime.clone()).await
                    {
                        log_event(
                            "WARN",
                            "server.consensus",
                            &format!("[{}] {}: {err}", err.code(), err.name()),
                        );
                    }

                    if runtime.replication.election_due().await
                        && let Err(err) = run_election_round(engine.clone(), runtime.clone()).await
                    {
                        log_event(
                            "WARN",
                            "server.consensus",
                            &format!("[{}] {}: {err}", err.code(), err.name()),
                        );
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }))
}

async fn run_election_round(engine: EngineHandle, runtime: ServerRuntimeConfig) -> Result<()> {
    if !runtime.replication.election_due().await {
        return Ok(());
    }
    let (probe_term, members, last_log_index, last_log_term) =
        runtime.replication.election_probe().await;
    let candidate_addr = runtime
        .replication
        .local_advertise_addr()
        .await
        .ok_or_else(|| {
            ServerError::InvalidArguments("candidate advertise address is unavailable".to_string())
        })?;
    let local_node_id = runtime.replication.config().node_id.clone();
    log_event(
        "INFO",
        "server.consensus",
        &format!(
            "prevote start node={} probe_term={} last_log_index={} last_log_term={:?}",
            local_node_id, probe_term, last_log_index, last_log_term
        ),
    );
    let mut voter_count = 0usize;
    let mut prevotes_granted = 1usize;

    for member in &members {
        if member.voter {
            voter_count += 1;
        }
        if !member.voter || member.node_id == local_node_id {
            continue;
        }
        match send_vote_request(
            runtime.clone(),
            member,
            VoteRequest {
                term: probe_term,
                candidate_node_id: local_node_id.clone(),
                candidate_addr: candidate_addr.clone(),
                last_log_index,
                last_log_term,
                prevote: true,
            },
        )
        .await
        {
            Ok(response) => {
                if response.vote_granted {
                    prevotes_granted += 1;
                }
            }
            Err(err) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!(
                        "vote request to {} failed [{}] {}: {err}",
                        member.node_id,
                        err.code(),
                        err.name()
                    ),
                );
            }
        }
    }

    if prevotes_granted < ((voter_count.max(1) / 2) + 1) {
        log_event(
            "INFO",
            "server.consensus",
            &format!(
                "prevote lost node={} probe_term={} granted={}/{}",
                local_node_id,
                probe_term,
                prevotes_granted,
                voter_count.max(1)
            ),
        );
        runtime.replication.defer_election().await;
        return Ok(());
    }

    // Abort escalation if a valid leader heartbeat arrived during prevote.
    if !runtime.replication.election_due().await {
        log_event(
            "INFO",
            "server.consensus",
            &format!(
                "prevote stale node={} probe_term={} aborted_before_election=true",
                local_node_id, probe_term
            ),
        );
        return Ok(());
    }

    let (term, members, last_log_index, last_log_term) =
        runtime.replication.begin_election().await?;
    log_event(
        "INFO",
        "server.consensus",
        &format!(
            "election start node={} term={} last_log_index={} last_log_term={:?}",
            local_node_id, term, last_log_index, last_log_term
        ),
    );
    let mut votes_granted = 1usize;
    let mut voter_count = 0usize;

    for member in &members {
        if member.voter {
            voter_count += 1;
        }
        if !member.voter || member.node_id == local_node_id {
            continue;
        }
        match send_vote_request(
            runtime.clone(),
            member,
            VoteRequest {
                term,
                candidate_node_id: local_node_id.clone(),
                candidate_addr: candidate_addr.clone(),
                last_log_index,
                last_log_term,
                prevote: false,
            },
        )
        .await
        {
            Ok(response) => {
                if response.term > term {
                    runtime
                        .replication
                        .observe_remote_term(response.term)
                        .await?;
                    return Ok(());
                }
                if response.vote_granted {
                    votes_granted += 1;
                }
            }
            Err(err) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!(
                        "vote request to {} failed [{}] {}: {err}",
                        member.node_id,
                        err.code(),
                        err.name()
                    ),
                );
            }
        }
    }

    let won = runtime
        .replication
        .finalize_election(term, votes_granted, voter_count.max(1))
        .await?;
    if won {
        let _role_guard = runtime.replication_apply_lock.lock().await;
        if !runtime.replication.is_leader_for_term(term).await {
            return Ok(());
        }
        let noop = engine.append_noop(term).await?;
        runtime
            .replication
            .set_local_last_applied_state(
                noop.last_applied_sequence,
                noop.last_applied_term,
                noop.last_applied_checksum,
            )
            .await;
        let catchup_deadline = Instant::now() + runtime.replication.config().ack_timeout;
        loop {
            send_cluster_heartbeats_role_guarded_with_timeout(engine.clone(), runtime.clone())
                .await?;
            if runtime.replication.role().await != ReplicationRole::Leader {
                break;
            }
            if runtime.replication.commit_sequence().await >= noop.last_applied_sequence {
                break;
            }
            if Instant::now() >= catchup_deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        record_runtime_event(
            &runtime.audit_logger,
            "leader_elected",
            [
                ("term".to_string(), term.to_string()),
                ("node_id".to_string(), local_node_id),
                ("votes".to_string(), votes_granted.to_string()),
            ]
            .into_iter()
            .collect(),
        );
    }
    Ok(())
}

async fn send_cluster_heartbeats_role_guarded(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
) -> Result<()> {
    let _fanout_guard = runtime.replication_fanout_lock.lock().await;
    let peer_timeout = runtime.replication.config().ack_timeout;
    send_cluster_appends_locked(engine, runtime.clone(), peer_timeout).await
}

async fn send_cluster_heartbeats_role_guarded_with_timeout(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
) -> Result<()> {
    let background_window = runtime
        .replication
        .config()
        .heartbeat_interval
        .saturating_mul(5)
        .max(Duration::from_millis(250))
        .min(runtime.replication.config().ack_timeout);
    let fanout_timeout = runtime
        .replication
        .config()
        .ack_timeout
        .saturating_add(background_window);
    match timeout(
        fanout_timeout,
        send_cluster_heartbeats_role_guarded(engine, runtime),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(ServerError::ReplicationAckUnavailable),
    }
}

async fn try_send_cluster_heartbeats(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
) -> Result<()> {
    let Ok(_role_guard) = runtime.replication_apply_lock.try_lock() else {
        return Ok(());
    };
    let Ok(_fanout_guard) = runtime.replication_fanout_lock.try_lock() else {
        return Ok(());
    };
    let peer_timeout = runtime
        .replication
        .config()
        .heartbeat_interval
        .saturating_mul(5)
        .max(Duration::from_millis(250))
        .min(runtime.replication.config().ack_timeout);
    send_cluster_appends_locked(engine, runtime.clone(), peer_timeout).await
}

async fn send_cluster_appends_locked(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
    peer_timeout: Duration,
) -> Result<()> {
    if runtime.replication.role().await != ReplicationRole::Leader {
        return Ok(());
    }
    let local_sequence = parse_last_applied_sequence(&engine.info().await?);
    let local_term = engine.wal_entry_term(local_sequence).await?;
    let local_checksum = engine.wal_entry_checksum(local_sequence).await?;
    runtime
        .replication
        .set_local_last_applied_state(local_sequence, local_term, local_checksum)
        .await;
    let Some((
        term,
        leader_node_id,
        leader_addr,
        commit_sequence,
        leader_frontier_sequence,
        members,
    )) = runtime.replication.leader_heartbeat_payload().await
    else {
        return Ok(());
    };
    runtime.replication.note_leader_activity().await;
    let cluster_members = runtime.replication.current_members().await;
    let mut peer_plans = Vec::new();
    for member in members {
        if !member.voter || member.node_id == leader_node_id {
            continue;
        }
        let next_sequence = runtime
            .replication
            .follower_next_sequence(&member.node_id)
            .await;
        let prev_sequence = next_sequence.saturating_sub(1);
        peer_plans.push((member, next_sequence, prev_sequence));
    }
    let min_prev_sequence = peer_plans
        .iter()
        .map(|(_, _, prev_sequence)| *prev_sequence)
        .min()
        .unwrap_or(local_sequence);
    let fetch_batch_size = runtime.replication.config().fetch_batch_size;
    let cache_after_sequence = min_prev_sequence.saturating_sub(1);
    let cache_limit = local_sequence
        .saturating_sub(cache_after_sequence)
        .saturating_add(fetch_batch_size as u64)
        .max(fetch_batch_size as u64)
        .min(usize::MAX as u64) as usize;
    let cached_entries = engine
        .wal_entries_since(cache_after_sequence, cache_limit)
        .await?;
    let cached_by_sequence = cached_entries
        .iter()
        .map(|entry| (entry.sequence, entry.clone()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut fanout = JoinSet::new();
    for (member, _next_sequence, prev_sequence) in peer_plans {
        let (prev_term, prev_entry_checksum, entries, snapshot) = if prev_sequence == 0 {
            let entries = cached_entries
                .iter()
                .filter(|entry| entry.sequence > prev_sequence)
                .take(fetch_batch_size)
                .cloned()
                .collect::<Vec<_>>();
            (None, None, entries, None)
        } else if let Some(prev_entry) = cached_by_sequence.get(&prev_sequence).cloned() {
            let prev_term = Some(prev_entry.term);
            let prev_entry_checksum = Some(prev_entry.checksum().map_err(ServerError::from)?);
            let entries = cached_entries
                .iter()
                .filter(|entry| entry.sequence > prev_sequence)
                .take(fetch_batch_size)
                .cloned()
                .collect::<Vec<_>>();
            (prev_term, prev_entry_checksum, entries, None)
        } else {
            (
                None,
                None,
                Vec::new(),
                Some(engine.replication_snapshot().await?),
            )
        };
        let runtime = runtime.clone();
        let cluster_members = cluster_members.clone();
        let leader_node_id = leader_node_id.clone();
        let leader_addr = leader_addr.clone();
        fanout.spawn(async move {
            let response = if let Some(snapshot) = snapshot {
                let response = timeout(
                    peer_timeout,
                    send_install_snapshot_request(
                        runtime.clone(),
                        SnapshotInstallCall {
                            member: member.clone(),
                            payload: SnapshotInstallRequest {
                                term,
                                leader_node_id: leader_node_id.clone(),
                                leader_addr: leader_addr.clone(),
                                commit_sequence,
                                members: cluster_members.clone(),
                                snapshot,
                            },
                        },
                    ),
                )
                .await
                .map_err(|_| ServerError::ReplicationAckUnavailable)??;
                AppendEntriesResponse {
                    term: response.term,
                    accepted: response.accepted,
                    match_sequence: response.applied_sequence,
                }
            } else {
                timeout(
                    peer_timeout,
                    send_append_request(
                        runtime.clone(),
                        &member,
                        AppendEntriesRequest {
                            term,
                            leader_node_id: leader_node_id.clone(),
                            leader_addr: leader_addr.clone(),
                            commit_sequence,
                            leader_frontier_sequence,
                            prev_sequence,
                            prev_term,
                            prev_entry_checksum,
                            entries,
                            members: cluster_members.clone(),
                        },
                    ),
                )
                .await
                .map_err(|_| ServerError::ReplicationAckUnavailable)??
            };
            Ok::<_, ServerError>((member, response, leader_node_id, local_sequence))
        });
    }

    while let Some(result) = fanout.join_next().await {
        match result {
            Ok(Ok((member, response, leader_node_id, local_sequence))) => {
                if response.term > term {
                    runtime
                        .replication
                        .observe_remote_term(response.term)
                        .await?;
                    return Ok(());
                }
                runtime
                    .replication
                    .record_append_result(
                        member.node_id.clone(),
                        response.accepted,
                        response.match_sequence,
                        term,
                        &leader_node_id,
                    )
                    .await;
                if !response.accepted {
                    log_event(
                        "WARN",
                        "server.consensus",
                        &format!(
                            "append rejected by {} term={} match_sequence={} leader_term={} commit_sequence={}",
                            member.node_id,
                            response.term,
                            response.match_sequence,
                            term,
                            commit_sequence
                        ),
                    );
                }
                if runtime.replication.config().write_ack_mode
                    == crate::replication::WriteAckMode::Replica
                    && runtime.replication.commit_sequence().await >= local_sequence
                {
                    fanout.abort_all();
                    break;
                }
            }
            Ok(Err(err)) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!(
                        "heartbeat fanout failed [{}] {}: {err}",
                        err.code(),
                        err.name()
                    ),
                );
            }
            Err(err) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!("heartbeat fanout task failed: {err}"),
                );
            }
        }
    }
    Ok(())
}

async fn send_cluster_liveness_heartbeats(
    engine: EngineHandle,
    runtime: ServerRuntimeConfig,
) -> Result<()> {
    if runtime.replication.role().await != ReplicationRole::Leader {
        return Ok(());
    }
    let local_sequence = parse_last_applied_sequence(&engine.info().await?);
    let local_term = engine.wal_entry_term(local_sequence).await?;
    let local_checksum = engine.wal_entry_checksum(local_sequence).await?;
    runtime
        .replication
        .set_local_last_applied_state(local_sequence, local_term, local_checksum)
        .await;
    let Some((
        term,
        leader_node_id,
        leader_addr,
        commit_sequence,
        leader_frontier_sequence,
        members,
    )) = runtime.replication.leader_heartbeat_payload().await
    else {
        return Ok(());
    };
    runtime.replication.note_leader_activity().await;
    let mut fanout = JoinSet::new();
    for member in members {
        if !member.voter || member.node_id == leader_node_id {
            continue;
        }
        let runtime = runtime.clone();
        let leader_node_id = leader_node_id.clone();
        let leader_addr = leader_addr.clone();
        let members = runtime.replication.current_members().await;
        let peer_timeout = runtime
            .replication
            .config()
            .heartbeat_interval
            .saturating_mul(5)
            .min(runtime.replication.config().ack_timeout);
        fanout.spawn(async move {
            let response = timeout(
                peer_timeout,
                send_heartbeat_request(
                    runtime,
                    &member,
                    HeartbeatRequest {
                        term,
                        leader_node_id,
                        leader_addr,
                        commit_sequence,
                        leader_frontier_sequence,
                        members,
                    },
                ),
            )
            .await
            .map_err(|_| ServerError::ReplicationAckUnavailable)??;
            Ok::<_, ServerError>((member, response))
        });
    }

    while let Some(result) = fanout.join_next().await {
        match result {
            Ok(Ok((member, response))) => {
                if response.term > term {
                    runtime
                        .replication
                        .observe_remote_term(response.term)
                        .await?;
                    return Ok(());
                }
                if !response.accepted {
                    log_event(
                        "WARN",
                        "server.consensus",
                        &format!(
                            "heartbeat rejected by {} term={} leader_term={}",
                            member.node_id, response.term, term
                        ),
                    );
                }
            }
            Ok(Err(err)) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!(
                        "heartbeat request failed [{}] {}: {err}",
                        err.code(),
                        err.name()
                    ),
                );
            }
            Err(err) => {
                log_event(
                    "WARN",
                    "server.consensus",
                    &format!("heartbeat task failed: {err}"),
                );
            }
        }
    }
    Ok(())
}

async fn drive_write_commit(
    engine: &EngineHandle,
    runtime: &ServerRuntimeConfig,
    sequence: u64,
) -> Result<()> {
    if runtime.replication.config().write_ack_mode == crate::replication::WriteAckMode::Local {
        return Ok(());
    }

    let deadline = Instant::now() + runtime.replication.config().ack_timeout;
    loop {
        if runtime.replication.commit_sequence().await >= sequence {
            return Ok(());
        }
        if runtime.replication.role().await != ReplicationRole::Leader {
            return Err(ServerError::ReplicationAckUnavailable);
        }
        if Instant::now() >= deadline {
            return runtime.replication.wait_for_write_ack(sequence).await;
        }
        if let Err(_err) =
            send_cluster_heartbeats_role_guarded_with_timeout(engine.clone(), runtime.clone()).await
        {
            if runtime.replication.commit_sequence().await >= sequence {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return runtime.replication.wait_for_write_ack(sequence).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn rollback_uncommitted_tail(
    engine: &EngineHandle,
    runtime: &ServerRuntimeConfig,
) -> Result<()> {
    let commit_sequence = runtime.replication.commit_sequence().await;
    let current_sequence = parse_last_applied_sequence(&engine.info().await?);
    if current_sequence <= commit_sequence {
        return Ok(());
    }
    let applied_sequence = engine
        .replace_replication_suffix(commit_sequence, Vec::new())
        .await?;
    let applied_term = engine.wal_entry_term(applied_sequence).await?;
    let applied_checksum = engine.wal_entry_checksum(applied_sequence).await?;
    runtime
        .replication
        .set_local_last_applied_state(applied_sequence, applied_term, applied_checksum)
        .await;
    Ok(())
}

async fn send_vote_request(
    runtime: ServerRuntimeConfig,
    member: &ClusterMember,
    payload: VoteRequest,
) -> Result<crate::replication::VoteResponse> {
    let mut stream = connect_replication_peer(runtime.clone(), &member.advertise_addr).await?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, &runtime).await?;
    let request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationVote,
        serde_json::to_vec(&payload)
            .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &request, transport).await?;
    let response = transport::read_response_from_async_with_options(&mut stream, transport).await?;
    ensure_replication_ok(&response)?;
    decode_json_payload(&response.payload)
}

async fn send_append_request(
    runtime: ServerRuntimeConfig,
    member: &ClusterMember,
    payload: AppendEntriesRequest,
) -> Result<AppendEntriesResponse> {
    let mut stream = connect_replication_peer(runtime.clone(), &member.advertise_addr).await?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, &runtime).await?;
    let request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationAppend,
        serde_json::to_vec(&payload)
            .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &request, transport).await?;
    let response = transport::read_response_from_async_with_options(&mut stream, transport).await?;
    ensure_replication_ok(&response)?;
    decode_json_payload(&response.payload)
}

async fn send_heartbeat_request(
    runtime: ServerRuntimeConfig,
    member: &ClusterMember,
    payload: HeartbeatRequest,
) -> Result<crate::replication::HeartbeatResponse> {
    let mut stream = connect_replication_peer(runtime.clone(), &member.advertise_addr).await?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, &runtime).await?;
    let request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationHeartbeat,
        serde_json::to_vec(&payload)
            .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &request, transport).await?;
    let response = transport::read_response_from_async_with_options(&mut stream, transport).await?;
    ensure_replication_ok(&response)?;
    decode_json_payload(&response.payload)
}

fn spawn_replication_append_ack(
    runtime: ServerRuntimeConfig,
    leader_addr: String,
    term: u64,
    leader_node_id: String,
    applied_sequence: u64,
) {
    tokio::spawn(async move {
        if let Err(err) = send_replication_append_ack(
            runtime,
            leader_addr,
            term,
            leader_node_id,
            applied_sequence,
        )
        .await
        {
            log_event(
                "WARN",
                "server.consensus",
                &format!("append ack failed [{}] {}: {err}", err.code(), err.name()),
            );
        }
    });
}

async fn send_replication_append_ack(
    runtime: ServerRuntimeConfig,
    leader_addr: String,
    term: u64,
    leader_node_id: String,
    applied_sequence: u64,
) -> Result<()> {
    let mut stream = connect_replication_peer(runtime.clone(), &leader_addr).await?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, &runtime).await?;
    let request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationAck,
        serde_json::to_vec(&ReplicationAckRequest {
            follower_node_id: runtime.replication.config().node_id.clone(),
            applied_sequence,
            term,
            leader_node_id,
        })
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &request, transport).await?;
    let response = transport::read_response_from_async_with_options(&mut stream, transport).await?;
    ensure_replication_ok(&response)
}

struct SnapshotInstallCall {
    member: ClusterMember,
    payload: SnapshotInstallRequest,
}

async fn send_install_snapshot_request(
    runtime: ServerRuntimeConfig,
    call: SnapshotInstallCall,
) -> Result<SnapshotInstallResponse> {
    let mut stream = connect_replication_peer(runtime.clone(), &call.member.advertise_addr).await?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, &runtime).await?;
    let request = Request::new(
        Uuid::now_v7(),
        transport::Opcode::ReplicationInstallSnapshot,
        serde_json::to_vec(&call.payload)
            .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(&mut stream, &request, transport).await?;
    let response = transport::read_response_from_async_with_options(&mut stream, transport).await?;
    ensure_replication_ok(&response)?;
    decode_json_payload(&response.payload)
}

fn ensure_replication_ok(response: &Response) -> Result<()> {
    if response.status == Status::Ok {
        return Ok(());
    }

    if let Ok(payload) = response.decode_error() {
        return Err(ServerError::InvalidArguments(format!(
            "replication peer returned {} {}: {}",
            payload.code, payload.name, payload.message
        )));
    }

    Err(ServerError::UnsupportedRemoteCommand)
}

async fn connect_replication_peer(_runtime: ServerRuntimeConfig, addr: &str) -> Result<TcpStream> {
    TcpStream::connect(addr).await.map_err(ServerError::Accept)
}

async fn negotiate_replication_transport(stream: &mut TcpStream) -> Result<CodecOptions> {
    let hello = transport::ClientHello::new("vaylix-replication", env!("CARGO_PKG_VERSION"));
    transport::write_client_hello_to_async(stream, &hello).await?;
    let server_hello = transport::read_server_hello_from_async(stream).await?;
    transport::client_options_from_server_hello(&server_hello).map_err(ServerError::from)
}

async fn authenticate_replication_peer(
    stream: &mut TcpStream,
    transport: CodecOptions,
    runtime: &ServerRuntimeConfig,
) -> Result<()> {
    if let (Some(username), Some(password)) = (
        runtime.replication.config().upstream_username.clone(),
        runtime.replication.config().upstream_password.clone(),
    ) {
        let auth = Request::from_command(Uuid::now_v7(), Command::Auth { username, password })?;
        transport::write_request_to_async_with_options(stream, &auth, transport).await?;
        let response = transport::read_response_from_async_with_options(stream, transport).await?;
        if response.status != Status::Ok {
            return Err(ServerError::AuthenticationFailed);
        }
    }
    Ok(())
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

        if is_internal_replication_opcode(request.opcode) {
            let request_id = request.request_id;
            let response = match process_internal_replication_request(
                engine.clone(),
                Arc::clone(&metrics),
                &runtime,
                &mut session,
                request,
            )
            .await
            {
                Ok(response) => response,
                Err(err) => error_response(request_id, err.code(), err.name(), &err.to_string()),
            };
            write_response_to_async_with_options(&mut stream, &response, transport).await?;
            continue;
        }

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

fn json_response<T: Serialize>(request_id: Uuid, value: &T) -> Result<Response> {
    let payload =
        serde_json::to_vec(value).map_err(|err| ServerError::InvalidArguments(err.to_string()))?;
    Ok(Response::new(request_id, Status::Ok, payload))
}

fn decode_json_payload<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T> {
    serde_json::from_slice(payload).map_err(|err| ServerError::InvalidArguments(err.to_string()))
}

fn parse_last_applied_sequence(entries: &[(String, String)]) -> u64 {
    entries
        .iter()
        .find_map(|(key, value)| {
            (key == "last_applied_sequence").then(|| value.parse::<u64>().unwrap_or(0))
        })
        .unwrap_or(0)
}

fn replication_entries_from_snapshot(
    snapshot: &ReplicationStatusSnapshot,
) -> Vec<(String, String)> {
    let mut entries = vec![
        ("node_id".to_string(), snapshot.node_id.clone()),
        ("group_id".to_string(), snapshot.group_id.clone()),
        ("role".to_string(), snapshot.role.clone()),
        (
            "advertise_addr".to_string(),
            snapshot
                .advertise_addr
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "leader_node_id".to_string(),
            snapshot
                .leader_node_id
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "leader_advertise_addr".to_string(),
            snapshot
                .leader_advertise_addr
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "upstream".to_string(),
            snapshot
                .upstream
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "write_ack_mode".to_string(),
            snapshot.write_ack_mode.clone(),
        ),
        (
            "current_term".to_string(),
            snapshot.current_term.to_string(),
        ),
        (
            "voted_for".to_string(),
            snapshot
                .voted_for
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        ("quorum_size".to_string(), snapshot.quorum_size.to_string()),
        ("paused".to_string(), snapshot.paused.to_string()),
        ("health".to_string(), snapshot.health.clone()),
        (
            "reason".to_string(),
            snapshot
                .reason
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "local_last_applied_sequence".to_string(),
            snapshot.local_last_applied_sequence.to_string(),
        ),
        (
            "commit_sequence".to_string(),
            snapshot.commit_sequence.to_string(),
        ),
        (
            "retention_floor_sequence".to_string(),
            snapshot
                .retention_floor_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "follower_phase".to_string(),
            snapshot
                .follower_phase
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "follower_lag_entries".to_string(),
            snapshot
                .follower_lag_entries
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "follower_lag_ms".to_string(),
            snapshot
                .follower_lag_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "known_followers".to_string(),
            snapshot.known_followers.to_string(),
        ),
    ];
    for follower in &snapshot.followers {
        entries.push((
            format!("follower.{}.applied_sequence", follower.node_id),
            follower.applied_sequence.to_string(),
        ));
        entries.push((
            format!("follower.{}.lag_entries", follower.node_id),
            follower.lag_entries.to_string(),
        ));
        entries.push((
            format!("follower.{}.lag_ms", follower.node_id),
            follower.lag_ms.to_string(),
        ));
        entries.push((
            format!("follower.{}.stale", follower.node_id),
            follower.stale.to_string(),
        ));
    }
    for member in &snapshot.members {
        entries.push((
            format!("member.{}.advertise_addr", member.node_id),
            member.advertise_addr.clone(),
        ));
        entries.push((
            format!("member.{}.voter", member.node_id),
            member.voter.to_string(),
        ));
    }
    entries
}

fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Set { .. }
            | Command::SetNx { .. }
            | Command::GetDel { .. }
            | Command::GetEx {
                expiration: Some(_),
                ..
            }
            | Command::GetEx { persist: true, .. }
            | Command::MSet { .. }
            | Command::Delete { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::Expire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. }
            | Command::Clear
            | Command::Save
            | Command::Snapshot
            | Command::Restore { .. }
            | Command::RestoreFrom { .. }
            | Command::AlterUserPassword { .. }
            | Command::CreateUser { .. }
            | Command::DropUser { .. }
            | Command::CreateRole { .. }
            | Command::DropRole { .. }
            | Command::GrantRole { .. }
            | Command::RevokeRole { .. }
            | Command::GrantPermission { .. }
            | Command::RevokePermission { .. }
            | Command::MaintenanceOn
            | Command::MaintenanceOff
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::PromoteFollower
            | Command::PauseReplication
            | Command::ResumeReplication
            | Command::ClusterJoin { .. }
            | Command::ClusterRemove { .. }
    )
}

async fn enforce_leader_writeability(
    runtime: &ServerRuntimeConfig,
    command: &Command,
) -> Result<()> {
    if !is_write_command(command) {
        return Ok(());
    }
    if !replication_role_accepts_writes(runtime.replication.role().await) {
        return Err(ServerError::ReplicationReadOnly);
    }
    if !runtime.replication.write_window_available().await {
        return Err(ServerError::ReplicationAckUnavailable);
    }
    Ok(())
}

fn replication_role_accepts_writes(role: ReplicationRole) -> bool {
    matches!(role, ReplicationRole::Standalone | ReplicationRole::Leader)
}

async fn process_internal_replication_request(
    engine: EngineHandle,
    _metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    request: Request,
) -> Result<Response> {
    if runtime.auth_config.is_some() && !session.is_authenticated() {
        return Err(ServerError::AuthenticationRequired);
    }
    if runtime.auth_config.is_some() {
        let Some(identity) = &session.identity else {
            return Err(ServerError::AuthenticationRequired);
        };
        if !identity.has(Permission::Admin) {
            return Err(ServerError::PermissionDenied);
        }
    }

    match request.opcode {
        transport::Opcode::ReplicationStatus => {
            let snapshot = runtime.replication.snapshot().await;
            json_response(request.request_id, &snapshot)
        }
        transport::Opcode::ReplicationSnapshot => {
            let snapshot = engine.replication_snapshot().await?;
            json_response(request.request_id, &snapshot)
        }
        transport::Opcode::ReplicationFetch => {
            let payload: ReplicationFetchRequest = decode_json_payload(&request.payload)?;
            let entries = engine
                .wal_entries_since(payload.after_sequence, payload.limit)
                .await?;
            json_response(request.request_id, &entries)
        }
        transport::Opcode::ReplicationAck => {
            let payload: ReplicationAckRequest = decode_json_payload(&request.payload)?;
            runtime
                .replication
                .register_follower_ack(
                    payload.follower_node_id,
                    payload.applied_sequence,
                    payload.term,
                    &payload.leader_node_id,
                )
                .await;
            Ok(Response::ok(request.request_id))
        }
        transport::Opcode::ReplicationAppend => {
            let _apply_guard = runtime.replication_apply_lock.lock().await;
            let payload: AppendEntriesRequest = decode_json_payload(&request.payload)?;
            let current_term = runtime.replication.current_term().await;
            if payload.term < current_term {
                let response = AppendEntriesResponse {
                    term: current_term,
                    accepted: false,
                    match_sequence: runtime.replication.local_last_applied_sequence().await,
                };
                return json_response(request.request_id, &response);
            }

            let heartbeat_response = runtime
                .replication
                .handle_heartbeat(HeartbeatRequest {
                    term: payload.term,
                    leader_node_id: payload.leader_node_id.clone(),
                    leader_addr: payload.leader_addr.clone(),
                    commit_sequence: payload.commit_sequence,
                    leader_frontier_sequence: payload.leader_frontier_sequence,
                    members: payload.members.clone(),
                })
                .await?;
            if !heartbeat_response.accepted {
                let response = AppendEntriesResponse {
                    term: heartbeat_response.term,
                    accepted: false,
                    match_sequence: runtime.replication.local_last_applied_sequence().await,
                };
                return json_response(request.request_id, &response);
            }

            let current_sequence = parse_last_applied_sequence(&engine.info().await?);
            let target_sequence = payload
                .entries
                .last()
                .map(|entry| entry.sequence)
                .unwrap_or(payload.commit_sequence)
                .max(payload.commit_sequence);
            runtime
                .replication
                .note_leader_frontier(target_sequence)
                .await;
            if current_sequence < target_sequence {
                runtime
                    .replication
                    .update_follower_phase(FollowerPhase::CatchingUp, None, None)
                    .await;
            }
            let local_prev_term = engine.wal_entry_term(payload.prev_sequence).await?;
            let local_prev_checksum = engine.wal_entry_checksum(payload.prev_sequence).await?;
            if local_prev_term != payload.prev_term
                || local_prev_checksum != payload.prev_entry_checksum
            {
                let response = AppendEntriesResponse {
                    term: heartbeat_response.term,
                    accepted: false,
                    match_sequence: payload.prev_sequence.saturating_sub(1),
                };
                return json_response(request.request_id, &response);
            }

            let applied_sequence = if payload.entries.is_empty() {
                if current_sequence < payload.prev_sequence {
                    let response = AppendEntriesResponse {
                        term: heartbeat_response.term,
                        accepted: false,
                        match_sequence: current_sequence,
                    };
                    return json_response(request.request_id, &response);
                }
                if current_sequence > payload.prev_sequence {
                    engine
                        .replace_replication_suffix(payload.prev_sequence, Vec::new())
                        .await?
                } else {
                    current_sequence
                }
            } else {
                let mut overlap_match_sequence = payload.prev_sequence;
                for entry in &payload.entries {
                    if entry.sequence > current_sequence {
                        break;
                    }
                    let local_term = engine.wal_entry_term(entry.sequence).await?;
                    let local_checksum = engine.wal_entry_checksum(entry.sequence).await?;
                    let entry_checksum = entry.checksum().map_err(ServerError::from)?;
                    if local_term != Some(entry.term) || local_checksum != Some(entry_checksum) {
                        let replacement = payload
                            .entries
                            .iter()
                            .skip_while(|candidate| candidate.sequence < entry.sequence)
                            .cloned()
                            .collect::<Vec<_>>();
                        let applied_sequence = engine
                            .replace_replication_suffix(
                                entry.sequence.saturating_sub(1),
                                replacement,
                            )
                            .await?;
                        let applied_term = engine.wal_entry_term(applied_sequence).await?;
                        let applied_checksum = engine.wal_entry_checksum(applied_sequence).await?;
                        runtime
                            .replication
                            .set_local_last_applied_state(
                                applied_sequence,
                                applied_term,
                                applied_checksum,
                            )
                            .await;
                        if applied_sequence >= target_sequence {
                            runtime
                                .replication
                                .update_follower_phase(FollowerPhase::Streaming, Some(0), Some(0))
                                .await;
                        }
                        let response = AppendEntriesResponse {
                            term: heartbeat_response.term,
                            accepted: true,
                            match_sequence: applied_sequence,
                        };
                        return json_response(request.request_id, &response);
                    }
                    overlap_match_sequence = entry.sequence;
                }

                let suffix_start = overlap_match_sequence.saturating_add(1);
                let suffix = payload
                    .entries
                    .iter()
                    .skip_while(|entry| entry.sequence < suffix_start)
                    .cloned()
                    .collect::<Vec<_>>();

                if suffix.is_empty() {
                    if current_sequence > overlap_match_sequence {
                        engine
                            .replace_replication_suffix(overlap_match_sequence, Vec::new())
                            .await?
                    } else {
                        current_sequence.max(overlap_match_sequence)
                    }
                } else if current_sequence.saturating_add(1) != suffix[0].sequence {
                    engine
                        .replace_replication_suffix(overlap_match_sequence, suffix)
                        .await?
                } else {
                    engine.apply_replication_entries(suffix).await?
                }
            };
            let applied_term = engine.wal_entry_term(applied_sequence).await?;
            let applied_checksum = engine.wal_entry_checksum(applied_sequence).await?;
            runtime
                .replication
                .set_local_last_applied_state(applied_sequence, applied_term, applied_checksum)
                .await;
            if applied_sequence >= target_sequence {
                runtime
                    .replication
                    .update_follower_phase(FollowerPhase::Streaming, Some(0), Some(0))
                    .await;
            }
            let response = AppendEntriesResponse {
                term: heartbeat_response.term,
                accepted: true,
                match_sequence: applied_sequence,
            };
            spawn_replication_append_ack(
                runtime.clone(),
                payload.leader_addr.clone(),
                payload.term,
                payload.leader_node_id.clone(),
                applied_sequence,
            );
            json_response(request.request_id, &response)
        }
        transport::Opcode::ReplicationInstallSnapshot => {
            let _apply_guard = runtime.replication_apply_lock.lock().await;
            let payload: SnapshotInstallRequest = decode_json_payload(&request.payload)?;
            let current_term = runtime.replication.current_term().await;
            if payload.term < current_term {
                let response = SnapshotInstallResponse {
                    term: current_term,
                    accepted: false,
                    applied_sequence: runtime.replication.local_last_applied_sequence().await,
                };
                return json_response(request.request_id, &response);
            }

            runtime
                .replication
                .handle_heartbeat(HeartbeatRequest {
                    term: payload.term,
                    leader_node_id: payload.leader_node_id,
                    leader_addr: payload.leader_addr,
                    commit_sequence: payload.commit_sequence,
                    leader_frontier_sequence: payload.commit_sequence,
                    members: payload.members,
                })
                .await?;
            let applied_sequence = engine.apply_replication_snapshot(payload.snapshot).await?;
            let applied_term = engine.wal_entry_term(applied_sequence).await?;
            let applied_checksum = engine.wal_entry_checksum(applied_sequence).await?;
            runtime
                .replication
                .set_local_last_applied_state(applied_sequence, applied_term, applied_checksum)
                .await;
            let response = SnapshotInstallResponse {
                term: runtime.replication.current_term().await,
                accepted: true,
                applied_sequence,
            };
            json_response(request.request_id, &response)
        }
        transport::Opcode::ReplicationVote => {
            let _transition_guard = runtime.replication_apply_lock.lock().await;
            let payload: VoteRequest = decode_json_payload(&request.payload)?;
            let response = runtime.replication.handle_vote_request(payload).await?;
            json_response(request.request_id, &response)
        }
        transport::Opcode::ReplicationHeartbeat => {
            let _transition_guard = runtime.replication_apply_lock.lock().await;
            let payload: HeartbeatRequest = decode_json_payload(&request.payload)?;
            let response = runtime.replication.handle_heartbeat(payload).await?;
            json_response(request.request_id, &response)
        }
        _ => Err(ServerError::UnsupportedRemoteCommand),
    }
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

    enforce_leader_writeability(runtime, &command).await?;

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
        Command::Health => {
            let snapshot = runtime.replication.snapshot().await;
            let mut entries = vec![
                ("status".to_string(), snapshot.health.clone()),
                (
                    "ready".to_string(),
                    (snapshot.health == "ready").to_string(),
                ),
                (
                    "reason".to_string(),
                    snapshot.reason.unwrap_or_else(|| "none".to_string()),
                ),
                ("role".to_string(), snapshot.role),
                (
                    "maintenance_mode".to_string(),
                    runtime.maintenance.is_enabled().to_string(),
                ),
            ];
            if let Some(phase) = snapshot.follower_phase {
                entries.push(("follower_phase".to_string(), phase));
            }
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Info => {
            let entries = structured_info(engine, metrics, runtime).await?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::ShowReplication => Ok(Response::entries(
            request_id,
            &replication_entries_from_snapshot(&runtime.replication.snapshot().await),
        )?),
        Command::ShowCluster => Ok(Response::entries(
            request_id,
            &cluster_entries_from_snapshot(&runtime.replication.snapshot().await),
        )?),
        Command::ClusterJoin { node_id, address } => {
            runtime
                .replication
                .add_member(ClusterMember {
                    node_id,
                    advertise_addr: address,
                    voter: true,
                })
                .await?;
            Ok(Response::ok(request_id))
        }
        Command::ClusterRemove { node_id } => {
            runtime.replication.remove_member(&node_id).await?;
            Ok(Response::ok(request_id))
        }
        Command::PromoteFollower => {
            runtime
                .replication
                .promote_follower(runtime.maintenance.is_enabled())
                .await?;
            record_runtime_event(
                &runtime.audit_logger,
                "replication_promote",
                [
                    ("result".to_string(), "ok".to_string()),
                    (
                        "node_id".to_string(),
                        runtime.replication.config().node_id.clone(),
                    ),
                ]
                .into_iter()
                .collect(),
            );
            Ok(Response::ok(request_id))
        }
        Command::PauseReplication => {
            runtime.replication.set_paused(true).await;
            record_runtime_event(
                &runtime.audit_logger,
                "replication_pause",
                [("paused".to_string(), "true".to_string())]
                    .into_iter()
                    .collect(),
            );
            Ok(Response::ok(request_id))
        }
        Command::ResumeReplication => {
            runtime.replication.set_paused(false).await;
            record_runtime_event(
                &runtime.audit_logger,
                "replication_pause",
                [("paused".to_string(), "false".to_string())]
                    .into_iter()
                    .collect(),
            );
            Ok(Response::ok(request_id))
        }
        Command::BackupTo { path } => {
            let response = engine.execute(request_id, 0, Command::Backup).await?;
            let dump = response.response.decode_value()?;
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
            Ok(engine
                .execute(request_id, 0, Command::Restore { dump })
                .await?
                .response)
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
        command => {
            if !is_write_command(&command) {
                let consensus_term = runtime.replication.current_term().await;
                return Ok(engine
                    .execute(request_id, consensus_term, command)
                    .await?
                    .response);
            }

            let _write_guard = runtime.replication_apply_lock.lock().await;
            enforce_leader_writeability(runtime, &command).await?;
            let consensus_term = runtime.replication.current_term().await;
            let execute_result = engine
                .execute(request_id, consensus_term, command.clone())
                .await?;
            let response = execute_result.response;
            if runtime.replication.role().await == ReplicationRole::Leader
                && !runtime.replication.is_leader_for_term(consensus_term).await
            {
                return Err(ServerError::ReplicationAckUnavailable);
            }
            if is_write_command(&command) {
                let last_applied_sequence = execute_result.last_applied_sequence;
                let last_applied_term = execute_result.last_applied_term;
                let last_applied_checksum = execute_result.last_applied_checksum;
                runtime
                    .replication
                    .set_local_last_applied_state(
                        last_applied_sequence,
                        last_applied_term,
                        last_applied_checksum,
                    )
                    .await;
                if runtime.replication.role().await == ReplicationRole::Leader
                    && let Err(err) = send_cluster_heartbeats_role_guarded_with_timeout(
                        engine.clone(),
                        runtime.clone(),
                    )
                    .await
                {
                    let _ = err;
                }
                if let Err(err) = drive_write_commit(&engine, runtime, last_applied_sequence).await
                {
                    rollback_uncommitted_tail(&engine, runtime).await?;
                    return Err(err);
                }
            }
            Ok(response)
        }
    }
}

fn cluster_entries_from_snapshot(snapshot: &ReplicationStatusSnapshot) -> Vec<(String, String)> {
    let mut entries = vec![
        ("node_id".to_string(), snapshot.node_id.clone()),
        ("group_id".to_string(), snapshot.group_id.clone()),
        ("role".to_string(), snapshot.role.clone()),
        (
            "current_term".to_string(),
            snapshot.current_term.to_string(),
        ),
        (
            "leader_node_id".to_string(),
            snapshot
                .leader_node_id
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "leader_advertise_addr".to_string(),
            snapshot
                .leader_advertise_addr
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "commit_index".to_string(),
            snapshot.commit_sequence.to_string(),
        ),
        (
            "last_applied_index".to_string(),
            snapshot.local_last_applied_sequence.to_string(),
        ),
        ("quorum_size".to_string(), snapshot.quorum_size.to_string()),
        (
            "member_count".to_string(),
            snapshot.members.len().to_string(),
        ),
        ("sync_policy".to_string(), snapshot.write_ack_mode.clone()),
        ("health".to_string(), snapshot.health.clone()),
    ];
    for member in &snapshot.members {
        entries.push((
            format!("member.{}.advertise_addr", member.node_id),
            member.advertise_addr.clone(),
        ));
        entries.push((
            format!("member.{}.voter", member.node_id),
            member.voter.to_string(),
        ));
    }
    entries
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
    let replication = runtime.replication.snapshot().await;
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
        (
            "server.mode".to_string(),
            if replication.role == "standalone" {
                "single-node".to_string()
            } else {
                "primary-replica".to_string()
            },
        ),
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
        ("replication.role".to_string(), replication.role.clone()),
        (
            "replication.node_id".to_string(),
            replication.node_id.clone(),
        ),
        (
            "replication.group_id".to_string(),
            replication.group_id.clone(),
        ),
        (
            "replication.write_ack_mode".to_string(),
            replication.write_ack_mode.clone(),
        ),
        (
            "replication.leader_node_id".to_string(),
            replication
                .leader_node_id
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "replication.upstream".to_string(),
            replication
                .upstream
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "replication.paused".to_string(),
            replication.paused.to_string(),
        ),
        ("replication.health".to_string(), replication.health.clone()),
        (
            "replication.reason".to_string(),
            replication.reason.unwrap_or_else(|| "none".to_string()),
        ),
        (
            "replication.commit_sequence".to_string(),
            replication.commit_sequence.to_string(),
        ),
        (
            "replication.retention_floor_sequence".to_string(),
            replication
                .retention_floor_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        ("health.status".to_string(), replication.health.clone()),
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
    use crate::replication::{
        ClusterMember, ReplicationConfig, ReplicationRole, ReplicationRuntime, WriteAckMode,
    };
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
            replication: Arc::new(
                ReplicationRuntime::new(ReplicationConfig {
                    node_id: "test-node".to_string(),
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
                    state_path: audit_path.parent().unwrap().join("cluster-state.json"),
                    state_tmp_path: audit_path.parent().unwrap().join("cluster-state.json.tmp"),
                    initial_members: Vec::new(),
                })
                .unwrap(),
            ),
            replication_fanout_lock: Arc::new(tokio::sync::Mutex::new(())),
            replication_apply_lock: Arc::new(tokio::sync::Mutex::new(())),
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
    fn rejects_transaction_exec_when_node_is_candidate() {
        let runtime_handle = tokio::runtime::Runtime::new().unwrap();
        let metrics = Arc::new(Metrics::default());
        let mut runtime = runtime();
        runtime.replication = Arc::new(
            ReplicationRuntime::new(ReplicationConfig {
                node_id: "candidate-node".to_string(),
                group_id: "candidate-group".to_string(),
                advertise_addr: Some("127.0.0.1:9173".to_string()),
                role: ReplicationRole::Candidate,
                upstream: None,
                upstream_username: None,
                upstream_password: None,
                write_ack_mode: WriteAckMode::Replica,
                ack_timeout: Duration::from_millis(100),
                poll_interval: Duration::from_millis(100),
                fetch_batch_size: 32,
                stale_after: Duration::from_secs(5),
                heartbeat_interval: Duration::from_millis(100),
                election_timeout_min: Duration::from_millis(250),
                election_timeout_max: Duration::from_millis(500),
                state_path: temp_dir("candidate-state").join("cluster-state.json"),
                state_tmp_path: temp_dir("candidate-state").join("cluster-state.json.tmp"),
                initial_members: vec![
                    ClusterMember {
                        node_id: "candidate-node".to_string(),
                        advertise_addr: "127.0.0.1:9173".to_string(),
                        voter: true,
                    },
                    ClusterMember {
                        node_id: "peer-node".to_string(),
                        advertise_addr: "127.0.0.1:9174".to_string(),
                        voter: true,
                    },
                ],
            })
            .unwrap(),
        );

        let engine = Engine::from_paths_with_options(
            Paths::from_data_dir(temp_dir("candidate-exec")).unwrap(),
            engine::EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("candidate-exec-key")),
                ..engine::EngineOptions::default()
            },
        )
        .unwrap();
        let handle = EngineHandle::new(engine);
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
        session.transaction_queue.push(Command::Multi);
        session.transaction_queue.push(Command::Set {
            key: "candidate:key".to_string(),
            value: "candidate:value".to_string(),
            options: CommandSetOptions::default(),
        });

        let err = runtime_handle
            .block_on(handle_transaction_command(
                handle,
                metrics,
                &runtime,
                &mut session,
                id(4),
                Command::Exec,
            ))
            .unwrap_err();
        assert!(matches!(err, crate::ServerError::ReplicationReadOnly));
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
