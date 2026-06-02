use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use command::Command;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use transport::{CodecOptions, Request, Response, Status};
use uuid::Uuid;

use super::ServerRuntimeConfig;
use crate::error::{Result, ServerError};
use crate::replication::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterMember, HeartbeatRequest, HeartbeatResponse,
};

#[derive(Default)]
struct PoolCounters {
    connections_opened: AtomicU64,
    reconnects: AtomicU64,
    append_failures: AtomicU64,
    heartbeat_failures: AtomicU64,
    append_requests: AtomicU64,
    append_latency_total_ms: AtomicU64,
}

/// Persistent peer connection pool for hot replication RPCs.
///
/// Vote and snapshot install RPCs intentionally remain one-shot. The pool is
/// optimized for the steady-state append/heartbeat path where opening a TCP
/// connection and repeating transport negotiation for every write dominates
/// local quorum latency.
#[derive(Default)]
pub struct ReplicationClientPool {
    peers: Mutex<BTreeMap<String, mpsc::Sender<PeerRequest>>>,
    counters: Arc<PoolCounters>,
}

impl ReplicationClientPool {
    pub(crate) async fn append(
        &self,
        runtime: ServerRuntimeConfig,
        member: ClusterMember,
        payload: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let started = Instant::now();
        let result = self
            .send_with_retry(runtime, member, |respond_to| PeerRequest::Append {
                payload: payload.clone(),
                respond_to,
            })
            .await;
        self.counters
            .append_requests
            .fetch_add(1, Ordering::Relaxed);
        self.counters.append_latency_total_ms.fetch_add(
            started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        if result.is_err() {
            self.counters
                .append_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(crate) async fn heartbeat(
        &self,
        runtime: ServerRuntimeConfig,
        member: ClusterMember,
        payload: HeartbeatRequest,
    ) -> Result<HeartbeatResponse> {
        let result = self
            .send_with_retry(runtime, member, |respond_to| PeerRequest::Heartbeat {
                payload: payload.clone(),
                respond_to,
            })
            .await;
        if result.is_err() {
            self.counters
                .heartbeat_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(crate) fn snapshot(&self) -> Vec<(String, String)> {
        let append_requests = self.counters.append_requests.load(Ordering::Relaxed);
        let append_latency_total_ms = self
            .counters
            .append_latency_total_ms
            .load(Ordering::Relaxed);
        let append_latency_avg_ms = append_latency_total_ms
            .checked_div(append_requests)
            .unwrap_or(0);
        vec![
            (
                "replication.client.connection.opened.count".to_string(),
                self.counters
                    .connections_opened
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "replication.client.reconnect.count".to_string(),
                self.counters.reconnects.load(Ordering::Relaxed).to_string(),
            ),
            (
                "replication.client.append.failure.count".to_string(),
                self.counters
                    .append_failures
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "replication.client.heartbeat.failure.count".to_string(),
                self.counters
                    .heartbeat_failures
                    .load(Ordering::Relaxed)
                    .to_string(),
            ),
            (
                "replication.client.append.latency.avg.ms".to_string(),
                append_latency_avg_ms.to_string(),
            ),
        ]
    }

    async fn send_with_retry<T>(
        &self,
        runtime: ServerRuntimeConfig,
        member: ClusterMember,
        build: impl Fn(oneshot::Sender<Result<T>>) -> PeerRequest + Copy,
    ) -> Result<T> {
        let key = peer_key(&member);
        let first = self
            .send_once(key.clone(), runtime.clone(), member.clone(), build)
            .await;
        if first.is_ok() {
            return first;
        }
        self.remove_peer(&key).await;
        self.counters.reconnects.fetch_add(1, Ordering::Relaxed);
        self.send_once(key, runtime, member, build).await
    }

    async fn send_once<T>(
        &self,
        key: String,
        runtime: ServerRuntimeConfig,
        member: ClusterMember,
        build: impl Fn(oneshot::Sender<Result<T>>) -> PeerRequest,
    ) -> Result<T> {
        let sender = self.peer_sender(key, runtime, member).await;
        let (respond_to, response) = oneshot::channel();
        sender
            .send(build(respond_to))
            .await
            .map_err(|_| ServerError::ReplicationAckUnavailable)?;
        response
            .await
            .map_err(|_| ServerError::ReplicationAckUnavailable)?
    }

    async fn peer_sender(
        &self,
        key: String,
        runtime: ServerRuntimeConfig,
        member: ClusterMember,
    ) -> mpsc::Sender<PeerRequest> {
        let mut peers = self.peers.lock().await;
        if let Some(sender) = peers.get(&key) {
            return sender.clone();
        }
        let (sender, receiver) = mpsc::channel(128);
        tokio::spawn(peer_worker(
            runtime,
            member,
            receiver,
            Arc::clone(&self.counters),
        ));
        peers.insert(key, sender.clone());
        sender
    }

    async fn remove_peer(&self, key: &str) {
        self.peers.lock().await.remove(key);
    }
}

enum PeerRequest {
    Append {
        payload: AppendEntriesRequest,
        respond_to: oneshot::Sender<Result<AppendEntriesResponse>>,
    },
    Heartbeat {
        payload: HeartbeatRequest,
        respond_to: oneshot::Sender<Result<HeartbeatResponse>>,
    },
}

struct PeerConnection {
    stream: TcpStream,
    transport: CodecOptions,
}

async fn peer_worker(
    runtime: ServerRuntimeConfig,
    member: ClusterMember,
    mut receiver: mpsc::Receiver<PeerRequest>,
    counters: Arc<PoolCounters>,
) {
    let mut connection = None;
    while let Some(request) = receiver.recv().await {
        match request {
            PeerRequest::Append {
                payload,
                respond_to,
            } => {
                let result =
                    send_append(&runtime, &member, &mut connection, &counters, payload).await;
                let _ = respond_to.send(result);
            }
            PeerRequest::Heartbeat {
                payload,
                respond_to,
            } => {
                let result =
                    send_heartbeat(&runtime, &member, &mut connection, &counters, payload).await;
                let _ = respond_to.send(result);
            }
        }
    }
}

async fn send_append(
    runtime: &ServerRuntimeConfig,
    member: &ClusterMember,
    connection: &mut Option<PeerConnection>,
    counters: &PoolCounters,
    payload: AppendEntriesRequest,
) -> Result<AppendEntriesResponse> {
    send_json_rpc(
        runtime,
        member,
        connection,
        counters,
        transport::Opcode::ReplicationAppend,
        &payload,
    )
    .await
}

async fn send_heartbeat(
    runtime: &ServerRuntimeConfig,
    member: &ClusterMember,
    connection: &mut Option<PeerConnection>,
    counters: &PoolCounters,
    payload: HeartbeatRequest,
) -> Result<HeartbeatResponse> {
    send_json_rpc(
        runtime,
        member,
        connection,
        counters,
        transport::Opcode::ReplicationHeartbeat,
        &payload,
    )
    .await
}

async fn send_json_rpc<T, R>(
    runtime: &ServerRuntimeConfig,
    member: &ClusterMember,
    connection: &mut Option<PeerConnection>,
    counters: &PoolCounters,
    opcode: transport::Opcode,
    payload: &T,
) -> Result<R>
where
    T: serde::Serialize,
    R: serde::de::DeserializeOwned,
{
    let first = send_json_rpc_once(runtime, member, connection, counters, opcode, payload).await;
    if first.is_ok() {
        return first;
    }
    *connection = None;
    send_json_rpc_once(runtime, member, connection, counters, opcode, payload).await
}

async fn send_json_rpc_once<T, R>(
    runtime: &ServerRuntimeConfig,
    member: &ClusterMember,
    connection: &mut Option<PeerConnection>,
    counters: &PoolCounters,
    opcode: transport::Opcode,
    payload: &T,
) -> Result<R>
where
    T: serde::Serialize,
    R: serde::de::DeserializeOwned,
{
    ensure_connection(runtime, member, connection, counters).await?;
    let connection = connection
        .as_mut()
        .ok_or(ServerError::ReplicationAckUnavailable)?;
    let request = Request::new(
        Uuid::now_v7(),
        opcode,
        serde_json::to_vec(payload)
            .map_err(|err| ServerError::InvalidArguments(err.to_string()))?,
    );
    transport::write_request_to_async_with_options(
        &mut connection.stream,
        &request,
        connection.transport,
    )
    .await?;
    let response = transport::read_response_from_async_with_options(
        &mut connection.stream,
        connection.transport,
    )
    .await?;
    ensure_replication_ok(&response)?;
    serde_json::from_slice(&response.payload)
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))
}

async fn ensure_connection(
    runtime: &ServerRuntimeConfig,
    member: &ClusterMember,
    connection: &mut Option<PeerConnection>,
    counters: &PoolCounters,
) -> Result<()> {
    if connection.is_some() {
        return Ok(());
    }
    let mut stream = TcpStream::connect(&member.advertise_addr)
        .await
        .map_err(ServerError::Accept)?;
    let transport = negotiate_replication_transport(&mut stream).await?;
    authenticate_replication_peer(&mut stream, transport, runtime).await?;
    *connection = Some(PeerConnection { stream, transport });
    counters.connections_opened.fetch_add(1, Ordering::Relaxed);
    Ok(())
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

fn peer_key(member: &ClusterMember) -> String {
    format!("{}@{}", member.node_id, member.advertise_addr)
}
