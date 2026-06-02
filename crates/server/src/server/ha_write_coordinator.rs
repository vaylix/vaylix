use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use command::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};
use transport::Response;
use uuid::Uuid;

use super::{
    EngineHandle, ServerRuntimeConfig, advance_read_index_to, enforce_leader_writeability,
    rollback_uncommitted_tail, send_cluster_heartbeats_role_guarded_with_timeout,
};
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::replication::ReplicationRole;

const HA_WRITE_QUEUE_CAPACITY: usize = 2048;
const HA_WRITE_BATCH_LIMIT: usize = 64;
const HA_WRITE_BATCH_WINDOW: Duration = Duration::from_micros(100);

struct HaWriteRequest {
    request_id: Uuid,
    command: Command,
    respond_to: oneshot::Sender<Result<Response>>,
}

struct PendingHaResponse {
    respond_to: oneshot::Sender<Result<Response>>,
}

/// Ordered leader write coordinator for quorum-backed data writes.
///
/// The coordinator preserves the single-leader write invariant while allowing
/// concurrent client writes to share one local WAL batch and one replication
/// frontier. Requests are admitted only after the normal connection path has
/// already completed auth, RBAC, validation, quotas, rate limits, maintenance
/// checks, and transaction-state checks.
#[derive(Clone)]
pub struct HaWriteCoordinator {
    sender: mpsc::Sender<HaWriteRequest>,
}

impl HaWriteCoordinator {
    pub(super) fn start(
        engine: EngineHandle,
        metrics: Arc<Metrics>,
        runtime: ServerRuntimeConfig,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(HA_WRITE_QUEUE_CAPACITY);
        tokio::spawn(run_ha_write_coordinator(engine, metrics, runtime, receiver));
        Self { sender }
    }

    pub async fn execute(&self, request_id: Uuid, command: Command) -> Result<Response> {
        let (respond_to, response) = oneshot::channel();
        self.sender
            .send(HaWriteRequest {
                request_id,
                command,
                respond_to,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        response
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?
    }
}

async fn run_ha_write_coordinator(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: ServerRuntimeConfig,
    mut receiver: mpsc::Receiver<HaWriteRequest>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        drain_ready_requests(&mut receiver, &mut batch);

        if batch.len() < HA_WRITE_BATCH_LIMIT {
            let deadline = Instant::now() + HA_WRITE_BATCH_WINDOW;
            loop {
                tokio::select! {
                    biased;
                    maybe_request = receiver.recv(), if batch.len() < HA_WRITE_BATCH_LIMIT => {
                        match maybe_request {
                            Some(request) => {
                                batch.push(request);
                                drain_ready_requests(&mut receiver, &mut batch);
                                if batch.len() >= HA_WRITE_BATCH_LIMIT {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = sleep_until(deadline) => break,
                }
            }
        }

        process_batch(&engine, metrics.as_ref(), &runtime, batch).await;
    }
}

fn drain_ready_requests(
    receiver: &mut mpsc::Receiver<HaWriteRequest>,
    batch: &mut Vec<HaWriteRequest>,
) {
    while batch.len() < HA_WRITE_BATCH_LIMIT {
        match receiver.try_recv() {
            Ok(request) => batch.push(request),
            Err(_) => break,
        }
    }
}

async fn process_batch(
    engine: &EngineHandle,
    metrics: &Metrics,
    runtime: &ServerRuntimeConfig,
    batch: Vec<HaWriteRequest>,
) {
    let _write_guard = runtime.replication_apply_lock.lock().await;
    metrics.ha_write_batches.fetch_add(1, Ordering::Relaxed);
    metrics
        .ha_write_coordinated
        .fetch_add(batch.len() as u64, Ordering::Relaxed);
    metrics
        .ha_write_batch_max_size
        .fetch_max(batch.len() as u64, Ordering::Relaxed);

    if runtime.replication.role().await != ReplicationRole::Leader {
        respond_all_err(batch, ServerError::ReplicationReadOnly);
        return;
    }

    for request in &batch {
        if let Err(err) = enforce_leader_writeability(runtime, &request.command).await {
            respond_all_err(batch, err);
            return;
        }
    }

    let consensus_term = runtime.replication.current_term().await;
    let mut pending_responses = Vec::with_capacity(batch.len());
    let mut requests = Vec::with_capacity(batch.len());
    for request in batch {
        pending_responses.push(PendingHaResponse {
            respond_to: request.respond_to,
        });
        requests.push((request.request_id, request.command));
    }

    let execute_results = match engine.execute_write_batch(consensus_term, requests).await {
        Ok(results) => results,
        Err(err) => {
            respond_pending_err(pending_responses, err);
            return;
        }
    };

    if runtime.replication.role().await != ReplicationRole::Leader
        || !runtime.replication.is_leader_for_term(consensus_term).await
    {
        if let Err(err) = rollback_uncommitted_tail(engine, runtime).await {
            respond_pending_err(pending_responses, err);
        } else {
            respond_pending_err(pending_responses, ServerError::ReplicationAckUnavailable);
        }
        return;
    }

    let Some(frontier) = execute_results
        .iter()
        .map(|result| {
            (
                result.last_applied_sequence,
                result.last_applied_term,
                result.last_applied_checksum,
            )
        })
        .max_by_key(|(sequence, _, _)| *sequence)
    else {
        respond_pending_err(pending_responses, ServerError::EngineWorkerClosed);
        return;
    };
    let (frontier_sequence, frontier_term, frontier_checksum) = frontier;

    runtime
        .replication
        .set_local_last_applied_state(frontier_sequence, frontier_term, frontier_checksum)
        .await;

    if let Err(err) =
        send_cluster_heartbeats_role_guarded_with_timeout(engine.clone(), runtime.clone()).await
    {
        let _ = err;
    }

    if let Err(err) = super::drive_write_commit(engine, runtime, frontier_sequence).await {
        if let Err(rollback_err) = rollback_uncommitted_tail(engine, runtime).await {
            respond_pending_err(pending_responses, rollback_err);
        } else {
            respond_pending_err(pending_responses, err);
        }
        return;
    }

    if let Err(err) = advance_read_index_to(engine, metrics, runtime, frontier_sequence).await {
        respond_pending_err(pending_responses, err);
        return;
    }

    for (pending_response, result) in pending_responses.into_iter().zip(execute_results) {
        if result.last_applied_sequence <= frontier_sequence {
            let _ = pending_response.respond_to.send(Ok(result.response));
        } else {
            let _ = pending_response
                .respond_to
                .send(Err(ServerError::ReplicationAckUnavailable));
        }
    }
}

fn respond_all_err(batch: Vec<HaWriteRequest>, err: ServerError) {
    let mut requests = batch.into_iter();
    if let Some(first) = requests.next() {
        let _ = first.respond_to.send(Err(err));
    }
    for request in requests {
        let _ = request
            .respond_to
            .send(Err(ServerError::ReplicationAckUnavailable));
    }
}

fn respond_pending_err(batch: Vec<PendingHaResponse>, err: ServerError) {
    let mut requests = batch.into_iter();
    if let Some(first) = requests.next() {
        let _ = first.respond_to.send(Err(err));
    }
    for request in requests {
        let _ = request
            .respond_to
            .send(Err(ServerError::ReplicationAckUnavailable));
    }
}

pub(super) fn is_ha_write_coordinator_command(command: &Command) -> bool {
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
            | Command::Expire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. }
            | Command::Clear
    )
}
