use std::sync::Arc;
use std::sync::atomic::Ordering;

use command::Command;
use transport::Response;
use uuid::Uuid;

use super::{
    EngineHandle, ServerRuntimeConfig, SessionState, authorize_command, current_time_millis,
    drive_write_commit, enforce_leader_writeability, map_transaction_result_payload,
    replication_role_accepts_writes, rollback_uncommitted_tail,
    send_cluster_heartbeats_role_guarded_with_timeout, validate_command,
    validate_transaction_command,
};
use crate::error::{Result, ServerError};
use crate::metrics::Metrics;
use crate::replication::ReplicationRole;

pub(super) async fn handle_transaction_command(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    runtime: &ServerRuntimeConfig,
    session: &mut SessionState,
    request_id: Uuid,
    command: Command,
) -> Result<Response> {
    expire_transaction_if_needed(metrics.clone(), runtime, session)?;
    if !replication_role_accepts_writes(runtime.replication.role().await) {
        return Err(ServerError::ReplicationReadOnly);
    }
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
            let _write_guard = runtime.replication_apply_lock.lock().await;
            enforce_leader_writeability(runtime, &Command::Exec).await?;
            let mut queued = std::mem::take(&mut session.transaction_queue);
            session.transaction_started_at_ms = None;
            if matches!(queued.first(), Some(Command::Multi)) {
                queued.remove(0);
            }
            for command in &queued {
                validate_transaction_command(command)?;
            }
            let consensus_term = runtime.replication.current_term().await;
            let results = engine
                .execute_batch(request_id, consensus_term, queued.clone())
                .await?;
            let mut encoded = Vec::with_capacity(results.len());
            for (_queued_command, result) in queued.iter().zip(results) {
                encoded.push(map_transaction_result_payload(result));
            }
            let last_applied = engine.last_applied_state().await?;
            runtime
                .replication
                .set_local_last_applied_state(
                    last_applied.last_applied_sequence,
                    last_applied.last_applied_term,
                    last_applied.last_applied_checksum,
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
            if let Err(err) =
                drive_write_commit(&engine, runtime, last_applied.last_applied_sequence).await
            {
                rollback_uncommitted_tail(&engine, runtime).await?;
                return Err(err);
            }
            metrics
                .transactions_committed
                .fetch_add(1, Ordering::Relaxed);
            Ok(Response::exec_results(request_id, &encoded)?)
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

pub(super) fn expire_transaction_if_needed(
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
