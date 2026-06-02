use std::{collections::VecDeque, thread, time::Duration};

use command::Command;
use engine::{Engine, StorageEngine, TransactionResult, WalSyncPolicy};
use tokio::sync::{mpsc, oneshot};
use transport::Response;
use uuid::Uuid;

use super::execute_command;
use crate::error::{Result, ServerError};

const MAX_WRITE_BATCH: usize = 64;
const SYNC_WRITE_BATCH_WINDOW: Duration = Duration::from_millis(2);

enum EngineRequest {
    Execute {
        request_id: Uuid,
        consensus_term: u64,
        command: Command,
        respond_to: oneshot::Sender<Result<ExecuteResult>>,
    },
    ExecuteBatch {
        request_id: Uuid,
        consensus_term: u64,
        commands: Vec<Command>,
        respond_to: oneshot::Sender<Result<Vec<TransactionResult>>>,
    },
    AppendNoop {
        consensus_term: u64,
        respond_to: oneshot::Sender<Result<LogAppendResult>>,
    },
    LastAppliedState {
        respond_to: oneshot::Sender<Result<LogAppendResult>>,
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
    ReplicationSnapshot {
        respond_to: oneshot::Sender<Result<engine::ReplicationSnapshot>>,
    },
    WalEntriesSince {
        after_sequence: u64,
        limit: usize,
        respond_to: oneshot::Sender<Result<Vec<engine::WalEntry>>>,
    },
    WalEntryChecksum {
        sequence: u64,
        respond_to: oneshot::Sender<Result<Option<u32>>>,
    },
    WalEntryTerm {
        sequence: u64,
        respond_to: oneshot::Sender<Result<Option<u64>>>,
    },
    ApplyReplicationSnapshot {
        snapshot: engine::ReplicationSnapshot,
        respond_to: oneshot::Sender<Result<u64>>,
    },
    ApplyReplicationEntries {
        entries: Vec<engine::WalEntry>,
        respond_to: oneshot::Sender<Result<u64>>,
    },
    ReplaceReplicationSuffix {
        prefix_sequence: u64,
        entries: Vec<engine::WalEntry>,
        respond_to: oneshot::Sender<Result<u64>>,
    },
}

struct PendingExecute {
    request_id: Uuid,
    consensus_term: u64,
    command: Command,
    respond_to: oneshot::Sender<Result<ExecuteResult>>,
}

pub(super) struct ExecuteResult {
    pub(super) response: Response,
    pub(super) last_applied_sequence: u64,
    pub(super) last_applied_term: Option<u64>,
    pub(super) last_applied_checksum: Option<u32>,
}

pub(super) struct LogAppendResult {
    pub(super) last_applied_sequence: u64,
    pub(super) last_applied_term: Option<u64>,
    pub(super) last_applied_checksum: Option<u32>,
}

/// Async facade over the single engine owner thread.
#[derive(Clone)]
pub(super) struct EngineHandle {
    sender: mpsc::Sender<EngineRequest>,
}

impl EngineHandle {
    pub(super) fn new(mut engine: Engine) -> Self {
        let (sender, mut receiver) = mpsc::channel(256);
        thread::spawn(move || {
            let mut pending = VecDeque::new();
            loop {
                let Some(request) = pending.pop_front().or_else(|| receiver.blocking_recv()) else {
                    break;
                };
                match request {
                    EngineRequest::Execute {
                        request_id,
                        consensus_term,
                        command,
                        respond_to,
                    } => {
                        let execute = PendingExecute {
                            request_id,
                            consensus_term,
                            command,
                            respond_to,
                        };
                        if is_batchable_engine_write(&execute.command) {
                            process_write_batch(&mut engine, execute, &mut receiver, &mut pending);
                        } else {
                            process_single_execute(&mut engine, execute);
                        }
                    }
                    EngineRequest::ExecuteBatch {
                        request_id: _request_id,
                        consensus_term,
                        commands,
                        respond_to,
                    } => {
                        engine.set_consensus_term(consensus_term);
                        let _ = respond_to.send(
                            engine
                                .execute_transaction(&commands)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::AppendNoop {
                        consensus_term,
                        respond_to,
                    } => {
                        engine.set_consensus_term(consensus_term);
                        let result = engine
                            .append_noop()
                            .map_err(ServerError::from)
                            .and_then(|()| last_applied_state(&engine));
                        let _ = respond_to.send(result);
                    }
                    EngineRequest::LastAppliedState { respond_to } => {
                        let _ = respond_to.send(last_applied_state(&engine));
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
                    EngineRequest::ReplicationSnapshot { respond_to } => {
                        let _ = respond_to.send(Ok(engine.replication_snapshot()));
                    }
                    EngineRequest::WalEntriesSince {
                        after_sequence,
                        limit,
                        respond_to,
                    } => {
                        let _ = respond_to.send(
                            engine
                                .wal_entries_since(after_sequence, limit)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::WalEntryChecksum {
                        sequence,
                        respond_to,
                    } => {
                        let _ = respond_to.send(
                            engine
                                .wal_entry_checksum(sequence)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::WalEntryTerm {
                        sequence,
                        respond_to,
                    } => {
                        let _ = respond_to
                            .send(engine.wal_entry_term(sequence).map_err(ServerError::from));
                    }
                    EngineRequest::ApplyReplicationSnapshot {
                        snapshot,
                        respond_to,
                    } => {
                        let applied_sequence = snapshot.state.metadata.last_applied_sequence;
                        let result = engine
                            .apply_replication_snapshot(snapshot)
                            .map(|()| applied_sequence)
                            .map_err(ServerError::from);
                        let _ = respond_to.send(result);
                    }
                    EngineRequest::ApplyReplicationEntries {
                        entries,
                        respond_to,
                    } => {
                        let _ = respond_to.send(
                            engine
                                .apply_replication_entries(&entries)
                                .map_err(ServerError::from),
                        );
                    }
                    EngineRequest::ReplaceReplicationSuffix {
                        prefix_sequence,
                        entries,
                        respond_to,
                    } => {
                        let _ = respond_to.send(
                            engine
                                .replace_replication_suffix(prefix_sequence, &entries)
                                .map_err(ServerError::from),
                        );
                    }
                }
            }
        });
        Self { sender }
    }

    pub(super) async fn execute(
        &self,
        request_id: Uuid,
        consensus_term: u64,
        command: Command,
    ) -> Result<ExecuteResult> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Execute {
                request_id,
                consensus_term,
                command,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn execute_batch(
        &self,
        request_id: Uuid,
        consensus_term: u64,
        commands: Vec<Command>,
    ) -> Result<Vec<TransactionResult>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ExecuteBatch {
                request_id,
                consensus_term,
                commands,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn append_noop(&self, consensus_term: u64) -> Result<LogAppendResult> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::AppendNoop {
                consensus_term,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn last_applied_state(&self) -> Result<LogAppendResult> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::LastAppliedState { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn info(&self) -> Result<Vec<(String, String)>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Info { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn snapshot(&self) -> Result<()> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::Snapshot { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn sweep_expired(&self) -> Result<usize> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::SweepExpired { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn validate_backup(&self, dump: String) -> Result<usize> {
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

    pub(super) async fn replication_snapshot(&self) -> Result<engine::ReplicationSnapshot> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ReplicationSnapshot { respond_to: send })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn wal_entries_since(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<engine::WalEntry>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::WalEntriesSince {
                after_sequence,
                limit,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn wal_entry_checksum(&self, sequence: u64) -> Result<Option<u32>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::WalEntryChecksum {
                sequence,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn wal_entry_term(&self, sequence: u64) -> Result<Option<u64>> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::WalEntryTerm {
                sequence,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn apply_replication_snapshot(
        &self,
        snapshot: engine::ReplicationSnapshot,
    ) -> Result<u64> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ApplyReplicationSnapshot {
                snapshot,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn apply_replication_entries(
        &self,
        entries: Vec<engine::WalEntry>,
    ) -> Result<u64> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ApplyReplicationEntries {
                entries,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }

    pub(super) async fn replace_replication_suffix(
        &self,
        prefix_sequence: u64,
        entries: Vec<engine::WalEntry>,
    ) -> Result<u64> {
        let (send, recv) = oneshot::channel();
        self.sender
            .send(EngineRequest::ReplaceReplicationSuffix {
                prefix_sequence,
                entries,
                respond_to: send,
            })
            .await
            .map_err(|_| ServerError::EngineWorkerClosed)?;
        recv.await.map_err(|_| ServerError::EngineWorkerClosed)?
    }
}

fn process_single_execute(engine: &mut Engine, execute: PendingExecute) {
    engine.set_consensus_term(execute.consensus_term);
    let result =
        execute_command(engine, execute.request_id, execute.command).and_then(|response| {
            let last_applied = last_applied_state(engine)?;
            Ok(ExecuteResult {
                response,
                last_applied_sequence: last_applied.last_applied_sequence,
                last_applied_term: last_applied.last_applied_term,
                last_applied_checksum: last_applied.last_applied_checksum,
            })
        });
    let _ = execute.respond_to.send(result);
}

fn process_write_batch(
    engine: &mut Engine,
    first: PendingExecute,
    receiver: &mut mpsc::Receiver<EngineRequest>,
    pending: &mut VecDeque<EngineRequest>,
) {
    let consensus_term = first.consensus_term;
    let mut batch = vec![first];

    drain_write_batch(receiver, pending, consensus_term, &mut batch);
    if engine.wal_sync_policy() == WalSyncPolicy::SyncData && batch.len() < MAX_WRITE_BATCH {
        thread::sleep(SYNC_WRITE_BATCH_WINDOW);
        drain_write_batch(receiver, pending, consensus_term, &mut batch);
    }

    engine.set_consensus_term(consensus_term);
    let commands = batch
        .iter()
        .map(|execute| execute.command.clone())
        .collect::<Vec<_>>();
    match engine
        .execute_command_batch(&commands)
        .map_err(ServerError::from)
    {
        Ok(results) => {
            for (execute, result) in batch.into_iter().zip(results) {
                let response = render_transaction_result(execute.request_id, result.result)
                    .and_then(|response| {
                        let last_applied =
                            applied_state_for_sequence(engine, result.last_applied_sequence)?;
                        Ok(ExecuteResult {
                            response,
                            last_applied_sequence: last_applied.last_applied_sequence,
                            last_applied_term: last_applied.last_applied_term,
                            last_applied_checksum: last_applied.last_applied_checksum,
                        })
                    });
                let _ = execute.respond_to.send(response);
            }
        }
        Err(err) => {
            let mut batch = batch.into_iter();
            if let Some(first) = batch.next() {
                let _ = first.respond_to.send(Err(err));
            }
            for execute in batch {
                let _ = execute
                    .respond_to
                    .send(Err(ServerError::EngineWorkerClosed));
            }
        }
    }
}

fn drain_write_batch(
    receiver: &mut mpsc::Receiver<EngineRequest>,
    pending: &mut VecDeque<EngineRequest>,
    consensus_term: u64,
    batch: &mut Vec<PendingExecute>,
) {
    while batch.len() < MAX_WRITE_BATCH {
        match receiver.try_recv() {
            Ok(EngineRequest::Execute {
                request_id,
                consensus_term: request_term,
                command,
                respond_to,
            }) if request_term == consensus_term && is_batchable_engine_write(&command) => {
                batch.push(PendingExecute {
                    request_id,
                    consensus_term: request_term,
                    command,
                    respond_to,
                });
            }
            Ok(other) => {
                pending.push_back(other);
                break;
            }
            Err(_) => break,
        }
    }
}

fn is_batchable_engine_write(command: &Command) -> bool {
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

fn render_transaction_result(request_id: Uuid, result: TransactionResult) -> Result<Response> {
    match result {
        TransactionResult::Ok => Ok(Response::ok(request_id)),
        TransactionResult::NotFound => Ok(Response::not_found(request_id)),
        TransactionResult::Value(value) => Ok(Response::value(request_id, &value)?),
        TransactionResult::Boolean(value) => Ok(Response::boolean(request_id, value)),
        TransactionResult::Count(value) => Ok(Response::count(request_id, value)),
        TransactionResult::Integer(value) => Ok(Response::integer(request_id, value)),
        TransactionResult::Entries(entries) => Ok(Response::entries(request_id, &entries)?),
        TransactionResult::Strings(values) => Ok(Response::strings(request_id, &values)?),
        TransactionResult::Scan(scan) => {
            Ok(Response::scan(request_id, scan.next_cursor, &scan.keys)?)
        }
    }
}

fn last_applied_state(engine: &Engine) -> Result<LogAppendResult> {
    applied_state_for_sequence(engine, engine.last_applied_sequence())
}

fn applied_state_for_sequence(
    engine: &Engine,
    last_applied_sequence: u64,
) -> Result<LogAppendResult> {
    let last_applied_term = engine
        .wal_entry_term(last_applied_sequence)
        .map_err(ServerError::from)?;
    let last_applied_checksum = engine
        .wal_entry_checksum(last_applied_sequence)
        .map_err(ServerError::from)?;
    Ok(LogAppendResult {
        last_applied_sequence,
        last_applied_term,
        last_applied_checksum,
    })
}
