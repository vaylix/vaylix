use std::thread;

use command::Command;
use engine::{Engine, StorageEngine, TransactionResult};
use tokio::sync::{mpsc, oneshot};
use transport::Response;
use uuid::Uuid;

use super::{execute_command, parse_last_applied_sequence};
use crate::error::{Result, ServerError};

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
            while let Some(request) = receiver.blocking_recv() {
                match request {
                    EngineRequest::Execute {
                        request_id,
                        consensus_term,
                        command,
                        respond_to,
                    } => {
                        engine.set_consensus_term(consensus_term);
                        let result = execute_command(&mut engine, request_id, command).and_then(
                            |response| {
                                let last_applied_sequence = engine
                                    .info()
                                    .map_err(ServerError::from)
                                    .map(|info| parse_last_applied_sequence(&info))?;
                                let last_applied_term = engine
                                    .wal_entry_term(last_applied_sequence)
                                    .map_err(ServerError::from)?;
                                let last_applied_checksum = engine
                                    .wal_entry_checksum(last_applied_sequence)
                                    .map_err(ServerError::from)?;
                                Ok(ExecuteResult {
                                    response,
                                    last_applied_sequence,
                                    last_applied_term,
                                    last_applied_checksum,
                                })
                            },
                        );
                        let _ = respond_to.send(result);
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
                        let result =
                            engine
                                .append_noop()
                                .map_err(ServerError::from)
                                .and_then(|()| {
                                    let last_applied_sequence = engine
                                        .info()
                                        .map_err(ServerError::from)
                                        .map(|info| parse_last_applied_sequence(&info))?;
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
                                });
                        let _ = respond_to.send(result);
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
