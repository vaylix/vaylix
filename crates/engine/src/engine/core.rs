use crate::EngineMetadata;
use crate::config::EngineOptions;
use crate::engine::{
    EngineState, Expiration, LogicalBackup, LogicalBackupEntry, ScanPage, SetCondition, SetOptions,
    SetOutcome, StorageEngine, TransactionResult,
};
use crate::error::Result;
use crate::paths::Paths;
use crate::store::{
    Manifest, STORAGE_FORMAT_VERSION, WalEntry, WalOperation, append, deserialize, keyring, load,
    load_manifest, replay, save, save_manifest, serialize, truncate,
};
use command::Command;
use crc32fast::hash;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_SCAN_COUNT: usize = 10;

/// Core string-to-string storage engine backed by snapshots and a write-ahead log.
pub struct Engine {
    state: EngineState,
    paths: Paths,
    options: EngineOptions,
}

impl Engine {
    /// Creates a new engine using the default filesystem layout and default options.
    pub fn new() -> Result<Self> {
        Self::from_paths_with_options(Paths::new()?, EngineOptions::default())
    }

    /// Creates a new engine using the default filesystem layout and caller-provided options.
    pub fn with_options(options: EngineOptions) -> Result<Self> {
        Self::from_paths_with_options(Paths::new()?, options)
    }

    /// Creates a new engine using an explicit filesystem layout and default options.
    pub fn from_paths(paths: Paths) -> Result<Self> {
        Self::from_paths_with_options(paths, EngineOptions::default())
    }

    /// Creates a new engine using an explicit filesystem layout and caller-provided options.
    pub fn from_paths_with_options(paths: Paths, options: EngineOptions) -> Result<Self> {
        let loaded = load(&paths.snapshot_path, options.keyring.as_ref())?;

        let mut state = match &loaded {
            Some(loaded) => deserialize(loaded)?,
            None => EngineState::new(),
        };

        if let Some(manifest) = load_manifest(&paths.manifest_path)? {
            if manifest.storage_format_version != STORAGE_FORMAT_VERSION {
                return Err(crate::EngineError::UnsupportedStorageFormat {
                    resource: "manifest",
                });
            }
            if let Some(snapshot) = &loaded
                && hash(snapshot) != manifest.snapshot_checksum
            {
                return Err(crate::EngineError::ChecksumMismatch {
                    resource: "snapshot",
                });
            }
            state.metadata.last_snapshot_at_ms = Some(manifest.last_snapshot_at_ms);
            state.metadata.last_applied_sequence = state
                .metadata
                .last_applied_sequence
                .max(manifest.last_snapshot_sequence);
        }

        for entry in replay(&paths.wal_path, options.keyring.as_ref())? {
            state.apply_entry(&entry)?;
        }

        Ok(Self {
            state,
            paths,
            options,
        })
    }

    /// Returns immutable access to the in-memory state for diagnostics and tests.
    pub fn state(&self) -> &EngineState {
        &self.state
    }

    fn append_and_apply(&mut self, operations: Vec<WalOperation>) -> Result<()> {
        let entry = self.next_entry(operations);
        append(
            &entry,
            &self.paths.wal_path,
            self.options.wal_sync,
            self.options.keyring.as_ref(),
        )?;
        self.state.apply_entry(&entry)?;
        Ok(())
    }

    fn next_entry(&self, operations: Vec<WalOperation>) -> WalEntry {
        WalEntry::new(
            self.state.metadata.last_applied_sequence + 1,
            now_millis(),
            operations,
        )
    }

    fn now(&self) -> u64 {
        now_millis()
    }

    fn info_entries(&self, metadata: &EngineMetadata, key_count: usize) -> Vec<(String, String)> {
        let wal_size = std::fs::metadata(&self.paths.wal_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);

        vec![
            ("engine_version".to_string(), metadata.version.to_string()),
            (
                "created_at_ms".to_string(),
                metadata.created_at_ms.to_string(),
            ),
            (
                "updated_at_ms".to_string(),
                metadata.updated_at_ms.to_string(),
            ),
            (
                "last_snapshot_at_ms".to_string(),
                metadata
                    .last_snapshot_at_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "last_applied_sequence".to_string(),
                metadata.last_applied_sequence.to_string(),
            ),
            ("key_count".to_string(), key_count.to_string()),
            ("wal_size_bytes".to_string(), wal_size.to_string()),
            (
                "wal_sync_policy".to_string(),
                self.options.wal_sync.as_str().to_string(),
            ),
            (
                "storage_encryption".to_string(),
                if self.options.keyring.is_some() {
                    "enabled".to_string()
                } else {
                    "disabled".to_string()
                },
            ),
            (
                "storage_key_id".to_string(),
                self.options
                    .keyring
                    .as_ref()
                    .map(|keyring| keyring.active.id.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            ),
        ]
    }

    fn resolve_expiration(now_ms: u64, expiration: Expiration) -> u64 {
        match expiration {
            Expiration::Seconds(seconds) => now_ms.saturating_add(seconds.saturating_mul(1_000)),
            Expiration::Milliseconds(milliseconds) => now_ms.saturating_add(milliseconds),
        }
    }

    fn maybe_get_existing_value(&mut self, key: &str) -> Option<String> {
        self.state.purge_expired(self.now());
        self.state.data.get(key).cloned()
    }

    fn map_command_expiration(expiration: Option<command::Expiration>) -> Option<Expiration> {
        expiration.map(|expiration| match expiration {
            command::Expiration::Ex(value) => Expiration::Seconds(value),
            command::Expiration::Px(value) => Expiration::Milliseconds(value),
        })
    }

    fn map_command_set_options(options: command::SetOptions) -> SetOptions {
        SetOptions {
            condition: options.condition.map(|condition| match condition {
                command::SetCondition::Nx => SetCondition::Nx,
                command::SetCondition::Xx => SetCondition::Xx,
            }),
            expiration: Self::map_command_expiration(options.expiration),
            keep_ttl: options.keep_ttl,
        }
    }

    /// Executes a queue of data commands as one serializable single-node transaction.
    pub fn execute_transaction(&mut self, commands: &[Command]) -> Result<Vec<TransactionResult>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let mut working = self.state.clone();
        let mut operations = Vec::new();
        let mut results = Vec::with_capacity(commands.len());

        for command in commands {
            results.push(self.evaluate_transaction_command(
                &mut working,
                now_ms,
                &mut operations,
                command,
            )?);
        }

        if operations.is_empty() {
            self.state = working;
            return Ok(results);
        }

        let entry = self.next_entry(operations);
        append(
            &entry,
            &self.paths.wal_path,
            self.options.wal_sync,
            self.options.keyring.as_ref(),
        )?;
        self.state.apply_entry(&entry)?;

        Ok(results)
    }

    fn evaluate_transaction_command(
        &self,
        state: &mut EngineState,
        now_ms: u64,
        operations: &mut Vec<WalOperation>,
        command: &Command,
    ) -> Result<TransactionResult> {
        state.purge_expired(now_ms);

        match command {
            Command::Ping { message } => Ok(TransactionResult::Value(
                message.clone().unwrap_or_else(|| "PONG".to_string()),
            )),
            Command::Get { key } => Ok(match state.data.get(key).cloned() {
                Some(value) => TransactionResult::Value(value),
                None => TransactionResult::NotFound,
            }),
            Command::GetDel { key } => {
                let value = state.data.remove(key);
                state.expirations.remove(key);
                match value {
                    Some(value) => {
                        operations.push(WalOperation::Delete { key: key.clone() });
                        Ok(TransactionResult::Value(value))
                    }
                    None => Ok(TransactionResult::NotFound),
                }
            }
            Command::GetEx {
                key,
                expiration,
                persist,
            } => {
                let Some(value) = state.data.get(key).cloned() else {
                    return Ok(TransactionResult::NotFound);
                };
                if let Some(expiration) = Self::map_command_expiration(*expiration) {
                    let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
                    if expires_at_ms <= now_ms {
                        state.data.remove(key);
                        state.expirations.remove(key);
                        operations.push(WalOperation::Delete { key: key.clone() });
                    } else {
                        state.expirations.insert(key.clone(), expires_at_ms);
                        operations.push(WalOperation::Expire {
                            key: key.clone(),
                            expires_at_ms,
                        });
                    }
                } else if *persist && state.expirations.remove(key).is_some() {
                    operations.push(WalOperation::Persist { key: key.clone() });
                }
                Ok(TransactionResult::Value(value))
            }
            Command::Set {
                key,
                value,
                options,
            } => {
                let previous = state.data.get(key).cloned();
                let previous_expiration = state.expirations.get(key).copied();
                let mapped = Self::map_command_set_options(options.clone());
                let allowed = match mapped.condition {
                    Some(SetCondition::Nx) => previous.is_none(),
                    Some(SetCondition::Xx) => previous.is_some(),
                    None => true,
                };
                if !allowed {
                    return Ok(if options.return_previous {
                        TransactionResult::NotFound
                    } else if options.condition.is_some() {
                        TransactionResult::Boolean(false)
                    } else {
                        TransactionResult::Ok
                    });
                }

                state.data.insert(key.clone(), value.clone());
                state.expirations.remove(key);
                operations.push(WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                });

                if let Some(expiration) = mapped.expiration {
                    let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
                    if expires_at_ms <= now_ms {
                        state.data.remove(key);
                        state.expirations.remove(key);
                        operations.push(WalOperation::Delete { key: key.clone() });
                    } else {
                        state.expirations.insert(key.clone(), expires_at_ms);
                        operations.push(WalOperation::Expire {
                            key: key.clone(),
                            expires_at_ms,
                        });
                    }
                } else if mapped.keep_ttl
                    && let Some(expires_at_ms) = previous_expiration
                {
                    state.expirations.insert(key.clone(), expires_at_ms);
                    operations.push(WalOperation::Expire {
                        key: key.clone(),
                        expires_at_ms,
                    });
                }

                Ok(if options.return_previous {
                    previous
                        .map(TransactionResult::Value)
                        .unwrap_or(TransactionResult::NotFound)
                } else if options.condition.is_some() {
                    TransactionResult::Boolean(true)
                } else {
                    TransactionResult::Ok
                })
            }
            Command::SetNx { key, value } => {
                if state.data.contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                state.data.insert(key.clone(), value.clone());
                state.expirations.remove(key);
                operations.push(WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                });
                Ok(TransactionResult::Boolean(true))
            }
            Command::MGet { keys } => Ok(TransactionResult::Strings(
                keys.iter()
                    .map(|key| state.data.get(key).cloned())
                    .collect(),
            )),
            Command::MSet { entries } => {
                for (key, value) in entries {
                    state.data.insert(key.clone(), value.clone());
                    state.expirations.remove(key);
                    operations.push(WalOperation::Set {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                Ok(TransactionResult::Ok)
            }
            Command::Delete { keys } => {
                let mut removed = 0_u64;
                for key in keys {
                    if state.data.remove(key).is_some() {
                        removed += 1;
                        state.expirations.remove(key);
                        operations.push(WalOperation::Delete { key: key.clone() });
                    }
                }
                Ok(TransactionResult::Count(removed))
            }
            Command::Exists { key } => Ok(TransactionResult::Boolean(state.data.contains_key(key))),
            Command::Incr { key } => {
                let current = state
                    .data
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                let parsed = current.parse::<i64>().map_err(|_| {
                    crate::EngineError::InvalidIntegerValue {
                        key: key.clone(),
                        value: current.clone(),
                    }
                })?;
                let next = parsed
                    .checked_add(1)
                    .ok_or_else(|| crate::EngineError::NumericOverflow { key: key.clone() })?;
                state.data.insert(key.clone(), next.to_string());
                state.expirations.remove(key);
                operations.push(WalOperation::CheckInteger {
                    key: key.clone(),
                    delta: 1,
                });
                Ok(TransactionResult::Integer(next))
            }
            Command::Decr { key } => {
                let current = state
                    .data
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
                let parsed = current.parse::<i64>().map_err(|_| {
                    crate::EngineError::InvalidIntegerValue {
                        key: key.clone(),
                        value: current.clone(),
                    }
                })?;
                let next = parsed
                    .checked_sub(1)
                    .ok_or_else(|| crate::EngineError::NumericOverflow { key: key.clone() })?;
                state.data.insert(key.clone(), next.to_string());
                state.expirations.remove(key);
                operations.push(WalOperation::CheckInteger {
                    key: key.clone(),
                    delta: -1,
                });
                Ok(TransactionResult::Integer(next))
            }
            Command::Expire { key, seconds } => {
                if !state.data.contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                if *seconds == 0 {
                    state.data.remove(key);
                    state.expirations.remove(key);
                    operations.push(WalOperation::Delete { key: key.clone() });
                    return Ok(TransactionResult::Boolean(true));
                }
                let expires_at_ms = now_ms.saturating_add(seconds.saturating_mul(1_000));
                state.expirations.insert(key.clone(), expires_at_ms);
                operations.push(WalOperation::Expire {
                    key: key.clone(),
                    expires_at_ms,
                });
                Ok(TransactionResult::Boolean(true))
            }
            Command::Ttl { key } => Ok(TransactionResult::Integer(state.ttl_for(key, now_ms))),
            Command::Persist { key } => {
                let removed =
                    state.expirations.remove(key).is_some() && state.data.contains_key(key);
                if removed {
                    operations.push(WalOperation::Persist { key: key.clone() });
                }
                Ok(TransactionResult::Boolean(removed))
            }
            Command::Rename {
                source,
                destination,
            } => {
                let Some(value) = state.data.remove(source) else {
                    return Ok(TransactionResult::Boolean(false));
                };
                let source_ttl = state.expirations.remove(source);
                state.data.insert(destination.clone(), value.clone());
                state.expirations.remove(destination);
                operations.push(WalOperation::Delete {
                    key: source.clone(),
                });
                operations.push(WalOperation::Set {
                    key: destination.clone(),
                    value,
                });
                if let Some(expires_at_ms) = source_ttl {
                    state.expirations.insert(destination.clone(), expires_at_ms);
                    operations.push(WalOperation::Expire {
                        key: destination.clone(),
                        expires_at_ms,
                    });
                }
                Ok(TransactionResult::Boolean(true))
            }
            Command::RenameNx {
                source,
                destination,
            } => {
                if state.data.contains_key(destination) {
                    return Ok(TransactionResult::Boolean(false));
                }
                self.evaluate_transaction_command(
                    state,
                    now_ms,
                    operations,
                    &Command::Rename {
                        source: source.clone(),
                        destination: destination.clone(),
                    },
                )
            }
            Command::Scan {
                cursor,
                pattern,
                count,
            } => {
                let mut keys = state.live_keys(now_ms);
                if let Some(pattern) = pattern.as_deref() {
                    keys.retain(|key| wildcard_matches(pattern, key));
                }
                if keys.is_empty() {
                    return Ok(TransactionResult::Scan(ScanPage {
                        next_cursor: 0,
                        keys: Vec::new(),
                    }));
                }
                let start = usize::try_from(*cursor).unwrap_or(usize::MAX);
                if start >= keys.len() {
                    return Ok(TransactionResult::Scan(ScanPage {
                        next_cursor: 0,
                        keys: Vec::new(),
                    }));
                }
                let limit = usize::from(count.unwrap_or(DEFAULT_SCAN_COUNT as u16)).max(1);
                let end = (start + limit).min(keys.len());
                let next_cursor = if end >= keys.len() { 0 } else { end as u64 };
                Ok(TransactionResult::Scan(ScanPage {
                    next_cursor,
                    keys: keys[start..end].to_vec(),
                }))
            }
            Command::DbSize | Command::Count => {
                Ok(TransactionResult::Count(state.data.len() as u64))
            }
            Command::List => Ok(TransactionResult::Entries(state.live_entries(now_ms))),
            Command::Clear => {
                if state.data.is_empty() && state.expirations.is_empty() {
                    return Ok(TransactionResult::Ok);
                }
                state.data.clear();
                state.expirations.clear();
                operations.push(WalOperation::Clear);
                Ok(TransactionResult::Ok)
            }
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
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Auth { .. }
            | Command::Help
            | Command::Exit => Err(crate::EngineError::UnsupportedCommand(
                "command is not supported inside transactions".to_string(),
            )),
        }
    }
}

impl StorageEngine for Engine {
    fn get(&mut self, key: &str) -> Result<Option<String>> {
        Ok(self.state.get_live(key, self.now()))
    }

    fn set_with_options(
        &mut self,
        key: String,
        value: String,
        options: SetOptions,
    ) -> Result<SetOutcome> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let previous = self.state.data.get(&key).cloned();
        let previous_expiration = self.state.expirations.get(&key).copied();

        let allowed = match options.condition {
            Some(SetCondition::Nx) => previous.is_none(),
            Some(SetCondition::Xx) => previous.is_some(),
            None => true,
        };

        if !allowed {
            return Ok(SetOutcome {
                applied: false,
                previous,
            });
        }

        let mut operations = vec![WalOperation::Set {
            key: key.clone(),
            value,
        }];

        if let Some(expiration) = options.expiration {
            let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
            if expires_at_ms <= now_ms {
                operations.push(WalOperation::Delete { key: key.clone() });
            } else {
                operations.push(WalOperation::Expire {
                    key: key.clone(),
                    expires_at_ms,
                });
            }
        } else if options.keep_ttl
            && let Some(expires_at_ms) = previous_expiration
        {
            operations.push(WalOperation::Expire {
                key: key.clone(),
                expires_at_ms,
            });
        }

        self.append_and_apply(operations)?;

        Ok(SetOutcome {
            applied: true,
            previous,
        })
    }

    fn get_del(&mut self, key: &str) -> Result<Option<String>> {
        let previous = self.maybe_get_existing_value(key);
        if previous.is_some() {
            self.append_and_apply(vec![WalOperation::Delete {
                key: key.to_string(),
            }])?;
        }
        Ok(previous)
    }

    fn get_ex(
        &mut self,
        key: &str,
        expiration: Option<Expiration>,
        persist: bool,
    ) -> Result<Option<String>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let previous = self.state.data.get(key).cloned();
        if previous.is_none() {
            return Ok(None);
        }

        let operation = if let Some(expiration) = expiration {
            let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
            if expires_at_ms <= now_ms {
                Some(WalOperation::Delete {
                    key: key.to_string(),
                })
            } else {
                Some(WalOperation::Expire {
                    key: key.to_string(),
                    expires_at_ms,
                })
            }
        } else if persist && self.state.expirations.contains_key(key) {
            Some(WalOperation::Persist {
                key: key.to_string(),
            })
        } else {
            None
        };

        if let Some(operation) = operation {
            self.append_and_apply(vec![operation])?;
        }

        Ok(previous)
    }

    fn mget(&mut self, keys: &[String]) -> Result<Vec<Option<String>>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        Ok(keys
            .iter()
            .map(|key| self.state.data.get(key).cloned())
            .collect())
    }

    fn mset(&mut self, entries: &[(String, String)]) -> Result<()> {
        let operations = entries
            .iter()
            .map(|(key, value)| WalOperation::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();

        self.append_and_apply(operations)
    }

    fn delete(&mut self, key: &str) -> Result<bool> {
        Ok(self.delete_many(&[key.to_string()])? > 0)
    }

    fn delete_many(&mut self, keys: &[String]) -> Result<usize> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let removed = keys
            .iter()
            .filter(|key| self.state.data.contains_key(key.as_str()))
            .count();

        if removed == 0 {
            return Ok(0);
        }

        let operations = keys
            .iter()
            .map(|key| WalOperation::Delete { key: key.clone() })
            .collect();
        self.append_and_apply(operations)?;

        Ok(removed)
    }

    fn exists(&mut self, key: &str) -> Result<bool> {
        Ok(self.state.has_live_key(key, self.now()))
    }

    fn incr(&mut self, key: &str) -> Result<i64> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let current = self
            .state
            .data
            .get(key)
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        let parsed =
            current
                .parse::<i64>()
                .map_err(|_| crate::EngineError::InvalidIntegerValue {
                    key: key.to_string(),
                    value: current.clone(),
                })?;
        let next = parsed
            .checked_add(1)
            .ok_or_else(|| crate::EngineError::NumericOverflow {
                key: key.to_string(),
            })?;

        self.append_and_apply(vec![WalOperation::CheckInteger {
            key: key.to_string(),
            delta: 1,
        }])?;

        Ok(next)
    }

    fn decr(&mut self, key: &str) -> Result<i64> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let current = self
            .state
            .data
            .get(key)
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        let parsed =
            current
                .parse::<i64>()
                .map_err(|_| crate::EngineError::InvalidIntegerValue {
                    key: key.to_string(),
                    value: current.clone(),
                })?;
        let next = parsed
            .checked_sub(1)
            .ok_or_else(|| crate::EngineError::NumericOverflow {
                key: key.to_string(),
            })?;

        self.append_and_apply(vec![WalOperation::CheckInteger {
            key: key.to_string(),
            delta: -1,
        }])?;

        Ok(next)
    }

    fn expire(&mut self, key: &str, seconds: u64) -> Result<bool> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        if !self.state.data.contains_key(key) {
            return Ok(false);
        }

        if seconds == 0 {
            self.append_and_apply(vec![WalOperation::Delete {
                key: key.to_string(),
            }])?;
            return Ok(true);
        }

        let expires_at_ms = now_ms.saturating_add(seconds.saturating_mul(1_000));
        self.append_and_apply(vec![WalOperation::Expire {
            key: key.to_string(),
            expires_at_ms,
        }])?;
        Ok(true)
    }

    fn ttl(&mut self, key: &str) -> Result<i64> {
        Ok(self.state.ttl_for(key, self.now()))
    }

    fn persist(&mut self, key: &str) -> Result<bool> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        if !self.state.data.contains_key(key) || !self.state.expirations.contains_key(key) {
            return Ok(false);
        }

        self.append_and_apply(vec![WalOperation::Persist {
            key: key.to_string(),
        }])?;
        Ok(true)
    }

    fn rename(&mut self, source: &str, destination: String) -> Result<bool> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let Some(value) = self.state.data.get(source).cloned() else {
            return Ok(false);
        };
        let source_ttl = self.state.expirations.get(source).copied();

        let mut operations = vec![
            WalOperation::Delete {
                key: source.to_string(),
            },
            WalOperation::Set {
                key: destination.clone(),
                value,
            },
        ];
        if let Some(expires_at_ms) = source_ttl {
            operations.push(WalOperation::Expire {
                key: destination,
                expires_at_ms,
            });
        }
        self.append_and_apply(operations)?;
        Ok(true)
    }

    fn rename_nx(&mut self, source: &str, destination: String) -> Result<bool> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        if self.state.data.contains_key(&destination) {
            return Ok(false);
        }
        self.rename(source, destination)
    }

    fn db_size(&mut self) -> Result<usize> {
        self.state.purge_expired(self.now());
        Ok(self.state.data.len())
    }

    fn scan(&mut self, cursor: u64, pattern: Option<&str>, count: Option<u16>) -> Result<ScanPage> {
        let mut keys = self.state.live_keys(self.now());
        if let Some(pattern) = pattern {
            keys.retain(|key| wildcard_matches(pattern, key));
        }

        if keys.is_empty() {
            return Ok(ScanPage {
                next_cursor: 0,
                keys: Vec::new(),
            });
        }

        let start = usize::try_from(cursor).unwrap_or(usize::MAX);
        if start >= keys.len() {
            return Ok(ScanPage {
                next_cursor: 0,
                keys: Vec::new(),
            });
        }

        let limit = usize::from(count.unwrap_or(DEFAULT_SCAN_COUNT as u16)).max(1);
        let end = (start + limit).min(keys.len());
        let next_cursor = if end >= keys.len() { 0 } else { end as u64 };

        Ok(ScanPage {
            next_cursor,
            keys: keys[start..end].to_vec(),
        })
    }

    fn list(&mut self) -> Result<Vec<(String, String)>> {
        Ok(self.state.live_entries(self.now()))
    }

    fn info(&mut self) -> Result<Vec<(String, String)>> {
        self.state.purge_expired(self.now());
        Ok(self.info_entries(&self.state.metadata, self.state.data.len()))
    }

    fn sweep_expired(&mut self) -> Result<usize> {
        Ok(self.state.purge_expired(self.now()))
    }

    fn clear(&mut self) -> Result<()> {
        if self.state.data.is_empty() && self.state.expirations.is_empty() {
            return Ok(());
        }

        self.append_and_apply(vec![WalOperation::Clear])
    }

    fn snapshot(&mut self) -> Result<()> {
        self.state.purge_expired(self.now());

        if let Some(keyring) = self.options.keyring.as_mut() {
            let _ = keyring::rotate_if_due(
                &self.paths.keyring_path,
                &self.paths.keyring_tmp_path,
                keyring,
            )?;
        }

        let sequence = self.state.metadata.last_applied_sequence;
        let snapshot_started_at_ms = self.now();

        let mut durable_state = self.state.clone();
        durable_state.mark_snapshot(snapshot_started_at_ms, sequence);

        let serialized = serialize(&durable_state)?;
        save(
            &serialized,
            &self.paths.snapshot_path,
            &self.paths.snapshot_tmp_path,
            self.options.keyring.as_ref(),
        )?;

        let manifest = Manifest {
            storage_format_version: STORAGE_FORMAT_VERSION,
            engine_version: durable_state.metadata.version,
            last_snapshot_sequence: sequence,
            last_snapshot_at_ms: snapshot_started_at_ms,
            snapshot_size_bytes: serialized.len() as u64,
            snapshot_checksum: hash(&serialized),
        };
        save_manifest(
            &manifest,
            &self.paths.manifest_path,
            &self.paths.manifest_tmp_path,
        )?;

        truncate(&self.paths.wal_path)?;
        self.state = durable_state;

        Ok(())
    }

    fn logical_backup(&mut self) -> Result<String> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let entries = self
            .state
            .data
            .iter()
            .map(|(key, value)| LogicalBackupEntry {
                key: key.clone(),
                value: value.clone(),
                expires_at_ms: self.state.expirations.get(key).copied(),
            })
            .collect();
        let backup = LogicalBackup {
            version: 1,
            created_at_ms: now_ms,
            source_engine_version: self.state.metadata.version,
            source_sequence: self.state.metadata.last_applied_sequence,
            entries,
        };

        serde_json::to_string(&backup)
            .map_err(|err| crate::EngineError::SnapshotSerialize(err.to_string()))
    }

    fn restore_logical_backup(&mut self, dump: &str) -> Result<usize> {
        let backup: LogicalBackup = serde_json::from_str(dump)
            .map_err(|err| crate::EngineError::SnapshotDeserialize(err.to_string()))?;
        if backup.version != 1 {
            return Err(crate::EngineError::UnsupportedStorageFormat {
                resource: "logical backup",
            });
        }

        let now_ms = self.now();
        let mut operations = Vec::with_capacity(1 + backup.entries.len().saturating_mul(2));
        operations.push(WalOperation::Clear);
        let mut restored = 0;

        for entry in backup.entries {
            if let Some(expires_at_ms) = entry.expires_at_ms
                && expires_at_ms <= now_ms
            {
                continue;
            }
            operations.push(WalOperation::Set {
                key: entry.key.clone(),
                value: entry.value,
            });
            if let Some(expires_at_ms) = entry.expires_at_ms {
                operations.push(WalOperation::Expire {
                    key: entry.key,
                    expires_at_ms,
                });
            }
            restored += 1;
        }

        self.append_and_apply(operations)?;
        Ok(restored)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

fn wildcard_matches(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();
    let mut dp = vec![vec![false; text_chars.len() + 1]; pattern_chars.len() + 1];
    dp[0][0] = true;

    for row in 1..=pattern_chars.len() {
        if pattern_chars[row - 1] == '*' {
            dp[row][0] = dp[row - 1][0];
        }
    }

    for row in 1..=pattern_chars.len() {
        for col in 1..=text_chars.len() {
            dp[row][col] = match pattern_chars[row - 1] {
                '*' => dp[row - 1][col] || dp[row][col - 1],
                '?' => dp[row - 1][col - 1],
                value => dp[row - 1][col - 1] && value == text_chars[col - 1],
            };
        }
    }

    dp[pattern_chars.len()][text_chars.len()]
}

#[cfg(test)]
mod tests {
    use super::Engine;
    use crate::{
        EngineOptions, Expiration, Paths, SetCondition, SetOptions, StorageEngine, StorageKey,
        StorageKeyring, WalSyncPolicy,
    };
    use command::Command;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(name: &str) -> PathBuf {
        let unique = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("veyra-engine-{name}-{unique}"));
        let _ = std::fs::remove_dir_all(&path);
        path
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

    fn engine() -> (Engine, PathBuf) {
        let root = temp_dir("root");
        let paths = Paths::from_data_dir(&root).unwrap();
        (
            Engine::from_paths_with_options(
                paths,
                EngineOptions {
                    wal_sync: WalSyncPolicy::Flush,
                    keyring: Some(test_keyring("test-data-key")),
                },
            )
            .unwrap(),
            root,
        )
    }

    #[test]
    fn supports_serious_v1_string_commands() {
        let (mut engine, root) = engine();

        engine.set("name".to_string(), "alice".to_string()).unwrap();
        assert_eq!(engine.get("name").unwrap(), Some("alice".to_string()));
        assert!(
            !engine
                .set_nx("name".to_string(), "bob".to_string())
                .unwrap()
        );
        assert!(
            engine
                .set_nx("city".to_string(), "paris".to_string())
                .unwrap()
        );
        assert_eq!(
            engine.mget(&["name".into(), "missing".into()]).unwrap(),
            vec![Some("alice".into()), None]
        );
        engine
            .mset(&[
                ("one".to_string(), "1".to_string()),
                ("two".to_string(), "2".to_string()),
            ])
            .unwrap();
        assert_eq!(engine.db_size().unwrap(), 4);
        assert_eq!(engine.incr("counter").unwrap(), 1);
        assert_eq!(engine.decr("counter").unwrap(), 0);
        assert!(engine.exists("city").unwrap());
        assert_eq!(
            engine
                .delete_many(&["city".into(), "missing".into()])
                .unwrap(),
            1
        );
        assert_eq!(engine.count().unwrap(), 4);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn supports_set_getdel_getex_and_scan_match() {
        let (mut engine, root) = engine();

        engine
            .set("user:1".to_string(), "alice".to_string())
            .unwrap();
        let outcome = engine
            .set_with_options(
                "user:1".to_string(),
                "bob".to_string(),
                SetOptions {
                    condition: Some(SetCondition::Xx),
                    expiration: Some(Expiration::Seconds(60)),
                    keep_ttl: false,
                },
            )
            .unwrap();
        assert!(outcome.applied);
        assert_eq!(outcome.previous, Some("alice".to_string()));

        assert_eq!(engine.get_del("user:1").unwrap(), Some("bob".to_string()));
        assert_eq!(engine.get("user:1").unwrap(), None);

        engine
            .set("user:2".to_string(), "carol".to_string())
            .unwrap();
        assert_eq!(
            engine
                .get_ex("user:2", Some(Expiration::Seconds(60)), false)
                .unwrap(),
            Some("carol".to_string())
        );
        assert!(engine.ttl("user:2").unwrap() > 0);

        engine
            .mset(&[
                ("user:alpha".to_string(), "1".to_string()),
                ("sys:beta".to_string(), "2".to_string()),
                ("user:gamma".to_string(), "3".to_string()),
            ])
            .unwrap();

        let page = engine.scan(0, Some("user:*"), Some(10)).unwrap();
        assert_eq!(page.keys, vec!["user:2", "user:alpha", "user:gamma"]);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn snapshot_persists_state_and_flushes_wal() {
        let (mut engine, root) = engine();

        engine.set("name".to_string(), "alice".to_string()).unwrap();
        engine.snapshot().unwrap();

        let wal_len = fs::metadata(root.join("wal.log")).unwrap().len();
        assert_eq!(wal_len, 0);
        assert!(root.join("snapshot.bin").exists());
        assert!(root.join("manifest.bin").exists());

        let paths = Paths::from_data_dir(&root).unwrap();
        let mut reloaded = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
            },
        )
        .unwrap();
        assert_eq!(reloaded.get("name").unwrap(), Some("alice".to_string()));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn logical_backup_restore_round_trip_replaces_state_atomically() {
        let root = temp_dir("logical-backup");
        let paths = Paths::from_data_dir(&root).unwrap();
        let mut source = Engine::from_paths_with_options(
            paths.clone(),
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("source-key")),
            },
        )
        .unwrap();
        source.set("a".to_string(), "1".to_string()).unwrap();
        source.expire("a", 60).unwrap();
        source.set("b".to_string(), "2".to_string()).unwrap();

        let dump = source.logical_backup().unwrap();

        let restore_root = temp_dir("logical-restore");
        let restore_paths = Paths::from_data_dir(&restore_root).unwrap();
        let mut restored = Engine::from_paths_with_options(
            restore_paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("restore-key")),
            },
        )
        .unwrap();
        restored
            .set("old".to_string(), "value".to_string())
            .unwrap();

        assert_eq!(restored.restore_logical_backup(&dump).unwrap(), 2);
        assert_eq!(restored.get("a").unwrap().as_deref(), Some("1"));
        assert_eq!(restored.get("b").unwrap().as_deref(), Some("2"));
        assert_eq!(restored.get("old").unwrap(), None);
        assert!(restored.ttl("a").unwrap() > 0);

        fs::remove_dir_all(root).ok();
        fs::remove_dir_all(restore_root).ok();
    }

    #[test]
    fn rejects_wrong_data_key_on_recovery() {
        let (mut engine, root) = engine();
        engine.set("name".to_string(), "alice".to_string()).unwrap();
        engine.snapshot().unwrap();

        let paths = Paths::from_data_dir(&root).unwrap();
        let reopened = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("wrong-data-key")),
            },
        );

        assert!(reopened.is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn ttl_and_persist_behave_consistently() {
        let (mut engine, root) = engine();

        engine
            .set("session".to_string(), "abc".to_string())
            .unwrap();
        assert_eq!(engine.ttl("session").unwrap(), -1);
        assert!(engine.expire("session", 60).unwrap());
        assert!(engine.ttl("session").unwrap() > 0);
        assert!(engine.persist("session").unwrap());
        assert_eq!(engine.ttl("session").unwrap(), -1);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn scan_is_cursor_based() {
        let (mut engine, root) = engine();

        engine
            .mset(&[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ])
            .unwrap();

        let first = engine.scan(0, None, Some(2)).unwrap();
        assert_eq!(first.keys, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(first.next_cursor, 2);

        let second = engine.scan(first.next_cursor, None, Some(2)).unwrap();
        assert_eq!(second.keys, vec!["c".to_string()]);
        assert_eq!(second.next_cursor, 0);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn execute_transaction_is_atomic_on_failure() {
        let (mut engine, root) = engine();
        let commands = vec![
            Command::Set {
                key: "counter".to_string(),
                value: "abc".to_string(),
                options: command::SetOptions::default(),
            },
            Command::Incr {
                key: "counter".to_string(),
            },
        ];

        assert!(engine.execute_transaction(&commands).is_err());
        assert_eq!(engine.get("counter").unwrap(), None);

        fs::remove_dir_all(root).ok();
    }
}
