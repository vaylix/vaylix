use crate::config::{EngineOptions, WalSyncPolicy};
use crate::engine::{
    EngineState, EngineStore, Expiration, LogicalBackup, LogicalBackupEntry, ScanPage,
    SetCondition, SetOptions, SetOutcome, StorageEngine, StoredValue, TransactionResult,
};
use crate::error::Result;
use crate::paths::Paths;
use crate::store::{
    Manifest, STORAGE_FORMAT_VERSION, WalEntry, WalOperation, WalReplayTarget, WalWriterHandle,
    create_active_segment, deserialize, inspect_wal, keyring, load, load_manifest,
    migrate_legacy_wal, prune_sealed_segments, replay, replay_until, save, save_keyring,
    save_manifest, seal_active, serialize,
};
use crate::{EngineMetadata, StorageKeyring};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use command::Command;
use crc32fast::hash;
use std::collections::BTreeMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_SCAN_COUNT: usize = 10;

fn next_value_version(previous: Option<&StoredValue>) -> u64 {
    previous
        .map(|entry| entry.version.saturating_add(1))
        .unwrap_or(1)
}

fn parse_integer_value(key: &str, value: &[u8]) -> Result<i64> {
    let text = std::str::from_utf8(value).map_err(|_| crate::EngineError::InvalidIntegerValue {
        key: key.to_string(),
        value: String::from_utf8_lossy(value).into_owned(),
    })?;
    text.parse::<i64>()
        .map_err(|_| crate::EngineError::InvalidIntegerValue {
            key: key.to_string(),
            value: text.to_string(),
        })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationSnapshot {
    pub state: EngineState,
    pub storage_format_version: u32,
    pub exported_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInspection {
    pub snapshot_present: bool,
    pub storage_format_version: u32,
    pub snapshot_size_bytes: u64,
    pub last_snapshot_sequence: u64,
    pub last_snapshot_at_ms: Option<u64>,
    pub wal_segment_count: usize,
    pub sealed_wal_segment_count: usize,
    pub active_wal_segment_count: usize,
    pub active_wal_start_sequence: u64,
    pub oldest_retained_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
    pub wal_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandBatchResult {
    pub result: TransactionResult,
    pub last_applied_sequence: u64,
}

#[derive(Default)]
struct BatchRollback {
    metadata: Option<EngineMetadata>,
    keys: std::collections::BTreeMap<String, Option<StoredValue>>,
    clear_snapshot: Option<EngineStore>,
}

impl BatchRollback {
    fn new(state: &EngineState) -> Self {
        Self {
            metadata: Some(state.metadata.clone()),
            keys: std::collections::BTreeMap::new(),
            clear_snapshot: None,
        }
    }

    fn remember_command(&mut self, state: &EngineState, command: &Command) {
        match command {
            Command::Set { key, .. }
            | Command::SetNx { key, .. }
            | Command::GetDel { key }
            | Command::GetEx { key, .. }
            | Command::Expire { key, .. }
            | Command::Persist { key }
            | Command::Incr { key }
            | Command::Decr { key } => self.remember_key(state, key),
            Command::MSet { entries } => {
                for (key, _) in entries {
                    self.remember_key(state, key);
                }
            }
            Command::Delete { keys } | Command::MGet { keys } => {
                for key in keys {
                    self.remember_key(state, key);
                }
            }
            Command::Rename {
                source,
                destination,
            }
            | Command::RenameNx {
                source,
                destination,
            } => {
                self.remember_key(state, source);
                self.remember_key(state, destination);
            }
            Command::Clear => self.remember_clear(state),
            _ => {}
        }
    }

    fn remember_key(&mut self, state: &EngineState, key: &str) {
        if self.clear_snapshot.is_some() || self.keys.contains_key(key) {
            return;
        }
        self.keys.insert(key.to_string(), state.raw_entry(key));
    }

    fn remember_clear(&mut self, state: &EngineState) {
        if self.clear_snapshot.is_none() {
            self.clear_snapshot = Some(state.store.clone());
            self.keys.clear();
        }
    }

    fn rollback(self, state: &mut EngineState) {
        if let Some(store) = self.clear_snapshot {
            state.store = store;
        } else {
            for (key, value) in self.keys {
                match value {
                    Some(value) => state.set_raw_entry(key, value),
                    None => {
                        state.remove_raw(&key);
                    }
                }
            }
        }
        if let Some(metadata) = self.metadata {
            state.metadata = metadata;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointInTimeTarget {
    Sequence(u64),
    TimestampMs(u64),
}

/// Core string-to-string storage engine backed by snapshots and a write-ahead log.
pub struct Engine {
    state: EngineState,
    paths: Paths,
    options: EngineOptions,
    wal_writer: WalWriterHandle,
    wal_entries: Vec<WalEntry>,
    wal_entry_identities: BTreeMap<u64, WalEntryIdentity>,
    current_consensus_term: u64,
    recovery_duration_ms: u64,
    wal_entries_replayed_total: u64,
    last_snapshot_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalEntryIdentity {
    term: u64,
    checksum: u32,
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
        let recovery_started_at_ms = now_millis();
        if paths.wal_path.exists() {
            return Err(crate::EngineError::StorageMigrationRequired {
                resource: "legacy wal layout",
            });
        }

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

        let replay = replay(&paths.wal_dir, options.keyring.as_ref())?;
        let wal_entries = replay.entries;
        let wal_entry_identities = build_wal_entry_identities(&wal_entries)?;
        let wal_entries_replayed_total = wal_entries.len() as u64;
        for entry in &wal_entries {
            state.apply_entry(entry)?;
        }
        let wal_writer = WalWriterHandle::open(
            &paths.wal_dir,
            options.wal_sync,
            options.wal_segment_size_bytes,
            state.metadata.last_applied_sequence.saturating_add(1),
        )?;

        Ok(Self {
            state,
            paths,
            options,
            wal_writer,
            wal_entries,
            wal_entry_identities,
            current_consensus_term: 0,
            recovery_duration_ms: now_millis().saturating_sub(recovery_started_at_ms),
            wal_entries_replayed_total,
            last_snapshot_duration_ms: None,
        })
    }

    pub fn inspect_storage(
        paths: &Paths,
        keyring: Option<&StorageKeyring>,
    ) -> Result<StorageInspection> {
        let loaded = load(&paths.snapshot_path, keyring)?;
        let manifest = load_manifest(&paths.manifest_path)?;
        let wal_report = inspect_wal(&paths.wal_dir)?;
        Ok(build_storage_inspection(
            loaded.as_ref(),
            manifest.as_ref(),
            &wal_report,
        ))
    }

    pub fn verify_storage(paths: &Paths, options: EngineOptions) -> Result<StorageInspection> {
        let engine = Self::from_paths_with_options(paths.clone(), options)?;
        Ok(engine.storage_inspection())
    }

    pub fn replication_snapshot(&self) -> ReplicationSnapshot {
        ReplicationSnapshot {
            state: self.state.clone(),
            storage_format_version: STORAGE_FORMAT_VERSION,
            exported_at_ms: now_millis(),
        }
    }

    pub fn wal_entries_since(&self, after_sequence: u64, limit: usize) -> Result<Vec<WalEntry>> {
        self.wal_entries_since_capped(after_sequence, limit, None)
    }

    pub fn wal_entry_checksum(&self, sequence: u64) -> Result<Option<u32>> {
        if sequence == 0 {
            return Ok(None);
        }
        Ok(self
            .wal_entry_identities
            .get(&sequence)
            .map(|identity| identity.checksum))
    }

    pub fn wal_entry_term(&self, sequence: u64) -> Result<Option<u64>> {
        if sequence == 0 {
            return Ok(None);
        }
        Ok(self
            .wal_entry_identities
            .get(&sequence)
            .map(|identity| identity.term))
    }

    pub fn last_applied_sequence(&self) -> u64 {
        self.state.metadata.last_applied_sequence
    }

    pub fn wal_sync_policy(&self) -> WalSyncPolicy {
        self.options.wal_sync
    }

    pub fn set_consensus_term(&mut self, term: u64) {
        self.current_consensus_term = term;
    }

    pub fn wal_entries_since_capped(
        &self,
        after_sequence: u64,
        limit: usize,
        max_sequence: Option<u64>,
    ) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        for entry in self.wal_entries.iter().filter(|entry| {
            entry.sequence > after_sequence && max_sequence.is_none_or(|max| entry.sequence <= max)
        }) {
            push_unique_wal_entry(&mut entries, entry.clone())?;
        }
        if limit > 0 && entries.len() > limit {
            entries.truncate(limit);
        }
        Ok(entries)
    }

    pub fn apply_replication_snapshot(&mut self, snapshot: ReplicationSnapshot) -> Result<()> {
        if snapshot.storage_format_version != STORAGE_FORMAT_VERSION {
            return Err(crate::EngineError::UnsupportedStorageFormat {
                resource: "replication snapshot",
            });
        }

        self.state = snapshot.state;
        let sequence = self.state.metadata.last_applied_sequence;
        let persisted_at_ms = now_millis();
        self.state.metadata.last_snapshot_at_ms = Some(persisted_at_ms);
        self.state.metadata.updated_at_ms = persisted_at_ms;

        let serialized = serialize(&self.state)?;
        save(
            &serialized,
            &self.paths.snapshot_path,
            &self.paths.snapshot_tmp_path,
            self.options.keyring.as_ref(),
        )?;
        self.wal_writer.close_active()?;
        if self.paths.wal_dir.exists() {
            fs::remove_dir_all(&self.paths.wal_dir)?;
        }
        create_active_segment(&self.paths.wal_dir, sequence.saturating_add(1))?;
        self.reset_wal_writer(sequence.saturating_add(1))?;
        self.wal_entries.clear();
        self.wal_entry_identities.clear();
        save_manifest(
            &Manifest {
                storage_format_version: STORAGE_FORMAT_VERSION,
                engine_version: self.state.metadata.version,
                last_snapshot_sequence: sequence,
                last_snapshot_at_ms: persisted_at_ms,
                snapshot_size_bytes: serialized.len() as u64,
                snapshot_checksum: hash(&serialized),
                active_wal_start_sequence: sequence.saturating_add(1),
                oldest_retained_sequence: sequence.saturating_add(1),
            },
            &self.paths.manifest_path,
            &self.paths.manifest_tmp_path,
        )?;
        Ok(())
    }

    pub fn apply_replication_entries(&mut self, entries: &[WalEntry]) -> Result<u64> {
        let mut last_applied = self.state.metadata.last_applied_sequence;
        let mut staged = self.state.clone();
        for entry in entries {
            let expected = last_applied.saturating_add(1);
            if entry.sequence != expected {
                return Err(crate::EngineError::InvalidStorageOperation(format!(
                    "replication WAL sequence gap: expected {expected}, got {}",
                    entry.sequence
                )));
            }
            staged.apply_entry(entry)?;
            last_applied = entry.sequence;
        }
        let identities = build_wal_entry_identities(entries)?;
        self.wal_writer
            .append_batch(entries, self.options.keyring.as_ref())?;
        self.state = staged;
        self.wal_entries.extend(entries.iter().cloned());
        self.wal_entry_identities.extend(identities);
        Ok(last_applied)
    }

    pub fn replace_replication_suffix(
        &mut self,
        prefix_sequence: u64,
        entries: &[WalEntry],
    ) -> Result<u64> {
        let snapshot_state = match load(&self.paths.snapshot_path, self.options.keyring.as_ref())? {
            Some(bytes) => deserialize(&bytes)?,
            None => EngineState::new(),
        };
        let baseline_sequence = snapshot_state.metadata.last_applied_sequence;
        if prefix_sequence < baseline_sequence {
            return Err(crate::EngineError::InvalidStorageOperation(format!(
                "replication prefix {prefix_sequence} is older than snapshot baseline {baseline_sequence}"
            )));
        }

        let mut retained = Vec::new();
        for entry in self
            .wal_entries
            .iter()
            .filter(|entry| entry.sequence <= prefix_sequence)
        {
            push_unique_wal_entry(&mut retained, entry.clone())?;
        }

        if let Some(last_retained) = retained.last()
            && last_retained.sequence != prefix_sequence
        {
            return Err(crate::EngineError::InvalidStorageOperation(format!(
                "replication prefix gap: expected retained sequence {prefix_sequence}, got {}",
                last_retained.sequence
            )));
        }
        if retained.is_empty() && prefix_sequence != baseline_sequence {
            return Err(crate::EngineError::InvalidStorageOperation(format!(
                "replication prefix gap: expected baseline {baseline_sequence}, got {prefix_sequence}"
            )));
        }

        let mut replacement_entries = Vec::new();
        for entry in entries {
            if entry.sequence <= prefix_sequence {
                if entry.sequence > baseline_sequence {
                    let Some(local_entry) = retained
                        .iter()
                        .find(|candidate| candidate.sequence == entry.sequence)
                    else {
                        return Err(crate::EngineError::InvalidStorageOperation(format!(
                            "replication replacement overlap missing retained sequence {}",
                            entry.sequence
                        )));
                    };
                    if local_entry.term != entry.term
                        || local_entry.checksum()? != entry.checksum()?
                    {
                        return Err(crate::EngineError::InvalidStorageOperation(format!(
                            "replication replacement overlap mismatch at sequence {}",
                            entry.sequence
                        )));
                    }
                }
                continue;
            }
            push_unique_wal_entry(&mut replacement_entries, entry.clone())?;
        }

        for (idx, entry) in replacement_entries.iter().enumerate() {
            let expected = prefix_sequence.saturating_add(idx as u64).saturating_add(1);
            if entry.sequence != expected {
                return Err(crate::EngineError::InvalidStorageOperation(format!(
                    "replication replacement gap: expected {expected}, got {}",
                    entry.sequence
                )));
            }
        }
        retained.extend(replacement_entries);

        let mut rebuilt_state = snapshot_state;
        for entry in &retained {
            rebuilt_state.apply_entry(entry)?;
        }

        self.wal_writer.close_active()?;
        if self.paths.wal_dir.exists() {
            fs::remove_dir_all(&self.paths.wal_dir)?;
        }
        if retained.is_empty() {
            create_active_segment(&self.paths.wal_dir, baseline_sequence.saturating_add(1))?;
        } else {
            crate::store::write_entries(
                &retained,
                &self.paths.wal_dir,
                self.options.wal_sync,
                self.options.keyring.as_ref(),
                self.options.wal_segment_size_bytes,
            )?;
        }
        let next_sequence = retained
            .last()
            .map(|entry| entry.sequence.saturating_add(1))
            .unwrap_or_else(|| baseline_sequence.saturating_add(1));
        self.reset_wal_writer(next_sequence)?;

        self.state = rebuilt_state;
        self.wal_entries = retained;
        self.wal_entry_identities = build_wal_entry_identities(&self.wal_entries)?;
        Ok(self.state.metadata.last_applied_sequence)
    }

    pub fn migrate_storage(paths: &Paths, options: &EngineOptions) -> Result<StorageInspection> {
        if paths.wal_path.exists() {
            migrate_legacy_wal(
                &paths.wal_path,
                &paths.wal_dir,
                options.wal_sync,
                options.keyring.as_ref(),
                options.wal_segment_size_bytes,
            )?;
        }
        if inspect_wal(&paths.wal_dir)?.active_segment_count == 0 {
            let start_sequence = load_manifest(&paths.manifest_path)?
                .map(|manifest| manifest.last_snapshot_sequence.saturating_add(1))
                .unwrap_or(1);
            create_active_segment(&paths.wal_dir, start_sequence)?;
        }
        Self::inspect_storage(paths, options.keyring.as_ref())
    }

    pub fn restore_to_point(
        source_paths: &Paths,
        target_paths: &Paths,
        options: EngineOptions,
        target: PointInTimeTarget,
    ) -> Result<StorageInspection> {
        if source_paths.data_dir == target_paths.data_dir {
            return Err(crate::EngineError::InvalidStorageOperation(
                "source and target data directories must differ".to_string(),
            ));
        }
        if source_paths.wal_path.exists() {
            return Err(crate::EngineError::StorageMigrationRequired {
                resource: "legacy wal layout",
            });
        }

        let loaded = load(&source_paths.snapshot_path, options.keyring.as_ref())?;
        let mut state = match &loaded {
            Some(bytes) => deserialize(bytes)?,
            None => EngineState::new(),
        };
        let manifest = load_manifest(&source_paths.manifest_path)?;
        let snapshot_sequence = manifest
            .as_ref()
            .map(|value| value.last_snapshot_sequence)
            .unwrap_or(0);
        let snapshot_time_ms = manifest
            .as_ref()
            .map(|value| value.last_snapshot_at_ms)
            .unwrap_or(0);

        if let Some(manifest) = &manifest {
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

        match target {
            PointInTimeTarget::Sequence(sequence) if sequence < snapshot_sequence => {
                return Err(crate::EngineError::RestorePointUnavailable(format!(
                    "sequence {sequence} is older than snapshot baseline {snapshot_sequence}"
                )));
            }
            PointInTimeTarget::TimestampMs(timestamp_ms) if timestamp_ms < snapshot_time_ms => {
                return Err(crate::EngineError::RestorePointUnavailable(format!(
                    "timestamp {timestamp_ms} is older than snapshot baseline {snapshot_time_ms}"
                )));
            }
            _ => {}
        }

        let replay_target = match target {
            PointInTimeTarget::Sequence(sequence) => WalReplayTarget::Sequence(sequence),
            PointInTimeTarget::TimestampMs(timestamp_ms) => {
                WalReplayTarget::TimestampMs(timestamp_ms)
            }
        };
        let replay = replay_until(
            &source_paths.wal_dir,
            options.keyring.as_ref(),
            replay_target,
        )?;
        match target {
            PointInTimeTarget::Sequence(sequence)
                if sequence > replay.newest_sequence.unwrap_or(snapshot_sequence) =>
            {
                return Err(crate::EngineError::RestorePointUnavailable(format!(
                    "sequence {sequence} is outside retained WAL history"
                )));
            }
            _ => {}
        }

        for entry in replay.entries {
            state.apply_entry(&entry)?;
        }

        if target_paths.data_dir.exists() {
            for path in [
                &target_paths.snapshot_path,
                &target_paths.manifest_path,
                &target_paths.keyring_path,
                &target_paths.maintenance_path,
                &target_paths.auth_path,
            ] {
                fs::remove_file(path).ok();
            }
            if target_paths.wal_dir.exists() {
                fs::remove_dir_all(&target_paths.wal_dir)?;
            }
        }

        if let Some(keyring) = options.keyring.as_ref() {
            save_keyring(
                keyring,
                &target_paths.keyring_path,
                &target_paths.keyring_tmp_path,
            )?;
        }

        let restored_at_ms = now_millis();
        let sequence = state.metadata.last_applied_sequence;
        state.mark_snapshot(restored_at_ms, sequence);
        let serialized = serialize(&state)?;
        save(
            &serialized,
            &target_paths.snapshot_path,
            &target_paths.snapshot_tmp_path,
            options.keyring.as_ref(),
        )?;
        create_active_segment(&target_paths.wal_dir, sequence.saturating_add(1))?;
        let manifest = Manifest {
            storage_format_version: STORAGE_FORMAT_VERSION,
            engine_version: state.metadata.version,
            last_snapshot_sequence: sequence,
            last_snapshot_at_ms: restored_at_ms,
            snapshot_size_bytes: serialized.len() as u64,
            snapshot_checksum: hash(&serialized),
            active_wal_start_sequence: sequence.saturating_add(1),
            oldest_retained_sequence: sequence.saturating_add(1),
        };
        save_manifest(
            &manifest,
            &target_paths.manifest_path,
            &target_paths.manifest_tmp_path,
        )?;

        Self::inspect_storage(target_paths, options.keyring.as_ref())
    }

    /// Returns immutable access to the in-memory state for diagnostics and tests.
    pub fn state(&self) -> &EngineState {
        &self.state
    }

    fn storage_inspection(&self) -> StorageInspection {
        let snapshot_bytes = fs::read(&self.paths.snapshot_path).ok();
        let manifest = load_manifest(&self.paths.manifest_path).ok().flatten();
        let wal_report = inspect_wal(&self.paths.wal_dir).unwrap_or(crate::WalSegmentReport {
            segment_count: 0,
            sealed_segment_count: 0,
            active_segment_count: 0,
            oldest_retained_sequence: None,
            active_start_sequence: 1,
            newest_sequence: None,
            total_size_bytes: 0,
        });
        build_storage_inspection(snapshot_bytes.as_ref(), manifest.as_ref(), &wal_report)
    }

    fn append_and_apply(&mut self, operations: Vec<WalOperation>) -> Result<()> {
        let entry = self.next_entry(operations);
        let identity = wal_entry_identity(&entry)?;
        self.wal_writer
            .append_batch(std::slice::from_ref(&entry), self.options.keyring.as_ref())?;
        self.state.apply_entry(&entry)?;
        self.wal_entry_identities.insert(entry.sequence, identity);
        self.wal_entries.push(entry);
        Ok(())
    }

    fn reset_wal_writer(&mut self, start_sequence: u64) -> Result<()> {
        self.wal_writer.reset(start_sequence)
    }

    pub fn append_noop(&mut self) -> Result<()> {
        self.append_and_apply(Vec::new())
    }

    fn next_entry(&self, operations: Vec<WalOperation>) -> WalEntry {
        WalEntry::new(
            self.state.metadata.last_applied_sequence + 1,
            self.current_consensus_term,
            now_millis(),
            operations,
        )
    }

    fn now(&self) -> u64 {
        now_millis()
    }

    fn info_entries(&self, metadata: &EngineMetadata, key_count: usize) -> Vec<(String, String)> {
        let wal_report = inspect_wal(&self.paths.wal_dir).ok();
        let wal_size = wal_report
            .as_ref()
            .map(|report| report.total_size_bytes)
            .unwrap_or(0);
        let wal_segment_count = wal_report
            .as_ref()
            .map(|report| report.segment_count)
            .unwrap_or(0);
        let oldest_retained_sequence = wal_report
            .as_ref()
            .and_then(|report| report.oldest_retained_sequence)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string());

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
                "wal_segment_count".to_string(),
                wal_segment_count.to_string(),
            ),
            (
                "oldest_retained_sequence".to_string(),
                oldest_retained_sequence,
            ),
            (
                "wal_sync_policy".to_string(),
                self.options.wal_sync.as_str().to_string(),
            ),
            (
                "wal_entries_replayed_total".to_string(),
                self.wal_entries_replayed_total.to_string(),
            ),
            (
                "recovery_duration_ms".to_string(),
                self.recovery_duration_ms.to_string(),
            ),
            (
                "last_snapshot_duration_ms".to_string(),
                self.last_snapshot_duration_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "wal_segment_size_bytes".to_string(),
                self.options.wal_segment_size_bytes.to_string(),
            ),
            (
                "wal_retain_segments".to_string(),
                self.options.wal_retain_segments.to_string(),
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

    fn maybe_get_existing_value(&mut self, key: &str) -> Option<Vec<u8>> {
        self.state.purge_expired(self.now());
        self.state.raw_value(key)
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
            if_version: options.if_version,
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
            results.push(Self::evaluate_transaction_command(
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
        let identity = wal_entry_identity(&entry)?;
        self.wal_writer
            .append_batch(std::slice::from_ref(&entry), self.options.keyring.as_ref())?;
        self.state.apply_entry(&entry)?;
        self.wal_entry_identities.insert(entry.sequence, identity);
        self.wal_entries.push(entry);

        Ok(results)
    }

    /// Executes independent data commands as one WAL flush group.
    ///
    /// Each command that produces mutations still receives its own WAL sequence,
    /// preserving replication identity and command-level commit accounting.
    pub fn execute_command_batch(
        &mut self,
        commands: &[Command],
    ) -> Result<Vec<CommandBatchResult>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let mut rollback = BatchRollback::new(&self.state);
        let mut entries = Vec::new();
        let mut results = Vec::with_capacity(commands.len());
        let mut next_sequence = self.state.metadata.last_applied_sequence.saturating_add(1);
        let mut visible_sequence = self.state.metadata.last_applied_sequence;

        for command in commands {
            rollback.remember_command(&self.state, command);
            let mut operations = Vec::new();
            let result = match Self::evaluate_transaction_command(
                &mut self.state,
                now_ms,
                &mut operations,
                command,
            ) {
                Ok(result) => result,
                Err(err) => {
                    rollback.rollback(&mut self.state);
                    return Err(err);
                }
            };
            if !operations.is_empty() {
                let entry = WalEntry::new(
                    next_sequence,
                    self.current_consensus_term,
                    now_ms,
                    operations,
                );
                self.state.metadata.last_applied_sequence = entry.sequence;
                self.state.metadata.updated_at_ms = entry.created_at_ms;
                visible_sequence = next_sequence;
                next_sequence = next_sequence.saturating_add(1);
                entries.push(entry);
            }
            results.push(CommandBatchResult {
                result,
                last_applied_sequence: visible_sequence,
            });
        }

        if !entries.is_empty() {
            let identities = build_wal_entry_identities(&entries)?;
            if let Err(err) = self
                .wal_writer
                .append_batch(&entries, self.options.keyring.as_ref())
            {
                rollback.rollback(&mut self.state);
                return Err(err);
            }
            self.wal_entry_identities.extend(identities);
            self.wal_entries.extend(entries);
        }
        Ok(results)
    }

    fn evaluate_transaction_command(
        state: &mut EngineState,
        now_ms: u64,
        operations: &mut Vec<WalOperation>,
        command: &Command,
    ) -> Result<TransactionResult> {
        state.purge_expired(now_ms);

        match command {
            Command::Ping { message } => Ok(TransactionResult::Value(
                message
                    .clone()
                    .unwrap_or_else(|| "PONG".to_string())
                    .into_bytes(),
            )),
            Command::Get { key } => Ok(match state.raw_value(key) {
                Some(value) => TransactionResult::Value(value),
                None => TransactionResult::NotFound,
            }),
            Command::GetDel { key } => {
                let entry = state.remove_raw(key);
                match entry {
                    Some(entry) => {
                        operations.push(WalOperation::Delete { key: key.clone() });
                        Ok(TransactionResult::Value(entry.value))
                    }
                    None => Ok(TransactionResult::NotFound),
                }
            }
            Command::GetEx {
                key,
                expiration,
                persist,
            } => {
                let Some(entry) = state.raw_entry(key) else {
                    return Ok(TransactionResult::NotFound);
                };
                if let Some(expiration) = Self::map_command_expiration(*expiration) {
                    let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
                    if expires_at_ms <= now_ms {
                        state.remove_raw(key);
                        operations.push(WalOperation::Delete { key: key.clone() });
                    } else {
                        state.set_expiration_if_present(key, expires_at_ms);
                        operations.push(WalOperation::Expire {
                            key: key.clone(),
                            expires_at_ms,
                        });
                    }
                } else if *persist && state.clear_expiration(key) {
                    operations.push(WalOperation::Persist { key: key.clone() });
                }
                Ok(TransactionResult::Value(entry.value))
            }
            Command::Set {
                key,
                value,
                options,
            } => {
                let previous_entry = state.raw_entry(key);
                let previous = previous_entry.as_ref().map(|entry| entry.value.clone());
                let previous_expiration = previous_entry
                    .as_ref()
                    .and_then(|entry| entry.expires_at_ms);
                let previous_version = previous_entry.as_ref().map(|entry| entry.version);
                let mapped = Self::map_command_set_options(options.clone());
                let allowed = match mapped.condition {
                    Some(SetCondition::Nx) => previous.is_none(),
                    Some(SetCondition::Xx) => previous.is_some(),
                    None => true,
                } && mapped
                    .if_version
                    .map(|expected| previous_version == Some(expected))
                    .unwrap_or(true);
                if !allowed {
                    return Ok(if options.return_previous {
                        previous
                            .map(TransactionResult::Value)
                            .unwrap_or(TransactionResult::NotFound)
                    } else if options.condition.is_some() || options.if_version.is_some() {
                        TransactionResult::Boolean(false)
                    } else {
                        TransactionResult::Ok
                    });
                }

                let version = next_value_version(previous_entry.as_ref());
                state.set_raw_value_with_version(key.clone(), value.clone(), version);
                operations.push(WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                    version,
                });

                if let Some(expiration) = mapped.expiration {
                    let expires_at_ms = Self::resolve_expiration(now_ms, expiration);
                    if expires_at_ms <= now_ms {
                        state.remove_raw(key);
                        operations.push(WalOperation::Delete { key: key.clone() });
                    } else {
                        state.set_expiration_if_present(key, expires_at_ms);
                        operations.push(WalOperation::Expire {
                            key: key.clone(),
                            expires_at_ms,
                        });
                    }
                } else if mapped.keep_ttl
                    && let Some(expires_at_ms) = previous_expiration
                {
                    state.set_expiration_if_present(key, expires_at_ms);
                    operations.push(WalOperation::Expire {
                        key: key.clone(),
                        expires_at_ms,
                    });
                }

                Ok(if options.return_previous {
                    previous
                        .map(TransactionResult::Value)
                        .unwrap_or(TransactionResult::NotFound)
                } else if options.condition.is_some() || options.if_version.is_some() {
                    TransactionResult::Boolean(true)
                } else {
                    TransactionResult::Ok
                })
            }
            Command::SetNx { key, value } => {
                if state.raw_contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                let version = next_value_version(None);
                state.set_raw_value_with_version(key.clone(), value.clone(), version);
                operations.push(WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                    version,
                });
                Ok(TransactionResult::Boolean(true))
            }
            Command::MGet { keys } => Ok(TransactionResult::Strings(
                keys.iter().map(|key| state.raw_value(key)).collect(),
            )),
            Command::MSet { entries } => {
                for (key, value) in entries {
                    let previous_entry = state.raw_entry(key);
                    let version = next_value_version(previous_entry.as_ref());
                    state.set_raw_value_with_version(key.clone(), value.clone(), version);
                    operations.push(WalOperation::Set {
                        key: key.clone(),
                        value: value.clone(),
                        version,
                    });
                }
                Ok(TransactionResult::Ok)
            }
            Command::Delete { keys } => {
                let mut removed = 0_u64;
                for key in keys {
                    if state.remove_raw(key).is_some() {
                        removed += 1;
                        operations.push(WalOperation::Delete { key: key.clone() });
                    }
                }
                Ok(TransactionResult::Count(removed))
            }
            Command::Exists { key } => Ok(TransactionResult::Boolean(state.raw_contains_key(key))),
            Command::Incr { key } => {
                let current = state.raw_value(key).unwrap_or_else(|| b"0".to_vec());
                let parsed = parse_integer_value(key, &current)?;
                let next = parsed
                    .checked_add(1)
                    .ok_or_else(|| crate::EngineError::NumericOverflow { key: key.clone() })?;
                let version = next_value_version(state.raw_entry(key).as_ref());
                state.set_raw_value_with_version(
                    key.clone(),
                    next.to_string().into_bytes(),
                    version,
                );
                operations.push(WalOperation::CheckInteger {
                    key: key.clone(),
                    delta: 1,
                });
                Ok(TransactionResult::Integer(next))
            }
            Command::Decr { key } => {
                let current = state.raw_value(key).unwrap_or_else(|| b"0".to_vec());
                let parsed = parse_integer_value(key, &current)?;
                let next = parsed
                    .checked_sub(1)
                    .ok_or_else(|| crate::EngineError::NumericOverflow { key: key.clone() })?;
                let version = next_value_version(state.raw_entry(key).as_ref());
                state.set_raw_value_with_version(
                    key.clone(),
                    next.to_string().into_bytes(),
                    version,
                );
                operations.push(WalOperation::CheckInteger {
                    key: key.clone(),
                    delta: -1,
                });
                Ok(TransactionResult::Integer(next))
            }
            Command::Expire { key, seconds } => {
                if !state.raw_contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                if *seconds == 0 {
                    state.remove_raw(key);
                    operations.push(WalOperation::Delete { key: key.clone() });
                    return Ok(TransactionResult::Boolean(true));
                }
                let expires_at_ms = now_ms.saturating_add(seconds.saturating_mul(1_000));
                state.set_expiration_if_present(key, expires_at_ms);
                operations.push(WalOperation::Expire {
                    key: key.clone(),
                    expires_at_ms,
                });
                Ok(TransactionResult::Boolean(true))
            }
            Command::Ttl { key } => Ok(TransactionResult::Integer(state.ttl_for(key, now_ms))),
            Command::Persist { key } => {
                let removed = state.raw_contains_key(key) && state.clear_expiration(key);
                if removed {
                    operations.push(WalOperation::Persist { key: key.clone() });
                }
                Ok(TransactionResult::Boolean(removed))
            }
            Command::Rename {
                source,
                destination,
            } => {
                let Some(entry) = state.remove_raw(source) else {
                    return Ok(TransactionResult::Boolean(false));
                };
                let version = next_value_version(state.raw_entry(destination).as_ref());
                state.set_raw_value_with_version(destination.clone(), entry.value.clone(), version);
                operations.push(WalOperation::Delete {
                    key: source.clone(),
                });
                operations.push(WalOperation::Set {
                    key: destination.clone(),
                    value: entry.value,
                    version,
                });
                if let Some(expires_at_ms) = entry.expires_at_ms {
                    state.set_expiration_if_present(destination, expires_at_ms);
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
                if state.raw_contains_key(destination) {
                    return Ok(TransactionResult::Boolean(false));
                }
                Self::evaluate_transaction_command(
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
                Ok(TransactionResult::Count(state.key_count() as u64))
            }
            Command::List => Ok(TransactionResult::Entries(state.live_entries(now_ms))),
            Command::Clear => {
                if state.is_empty() {
                    return Ok(TransactionResult::Ok);
                }
                state.clear_all();
                operations.push(WalOperation::Clear);
                Ok(TransactionResult::Ok)
            }
            Command::Info
            | Command::Metrics
            | Command::MetricsProm
            | Command::Save
            | Command::Snapshot
            | Command::Backup
            | Command::BackupTo { .. }
            | Command::BackupVerify { .. }
            | Command::BackupVerifyFrom { .. }
            | Command::Restore { .. }
            | Command::RestoreFrom { .. }
            | Command::RestoreCheck { .. }
            | Command::RestoreCheckFrom { .. }
            | Command::AlterUserPassword { .. }
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
            | Command::ShowGrants
            | Command::ShowGrantsForUser { .. }
            | Command::ShowGrantsForRole { .. }
            | Command::WhoAmI
            | Command::MaintenanceOn
            | Command::MaintenanceOff
            | Command::MaintenanceStatus
            | Command::Health
            | Command::ShowCluster
            | Command::ClusterJoin { .. }
            | Command::ClusterRemove { .. }
            | Command::ShowReplication
            | Command::PromoteFollower
            | Command::PauseReplication
            | Command::ResumeReplication
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

fn push_unique_wal_entry(entries: &mut Vec<WalEntry>, entry: WalEntry) -> Result<()> {
    if let Some(last) = entries.last() {
        if entry.sequence < last.sequence {
            return Err(crate::EngineError::InvalidStorageOperation(format!(
                "non-monotonic in-memory WAL cache: previous {}, got {}",
                last.sequence, entry.sequence
            )));
        }
        if entry.sequence == last.sequence {
            if entry.term != last.term || entry.checksum()? != last.checksum()? {
                return Err(crate::EngineError::InvalidStorageOperation(format!(
                    "conflicting duplicate in-memory WAL entry at sequence {}",
                    entry.sequence
                )));
            }
            return Ok(());
        }
    }
    entries.push(entry);
    Ok(())
}

fn wal_entry_identity(entry: &WalEntry) -> Result<WalEntryIdentity> {
    Ok(WalEntryIdentity {
        term: entry.term,
        checksum: entry.checksum()?,
    })
}

fn build_wal_entry_identities(entries: &[WalEntry]) -> Result<BTreeMap<u64, WalEntryIdentity>> {
    let mut identities = BTreeMap::new();
    for entry in entries {
        let identity = wal_entry_identity(entry)?;
        if let Some(previous) = identities.insert(entry.sequence, identity)
            && previous != identity
        {
            return Err(crate::EngineError::InvalidStorageOperation(format!(
                "conflicting duplicate WAL identity at sequence {}",
                entry.sequence
            )));
        }
    }
    Ok(identities)
}

impl StorageEngine for Engine {
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.state.get_live(key, self.now()))
    }

    fn set_with_options(
        &mut self,
        key: String,
        value: Vec<u8>,
        options: SetOptions,
    ) -> Result<SetOutcome> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let previous_entry = self.state.raw_entry(&key);
        let previous = previous_entry.as_ref().map(|entry| entry.value.clone());
        let previous_expiration = previous_entry
            .as_ref()
            .and_then(|entry| entry.expires_at_ms);
        let previous_version = previous_entry.as_ref().map(|entry| entry.version);

        let allowed = match options.condition {
            Some(SetCondition::Nx) => previous.is_none(),
            Some(SetCondition::Xx) => previous.is_some(),
            None => true,
        } && options
            .if_version
            .map(|expected| previous_version == Some(expected))
            .unwrap_or(true);

        if !allowed {
            return Ok(SetOutcome {
                applied: false,
                previous,
                version: previous_version,
            });
        }

        let version = next_value_version(previous_entry.as_ref());

        let mut operations = vec![WalOperation::Set {
            key: key.clone(),
            value,
            version,
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
            version: Some(version),
        })
    }

    fn get_del(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
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
    ) -> Result<Option<Vec<u8>>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        let previous = self.state.raw_value(key);
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
        } else if persist && self.state.raw_expiration(key).is_some() {
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

    fn mget(&mut self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);

        Ok(keys.iter().map(|key| self.state.raw_value(key)).collect())
    }

    fn mset(&mut self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        let mut next_versions = BTreeMap::<String, u64>::new();
        let operations = entries
            .iter()
            .map(|(key, value)| {
                let version = next_versions
                    .remove(key)
                    .unwrap_or_else(|| next_value_version(self.state.raw_entry(key).as_ref()));
                next_versions.insert(key.clone(), version.saturating_add(1));
                WalOperation::Set {
                    key: key.clone(),
                    value: value.clone(),
                    version,
                }
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
            .filter(|key| self.state.raw_contains_key(key.as_str()))
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
        let current = self.state.raw_value(key).unwrap_or_else(|| b"0".to_vec());
        let parsed = parse_integer_value(key, &current)?;
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
        let current = self.state.raw_value(key).unwrap_or_else(|| b"0".to_vec());
        let parsed = parse_integer_value(key, &current)?;
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

        if !self.state.raw_contains_key(key) {
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

        if !self.state.raw_contains_key(key) || self.state.raw_expiration(key).is_none() {
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

        let Some(entry) = self.state.raw_entry(source) else {
            return Ok(false);
        };

        let mut operations = vec![
            WalOperation::Delete {
                key: source.to_string(),
            },
            WalOperation::Set {
                key: destination.clone(),
                value: entry.value,
                version: next_value_version(self.state.raw_entry(&destination).as_ref()),
            },
        ];
        if let Some(expires_at_ms) = entry.expires_at_ms {
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
        if self.state.raw_contains_key(&destination) {
            return Ok(false);
        }
        self.rename(source, destination)
    }

    fn db_size(&mut self) -> Result<usize> {
        self.state.purge_expired(self.now());
        Ok(self.state.key_count())
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

    fn list(&mut self) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self.state.live_entries(self.now()))
    }

    fn info(&mut self) -> Result<Vec<(String, String)>> {
        self.state.purge_expired(self.now());
        Ok(self.info_entries(&self.state.metadata, self.state.key_count()))
    }

    fn sweep_expired(&mut self) -> Result<usize> {
        Ok(self.state.purge_expired(self.now()))
    }

    fn clear(&mut self) -> Result<()> {
        if self.state.is_empty() {
            return Ok(());
        }

        self.append_and_apply(vec![WalOperation::Clear])
    }

    fn snapshot(&mut self) -> Result<()> {
        let snapshot_started_at = now_millis();
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

        self.wal_writer.close_active()?;
        let _ = seal_active(&self.paths.wal_dir, self.options.keyring.as_ref())?;
        let active_wal_start_sequence = sequence.saturating_add(1);
        create_active_segment(&self.paths.wal_dir, active_wal_start_sequence)?;
        self.wal_writer.reset(active_wal_start_sequence)?;
        let _ = prune_sealed_segments(&self.paths.wal_dir, self.options.wal_retain_segments)?;
        let wal_report = inspect_wal(&self.paths.wal_dir).ok();
        let oldest_retained_sequence = wal_report
            .as_ref()
            .and_then(|report| report.oldest_retained_sequence)
            .unwrap_or(active_wal_start_sequence);

        let manifest = Manifest {
            storage_format_version: STORAGE_FORMAT_VERSION,
            engine_version: durable_state.metadata.version,
            last_snapshot_sequence: sequence,
            last_snapshot_at_ms: snapshot_started_at_ms,
            snapshot_size_bytes: serialized.len() as u64,
            snapshot_checksum: hash(&serialized),
            active_wal_start_sequence,
            oldest_retained_sequence,
        };
        save_manifest(
            &manifest,
            &self.paths.manifest_path,
            &self.paths.manifest_tmp_path,
        )?;

        self.state = durable_state;
        self.wal_entries = replay(&self.paths.wal_dir, self.options.keyring.as_ref())?.entries;
        self.wal_entry_identities = build_wal_entry_identities(&self.wal_entries)?;
        self.last_snapshot_duration_ms = Some(now_millis().saturating_sub(snapshot_started_at));

        Ok(())
    }

    fn logical_backup(&mut self) -> Result<String> {
        let now_ms = self.now();
        self.state.purge_expired(now_ms);
        let entries = self
            .state
            .store
            .to_entries()
            .into_iter()
            .map(|(key, entry)| LogicalBackupEntry {
                expires_at_ms: entry.expires_at_ms,
                key,
                value_base64: BASE64.encode(entry.value),
                version: entry.version,
            })
            .collect();
        let backup = LogicalBackup {
            version: 2,
            created_at_ms: now_ms,
            source_engine_version: self.state.metadata.version,
            source_sequence: self.state.metadata.last_applied_sequence,
            entries,
        };

        serde_json::to_string(&backup)
            .map_err(|err| crate::EngineError::SnapshotSerialize(err.to_string()))
    }

    fn restore_logical_backup(&mut self, dump: &str) -> Result<usize> {
        let backup = parse_logical_backup(dump)?;
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
            let value = BASE64
                .decode(entry.value_base64.as_bytes())
                .map_err(|err| crate::EngineError::SnapshotDeserialize(err.to_string()))?;
            operations.push(WalOperation::Set {
                key: entry.key.clone(),
                value,
                version: entry.version.max(1),
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

    fn validate_logical_backup(&mut self, dump: &str) -> Result<usize> {
        let backup = parse_logical_backup(dump)?;
        let now_ms = self.now();
        Ok(backup
            .entries
            .into_iter()
            .filter(|entry| {
                entry
                    .expires_at_ms
                    .map(|expires_at_ms| expires_at_ms > now_ms)
                    .unwrap_or(true)
            })
            .count())
    }
}

fn parse_logical_backup(dump: &str) -> Result<LogicalBackup> {
    let header: LogicalBackupHeader = serde_json::from_str(dump)
        .map_err(|err| crate::EngineError::SnapshotDeserialize(err.to_string()))?;
    match header.version {
        2 => serde_json::from_str(dump)
            .map_err(|err| crate::EngineError::SnapshotDeserialize(err.to_string())),
        1 => {
            let legacy: LegacyLogicalBackup = serde_json::from_str(dump)
                .map_err(|err| crate::EngineError::SnapshotDeserialize(err.to_string()))?;
            Ok(LogicalBackup {
                version: 2,
                created_at_ms: legacy.created_at_ms,
                source_engine_version: legacy.source_engine_version,
                source_sequence: legacy.source_sequence,
                entries: legacy
                    .entries
                    .into_iter()
                    .map(|entry| LogicalBackupEntry {
                        key: entry.key,
                        value_base64: BASE64.encode(entry.value.as_bytes()),
                        expires_at_ms: entry.expires_at_ms,
                        version: 1,
                    })
                    .collect(),
            })
        }
        _ => Err(crate::EngineError::UnsupportedStorageFormat {
            resource: "logical backup",
        }),
    }
}

#[derive(serde::Deserialize)]
struct LogicalBackupHeader {
    version: u32,
}

#[derive(serde::Deserialize)]
struct LegacyLogicalBackup {
    created_at_ms: u64,
    source_engine_version: u32,
    source_sequence: u64,
    entries: Vec<LegacyLogicalBackupEntry>,
}

#[derive(serde::Deserialize)]
struct LegacyLogicalBackupEntry {
    key: String,
    value: String,
    expires_at_ms: Option<u64>,
}

fn build_storage_inspection(
    snapshot: Option<&Vec<u8>>,
    manifest: Option<&Manifest>,
    wal_report: &crate::WalSegmentReport,
) -> StorageInspection {
    StorageInspection {
        snapshot_present: snapshot.is_some(),
        storage_format_version: manifest
            .map(|value| value.storage_format_version)
            .unwrap_or(STORAGE_FORMAT_VERSION),
        snapshot_size_bytes: manifest
            .map(|value| value.snapshot_size_bytes)
            .unwrap_or_else(|| snapshot.map(|bytes| bytes.len() as u64).unwrap_or(0)),
        last_snapshot_sequence: manifest
            .map(|value| value.last_snapshot_sequence)
            .unwrap_or(0),
        last_snapshot_at_ms: manifest.map(|value| value.last_snapshot_at_ms),
        wal_segment_count: wal_report.segment_count,
        sealed_wal_segment_count: wal_report.sealed_segment_count,
        active_wal_segment_count: wal_report.active_segment_count,
        active_wal_start_sequence: wal_report.active_start_sequence,
        oldest_retained_sequence: wal_report.oldest_retained_sequence,
        newest_sequence: wal_report.newest_sequence,
        wal_size_bytes: wal_report.total_size_bytes,
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
        store::{Manifest, load_manifest, save_manifest},
    };
    use command::Command;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use uuid::Uuid;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(name: &str) -> PathBuf {
        let unique = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("vaylix-engine-{name}-{unique}"));
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
                    ..EngineOptions::default()
                },
            )
            .unwrap(),
            root,
        )
    }

    fn bytes(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    #[test]
    fn supports_serious_v1_string_commands() {
        let (mut engine, root) = engine();

        engine.set("name".to_string(), bytes("alice")).unwrap();
        assert_eq!(
            engine.get("name").unwrap().as_deref(),
            Some(b"alice".as_slice())
        );
        assert!(!engine.set_nx("name".to_string(), bytes("bob")).unwrap());
        assert!(engine.set_nx("city".to_string(), bytes("paris")).unwrap());
        assert_eq!(
            engine.mget(&["name".into(), "missing".into()]).unwrap(),
            vec![Some(bytes("alice")), None]
        );
        engine
            .mset(&[
                ("one".to_string(), bytes("1")),
                ("two".to_string(), bytes("2")),
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
    fn duplicate_mset_entries_advance_versions_in_order() {
        let (mut engine, root) = engine();

        engine.set("dup".to_string(), bytes("base")).unwrap();
        let base_version = engine.state().raw_entry("dup").unwrap().version;
        engine
            .mset(&[
                ("dup".to_string(), bytes("one")),
                ("dup".to_string(), bytes("two")),
            ])
            .unwrap();

        let entry = engine.state().raw_entry("dup").unwrap();
        assert_eq!(entry.value, bytes("two"));
        assert_eq!(entry.version, base_version + 2);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn supports_set_getdel_getex_and_scan_match() {
        let (mut engine, root) = engine();

        engine.set("user:1".to_string(), bytes("alice")).unwrap();
        let outcome = engine
            .set_with_options(
                "user:1".to_string(),
                bytes("bob"),
                SetOptions {
                    condition: Some(SetCondition::Xx),
                    expiration: Some(Expiration::Seconds(60)),
                    keep_ttl: false,
                    if_version: None,
                },
            )
            .unwrap();
        assert!(outcome.applied);
        assert_eq!(outcome.previous.as_deref(), Some(b"alice".as_slice()));

        assert_eq!(
            engine.get_del("user:1").unwrap().as_deref(),
            Some(b"bob".as_slice())
        );
        assert_eq!(engine.get("user:1").unwrap(), None);

        engine.set("user:2".to_string(), bytes("carol")).unwrap();
        assert_eq!(
            engine
                .get_ex("user:2", Some(Expiration::Seconds(60)), false)
                .unwrap(),
            Some(bytes("carol"))
        );
        assert!(engine.ttl("user:2").unwrap() > 0);

        engine
            .mset(&[
                ("user:alpha".to_string(), bytes("1")),
                ("sys:beta".to_string(), bytes("2")),
                ("user:gamma".to_string(), bytes("3")),
            ])
            .unwrap();

        let page = engine.scan(0, Some("user:*"), Some(10)).unwrap();
        assert_eq!(page.keys, vec!["user:2", "user:alpha", "user:gamma"]);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn snapshot_persists_state_and_retains_segmented_wal_history() {
        let (mut engine, root) = engine();

        engine.set("name".to_string(), bytes("alice")).unwrap();
        engine.snapshot().unwrap();

        let wal_dir = root.join("wal");
        assert!(wal_dir.exists());
        assert!(
            fs::read_dir(&wal_dir)
                .unwrap()
                .filter_map(|entry| entry.ok())
                .any(
                    |entry| entry.path().extension().and_then(|value| value.to_str())
                        == Some("wal")
                )
        );
        assert!(root.join("snapshot.bin").exists());
        assert!(root.join("manifest.bin").exists());

        let paths = Paths::from_data_dir(&root).unwrap();
        let mut reloaded = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            reloaded.get("name").unwrap().as_deref(),
            Some(b"alice".as_slice())
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn snapshot_before_first_write_keeps_wal_writer_on_visible_active_segment() {
        let (mut engine, root) = engine();

        engine.snapshot().unwrap();
        engine.set("name".to_string(), bytes("alice")).unwrap();
        engine.snapshot().unwrap();

        let wal_report = crate::inspect_wal(&root.join("wal")).unwrap();
        assert_eq!(wal_report.active_segment_count, 1);
        assert!(
            wal_report.sealed_segment_count >= 1,
            "expected a sealed segment after write and snapshot, got {wal_report:?}"
        );

        let paths = Paths::from_data_dir(&root).unwrap();
        let mut reloaded = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            reloaded.get("name").unwrap().as_deref(),
            Some(b"alice".as_slice())
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn wal_entries_since_capped_hides_uncommitted_tail() {
        let (mut engine, root) = engine();

        engine.set("a".to_string(), bytes("1")).unwrap();
        engine.set("b".to_string(), bytes("2")).unwrap();
        engine.set("c".to_string(), bytes("3")).unwrap();

        let committed = engine.wal_entries_since_capped(0, 32, Some(2)).unwrap();
        assert_eq!(
            committed
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let full = engine.wal_entries_since(0, 32).unwrap();
        assert_eq!(
            full.iter().map(|entry| entry.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn command_batches_advance_metadata_sequence() {
        let (mut engine, root) = engine();

        let first = engine
            .execute_command_batch(&[Command::Set {
                key: "batch:1".to_string(),
                value: bytes("one"),
                options: command::SetOptions::default(),
            }])
            .unwrap();
        assert_eq!(first[0].last_applied_sequence, 1);

        let second = engine
            .execute_command_batch(&[Command::Set {
                key: "batch:2".to_string(),
                value: bytes("two"),
                options: command::SetOptions::default(),
            }])
            .unwrap();
        assert_eq!(second[0].last_applied_sequence, 2);
        assert_eq!(engine.state().metadata.last_applied_sequence, 2);

        let sequences = engine
            .wal_entries_since(0, 32)
            .unwrap()
            .into_iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, vec![1, 2]);

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
                ..EngineOptions::default()
            },
        )
        .unwrap();
        source.set("a".to_string(), bytes("1")).unwrap();
        source.expire("a", 60).unwrap();
        source.set("b".to_string(), bytes("2")).unwrap();

        let dump = source.logical_backup().unwrap();

        let restore_root = temp_dir("logical-restore");
        let restore_paths = Paths::from_data_dir(&restore_root).unwrap();
        let mut restored = Engine::from_paths_with_options(
            restore_paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("restore-key")),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        restored.set("old".to_string(), bytes("value")).unwrap();

        assert_eq!(restored.restore_logical_backup(&dump).unwrap(), 2);
        assert_eq!(restored.get("a").unwrap().as_deref(), Some(b"1".as_slice()));
        assert_eq!(restored.get("b").unwrap().as_deref(), Some(b"2".as_slice()));
        assert_eq!(restored.get("old").unwrap(), None);
        assert!(restored.ttl("a").unwrap() > 0);

        fs::remove_dir_all(root).ok();
        fs::remove_dir_all(restore_root).ok();
    }

    #[test]
    fn rejects_wrong_data_key_on_recovery() {
        let (mut engine, root) = engine();
        engine.set("name".to_string(), bytes("alice")).unwrap();
        engine.snapshot().unwrap();

        let paths = Paths::from_data_dir(&root).unwrap();
        let reopened = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("wrong-data-key")),
                ..EngineOptions::default()
            },
        );

        assert!(reopened.is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_corrupted_snapshot_on_recovery() {
        let (mut engine, root) = engine();
        engine.set("name".to_string(), bytes("alice")).unwrap();
        engine.snapshot().unwrap();

        let paths = Paths::from_data_dir(&root).unwrap();
        fs::write(&paths.snapshot_path, b"corrupted snapshot").unwrap();
        let reopened = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        );

        assert!(reopened.is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_snapshot_manifest_checksum_mismatch_on_recovery() {
        let (mut engine, root) = engine();
        engine.set("name".to_string(), bytes("alice")).unwrap();
        engine.snapshot().unwrap();

        let paths = Paths::from_data_dir(&root).unwrap();
        let mut manifest = load_manifest(&paths.manifest_path).unwrap().unwrap();
        manifest.snapshot_checksum ^= 0xffff;
        save_manifest(&manifest, &paths.manifest_path, &paths.manifest_tmp_path).unwrap();

        let reopened = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        );

        assert!(reopened.is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_unsupported_manifest_storage_format_on_recovery() {
        let root = temp_dir("unsupported-manifest");
        let paths = Paths::from_data_dir(&root).unwrap();
        fs::create_dir_all(&root).unwrap();
        save_manifest(
            &Manifest {
                storage_format_version: 999,
                engine_version: 2,
                last_snapshot_sequence: 0,
                last_snapshot_at_ms: now_ms(),
                snapshot_size_bytes: 0,
                snapshot_checksum: 0,
                active_wal_start_sequence: 1,
                oldest_retained_sequence: 1,
            },
            &paths.manifest_path,
            &paths.manifest_tmp_path,
        )
        .unwrap();

        let reopened = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        );

        assert!(reopened.is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_unsupported_logical_backup_version() {
        let (mut engine, root) = engine();
        let dump = serde_json::json!({
            "version": 999,
            "created_at_ms": now_ms(),
            "source_engine_version": 2,
            "source_sequence": 0,
            "entries": []
        })
        .to_string();

        assert!(engine.validate_logical_backup(&dump).is_err());
        assert!(engine.restore_logical_backup(&dump).is_err());
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn ttl_and_persist_behave_consistently() {
        let (mut engine, root) = engine();

        engine.set("session".to_string(), bytes("abc")).unwrap();
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
                ("a".to_string(), bytes("1")),
                ("b".to_string(), bytes("2")),
                ("c".to_string(), bytes("3")),
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
                value: bytes("abc"),
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

    #[test]
    fn stores_binary_values_across_snapshot_and_recovery() {
        let (mut engine, root) = engine();
        let payload = vec![0, 159, 146, 150, b'a', 0, 255];

        engine.set("bin".to_string(), payload.clone()).unwrap();
        engine.snapshot().unwrap();

        let paths = Paths::from_data_dir(&root).unwrap();
        let mut reloaded = Engine::from_paths_with_options(
            paths,
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                keyring: Some(test_keyring("test-data-key")),
                ..EngineOptions::default()
            },
        )
        .unwrap();

        assert_eq!(reloaded.get("bin").unwrap(), Some(payload));
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn logical_backup_round_trips_binary_values_and_versions() {
        let (mut source, root) = engine();
        let payload = vec![0, 1, 2, 250, 255];
        source.set("bin".to_string(), payload.clone()).unwrap();
        let version = source.state().raw_entry("bin").unwrap().version;
        let dump = source.logical_backup().unwrap();

        let (mut restored, restore_root) = engine();
        restored.restore_logical_backup(&dump).unwrap();

        let restored_entry = restored.state().raw_entry("bin").unwrap();
        assert_eq!(restored_entry.value, payload);
        assert_eq!(restored_entry.version, version);

        fs::remove_dir_all(root).ok();
        fs::remove_dir_all(restore_root).ok();
    }

    #[test]
    fn logical_backup_restore_accepts_legacy_v1_text_values() {
        let (mut restored, root) = engine();
        let dump = serde_json::json!({
            "version": 1,
            "created_at_ms": now_ms(),
            "source_engine_version": 2,
            "source_sequence": 7,
            "entries": [
                {
                    "key": "legacy",
                    "value": "text",
                    "expires_at_ms": null
                }
            ]
        })
        .to_string();

        assert_eq!(restored.restore_logical_backup(&dump).unwrap(), 1);
        let entry = restored.state().raw_entry("legacy").unwrap();
        assert_eq!(entry.value, bytes("text"));
        assert_eq!(entry.version, 1);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn cas_succeeds_fails_and_preserves_versions() {
        let (mut engine, root) = engine();

        engine.set("item".to_string(), bytes("one")).unwrap();
        let initial_version = engine.state().raw_entry("item").unwrap().version;

        let success = engine
            .set_with_options(
                "item".to_string(),
                bytes("two"),
                SetOptions {
                    if_version: Some(initial_version),
                    ..SetOptions::default()
                },
            )
            .unwrap();
        assert!(success.applied);
        let next_version = engine.state().raw_entry("item").unwrap().version;
        assert!(next_version > initial_version);

        let failure = engine
            .set_with_options(
                "item".to_string(),
                bytes("stale"),
                SetOptions {
                    if_version: Some(initial_version),
                    ..SetOptions::default()
                },
            )
            .unwrap();
        assert!(!failure.applied);
        assert_eq!(
            engine.get("item").unwrap().as_deref(),
            Some(b"two".as_slice())
        );
        assert_eq!(
            engine.state().raw_entry("item").unwrap().version,
            next_version
        );

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn replicated_entries_preserve_binary_value_and_version() {
        let (mut leader, leader_root) = engine();
        let (mut follower, follower_root) = engine();
        let payload = vec![0, 1, 2, 200, 255];

        leader.set("bin".to_string(), payload.clone()).unwrap();
        let entries = leader.wal_entries_since(0, 32).unwrap();
        follower.apply_replication_entries(&entries).unwrap();

        let follower_entry = follower.state().raw_entry("bin").unwrap();
        let leader_entry = leader.state().raw_entry("bin").unwrap();
        assert_eq!(follower_entry.value, payload);
        assert_eq!(follower_entry.version, leader_entry.version);

        fs::remove_dir_all(leader_root).ok();
        fs::remove_dir_all(follower_root).ok();
    }
}
