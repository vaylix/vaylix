use crate::engine::{EngineStore, StoredValue};
use crate::error::{EngineError, Result};
use crate::store::{WalEntry, WalOperation};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current on-disk state schema version.
pub const ENGINE_VERSION: u32 = 2;

/// TTL return value used when a key exists without an expiration.
pub const TTL_NO_EXPIRY: i64 = -1;

/// TTL return value used when a key does not exist.
pub const TTL_KEY_MISSING: i64 = -2;

/// Metadata tracked alongside the database state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineMetadata {
    /// State schema version.
    pub version: u32,
    /// Creation time for the logical database state.
    pub created_at_ms: u64,
    /// Last successful mutation time.
    pub updated_at_ms: u64,
    /// Last successful snapshot completion time.
    pub last_snapshot_at_ms: Option<u64>,
    /// Last WAL sequence number applied into memory.
    pub last_applied_sequence: u64,
}

/// In-memory database state rebuilt from snapshots and the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineState {
    /// Sharded primary key/value store.
    pub store: EngineStore,
    /// Engine metadata.
    pub metadata: EngineMetadata,
}

#[derive(Serialize, Deserialize)]
struct PersistedEngineState {
    data: BTreeMap<String, String>,
    expirations: BTreeMap<String, u64>,
    metadata: EngineMetadata,
}

impl Serialize for EngineState {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (data, expirations) = self.store.to_parts();
        PersistedEngineState {
            data,
            expirations,
            metadata: self.metadata.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EngineState {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let persisted = PersistedEngineState::deserialize(deserializer)?;
        Ok(Self {
            store: EngineStore::from_parts(persisted.data, persisted.expirations),
            metadata: persisted.metadata,
        })
    }
}

/// Rollback checkpoint used to revert a partially applied state mutation.
#[derive(Debug, Clone)]
pub struct RollbackPlan {
    previous: EngineState,
}

#[derive(Default)]
struct EntryRollback {
    keys: BTreeMap<String, Option<StoredValue>>,
    clear_snapshot: Option<EngineStore>,
}

impl EntryRollback {
    fn remember_key(&mut self, state: &EngineState, key: &str) {
        if self.clear_snapshot.is_some() || self.keys.contains_key(key) {
            return;
        }
        self.keys.insert(key.to_string(), state.store.get(key));
    }

    fn remember_clear(&mut self, state: &EngineState) {
        if self.clear_snapshot.is_none() {
            self.clear_snapshot = Some(state.store.clone());
            self.keys.clear();
        }
    }

    fn rollback(self, state: &mut EngineState, metadata: EngineMetadata) {
        if let Some(store) = self.clear_snapshot {
            state.store = store;
        } else {
            for (key, value) in self.keys {
                match value {
                    Some(value) => {
                        state.store.insert_entry(key, value);
                    }
                    None => {
                        state.store.remove(&key);
                    }
                }
            }
        }
        state.metadata = metadata;
    }
}

impl EngineState {
    /// Builds an empty state with current metadata.
    pub fn new() -> Self {
        let now_ms = now_millis();

        Self {
            store: EngineStore::new(),
            metadata: EngineMetadata {
                version: ENGINE_VERSION,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                last_snapshot_at_ms: None,
                last_applied_sequence: 0,
            },
        }
    }

    /// Creates a rollback checkpoint for the current state.
    pub fn rollback_point(&self) -> RollbackPlan {
        RollbackPlan {
            previous: self.clone(),
        }
    }

    /// Reverts the current state to a previous rollback checkpoint.
    pub fn rollback(&mut self, rollback: RollbackPlan) {
        *self = rollback.previous;
    }

    /// Applies a single WAL entry to the in-memory state atomically.
    pub fn apply_entry(&mut self, entry: &WalEntry) -> Result<()> {
        let metadata = self.metadata.clone();
        let mut rollback = EntryRollback::default();

        for operation in &entry.operations {
            if let Err(err) = self.apply_operation(operation, &mut rollback) {
                rollback.rollback(self, metadata);
                return Err(err);
            }
        }

        self.metadata.last_applied_sequence = entry.sequence;
        self.metadata.updated_at_ms = entry.created_at_ms;

        Ok(())
    }

    /// Removes expired keys and returns the number of keys dropped.
    pub fn purge_expired(&mut self, now_ms: u64) -> usize {
        let removed = self.store.purge_expired(now_ms);
        if removed > 0 {
            self.metadata.updated_at_ms = now_ms;
        }
        removed
    }

    /// Returns the live value for a key after expiration cleanup.
    pub fn get_live(&mut self, key: &str, now_ms: u64) -> Option<String> {
        self.purge_expired(now_ms);
        self.store.get_value(key)
    }

    /// Returns whether the key is currently live after expiration cleanup.
    pub fn has_live_key(&mut self, key: &str, now_ms: u64) -> bool {
        self.purge_expired(now_ms);
        self.store.contains_key(key)
    }

    /// Returns the remaining TTL for a key after expiration cleanup.
    pub fn ttl_for(&mut self, key: &str, now_ms: u64) -> i64 {
        self.purge_expired(now_ms);

        if !self.store.contains_key(key) {
            return TTL_KEY_MISSING;
        }

        match self.store.expiration(key) {
            Some(expires_at_ms) if expires_at_ms <= now_ms => {
                self.store.remove(key);
                self.metadata.updated_at_ms = now_ms;
                TTL_KEY_MISSING
            }
            Some(expires_at_ms) => remaining_seconds(expires_at_ms.saturating_sub(now_ms)),
            None => TTL_NO_EXPIRY,
        }
    }

    /// Returns all live keys in deterministic order.
    pub fn live_keys(&mut self, now_ms: u64) -> Vec<String> {
        self.store.live_keys_sorted(now_ms)
    }

    /// Returns all live entries in deterministic order.
    pub fn live_entries(&mut self, now_ms: u64) -> Vec<(String, String)> {
        self.store.live_entries_sorted(now_ms)
    }

    /// Returns all data and expiration entries in deterministic persisted form.
    pub fn to_persisted_parts(&self) -> (BTreeMap<String, String>, BTreeMap<String, u64>) {
        self.store.to_parts()
    }

    /// Returns the current key count without sweeping expirations.
    pub fn key_count(&self) -> usize {
        self.store.len()
    }

    /// Returns whether the keyspace is empty without sweeping expirations.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Returns the stored value without sweeping expirations.
    pub fn raw_value(&self, key: &str) -> Option<String> {
        self.store.get_value(key)
    }

    /// Returns the stored value and expiration without sweeping expirations.
    pub fn raw_entry(&self, key: &str) -> Option<StoredValue> {
        self.store.get(key)
    }

    /// Returns whether the key exists without sweeping expirations.
    pub fn raw_contains_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    /// Returns a raw expiration without sweeping expirations.
    pub fn raw_expiration(&self, key: &str) -> Option<u64> {
        self.store.expiration(key)
    }

    /// Inserts or replaces a value and removes any expiration.
    pub fn set_raw_value(&mut self, key: String, value: String) {
        self.store.insert_value(key, value);
    }

    /// Inserts or replaces a complete stored entry.
    pub fn set_raw_entry(&mut self, key: String, entry: StoredValue) {
        self.store.insert_entry(key, entry);
    }

    /// Removes a key and returns the previous entry, if present.
    pub fn remove_raw(&mut self, key: &str) -> Option<StoredValue> {
        self.store.remove(key)
    }

    /// Sets an expiration when the key exists.
    pub fn set_expiration_if_present(&mut self, key: &str, expires_at_ms: u64) -> bool {
        self.store.set_expiration_if_present(key, expires_at_ms)
    }

    /// Clears an expiration and reports whether one existed.
    pub fn clear_expiration(&mut self, key: &str) -> bool {
        self.store.clear_expiration(key)
    }

    /// Clears all stored keys.
    pub fn clear_all(&mut self) {
        self.store.clear();
    }

    /// Marks the state as successfully snapshotted.
    pub fn mark_snapshot(&mut self, now_ms: u64, sequence: u64) {
        self.metadata.updated_at_ms = now_ms;
        self.metadata.last_snapshot_at_ms = Some(now_ms);
        self.metadata.last_applied_sequence = sequence;
    }

    fn apply_operation(
        &mut self,
        operation: &WalOperation,
        rollback: &mut EntryRollback,
    ) -> Result<()> {
        match operation {
            WalOperation::Set { key, value } => {
                rollback.remember_key(self, key);
                self.store.insert_value(key.clone(), value.clone());
            }
            WalOperation::Delete { key } => {
                rollback.remember_key(self, key);
                self.store.remove(key);
            }
            WalOperation::Expire { key, expires_at_ms } => {
                rollback.remember_key(self, key);
                self.store.set_expiration_if_present(key, *expires_at_ms);
            }
            WalOperation::Persist { key } => {
                rollback.remember_key(self, key);
                self.store.clear_expiration(key);
            }
            WalOperation::Clear => {
                rollback.remember_clear(self);
                self.store.clear();
            }
            WalOperation::CheckInteger { key, delta } => {
                rollback.remember_key(self, key);
                let current = self.store.get_value(key).unwrap_or_else(|| "0".to_string());
                let parsed =
                    current
                        .parse::<i64>()
                        .map_err(|_| EngineError::InvalidIntegerValue {
                            key: key.clone(),
                            value: current.clone(),
                        })?;
                let next = parsed
                    .checked_add(*delta)
                    .ok_or_else(|| EngineError::NumericOverflow { key: key.clone() })?;
                self.store.insert_value(key.clone(), next.to_string());
            }
        }

        Ok(())
    }
}

impl Default for EngineState {
    fn default() -> Self {
        Self::new()
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

fn remaining_seconds(remaining_ms: u64) -> i64 {
    remaining_ms.div_ceil(1_000) as i64
}

#[cfg(test)]
mod tests {
    use super::{ENGINE_VERSION, EngineState, TTL_KEY_MISSING, TTL_NO_EXPIRY};
    use crate::store::{WalEntry, WalOperation};

    fn entry(sequence: u64, operation: WalOperation) -> WalEntry {
        WalEntry::new(sequence, 0, 1_000, vec![operation])
    }

    #[test]
    fn initializes_with_metadata_and_empty_data() {
        let state = EngineState::new();

        assert!(state.is_empty());
        assert_eq!(state.metadata.version, ENGINE_VERSION);
        assert!(state.metadata.created_at_ms > 0);
        assert_eq!(state.metadata.last_applied_sequence, 0);
    }

    #[test]
    fn applies_entries_and_tracks_sequence() {
        let mut state = EngineState::new();

        state
            .apply_entry(&entry(
                1,
                WalOperation::Set {
                    key: "name".to_string(),
                    value: "alice".to_string(),
                },
            ))
            .unwrap();
        assert_eq!(state.raw_value("name").as_deref(), Some("alice"));

        state
            .apply_entry(&entry(
                2,
                WalOperation::Delete {
                    key: "name".to_string(),
                },
            ))
            .unwrap();
        assert!(!state.raw_contains_key("name"));
        assert_eq!(state.metadata.last_applied_sequence, 2);
    }

    #[test]
    fn ttl_semantics_match_redis_style_values() {
        let mut state = EngineState::new();
        state
            .apply_entry(&entry(
                1,
                WalOperation::Set {
                    key: "token".to_string(),
                    value: "abc".to_string(),
                },
            ))
            .unwrap();

        assert_eq!(state.ttl_for("missing", 1_000), TTL_KEY_MISSING);
        assert_eq!(state.ttl_for("token", 1_000), TTL_NO_EXPIRY);

        state
            .apply_entry(&entry(
                2,
                WalOperation::Expire {
                    key: "token".to_string(),
                    expires_at_ms: 3_500,
                },
            ))
            .unwrap();

        assert_eq!(state.ttl_for("token", 2_000), 2);
        assert_eq!(state.ttl_for("token", 4_000), TTL_KEY_MISSING);
    }

    #[test]
    fn rolls_back_failed_numeric_updates() {
        let mut state = EngineState::new();
        state
            .apply_entry(&entry(
                1,
                WalOperation::Set {
                    key: "counter".to_string(),
                    value: "abc".to_string(),
                },
            ))
            .unwrap();

        let before = state.clone();
        assert!(
            state
                .apply_entry(&entry(
                    2,
                    WalOperation::CheckInteger {
                        key: "counter".to_string(),
                        delta: 1,
                    },
                ))
                .is_err()
        );
        assert_eq!(state, before);
    }
}
