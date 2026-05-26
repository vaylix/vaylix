use crate::error::{EngineError, Result};
use crate::store::{WalEntry, WalOperation};
use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EngineState {
    /// Primary key/value store.
    pub data: BTreeMap<String, String>,
    /// Per-key expiration timestamps in unix milliseconds.
    pub expirations: BTreeMap<String, u64>,
    /// Engine metadata.
    pub metadata: EngineMetadata,
}

/// Rollback checkpoint used to revert a partially applied state mutation.
#[derive(Debug, Clone)]
pub struct RollbackPlan {
    previous: EngineState,
}

impl EngineState {
    /// Builds an empty state with current metadata.
    pub fn new() -> Self {
        let now_ms = now_millis();

        Self {
            data: BTreeMap::new(),
            expirations: BTreeMap::new(),
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
        let rollback = self.rollback_point();

        for operation in &entry.operations {
            if let Err(err) = self.apply_operation(operation) {
                self.rollback(rollback);
                return Err(err);
            }
        }

        self.metadata.last_applied_sequence = entry.sequence;
        self.metadata.updated_at_ms = entry.created_at_ms;

        Ok(())
    }

    /// Removes expired keys and returns the number of keys dropped.
    pub fn purge_expired(&mut self, now_ms: u64) -> usize {
        let expired_keys: Vec<String> = self
            .expirations
            .iter()
            .filter(|(_, expires_at_ms)| **expires_at_ms <= now_ms)
            .map(|(key, _)| key.clone())
            .collect();

        for key in &expired_keys {
            self.data.remove(key);
            self.expirations.remove(key);
        }

        if !expired_keys.is_empty() {
            self.metadata.updated_at_ms = now_ms;
        }

        expired_keys.len()
    }

    /// Returns the live value for a key after expiration cleanup.
    pub fn get_live(&mut self, key: &str, now_ms: u64) -> Option<String> {
        self.purge_expired(now_ms);
        self.data.get(key).cloned()
    }

    /// Returns whether the key is currently live after expiration cleanup.
    pub fn has_live_key(&mut self, key: &str, now_ms: u64) -> bool {
        self.purge_expired(now_ms);
        self.data.contains_key(key)
    }

    /// Returns the remaining TTL for a key after expiration cleanup.
    pub fn ttl_for(&mut self, key: &str, now_ms: u64) -> i64 {
        self.purge_expired(now_ms);

        if !self.data.contains_key(key) {
            return TTL_KEY_MISSING;
        }

        match self.expirations.get(key).copied() {
            Some(expires_at_ms) if expires_at_ms <= now_ms => {
                self.data.remove(key);
                self.expirations.remove(key);
                self.metadata.updated_at_ms = now_ms;
                TTL_KEY_MISSING
            }
            Some(expires_at_ms) => remaining_seconds(expires_at_ms.saturating_sub(now_ms)),
            None => TTL_NO_EXPIRY,
        }
    }

    /// Returns all live keys in deterministic order.
    pub fn live_keys(&mut self, now_ms: u64) -> Vec<String> {
        self.purge_expired(now_ms);
        self.data.keys().cloned().collect()
    }

    /// Returns all live entries in deterministic order.
    pub fn live_entries(&mut self, now_ms: u64) -> Vec<(String, String)> {
        self.purge_expired(now_ms);
        self.data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Marks the state as successfully snapshotted.
    pub fn mark_snapshot(&mut self, now_ms: u64, sequence: u64) {
        self.metadata.updated_at_ms = now_ms;
        self.metadata.last_snapshot_at_ms = Some(now_ms);
        self.metadata.last_applied_sequence = sequence;
    }

    fn apply_operation(&mut self, operation: &WalOperation) -> Result<()> {
        match operation {
            WalOperation::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
                self.expirations.remove(key);
            }
            WalOperation::Delete { key } => {
                self.data.remove(key);
                self.expirations.remove(key);
            }
            WalOperation::Expire { key, expires_at_ms } => {
                if self.data.contains_key(key) {
                    self.expirations.insert(key.clone(), *expires_at_ms);
                }
            }
            WalOperation::Persist { key } => {
                self.expirations.remove(key);
            }
            WalOperation::Clear => {
                self.data.clear();
                self.expirations.clear();
            }
            WalOperation::CheckInteger { key, delta } => {
                let current = self
                    .data
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());
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
                self.data.insert(key.clone(), next.to_string());
                self.expirations.remove(key);
            }
        }

        Ok(())
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
        WalEntry::new(sequence, 1_000, vec![operation])
    }

    #[test]
    fn initializes_with_metadata_and_empty_data() {
        let state = EngineState::new();

        assert!(state.data.is_empty());
        assert!(state.expirations.is_empty());
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
        assert_eq!(state.data.get("name").map(String::as_str), Some("alice"));

        state
            .apply_entry(&entry(
                2,
                WalOperation::Delete {
                    key: "name".to_string(),
                },
            ))
            .unwrap();
        assert!(!state.data.contains_key("name"));
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
