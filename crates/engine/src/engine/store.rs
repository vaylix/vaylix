use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Value stored in the live in-memory keyspace.
///
/// Keeping the TTL beside the value avoids a second map lookup for the common
/// `GET`/`TTL`/write-overwrite paths and keeps shard ownership localized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredValue {
    #[serde(
        serialize_with = "crate::value::serialize_bytes",
        deserialize_with = "crate::value::deserialize_bytes"
    )]
    pub value: Vec<u8>,
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub version: u64,
}

/// Sharded concurrent live key/value store used by the engine.
///
/// Persistence surfaces still export deterministic `BTreeMap` views so old
/// snapshot payloads, logical backups, and replication snapshots remain stable.
#[derive(Debug, Default)]
pub struct EngineStore {
    entries: DashMap<String, StoredValue>,
}

impl EngineStore {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    pub fn from_parts(data: BTreeMap<String, Vec<u8>>, expirations: BTreeMap<String, u64>) -> Self {
        let store = Self::new();
        for (key, value) in data {
            store.entries.insert(
                key.clone(),
                StoredValue {
                    value,
                    expires_at_ms: expirations.get(&key).copied(),
                    version: 1,
                },
            );
        }
        store
    }

    pub fn from_entries(entries: BTreeMap<String, StoredValue>) -> Self {
        let store = Self::new();
        for (key, value) in entries {
            store.entries.insert(key, value);
        }
        store
    }

    pub fn to_entries(&self) -> BTreeMap<String, StoredValue> {
        let mut entries = BTreeMap::new();
        for entry in self.entries.iter() {
            entries.insert(entry.key().clone(), entry.value().clone());
        }
        entries
    }

    pub fn to_parts(&self) -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, u64>) {
        let mut data = BTreeMap::new();
        let mut expirations = BTreeMap::new();
        for entry in self.entries.iter() {
            data.insert(entry.key().clone(), entry.value().value.clone());
            if let Some(expires_at_ms) = entry.value().expires_at_ms {
                expirations.insert(entry.key().clone(), expires_at_ms);
            }
        }
        (data, expirations)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&self) {
        self.entries.clear();
    }

    pub fn get(&self, key: &str) -> Option<StoredValue> {
        self.entries.get(key).map(|entry| entry.value().clone())
    }

    pub fn get_value(&self, key: &str) -> Option<Vec<u8>> {
        self.entries
            .get(key)
            .map(|entry| entry.value().value.clone())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    pub fn expiration(&self, key: &str) -> Option<u64> {
        self.entries
            .get(key)
            .and_then(|entry| entry.value().expires_at_ms)
    }

    pub fn insert_value(&self, key: String, value: Vec<u8>) -> Option<StoredValue> {
        let version = self
            .entries
            .get(&key)
            .map(|entry| entry.value().version.saturating_add(1))
            .unwrap_or(1);
        self.insert_value_with_version(key, value, version)
    }

    pub fn insert_value_with_version(
        &self,
        key: String,
        value: Vec<u8>,
        version: u64,
    ) -> Option<StoredValue> {
        self.entries.insert(
            key,
            StoredValue {
                value,
                expires_at_ms: None,
                version,
            },
        )
    }

    pub fn insert_entry(&self, key: String, entry: StoredValue) -> Option<StoredValue> {
        self.entries.insert(key, entry)
    }

    pub fn remove(&self, key: &str) -> Option<StoredValue> {
        self.entries.remove(key).map(|(_, value)| value)
    }

    pub fn set_expiration_if_present(&self, key: &str, expires_at_ms: u64) -> bool {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.expires_at_ms = Some(expires_at_ms);
            true
        } else {
            false
        }
    }

    pub fn clear_expiration(&self, key: &str) -> bool {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.expires_at_ms.take().is_some()
        } else {
            false
        }
    }

    pub fn purge_expired(&self, now_ms: u64) -> usize {
        let expired_keys = self
            .entries
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
            })
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut removed = 0;
        for key in expired_keys {
            if self.entries.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub fn keys_sorted(&self) -> Vec<String> {
        let mut keys = self
            .entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    pub fn live_keys_sorted(&self, now_ms: u64) -> Vec<String> {
        self.purge_expired(now_ms);
        self.keys_sorted()
    }

    pub fn live_entries_sorted(&self, now_ms: u64) -> Vec<(String, Vec<u8>)> {
        self.purge_expired(now_ms);
        let mut entries = self
            .entries
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().value.clone()))
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries
    }
}

impl Clone for EngineStore {
    fn clone(&self) -> Self {
        let clone = Self::new();
        for entry in self.entries.iter() {
            clone
                .entries
                .insert(entry.key().clone(), entry.value().clone());
        }
        clone
    }
}

impl PartialEq for EngineStore {
    fn eq(&self, other: &Self) -> bool {
        self.to_parts() == other.to_parts()
    }
}

impl Eq for EngineStore {}
