use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use command::Command;
use engine::{EngineState, WalEntry, WalOperation};
use parking_lot::RwLock;
use transport::Response;
use uuid::Uuid;

use crate::error::{Result, ServerError};

const TTL_NO_EXPIRY: i64 = -1;
const TTL_KEY_MISSING: i64 = -2;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadEntry {
    value: Vec<u8>,
    expires_at_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct ReadIndexState {
    entries: HashMap<String, ReadEntry>,
    committed_sequence: u64,
}

/// Committed in-memory projection used by safe read commands.
///
/// The index is intentionally server-side and WAL-driven. Writers update it only
/// after the configured local durability and HA acknowledgement boundary has
/// completed, so leader reads never observe an uncommitted local tail.
#[derive(Debug, Default)]
pub struct CommittedReadIndex {
    state: RwLock<ReadIndexState>,
}

impl CommittedReadIndex {
    pub(super) fn rebuild_from_engine_state(&self, engine_state: &EngineState) {
        let (data, expirations) = engine_state.to_persisted_parts();
        let mut entries = HashMap::with_capacity(data.len());
        for (key, value) in data {
            entries.insert(
                key.clone(),
                ReadEntry {
                    value,
                    expires_at_ms: expirations.get(&key).copied(),
                },
            );
        }
        let mut state = self.state.write();
        state.entries = entries;
        state.committed_sequence = engine_state.metadata.last_applied_sequence;
    }

    pub(super) fn apply_entries(&self, entries: &[WalEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut state = self.state.write();
        for entry in entries {
            if entry.sequence <= state.committed_sequence {
                continue;
            }
            for operation in &entry.operations {
                apply_operation(&mut state.entries, operation)?;
            }
            state.committed_sequence = entry.sequence;
        }
        Ok(())
    }

    pub(super) fn committed_sequence(&self) -> u64 {
        self.state.read().committed_sequence
    }

    pub(super) fn lag_to(&self, target_sequence: u64) -> u64 {
        target_sequence.saturating_sub(self.committed_sequence())
    }

    pub(super) fn read_command(&self, request_id: Uuid, command: &Command) -> Result<Response> {
        let state = self.state.read();
        match command {
            Command::Get { key } => match live_entry(&state.entries, key) {
                Some(entry) => Ok(Response::value_bytes(request_id, &entry.value)?),
                None => Ok(Response::not_found(request_id)),
            },
            Command::MGet { keys } => {
                let mut now_ms = None;
                let values = keys
                    .iter()
                    .map(|key| {
                        live_entry_with_cached_time(&state.entries, key, &mut now_ms)
                            .map(|entry| entry.value.clone())
                    })
                    .collect::<Vec<_>>();
                Ok(Response::byte_strings(request_id, &values)?)
            }
            Command::Exists { key } => Ok(Response::boolean(
                request_id,
                live_entry(&state.entries, key).is_some(),
            )),
            Command::Ttl { key } => {
                let now_ms = now_millis();
                let ttl = match state.entries.get(key) {
                    Some(entry) if entry_expired(entry, now_ms) => TTL_KEY_MISSING,
                    Some(entry) => match entry.expires_at_ms {
                        Some(expires_at_ms) => {
                            remaining_seconds(expires_at_ms.saturating_sub(now_ms))
                        }
                        None => TTL_NO_EXPIRY,
                    },
                    None => TTL_KEY_MISSING,
                };
                Ok(Response::integer(request_id, ttl))
            }
            _ => Err(ServerError::UnsupportedRemoteCommand),
        }
    }
}

pub(super) fn is_fast_path_read(command: &Command) -> bool {
    matches!(
        command,
        Command::Get { .. } | Command::MGet { .. } | Command::Exists { .. } | Command::Ttl { .. }
    )
}

fn apply_operation(
    entries: &mut HashMap<String, ReadEntry>,
    operation: &WalOperation,
) -> Result<()> {
    match operation {
        WalOperation::Set { key, value, .. } => {
            entries.insert(
                key.clone(),
                ReadEntry {
                    value: value.clone(),
                    expires_at_ms: None,
                },
            );
        }
        WalOperation::Delete { key } => {
            entries.remove(key);
        }
        WalOperation::Expire { key, expires_at_ms } => {
            if let Some(entry) = entries.get_mut(key) {
                entry.expires_at_ms = Some(*expires_at_ms);
            }
        }
        WalOperation::Persist { key } => {
            if let Some(entry) = entries.get_mut(key) {
                entry.expires_at_ms = None;
            }
        }
        WalOperation::Clear => {
            entries.clear();
        }
        WalOperation::CheckInteger { key, delta } => {
            let current = entries
                .get(key)
                .map(|entry| entry.value.clone())
                .unwrap_or_else(|| b"0".to_vec());
            let current_text = String::from_utf8(current.clone()).map_err(|_| {
                engine::EngineError::InvalidIntegerValue {
                    key: key.clone(),
                    value: String::from_utf8_lossy(&current).into_owned(),
                }
            })?;
            let parsed = current_text.parse::<i64>().map_err(|_| {
                engine::EngineError::InvalidIntegerValue {
                    key: key.clone(),
                    value: current_text.clone(),
                }
            })?;
            let next = parsed
                .checked_add(*delta)
                .ok_or_else(|| engine::EngineError::NumericOverflow { key: key.clone() })?;
            entries.insert(
                key.clone(),
                ReadEntry {
                    value: next.to_string().into_bytes(),
                    expires_at_ms: None,
                },
            );
        }
    }
    Ok(())
}

fn live_entry<'a>(entries: &'a HashMap<String, ReadEntry>, key: &str) -> Option<&'a ReadEntry> {
    let entry = entries.get(key)?;
    match entry.expires_at_ms {
        Some(expires_at_ms) if expires_at_ms <= now_millis() => None,
        _ => Some(entry),
    }
}

fn live_entry_with_cached_time<'a>(
    entries: &'a HashMap<String, ReadEntry>,
    key: &str,
    now_ms: &mut Option<u64>,
) -> Option<&'a ReadEntry> {
    let entry = entries.get(key)?;
    match entry.expires_at_ms {
        Some(expires_at_ms) => {
            let now = *now_ms.get_or_insert_with(now_millis);
            (expires_at_ms > now).then_some(entry)
        }
        None => Some(entry),
    }
}

fn entry_expired(entry: &ReadEntry, now_ms: u64) -> bool {
    entry
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}

fn remaining_seconds(remaining_ms: u64) -> i64 {
    remaining_ms.div_ceil(1_000) as i64
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::WalEntry;

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[test]
    fn projects_set_get_exists_and_ttl() {
        let index = CommittedReadIndex::default();
        index
            .apply_entries(&[WalEntry::new(
                1,
                0,
                1_000,
                vec![
                    WalOperation::Set {
                        key: "token".to_string(),
                        value: b"abc".to_vec(),
                        version: 1,
                    },
                    WalOperation::Expire {
                        key: "token".to_string(),
                        expires_at_ms: now_millis().saturating_add(5_000),
                    },
                ],
            )])
            .unwrap();

        assert_eq!(
            index
                .read_command(
                    id(1),
                    &Command::Get {
                        key: "token".to_string()
                    }
                )
                .unwrap()
                .decode_value()
                .unwrap(),
            "abc"
        );
        assert!(
            index
                .read_command(
                    id(2),
                    &Command::Exists {
                        key: "token".to_string()
                    }
                )
                .unwrap()
                .decode_bool()
                .unwrap()
        );
        assert!(
            index
                .read_command(
                    id(3),
                    &Command::Ttl {
                        key: "token".to_string()
                    }
                )
                .unwrap()
                .decode_integer()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn expired_entries_are_misses_without_mutation() {
        let index = CommittedReadIndex::default();
        index
            .apply_entries(&[WalEntry::new(
                1,
                0,
                1_000,
                vec![
                    WalOperation::Set {
                        key: "token".to_string(),
                        value: b"abc".to_vec(),
                        version: 1,
                    },
                    WalOperation::Expire {
                        key: "token".to_string(),
                        expires_at_ms: 1,
                    },
                ],
            )])
            .unwrap();

        let response = index
            .read_command(
                id(1),
                &Command::Get {
                    key: "token".to_string(),
                },
            )
            .unwrap();
        assert_eq!(response.status, transport::Status::NotFound);
        assert_eq!(index.committed_sequence(), 1);
    }

    #[test]
    fn numeric_wal_operations_match_engine_semantics() {
        let index = CommittedReadIndex::default();
        index
            .apply_entries(&[WalEntry::new(
                1,
                0,
                1_000,
                vec![WalOperation::CheckInteger {
                    key: "counter".to_string(),
                    delta: 1,
                }],
            )])
            .unwrap();

        assert_eq!(
            index
                .read_command(
                    id(1),
                    &Command::Get {
                        key: "counter".to_string(),
                    }
                )
                .unwrap()
                .decode_value()
                .unwrap(),
            "1"
        );
    }
}
