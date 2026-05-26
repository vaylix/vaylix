use crate::store::WalEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub const ENGINE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineState {
    pub data: HashMap<String, String>,
    pub created_at: u64,
    pub version: u32,
}

impl EngineState {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            version: ENGINE_VERSION,
        }
    }

    pub fn apply(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Set { key, value } => {
                self.data.insert(key, value);
            }

            WalEntry::Delete { key } => {
                self.data.remove(&key);
            }

            WalEntry::Clear => {
                self.data.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ENGINE_VERSION, EngineState};
    use crate::store::WalEntry;

    #[test]
    fn initializes_with_version_and_empty_data() {
        let state = EngineState::new();

        assert!(state.data.is_empty());
        assert_eq!(state.version, ENGINE_VERSION);
        assert!(state.created_at > 0);
    }

    #[test]
    fn applies_set_delete_and_clear_entries() {
        let mut state = EngineState::new();

        state.apply(WalEntry::Set {
            key: "name".to_string(),
            value: "alice".to_string(),
        });
        assert_eq!(state.data.get("name").map(String::as_str), Some("alice"));

        state.apply(WalEntry::Delete {
            key: "name".to_string(),
        });
        assert!(!state.data.contains_key("name"));

        state.apply(WalEntry::Set {
            key: "city".to_string(),
            value: "paris".to_string(),
        });
        state.apply(WalEntry::Clear);

        assert!(state.data.is_empty());
    }
}
