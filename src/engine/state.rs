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
