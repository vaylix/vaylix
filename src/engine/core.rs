use crate::engine::{EngineState, StorageEngine};
use crate::paths::VeyraPaths;
use crate::store::{WalEntry, append, replay};
use crate::store::{deserialize, load, save, serialize, truncate};
use anyhow::Result;

pub struct Engine {
    state: EngineState,
    paths: VeyraPaths,
}

impl Engine {
    pub fn new() -> Result<Self> {
        let paths = VeyraPaths::new()?;

        let loaded = load(&paths.snapshot_path)?;

        let mut state = match loaded {
            Some(loaded) => deserialize(&loaded)?,
            None => EngineState::new(),
        };

        let entries = replay(&paths.wal_path)?;

        for entry in entries {
            state.apply(entry);
        }

        Ok(Self { state, paths })
    }
}

impl StorageEngine for Engine {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let value = self.state.data.get(key).cloned();

        Ok(value)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry = WalEntry::Set { key, value };

        append(&entry, &self.paths.wal_path)?;

        self.state.apply(entry);

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        let entry = WalEntry::Delete {
            key: key.to_string(),
        };

        append(&entry, &self.paths.wal_path)?;

        self.state.apply(entry);

        Ok(())
    }

    fn delete_many(&mut self, keys: &[String]) -> Result<()> {
        for key in keys {
            let entry = WalEntry::Delete {
                key: key.to_string(),
            };

            append(&entry, &self.paths.wal_path)?;

            self.state.apply(entry);
        }

        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let value_exists = self.state.data.contains_key(key);

        Ok(value_exists)
    }

    fn count(&self) -> Result<usize> {
        let count = self.state.data.len();
        Ok(count)
    }

    fn clear(&mut self) -> Result<()> {
        let entry = WalEntry::Clear;

        append(&entry, &self.paths.wal_path)?;

        self.state.apply(entry);

        Ok(())
    }

    fn list(&self) -> Result<Vec<(String, String)>> {
        let key_value: Vec<(String, String)> = self
            .state
            .data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        Ok(key_value)
    }

    fn snapshot(&self) -> Result<()> {
        let serialized = serialize(&self.state)?;
        save(&serialized, &self.paths.snapshot_path)?;

        truncate(&self.paths.wal_path)?;

        Ok(())
    }
}
