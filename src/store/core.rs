use crate::paths::VeyraPaths;
use crate::store::{EngineState, StorageEngine};
use crate::store::{deserialize, load, save, serialize};
use anyhow::Result;

pub struct Engine {
    state: EngineState,
    paths: VeyraPaths,
}

impl Engine {
    pub fn new() -> Result<Self> {
        let paths = VeyraPaths::new()?;

        let loaded = load(&paths.snapshot_path)?;

        match loaded {
            Some(loaded) => {
                let deserialized = deserialize(&loaded)?;
                Ok(Self {
                    state: deserialized,
                    paths,
                })
            }
            None => Ok(Self {
                state: EngineState::new(),
                paths,
            }),
        }
    }

    fn persist(&self) -> Result<()> {
        let serialized = serialize(&self.state)?;
        save(&serialized, &self.paths.snapshot_path)?;

        Ok(())
    }
}

impl StorageEngine for Engine {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let value = self.state.data.get(key).cloned();

        Ok(value)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.state.data.insert(key, value);

        self.persist()?;

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.state.data.remove(key);

        self.persist()?;

        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let value_exists = self.state.data.contains_key(key);

        Ok(value_exists)
    }

    fn delete_many(&mut self, keys: &[String]) -> Result<()> {
        self.state.data.retain(|key, _| !keys.contains(key));

        self.persist()?;

        Ok(())
    }

    fn count(&self) -> Result<usize> {
        let count = self.state.data.len();
        Ok(count)
    }

    fn clear(&mut self) -> Result<()> {
        self.state.data.clear();

        self.persist()?;

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
}
