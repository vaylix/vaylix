use crate::paths::VeyraPaths;
use crate::store::StorageEngine;
use crate::store::{deserialize, load, save, serialize};
use anyhow::Result;
use std::collections::HashMap;

pub struct Engine {
    data: HashMap<String, String>,
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
                    data: deserialized,
                    paths,
                })
            }
            None => Ok(Self {
                data: HashMap::new(),
                paths,
            }),
        }
    }

    fn persist(&mut self) -> Result<()> {
        let serialized = serialize(&self.data)?;
        save(&serialized, &self.paths.snapshot_path)?;

        Ok(())
    }
}

impl StorageEngine for Engine {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let value = self.data.get(key).cloned();

        Ok(value)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.data.insert(key, value);

        self.persist()?;

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<()> {
        self.data.remove(key);

        self.persist()?;

        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool> {
        let value_exists = self.data.contains_key(key);

        Ok(value_exists)
    }

    fn delete_many(&mut self, keys: &[String]) -> Result<()> {
        self.data.retain(|key, _| !keys.contains(key));

        self.persist()?;

        Ok(())
    }

    fn count(&self) -> Result<usize> {
        let count = self.data.keys().count();
        Ok(count)
    }

    fn clear(&mut self) -> Result<()> {
        self.data.clear();

        self.persist()?;

        Ok(())
    }

    fn list(&self) -> Result<Vec<(String, String)>> {
        let key_value: Vec<(String, String)> = self
            .data
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        Ok(key_value)
    }
}
