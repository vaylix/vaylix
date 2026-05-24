use anyhow::{Ok, Result, bail};
use std::collections::HashMap;

pub struct Store {
    data: HashMap<String, String>,
}

impl Store {
    pub fn new() -> Result<Self> {
        Ok(Self {
            data: HashMap::new(),
        })
    }

    pub fn get(&self, key: &str) -> Result<&str> {
        let value = self.data.get(key);

        match value {
            Some(value) => Ok(value),
            None => bail!("Not found"),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let value = self.data.insert(key, value);

        match value {
            Some(_) => Ok(()),
            None => bail!("Could not insert"),
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        let value = self.data.remove(key);

        match value {
            Some(_) => Ok(()),
            None => bail!("Not found"),
        }
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let value_exists = self.data.contains_key(key);
        Ok(value_exists)
    }

    pub fn delete_many(&mut self, keys: &[String]) -> Result<()> {
        self.data.retain(|key, _| !keys.contains(key));
        Ok(())
    }

    pub fn count(&self) -> Result<usize> {
        let count = self.data.keys().count();
        Ok(count)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.data.clear();
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<(&String, &String)>> {
        let key_value: Vec<(&String, &String)> = self.data.iter().collect();

        Ok(key_value)
    }
}
