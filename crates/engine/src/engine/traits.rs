use crate::Result;

pub trait StorageEngine {
    fn get(&self, key: &str) -> Result<Option<String>>;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn delete(&mut self, key: &str) -> Result<()>;

    fn delete_many(&mut self, keys: &[String]) -> Result<()>;

    fn exists(&self, key: &str) -> Result<bool>;

    fn count(&self) -> Result<usize>;

    fn clear(&mut self) -> Result<()>;

    fn list(&self) -> Result<Vec<(String, String)>>;

    fn snapshot(&self) -> Result<()>;
}
