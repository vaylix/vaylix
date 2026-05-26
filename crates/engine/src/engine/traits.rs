use crate::Result;

/// A single page of keys returned from a cursor-based scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    /// The cursor the caller should pass into the next `scan` request.
    pub next_cursor: u64,
    /// The page of keys returned for the current cursor.
    pub keys: Vec<String>,
}

/// Result of evaluating one command inside an atomic transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionResult {
    Ok,
    NotFound,
    Value(String),
    Boolean(bool),
    Count(u64),
    Integer(i64),
    Entries(Vec<(String, String)>),
    Strings(Vec<Option<String>>),
    Scan(ScanPage),
}

/// Conditional write behavior for `SET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCondition {
    /// Only set a value when the key is currently missing.
    Nx,
    /// Only set a value when the key already exists.
    Xx,
}

/// Expiration to apply in either seconds or milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiration {
    /// Absolute TTL in seconds.
    Seconds(u64),
    /// Absolute TTL in milliseconds.
    Milliseconds(u64),
}

/// Options that control a single `SET` operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SetOptions {
    /// Optional existence constraint for the write.
    pub condition: Option<SetCondition>,
    /// Optional new expiration to apply to the key.
    pub expiration: Option<Expiration>,
    /// Preserve the current TTL when overwriting.
    pub keep_ttl: bool,
}

/// Outcome of a `SET`-family write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetOutcome {
    /// Whether the write was applied.
    pub applied: bool,
    /// The previous value stored at the key, if any.
    pub previous: Option<String>,
}

/// Synchronous storage operations supported by the engine layer.
pub trait StorageEngine {
    /// Returns the value for a key when it exists and has not expired.
    fn get(&mut self, key: &str) -> Result<Option<String>>;

    /// Inserts or replaces the value associated with a key.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set_with_options(key, value, SetOptions::default())
            .map(|_| ())
    }

    /// Inserts or replaces the value using a richer set of write options.
    fn set_with_options(
        &mut self,
        key: String,
        value: String,
        options: SetOptions,
    ) -> Result<SetOutcome>;

    /// Inserts a value only when the key does not already exist.
    fn set_nx(&mut self, key: String, value: String) -> Result<bool> {
        self.set_with_options(
            key,
            value,
            SetOptions {
                condition: Some(SetCondition::Nx),
                expiration: None,
                keep_ttl: false,
            },
        )
        .map(|outcome| outcome.applied)
    }

    /// Returns the value for a key and then deletes it.
    fn get_del(&mut self, key: &str) -> Result<Option<String>>;

    /// Returns the value for a key and optionally updates its expiration.
    fn get_ex(
        &mut self,
        key: &str,
        expiration: Option<Expiration>,
        persist: bool,
    ) -> Result<Option<String>>;

    /// Returns the values for a set of keys in order.
    fn mget(&mut self, keys: &[String]) -> Result<Vec<Option<String>>>;

    /// Inserts or replaces a batch of values atomically.
    fn mset(&mut self, entries: &[(String, String)]) -> Result<()>;

    /// Deletes a single key and returns whether it was removed.
    fn delete(&mut self, key: &str) -> Result<bool>;

    /// Deletes multiple keys atomically and returns how many were removed.
    fn delete_many(&mut self, keys: &[String]) -> Result<usize>;

    /// Returns whether a key exists and has not expired.
    fn exists(&mut self, key: &str) -> Result<bool>;

    /// Increments a stored integer value by one.
    fn incr(&mut self, key: &str) -> Result<i64>;

    /// Decrements a stored integer value by one.
    fn decr(&mut self, key: &str) -> Result<i64>;

    /// Sets an expiration in seconds for an existing key.
    fn expire(&mut self, key: &str, seconds: u64) -> Result<bool>;

    /// Returns the remaining time-to-live in seconds.
    ///
    /// Follows Redis-style semantics:
    /// - `-2` when the key does not exist
    /// - `-1` when the key exists without an expiration
    fn ttl(&mut self, key: &str) -> Result<i64>;

    /// Removes the expiration from a key and returns whether one existed.
    fn persist(&mut self, key: &str) -> Result<bool>;

    /// Renames an existing key to a new destination, replacing any destination value.
    fn rename(&mut self, source: &str, destination: String) -> Result<bool>;

    /// Renames an existing key only when the destination does not exist.
    fn rename_nx(&mut self, source: &str, destination: String) -> Result<bool>;

    /// Returns the total number of live keys.
    fn db_size(&mut self) -> Result<usize>;

    /// Returns the total number of live keys.
    fn count(&mut self) -> Result<usize> {
        self.db_size()
    }

    /// Returns a cursor-based page of keys.
    fn scan(&mut self, cursor: u64, pattern: Option<&str>, count: Option<u16>) -> Result<ScanPage>;

    /// Returns all live key/value pairs currently stored.
    fn list(&mut self) -> Result<Vec<(String, String)>>;

    /// Returns engine metadata useful for operational introspection.
    fn info(&mut self) -> Result<Vec<(String, String)>>;

    /// Actively sweeps expired keys and returns the number of removals.
    fn sweep_expired(&mut self) -> Result<usize>;

    /// Removes every key from the engine.
    fn clear(&mut self) -> Result<()>;

    /// Persists a new snapshot and truncates the WAL.
    fn snapshot(&mut self) -> Result<()>;
}
