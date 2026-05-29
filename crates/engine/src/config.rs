/// Durability policy for write-ahead log appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncPolicy {
    /// Leave durability to the operating system page cache.
    Buffered,
    /// Flush Rust userspace buffers after each append.
    #[default]
    Flush,
    /// Force the kernel to sync written data after each append.
    SyncData,
}

impl WalSyncPolicy {
    /// Returns a stable string representation for diagnostics and CLI wiring.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Flush => "flush",
            Self::SyncData => "sync",
        }
    }
}

pub const DEFAULT_WAL_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_WAL_RETAIN_SEGMENTS: usize = 128;

use uuid::Uuid;

/// A single durable storage-encryption key managed by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageKey {
    pub id: Uuid,
    pub secret: String,
    pub created_at_ms: u64,
}

/// A server-managed keyring used to encrypt and decrypt persisted state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageKeyring {
    pub active: StorageKey,
    pub previous: Vec<StorageKey>,
}

impl StorageKeyring {
    pub fn active(&self) -> &StorageKey {
        &self.active
    }

    pub fn get(&self, id: Uuid) -> Option<&StorageKey> {
        if self.active.id == id {
            Some(&self.active)
        } else {
            self.previous.iter().find(|key| key.id == id)
        }
    }
}

/// Engine configuration used when opening the database.
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// WAL durability mode.
    pub wal_sync: WalSyncPolicy,
    /// Server-managed storage keyring used for WAL and snapshot encryption.
    pub keyring: Option<StorageKeyring>,
    /// Maximum size of one WAL segment before rotating to the next segment.
    pub wal_segment_size_bytes: u64,
    /// Maximum number of sealed WAL segments retained after snapshot pruning.
    pub wal_retain_segments: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            wal_sync: WalSyncPolicy::default(),
            keyring: None,
            wal_segment_size_bytes: DEFAULT_WAL_SEGMENT_SIZE_BYTES,
            wal_retain_segments: DEFAULT_WAL_RETAIN_SEGMENTS,
        }
    }
}
