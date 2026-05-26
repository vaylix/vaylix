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

/// Engine configuration used when opening the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EngineOptions {
    /// WAL durability mode.
    pub wal_sync: WalSyncPolicy,
}
