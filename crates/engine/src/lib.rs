mod config;
mod engine;
mod error;
mod paths;
mod store;

pub use config::{
    DEFAULT_WAL_RETAIN_SEGMENTS, DEFAULT_WAL_SEGMENT_SIZE_BYTES, EngineOptions, StorageKey,
    StorageKeyring, WalSyncPolicy,
};
pub use engine::{
    Engine, EngineMetadata, EngineState, Expiration, LogicalBackup, LogicalBackupEntry,
    PointInTimeTarget, ReplicationSnapshot, ScanPage, SetCondition, SetOptions, SetOutcome,
    StorageEngine, StorageInspection, TransactionResult,
};
pub use error::{EngineError, Result};
pub use paths::Paths;
pub use store::crypto::{decrypt as storage_decrypt, encrypt as storage_encrypt};
pub use store::{
    WalEntry, WalOperation, WalReplay, WalReplayTarget, WalSegmentReport, create_active_segment,
    inspect_wal, load_keyring, load_or_create_keyring, migrate_legacy_wal, prune_sealed_segments,
    prune_sealed_segments_with_floor, replay_until, rotate_keyring_if_due, save_keyring,
    seal_active, write_entries,
};
