mod config;
mod engine;
mod error;
mod paths;
mod store;

pub use config::{EngineOptions, StorageKey, StorageKeyring, WalSyncPolicy};
pub use engine::{
    Engine, EngineMetadata, EngineState, Expiration, LogicalBackup, LogicalBackupEntry, ScanPage,
    SetCondition, SetOptions, SetOutcome, StorageEngine, TransactionResult,
};
pub use error::{EngineError, Result};
pub use paths::Paths;
pub use store::crypto::{decrypt as storage_decrypt, encrypt as storage_encrypt};
pub use store::{load_or_create_keyring, rotate_keyring_if_due};
