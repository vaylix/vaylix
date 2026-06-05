use crate::{EngineError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use super::{binary, durable};

/// Current durable storage serialization format.
///
/// Bump this when WAL, snapshot, manifest, keyring, or value-version
/// encoding changes in a way an older binary cannot safely read. Add an
/// explicit migration path rather than silently accepting unknown formats.
pub const STORAGE_FORMAT_VERSION: u32 = 3;

/// Metadata persisted alongside snapshots to describe the durable baseline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    /// Durable storage envelope version for snapshot and WAL compatibility.
    pub storage_format_version: u32,
    /// Engine schema version captured by the snapshot.
    pub engine_version: u32,
    /// Highest WAL sequence number included in the snapshot.
    pub last_snapshot_sequence: u64,
    /// Snapshot completion time in unix milliseconds.
    pub last_snapshot_at_ms: u64,
    /// Snapshot payload size in bytes.
    pub snapshot_size_bytes: u64,
    /// CRC32 checksum of the durable snapshot payload.
    pub snapshot_checksum: u32,
    /// Starting sequence of the next active WAL segment.
    pub active_wal_start_sequence: u64,
    /// Oldest retained WAL sequence still available for PITR.
    pub oldest_retained_sequence: u64,
}

/// Saves a manifest atomically using a temporary file and rename.
pub fn save(manifest: &Manifest, path: &Path, temp_path: &Path) -> Result<()> {
    let bytes =
        binary::encode(manifest).map_err(|err| EngineError::ManifestSerialize(err.to_string()))?;
    durable::atomic_replace(path, temp_path, &bytes)
}

/// Loads the manifest when one exists.
pub fn load(path: &Path) -> Result<Option<Manifest>> {
    match fs::read(path) {
        Ok(bytes) => {
            let manifest = binary::decode(&bytes)
                .map_err(|err| EngineError::ManifestDeserialize(err.to_string()))?;
            Ok(Some(manifest))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{Manifest, load, save};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-{name}-{unique}.bin"))
    }

    #[test]
    fn saves_and_loads_manifest() {
        let path = temp_path("manifest");
        let temp_path = temp_path("manifest-tmp");
        let manifest = Manifest {
            storage_format_version: 1,
            engine_version: 2,
            last_snapshot_sequence: 44,
            last_snapshot_at_ms: 999,
            snapshot_size_bytes: 123,
            snapshot_checksum: 991,
            active_wal_start_sequence: 45,
            oldest_retained_sequence: 12,
        };

        save(&manifest, &path, &temp_path).unwrap();
        let loaded = load(&path).unwrap().unwrap();

        assert_eq!(loaded, manifest);

        fs::remove_file(path).ok();
        fs::remove_file(temp_path).ok();
    }
}
