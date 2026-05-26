use crate::{EngineError, Result};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::io::Write;
use std::path::Path;

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
}

/// Saves a manifest atomically using a temporary file and rename.
pub fn save(manifest: &Manifest, path: &Path, temp_path: &Path) -> Result<()> {
    let bytes = to_allocvec(manifest).map_err(EngineError::ManifestSerialize)?;
    let mut file = File::create(temp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temp_path, path)?;
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Loads the manifest when one exists.
pub fn load(path: &Path) -> Result<Option<Manifest>> {
    match fs::read(path) {
        Ok(bytes) => {
            let manifest = from_bytes(&bytes).map_err(EngineError::ManifestDeserialize)?;
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
        std::env::temp_dir().join(format!("veyra-{name}-{unique}.bin"))
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
        };

        save(&manifest, &path, &temp_path).unwrap();
        let loaded = load(&path).unwrap().unwrap();

        assert_eq!(loaded, manifest);

        fs::remove_file(path).ok();
        fs::remove_file(temp_path).ok();
    }
}
