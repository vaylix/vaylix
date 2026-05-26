use crate::Result;
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::Path,
};

/// Saves snapshot bytes atomically using a temporary file and rename.
pub fn save(data: &[u8], path: &Path, temp_path: &Path) -> Result<()> {
    let mut file = File::create(temp_path)?;
    file.write_all(data)?;
    file.sync_all()?;
    fs::rename(temp_path, path)?;
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Loads raw snapshot bytes when a snapshot exists.
pub fn load(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{load, save};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("veyra-{name}-{unique}.bin"))
    }

    #[test]
    fn saves_and_loads_snapshot_bytes() {
        let path = temp_path("snapshot");
        let temp_path = temp_path("snapshot-tmp");
        let payload = b"snapshot-bytes";

        save(payload, &path, &temp_path).unwrap();
        let loaded = load(&path).unwrap();

        assert_eq!(loaded.as_deref(), Some(payload.as_slice()));

        fs::remove_file(path).ok();
        fs::remove_file(temp_path).ok();
    }

    #[test]
    fn returns_none_for_missing_snapshot() {
        let path = temp_path("missing-snapshot");
        assert_eq!(load(&path).unwrap(), None);
    }
}
