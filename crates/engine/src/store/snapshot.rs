use crate::Result;
use std::{
    fs::{read, write},
    io::ErrorKind,
    path::PathBuf,
};

pub fn save(data: &[u8], path: &PathBuf) -> Result<()> {
    write(path, data)?;

    Ok(())
}

pub fn load(path: &PathBuf) -> Result<Option<Vec<u8>>> {
    match read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) => match err.kind() {
            ErrorKind::NotFound => Ok(None),
            _ => Err(err.into()),
        },
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
        let payload = b"snapshot-bytes";

        save(payload, &path).unwrap();
        let loaded = load(&path).unwrap();

        assert_eq!(loaded.as_deref(), Some(payload.as_slice()));

        fs::remove_file(path).ok();
    }

    #[test]
    fn returns_none_for_missing_snapshot() {
        let path = temp_path("missing-snapshot");
        assert_eq!(load(&path).unwrap(), None);
    }
}
