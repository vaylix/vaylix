use crate::{EngineError, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::{Path, PathBuf};

/// Filesystem layout used by the engine, server, and client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    /// Root data directory for the local node.
    pub data_dir: PathBuf,
    /// Shared readline history path used by the client.
    pub history_path: PathBuf,
    /// Durable snapshot path.
    pub snapshot_path: PathBuf,
    /// Temporary snapshot path used for atomic renames.
    pub snapshot_tmp_path: PathBuf,
    /// Snapshot manifest path.
    pub manifest_path: PathBuf,
    /// Temporary manifest path used for atomic renames.
    pub manifest_tmp_path: PathBuf,
    /// Write-ahead log path.
    pub wal_path: PathBuf,
    /// Storage keyring path used for server-managed encryption keys.
    pub keyring_path: PathBuf,
    /// Temporary keyring path used for atomic updates.
    pub keyring_tmp_path: PathBuf,
    /// Encrypted server authentication and RBAC metadata path.
    pub auth_path: PathBuf,
    /// Temporary authentication metadata path used for atomic updates.
    pub auth_tmp_path: PathBuf,
}

impl Paths {
    /// Builds the default project paths from the operating system data directory.
    pub fn new() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "veyra", "veyra")
            .ok_or(EngineError::ProjectDirsUnavailable)?;
        Self::from_data_dir(dirs.data_dir())
    }

    /// Builds a full path layout rooted at a caller-provided data directory.
    pub fn from_data_dir(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        Ok(Self {
            history_path: data_dir.join("history.txt"),
            snapshot_path: data_dir.join("snapshot.bin"),
            snapshot_tmp_path: data_dir.join("snapshot.bin.tmp"),
            manifest_path: data_dir.join("manifest.bin"),
            manifest_tmp_path: data_dir.join("manifest.bin.tmp"),
            wal_path: data_dir.join("wal.log"),
            keyring_path: data_dir.join("keyring.bin"),
            keyring_tmp_path: data_dir.join("keyring.bin.tmp"),
            auth_path: data_dir.join("auth.bin"),
            auth_tmp_path: data_dir.join("auth.bin.tmp"),
            data_dir,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Paths;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn builds_expected_paths_from_data_dir() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("veyra-paths-{unique}"));
        let paths = Paths::from_data_dir(&root).unwrap();

        assert_eq!(paths.data_dir, root);
        assert!(paths.snapshot_path.ends_with("snapshot.bin"));
        assert!(paths.snapshot_tmp_path.ends_with("snapshot.bin.tmp"));
        assert!(paths.manifest_path.ends_with("manifest.bin"));
        assert!(paths.manifest_tmp_path.ends_with("manifest.bin.tmp"));
        assert!(paths.wal_path.ends_with("wal.log"));
        assert!(paths.keyring_path.ends_with("keyring.bin"));
        assert!(paths.keyring_tmp_path.ends_with("keyring.bin.tmp"));
        assert!(paths.auth_path.ends_with("auth.bin"));
        assert!(paths.auth_tmp_path.ends_with("auth.bin.tmp"));
    }
}
