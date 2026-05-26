use anyhow::{Result, anyhow};
use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

pub struct Paths {
    pub history_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub wal_path: PathBuf,
}

impl Paths {
    pub fn new() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "veyra", "veyra")
            .ok_or_else(|| anyhow!("Could not determine project directories"))?;

        let data_dir = dirs.data_dir().to_path_buf();

        fs::create_dir_all(&data_dir)?;

        let history_path = data_dir.join("history.txt");

        let snapshot_path = data_dir.join("snapshot.bin");

        let wal_path = data_dir.join("wal.log");

        Ok(Self {
            history_path,
            snapshot_path,
            wal_path,
        })
    }
}
