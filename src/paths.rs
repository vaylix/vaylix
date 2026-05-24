use anyhow::{Result, anyhow};
use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

pub struct VeyraPaths {
    pub data_dir: PathBuf,
}

impl VeyraPaths {
    pub fn new() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "veyra", "veyra")
            .ok_or_else(|| anyhow!("Could not determine project directories"))?;

        let data_dir = dirs.data_dir().to_path_buf();

        fs::create_dir_all(&data_dir)?;

        Ok(Self { data_dir })
    }
}
