use crate::error::{ClientError, Result};
use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

pub struct Paths {
    pub history_path: PathBuf,
}

impl Paths {
    pub fn new() -> Result<Self> {
        let dirs = ProjectDirs::from("dev", "veyra", "veyra")
            .ok_or(ClientError::ProjectDirsUnavailable)?;

        let data_dir = dirs.data_dir().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        Ok(Self {
            history_path: data_dir.join("history.txt"),
        })
    }
}
