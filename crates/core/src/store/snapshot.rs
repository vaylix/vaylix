use anyhow::{Result, bail};
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
            _ => bail!(err),
        },
    }
}
