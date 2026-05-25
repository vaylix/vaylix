use anyhow::Result;
use postcard::{Error, from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    path::PathBuf,
};

const MAX_WAL_ENTRY_SIZE: u32 = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Set { key: String, value: String },

    Delete { key: String },

    Clear,
}

pub fn append(entry: &WalEntry, path: &PathBuf) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;

    let bytes = to_allocvec(entry)?;

    let length = bytes.len() as u32;

    file.write_all(&length.to_le_bytes())?;

    file.write_all(&bytes)?;

    file.flush()?;

    Ok(())
}

pub fn replay(path: &PathBuf) -> Result<Vec<WalEntry>> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,

        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                return Ok(Vec::new());
            }

            return Err(err.into());
        }
    };

    let mut entries = Vec::new();

    loop {
        let mut length_buf = [0u8; 4];

        match file.read_exact(&mut length_buf) {
            Ok(_) => {}

            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }

                return Err(err.into());
            }
        }

        let length = u32::from_le_bytes(length_buf);

        if length == 0 || length > MAX_WAL_ENTRY_SIZE {
            break;
        }

        let mut entry_buf = vec![0u8; length as usize];

        match file.read_exact(&mut entry_buf) {
            Ok(_) => {}

            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }

                return Err(err.into());
            }
        }

        let entry: WalEntry = match from_bytes(&entry_buf) {
            Ok(value) => value,

            Err(Error::DeserializeUnexpectedEnd) => {
                break;
            }

            Err(err) => {
                return Err(err.into());
            }
        };

        entries.push(entry);
    }

    Ok(entries)
}

pub fn truncate(path: &PathBuf) -> Result<()> {
    let file = OpenOptions::new().create(true).write(true).open(path)?;

    file.set_len(0)?;

    Ok(())
}
