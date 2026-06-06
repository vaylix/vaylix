use crate::Result;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
};

/// Atomically replaces a durable file.
///
/// Invariant: callers must pass a temporary path on the same filesystem as the
/// final path. The function fsyncs the temporary file, renames it into place,
/// fsyncs the parent directory on Unix, then fsyncs the final file. That order
/// prevents accepting a torn metadata update after process or host crash.
pub(super) fn atomic_replace(path: &Path, temp_path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(temp_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(temp_path, path)?;
    sync_parent_dir(path)?;
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_: &Path) -> Result<()> {
    Ok(())
}
