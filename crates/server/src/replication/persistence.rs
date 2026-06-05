use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use super::{
    PersistedReplicationState, ReplicationConfig, ReplicationState, assert_runtime_invariants,
};
use crate::error::{Result, ServerError};

pub(super) fn load_persisted_state(path: &PathBuf) -> Result<Option<PersistedReplicationState>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| ServerError::InvalidArguments(err.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

pub(super) fn persist_state(config: &ReplicationConfig, state: &ReplicationState) -> Result<()> {
    assert_runtime_invariants(state);
    let persisted = PersistedReplicationState {
        current_term: state.current_term,
        voted_for: state.voted_for.clone(),
        members: state.members.values().cloned().collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))?;
    if let Some(parent) = config.state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(&config.state_tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&config.state_tmp_path, &config.state_path)?;
    sync_parent_dir(&config.state_path)?;
    File::open(&config.state_path)?.sync_all()?;
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
