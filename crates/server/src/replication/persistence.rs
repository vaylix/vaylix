use std::fs;
use std::path::PathBuf;

use super::{PersistedReplicationState, ReplicationConfig, ReplicationState};
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
    fs::write(&config.state_tmp_path, bytes)?;
    fs::rename(&config.state_tmp_path, &config.state_path)?;
    Ok(())
}
