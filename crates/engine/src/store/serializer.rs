use crate::engine::EngineState;
use crate::{EngineError, Result};

use super::binary;

/// Serializes the current engine state into snapshot bytes.
pub fn serialize(data: &EngineState) -> Result<Vec<u8>> {
    let serialized =
        binary::encode(data).map_err(|err| EngineError::SnapshotSerialize(err.to_string()))?;

    Ok(serialized)
}

/// Deserializes snapshot bytes back into an engine state.
pub fn deserialize(bytes: &[u8]) -> Result<EngineState> {
    let deserialized: EngineState =
        binary::decode(bytes).map_err(|err| EngineError::SnapshotDeserialize(err.to_string()))?;

    Ok(deserialized)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{deserialize, serialize};
    use crate::EngineState;
    use crate::engine::EngineStore;
    use crate::engine::state::EngineMetadata;

    #[test]
    fn serializes_and_deserializes_engine_state() {
        let mut data = BTreeMap::new();
        data.insert("name".to_string(), "alice".to_string());
        let mut expirations = BTreeMap::new();
        expirations.insert("name".to_string(), 500);

        let state = EngineState {
            store: EngineStore::from_parts(data, expirations),
            metadata: EngineMetadata {
                version: 2,
                created_at_ms: 123,
                updated_at_ms: 456,
                last_snapshot_at_ms: Some(789),
                last_applied_sequence: 9,
            },
        };

        let bytes = serialize(&state).unwrap();
        let decoded = deserialize(&bytes).unwrap();

        assert_eq!(decoded.raw_value("name").as_deref(), Some("alice"));
        assert_eq!(decoded.raw_expiration("name"), Some(500));
        assert_eq!(decoded.metadata.created_at_ms, 123);
        assert_eq!(decoded.metadata.updated_at_ms, 456);
        assert_eq!(decoded.metadata.last_snapshot_at_ms, Some(789));
        assert_eq!(decoded.metadata.last_applied_sequence, 9);
    }
}
