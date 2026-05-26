use crate::engine::EngineState;
use crate::{EngineError, Result};
use postcard::{from_bytes, to_allocvec};

pub fn serialize(data: &EngineState) -> Result<Vec<u8>> {
    let serialized = to_allocvec(data).map_err(EngineError::SnapshotSerialize)?;

    Ok(serialized)
}

pub fn deserialize(bytes: &[u8]) -> Result<EngineState> {
    let deserialized: EngineState = from_bytes(bytes).map_err(EngineError::SnapshotDeserialize)?;

    Ok(deserialized)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{deserialize, serialize};
    use crate::EngineState;

    #[test]
    fn serializes_and_deserializes_engine_state() {
        let mut data = HashMap::new();
        data.insert("name".to_string(), "alice".to_string());

        let state = EngineState {
            data,
            created_at: 123,
            version: 1,
        };

        let bytes = serialize(&state).unwrap();
        let decoded = deserialize(&bytes).unwrap();

        assert_eq!(decoded.data.get("name").map(String::as_str), Some("alice"));
        assert_eq!(decoded.created_at, 123);
        assert_eq!(decoded.version, 1);
    }
}
