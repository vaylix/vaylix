use crate::engine::EngineState;
use anyhow::Result;
use postcard::{from_bytes, to_allocvec};

pub fn serialize(data: &EngineState) -> Result<Vec<u8>> {
    let serialized = to_allocvec(data)?;

    Ok(serialized)
}

pub fn deserialize(bytes: &[u8]) -> Result<EngineState> {
    let deserialized: EngineState = from_bytes(bytes)?;

    Ok(deserialized)
}
