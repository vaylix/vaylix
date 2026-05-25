use serde_json::{from_slice, to_vec};
use std::collections::HashMap;

use anyhow::Result;

pub fn serialize(data: &HashMap<String, String>) -> Result<Vec<u8>> {
    let serialized = to_vec(data)?;

    Ok(serialized)
}

pub fn deserialize(bytes: &[u8]) -> Result<HashMap<String, String>> {
    let deserialized: HashMap<String, String> = from_slice(bytes)?;

    Ok(deserialized)
}
