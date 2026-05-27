use serde::{Serialize, de::DeserializeOwned};

pub fn encode<T>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
where
    T: Serialize,
{
    rmp_serde::to_vec_named(value)
}

pub fn decode<T>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error>
where
    T: DeserializeOwned,
{
    rmp_serde::from_slice(bytes)
}
