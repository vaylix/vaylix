use bytes::{Buf, BufMut, BytesMut};
use command::Command;

use crate::error::{Result, TransportError};
use crate::opcode::Opcode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub request_id: u32,
    pub opcode: Opcode,
    pub payload: Vec<u8>,
}

impl Request {
    pub fn new(request_id: u32, opcode: Opcode, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            opcode,
            payload,
        }
    }

    pub fn from_command(request_id: u32, command: Command) -> Result<Self> {
        match command {
            Command::Get { key } => Ok(Self::new(request_id, Opcode::Get, encode_key(&key)?)),
            Command::Set { key, value } => Ok(Self::new(
                request_id,
                Opcode::Set,
                encode_key_value(&key, &value)?,
            )),
            Command::Delete { keys } => {
                Ok(Self::new(request_id, Opcode::Delete, encode_keys(&keys)?))
            }
            Command::Exists { key } => Ok(Self::new(request_id, Opcode::Exists, encode_key(&key)?)),
            Command::List => Ok(Self::new(request_id, Opcode::List, Vec::new())),
            Command::Clear => Ok(Self::new(request_id, Opcode::Clear, Vec::new())),
            Command::Count => Ok(Self::new(request_id, Opcode::Count, Vec::new())),
            Command::Snapshot => Ok(Self::new(request_id, Opcode::Snapshot, Vec::new())),
            Command::Help => Err(TransportError::UnsupportedCommand("help")),
            Command::Exit => Err(TransportError::UnsupportedCommand("exit")),
        }
    }

    pub fn into_command(self) -> Result<Command> {
        match self.opcode {
            Opcode::Get => Ok(Command::Get {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Set => {
                let (key, value) = decode_key_value(&self.payload)?;
                Ok(Command::Set { key, value })
            }
            Opcode::Delete => Ok(Command::Delete {
                keys: decode_keys(&self.payload)?,
            }),
            Opcode::Exists => Ok(Command::Exists {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::List => decode_empty(&self.payload).map(|()| Command::List),
            Opcode::Clear => decode_empty(&self.payload).map(|()| Command::Clear),
            Opcode::Count => decode_empty(&self.payload).map(|()| Command::Count),
            Opcode::Snapshot => decode_empty(&self.payload).map(|()| Command::Snapshot),
        }
    }
}

fn encode_key(key: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    Ok(buf.to_vec())
}

fn encode_key_value(key: &str, value: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    put_string_u32(&mut buf, value)?;
    Ok(buf.to_vec())
}

fn encode_keys(keys: &[String]) -> Result<Vec<u8>> {
    let key_count = u16::try_from(keys.len()).map_err(|_| TransportError::CorruptedPayload)?;
    let mut buf = BytesMut::new();
    buf.put_u16(key_count);

    for key in keys {
        put_string_u16(&mut buf, key)?;
    }

    Ok(buf.to_vec())
}

fn decode_single_key(payload: &[u8]) -> Result<String> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    ensure_empty(buf)?;
    Ok(key)
}

fn decode_key_value(payload: &[u8]) -> Result<(String, String)> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    let value = read_string_u32(&mut buf)?;
    ensure_empty(buf)?;
    Ok((key, value))
}

fn decode_keys(payload: &[u8]) -> Result<Vec<String>> {
    let mut buf = payload;

    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }

    let key_count = buf.get_u16() as usize;
    let mut keys = Vec::with_capacity(key_count);

    for _ in 0..key_count {
        keys.push(read_string_u16(&mut buf)?);
    }

    ensure_empty(buf)?;

    Ok(keys)
}

fn decode_empty(payload: &[u8]) -> Result<()> {
    if payload.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CorruptedPayload)
    }
}

fn put_string_u16(buf: &mut BytesMut, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).map_err(|_| TransportError::CorruptedPayload)?;
    buf.put_u16(length);
    buf.extend_from_slice(bytes);
    Ok(())
}

fn put_string_u32(buf: &mut BytesMut, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let length = u32::try_from(bytes.len()).map_err(|_| TransportError::CorruptedPayload)?;
    buf.put_u32(length);
    buf.extend_from_slice(bytes);
    Ok(())
}

fn read_string_u16(buf: &mut &[u8]) -> Result<String> {
    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }

    let length = buf.get_u16() as usize;
    read_string(buf, length)
}

fn read_string_u32(buf: &mut &[u8]) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(TransportError::UnexpectedEof);
    }

    let length = buf.get_u32() as usize;
    read_string(buf, length)
}

fn read_string(buf: &mut &[u8], length: usize) -> Result<String> {
    if buf.remaining() < length {
        return Err(TransportError::UnexpectedEof);
    }

    let bytes = buf.copy_to_bytes(length);
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn ensure_empty(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CorruptedPayload)
    }
}

#[cfg(test)]
mod tests {
    use command::Command;

    use super::Request;
    use crate::{Opcode, TransportError};

    #[test]
    fn request_command_round_trip() {
        let commands = vec![
            Command::Get {
                key: "name".to_string(),
            },
            Command::Set {
                key: "name".to_string(),
                value: "John Doe".to_string(),
            },
            Command::Delete {
                keys: vec!["one".to_string(), "two".to_string()],
            },
            Command::Exists {
                key: "name".to_string(),
            },
            Command::List,
            Command::Clear,
            Command::Count,
            Command::Snapshot,
        ];

        for command in commands {
            let request = Request::from_command(7, command.clone()).unwrap();
            assert_eq!(request.into_command().unwrap(), command);
        }
    }

    #[test]
    fn rejects_local_only_commands() {
        assert!(matches!(
            Request::from_command(1, Command::Help),
            Err(TransportError::UnsupportedCommand("help"))
        ));
        assert!(matches!(
            Request::from_command(1, Command::Exit),
            Err(TransportError::UnsupportedCommand("exit"))
        ));
    }

    #[test]
    fn rejects_corrupted_payloads_for_command_decoding() {
        let request = Request::new(1, Opcode::Get, vec![0, 4, b'n', b'a']);
        assert!(matches!(
            request.into_command(),
            Err(TransportError::UnexpectedEof)
        ));

        let request = Request::new(2, Opcode::List, vec![1]);
        assert!(matches!(
            request.into_command(),
            Err(TransportError::CorruptedPayload)
        ));
    }
}
