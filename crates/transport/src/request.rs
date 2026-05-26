use bytes::{Buf, BufMut, BytesMut};
use command::{Command, Expiration, SetCondition, SetOptions};
use uuid::Uuid;

use crate::error::{Result, TransportError};
use crate::opcode::Opcode;

/// A decoded client request without outer frame bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub request_id: Uuid,
    pub opcode: Opcode,
    pub payload: Vec<u8>,
}

impl Request {
    /// Builds a raw request from its request id, opcode, and payload.
    pub fn new(request_id: Uuid, opcode: Opcode, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            opcode,
            payload,
        }
    }

    /// Encodes a parsed command into a transport request.
    pub fn from_command(request_id: Uuid, command: Command) -> Result<Self> {
        match command {
            Command::Auth { username, password } => Ok(Self::new(
                request_id,
                Opcode::Auth,
                encode_key_value(&username, &password)?,
            )),
            Command::Ping { message } => Ok(Self::new(
                request_id,
                Opcode::Ping,
                encode_optional_string(message.as_deref())?,
            )),
            Command::Get { key } => Ok(Self::new(request_id, Opcode::Get, encode_key(&key)?)),
            Command::GetDel { key } => Ok(Self::new(request_id, Opcode::GetDel, encode_key(&key)?)),
            Command::GetEx {
                key,
                expiration,
                persist,
            } => Ok(Self::new(
                request_id,
                Opcode::GetEx,
                encode_get_ex(&key, expiration, persist)?,
            )),
            Command::Set {
                key,
                value,
                options,
            } => Ok(Self::new(
                request_id,
                Opcode::Set,
                encode_set(&key, &value, &options)?,
            )),
            Command::SetNx { key, value } => Ok(Self::new(
                request_id,
                Opcode::SetNx,
                encode_key_value(&key, &value)?,
            )),
            Command::MGet { keys } => Ok(Self::new(request_id, Opcode::MGet, encode_keys(&keys)?)),
            Command::MSet { entries } => {
                Ok(Self::new(request_id, Opcode::MSet, encode_pairs(&entries)?))
            }
            Command::Delete { keys } => {
                Ok(Self::new(request_id, Opcode::Delete, encode_keys(&keys)?))
            }
            Command::Exists { key } => Ok(Self::new(request_id, Opcode::Exists, encode_key(&key)?)),
            Command::Incr { key } => Ok(Self::new(request_id, Opcode::Incr, encode_key(&key)?)),
            Command::Decr { key } => Ok(Self::new(request_id, Opcode::Decr, encode_key(&key)?)),
            Command::Expire { key, seconds } => Ok(Self::new(
                request_id,
                Opcode::Expire,
                encode_key_u64(&key, seconds)?,
            )),
            Command::Ttl { key } => Ok(Self::new(request_id, Opcode::Ttl, encode_key(&key)?)),
            Command::Persist { key } => {
                Ok(Self::new(request_id, Opcode::Persist, encode_key(&key)?))
            }
            Command::Rename {
                source,
                destination,
            } => Ok(Self::new(
                request_id,
                Opcode::Rename,
                encode_key_value(&source, &destination)?,
            )),
            Command::RenameNx {
                source,
                destination,
            } => Ok(Self::new(
                request_id,
                Opcode::RenameNx,
                encode_key_value(&source, &destination)?,
            )),
            Command::Scan {
                cursor,
                pattern,
                count,
            } => Ok(Self::new(
                request_id,
                Opcode::Scan,
                encode_scan(cursor, pattern.as_deref(), count)?,
            )),
            Command::DbSize => Ok(Self::new(request_id, Opcode::DbSize, Vec::new())),
            Command::Info => Ok(Self::new(request_id, Opcode::Info, Vec::new())),
            Command::Metrics => Ok(Self::new(request_id, Opcode::Metrics, Vec::new())),
            Command::List => Ok(Self::new(request_id, Opcode::List, Vec::new())),
            Command::Clear => Ok(Self::new(request_id, Opcode::Clear, Vec::new())),
            Command::Count => Ok(Self::new(request_id, Opcode::Count, Vec::new())),
            Command::Save => Ok(Self::new(request_id, Opcode::Save, Vec::new())),
            Command::Snapshot => Ok(Self::new(request_id, Opcode::Snapshot, Vec::new())),
            Command::Multi => Ok(Self::new(request_id, Opcode::Multi, Vec::new())),
            Command::Exec => Ok(Self::new(request_id, Opcode::Exec, Vec::new())),
            Command::Discard => Ok(Self::new(request_id, Opcode::Discard, Vec::new())),
            Command::Help => Err(TransportError::UnsupportedCommand("help")),
            Command::Exit => Err(TransportError::UnsupportedCommand("exit")),
        }
    }

    /// Decodes this request back into a parsed command.
    pub fn into_command(self) -> Result<Command> {
        match self.opcode {
            Opcode::Auth => {
                let (username, password) = decode_key_value(&self.payload)?;
                Ok(Command::Auth { username, password })
            }
            Opcode::Ping => Ok(Command::Ping {
                message: decode_optional_string(&self.payload)?,
            }),
            Opcode::Get => Ok(Command::Get {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::GetDel => Ok(Command::GetDel {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::GetEx => {
                let (key, expiration, persist) = decode_get_ex(&self.payload)?;
                Ok(Command::GetEx {
                    key,
                    expiration,
                    persist,
                })
            }
            Opcode::Set => {
                let (key, value, options) = decode_set(&self.payload)?;
                Ok(Command::Set {
                    key,
                    value,
                    options,
                })
            }
            Opcode::SetNx => {
                let (key, value) = decode_key_value(&self.payload)?;
                Ok(Command::SetNx { key, value })
            }
            Opcode::MGet => Ok(Command::MGet {
                keys: decode_keys(&self.payload)?,
            }),
            Opcode::MSet => Ok(Command::MSet {
                entries: decode_pairs(&self.payload)?,
            }),
            Opcode::Delete => Ok(Command::Delete {
                keys: decode_keys(&self.payload)?,
            }),
            Opcode::Exists => Ok(Command::Exists {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Incr => Ok(Command::Incr {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Decr => Ok(Command::Decr {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Expire => {
                let (key, seconds) = decode_key_u64(&self.payload)?;
                Ok(Command::Expire { key, seconds })
            }
            Opcode::Ttl => Ok(Command::Ttl {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Persist => Ok(Command::Persist {
                key: decode_single_key(&self.payload)?,
            }),
            Opcode::Rename => {
                let (source, destination) = decode_key_value(&self.payload)?;
                Ok(Command::Rename {
                    source,
                    destination,
                })
            }
            Opcode::RenameNx => {
                let (source, destination) = decode_key_value(&self.payload)?;
                Ok(Command::RenameNx {
                    source,
                    destination,
                })
            }
            Opcode::Scan => {
                let (cursor, pattern, count) = decode_scan(&self.payload)?;
                Ok(Command::Scan {
                    cursor,
                    pattern,
                    count,
                })
            }
            Opcode::DbSize => decode_empty(&self.payload).map(|()| Command::DbSize),
            Opcode::Info => decode_empty(&self.payload).map(|()| Command::Info),
            Opcode::Metrics => decode_empty(&self.payload).map(|()| Command::Metrics),
            Opcode::List => decode_empty(&self.payload).map(|()| Command::List),
            Opcode::Clear => decode_empty(&self.payload).map(|()| Command::Clear),
            Opcode::Count => decode_empty(&self.payload).map(|()| Command::Count),
            Opcode::Save => decode_empty(&self.payload).map(|()| Command::Save),
            Opcode::Snapshot => decode_empty(&self.payload).map(|()| Command::Snapshot),
            Opcode::Multi => decode_empty(&self.payload).map(|()| Command::Multi),
            Opcode::Exec => decode_empty(&self.payload).map(|()| Command::Exec),
            Opcode::Discard => decode_empty(&self.payload).map(|()| Command::Discard),
        }
    }
}

fn encode_key(key: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    Ok(buf.to_vec())
}

fn encode_set(key: &str, value: &str, options: &SetOptions) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    put_string_u32(&mut buf, value)?;
    buf.put_u8(encode_condition(options.condition));
    encode_expiration(&mut buf, options.expiration)?;
    buf.put_u8(u8::from(options.keep_ttl));
    buf.put_u8(u8::from(options.return_previous));
    Ok(buf.to_vec())
}

fn encode_get_ex(key: &str, expiration: Option<Expiration>, persist: bool) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    encode_expiration(&mut buf, expiration)?;
    buf.put_u8(u8::from(persist));
    Ok(buf.to_vec())
}

fn encode_key_value(key: &str, value: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    put_string_u32(&mut buf, value)?;
    Ok(buf.to_vec())
}

fn encode_key_u64(key: &str, value: u64) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, key)?;
    buf.put_u64(value);
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

fn encode_pairs(entries: &[(String, String)]) -> Result<Vec<u8>> {
    let pair_count = u16::try_from(entries.len()).map_err(|_| TransportError::CorruptedPayload)?;
    let mut buf = BytesMut::new();
    buf.put_u16(pair_count);

    for (key, value) in entries {
        put_string_u16(&mut buf, key)?;
        put_string_u32(&mut buf, value)?;
    }

    Ok(buf.to_vec())
}

fn encode_optional_string(value: Option<&str>) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    match value {
        Some(value) => {
            buf.put_u8(1);
            put_string_u32(&mut buf, value)?;
        }
        None => buf.put_u8(0),
    }
    Ok(buf.to_vec())
}

fn encode_scan(cursor: u64, pattern: Option<&str>, count: Option<u16>) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_u64(cursor);
    match pattern {
        Some(pattern) => {
            buf.put_u8(1);
            put_string_u16(&mut buf, pattern)?;
        }
        None => buf.put_u8(0),
    }
    match count {
        Some(count) => {
            buf.put_u8(1);
            buf.put_u16(count);
        }
        None => buf.put_u8(0),
    }
    Ok(buf.to_vec())
}

fn encode_condition(condition: Option<SetCondition>) -> u8 {
    match condition {
        None => 0,
        Some(SetCondition::Nx) => 1,
        Some(SetCondition::Xx) => 2,
    }
}

fn encode_expiration(buf: &mut BytesMut, expiration: Option<Expiration>) -> Result<()> {
    match expiration {
        None => buf.put_u8(0),
        Some(Expiration::Ex(value)) => {
            buf.put_u8(1);
            buf.put_u64(value);
        }
        Some(Expiration::Px(value)) => {
            buf.put_u8(2);
            buf.put_u64(value);
        }
    }
    Ok(())
}

fn decode_single_key(payload: &[u8]) -> Result<String> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    ensure_empty(buf)?;
    Ok(key)
}

fn decode_set(payload: &[u8]) -> Result<(String, String, SetOptions)> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    let value = read_string_u32(&mut buf)?;
    let condition = decode_condition(&mut buf)?;
    let expiration = decode_expiration(&mut buf)?;
    let keep_ttl = read_bool(&mut buf)?;
    let return_previous = read_bool(&mut buf)?;
    ensure_empty(buf)?;

    Ok((
        key,
        value,
        SetOptions {
            condition,
            expiration,
            keep_ttl,
            return_previous,
        },
    ))
}

fn decode_get_ex(payload: &[u8]) -> Result<(String, Option<Expiration>, bool)> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    let expiration = decode_expiration(&mut buf)?;
    let persist = read_bool(&mut buf)?;
    ensure_empty(buf)?;
    Ok((key, expiration, persist))
}

fn decode_key_value(payload: &[u8]) -> Result<(String, String)> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    let value = read_string_u32(&mut buf)?;
    ensure_empty(buf)?;
    Ok((key, value))
}

fn decode_key_u64(payload: &[u8]) -> Result<(String, u64)> {
    let mut buf = payload;
    let key = read_string_u16(&mut buf)?;
    if buf.remaining() < 8 {
        return Err(TransportError::UnexpectedEof);
    }
    let value = buf.get_u64();
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

fn decode_pairs(payload: &[u8]) -> Result<Vec<(String, String)>> {
    let mut buf = payload;
    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }

    let pair_count = buf.get_u16() as usize;
    let mut entries = Vec::with_capacity(pair_count);

    for _ in 0..pair_count {
        entries.push((read_string_u16(&mut buf)?, read_string_u32(&mut buf)?));
    }

    ensure_empty(buf)?;
    Ok(entries)
}

fn decode_optional_string(payload: &[u8]) -> Result<Option<String>> {
    let mut buf = payload;
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    let flag = buf.get_u8();
    match flag {
        0 => {
            ensure_empty(buf)?;
            Ok(None)
        }
        1 => {
            let value = read_string_u32(&mut buf)?;
            ensure_empty(buf)?;
            Ok(Some(value))
        }
        _ => Err(TransportError::CorruptedPayload),
    }
}

fn decode_scan(payload: &[u8]) -> Result<(u64, Option<String>, Option<u16>)> {
    let mut buf = payload;
    if buf.remaining() < 10 {
        return Err(TransportError::UnexpectedEof);
    }

    let cursor = buf.get_u64();
    let pattern = match buf.get_u8() {
        0 => None,
        1 => Some(read_string_u16(&mut buf)?),
        _ => return Err(TransportError::CorruptedPayload),
    };
    let count = match buf.get_u8() {
        0 => None,
        1 => {
            if buf.remaining() < 2 {
                return Err(TransportError::UnexpectedEof);
            }
            Some(buf.get_u16())
        }
        _ => return Err(TransportError::CorruptedPayload),
    };

    ensure_empty(buf)?;
    Ok((cursor, pattern, count))
}

fn decode_condition(buf: &mut &[u8]) -> Result<Option<SetCondition>> {
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    match buf.get_u8() {
        0 => Ok(None),
        1 => Ok(Some(SetCondition::Nx)),
        2 => Ok(Some(SetCondition::Xx)),
        _ => Err(TransportError::CorruptedPayload),
    }
}

fn decode_expiration(buf: &mut &[u8]) -> Result<Option<Expiration>> {
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    match buf.get_u8() {
        0 => Ok(None),
        1 => {
            if buf.remaining() < 8 {
                return Err(TransportError::UnexpectedEof);
            }
            Ok(Some(Expiration::Ex(buf.get_u64())))
        }
        2 => {
            if buf.remaining() < 8 {
                return Err(TransportError::UnexpectedEof);
            }
            Ok(Some(Expiration::Px(buf.get_u64())))
        }
        _ => Err(TransportError::CorruptedPayload),
    }
}

fn read_bool(buf: &mut &[u8]) -> Result<bool> {
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    match buf.get_u8() {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TransportError::CorruptedPayload),
    }
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
    use command::{Command, Expiration, SetCondition, SetOptions};
    use uuid::Uuid;

    use super::Request;
    use crate::{Opcode, TransportError};

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[test]
    fn request_command_round_trip() {
        let commands = vec![
            Command::Ping { message: None },
            Command::Get {
                key: "name".to_string(),
            },
            Command::GetDel {
                key: "name".to_string(),
            },
            Command::GetEx {
                key: "session".to_string(),
                expiration: Some(Expiration::Px(1500)),
                persist: false,
            },
            Command::Set {
                key: "name".to_string(),
                value: "John Doe".to_string(),
                options: SetOptions {
                    condition: Some(SetCondition::Nx),
                    expiration: Some(Expiration::Ex(60)),
                    keep_ttl: false,
                    return_previous: true,
                },
            },
            Command::SetNx {
                key: "name".to_string(),
                value: "John Doe".to_string(),
            },
            Command::MGet {
                keys: vec!["one".to_string(), "two".to_string()],
            },
            Command::MSet {
                entries: vec![("one".to_string(), "1".to_string())],
            },
            Command::Delete {
                keys: vec!["one".to_string(), "two".to_string()],
            },
            Command::Exists {
                key: "name".to_string(),
            },
            Command::Incr {
                key: "counter".to_string(),
            },
            Command::Decr {
                key: "counter".to_string(),
            },
            Command::Expire {
                key: "token".to_string(),
                seconds: 60,
            },
            Command::Ttl {
                key: "token".to_string(),
            },
            Command::Persist {
                key: "token".to_string(),
            },
            Command::Scan {
                cursor: 0,
                pattern: Some("user:*".to_string()),
                count: Some(10),
            },
            Command::DbSize,
            Command::Info,
            Command::List,
            Command::Clear,
            Command::Count,
            Command::Save,
            Command::Snapshot,
        ];

        for command in commands {
            let request = Request::from_command(id(7), command.clone()).unwrap();
            assert_eq!(request.into_command().unwrap(), command);
        }
    }

    #[test]
    fn rejects_local_only_commands() {
        assert!(matches!(
            Request::from_command(id(1), Command::Help),
            Err(TransportError::UnsupportedCommand("help"))
        ));
        assert!(matches!(
            Request::from_command(id(1), Command::Exit),
            Err(TransportError::UnsupportedCommand("exit"))
        ));
    }

    #[test]
    fn rejects_corrupted_payloads_for_command_decoding() {
        let request = Request::new(id(1), Opcode::Get, vec![0, 4, b'n', b'a']);
        assert!(matches!(
            request.into_command(),
            Err(TransportError::UnexpectedEof)
        ));

        let request = Request::new(id(2), Opcode::List, vec![1]);
        assert!(matches!(
            request.into_command(),
            Err(TransportError::CorruptedPayload)
        ));
    }
}
