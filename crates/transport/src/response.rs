use bytes::{Buf, BufMut, BytesMut};
use uuid::Uuid;

use crate::error::{Result, TransportError};

/// Structured remote error metadata returned for failed requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorPayload {
    pub code: String,
    pub name: String,
    pub message: String,
}

/// Cursor-based scan result returned from the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPayload {
    pub next_cursor: u64,
    pub keys: Vec<String>,
}

/// Typed result of one command inside a committed `EXEC` batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecResultPayload {
    Ok,
    NotFound,
    Value(String),
    Boolean(bool),
    Count(u64),
    Integer(i64),
    Entries(Vec<(String, String)>),
    Strings(Vec<Option<String>>),
    Scan(ScanPayload),
}

/// Machine-readable status code for a transport response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0x00,
    Error = 0x01,
    NotFound = 0x02,
}

impl From<Status> for u8 {
    fn from(value: Status) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for Status {
    type Error = TransportError;

    fn try_from(value: u8) -> std::result::Result<Self, TransportError> {
        match value {
            0x00 => Ok(Self::Ok),
            0x01 => Ok(Self::Error),
            0x02 => Ok(Self::NotFound),
            other => Err(TransportError::UnknownStatus(other)),
        }
    }
}

/// A decoded server response without outer frame bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub request_id: Uuid,
    pub status: Status,
    pub payload: Vec<u8>,
}

impl Response {
    /// Builds a raw response from its request id, status, and payload.
    pub fn new(request_id: Uuid, status: Status, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            status,
            payload,
        }
    }

    /// Builds an empty successful response.
    pub fn ok(request_id: Uuid) -> Self {
        Self::new(request_id, Status::Ok, Vec::new())
    }

    /// Builds a not-found response for a missing key.
    pub fn not_found(request_id: Uuid) -> Self {
        Self::new(request_id, Status::NotFound, Vec::new())
    }

    /// Builds a structured error response.
    pub fn error(request_id: Uuid, code: &str, name: &str, message: &str) -> Result<Self> {
        Ok(Self::new(
            request_id,
            Status::Error,
            encode_error_payload(code, name, message)?,
        ))
    }

    /// Builds a response containing a single string value.
    pub fn value(request_id: Uuid, value: &str) -> Result<Self> {
        Ok(Self::new(request_id, Status::Ok, encode_string_u32(value)?))
    }

    /// Builds a response containing a boolean value.
    pub fn boolean(request_id: Uuid, value: bool) -> Self {
        Self::new(request_id, Status::Ok, vec![u8::from(value)])
    }

    /// Builds a response containing an unsigned count value.
    pub fn count(request_id: Uuid, count: u64) -> Self {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(count);
        Self::new(request_id, Status::Ok, buf.to_vec())
    }

    /// Builds a response containing a signed integer value.
    pub fn integer(request_id: Uuid, value: i64) -> Self {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_i64(value);
        Self::new(request_id, Status::Ok, buf.to_vec())
    }

    /// Builds a response containing a list of key/value pairs.
    pub fn entries(request_id: Uuid, entries: &[(String, String)]) -> Result<Self> {
        let entry_count =
            u32::try_from(entries.len()).map_err(|_| TransportError::CorruptedPayload)?;
        let mut buf = BytesMut::new();
        buf.put_u32(entry_count);

        for (key, value) in entries {
            put_string_u16(&mut buf, key)?;
            put_string_u32(&mut buf, value)?;
        }

        Ok(Self::new(request_id, Status::Ok, buf.to_vec()))
    }

    /// Builds a response containing a list of optional string values.
    pub fn strings(request_id: Uuid, values: &[Option<String>]) -> Result<Self> {
        let value_count =
            u32::try_from(values.len()).map_err(|_| TransportError::CorruptedPayload)?;
        let mut buf = BytesMut::new();
        buf.put_u32(value_count);

        for value in values {
            match value {
                Some(value) => {
                    buf.put_u8(1);
                    put_string_u32(&mut buf, value)?;
                }
                None => buf.put_u8(0),
            }
        }

        Ok(Self::new(request_id, Status::Ok, buf.to_vec()))
    }

    /// Builds a cursor-based scan response.
    pub fn scan(request_id: Uuid, next_cursor: u64, keys: &[String]) -> Result<Self> {
        let key_count = u32::try_from(keys.len()).map_err(|_| TransportError::CorruptedPayload)?;
        let mut buf = BytesMut::new();
        buf.put_u64(next_cursor);
        buf.put_u32(key_count);

        for key in keys {
            put_string_u16(&mut buf, key)?;
        }

        Ok(Self::new(request_id, Status::Ok, buf.to_vec()))
    }

    /// Builds a response containing typed `EXEC` results.
    pub fn exec_results(request_id: Uuid, results: &[ExecResultPayload]) -> Result<Self> {
        let result_count =
            u32::try_from(results.len()).map_err(|_| TransportError::CorruptedPayload)?;
        let mut buf = BytesMut::new();
        buf.put_u32(result_count);

        for result in results {
            encode_exec_result(&mut buf, result)?;
        }

        Ok(Self::new(request_id, Status::Ok, buf.to_vec()))
    }

    pub fn decode_value(&self) -> Result<String> {
        decode_string_u32(&self.payload)
    }

    pub fn decode_error(&self) -> Result<ErrorPayload> {
        decode_error_payload(&self.payload)
    }

    pub fn decode_error_message(&self) -> Result<String> {
        Ok(self.decode_error()?.message)
    }

    pub fn decode_bool(&self) -> Result<bool> {
        match self.payload.as_slice() {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(TransportError::CorruptedPayload),
        }
    }

    pub fn decode_count(&self) -> Result<u64> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 8 {
            return Err(TransportError::UnexpectedEof);
        }

        let count = buf.get_u64();
        ensure_empty(buf)?;
        Ok(count)
    }

    pub fn decode_integer(&self) -> Result<i64> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 8 {
            return Err(TransportError::UnexpectedEof);
        }

        let value = buf.get_i64();
        ensure_empty(buf)?;
        Ok(value)
    }

    pub fn decode_entries(&self) -> Result<Vec<(String, String)>> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 4 {
            return Err(TransportError::UnexpectedEof);
        }

        let entry_count = buf.get_u32() as usize;
        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            let key = read_string_u16(&mut buf)?;
            let value = read_string_u32(&mut buf)?;
            entries.push((key, value));
        }

        ensure_empty(buf)?;
        Ok(entries)
    }

    pub fn decode_strings(&self) -> Result<Vec<Option<String>>> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 4 {
            return Err(TransportError::UnexpectedEof);
        }

        let value_count = buf.get_u32() as usize;
        let mut values = Vec::with_capacity(value_count);

        for _ in 0..value_count {
            if buf.remaining() < 1 {
                return Err(TransportError::UnexpectedEof);
            }

            match buf.get_u8() {
                0 => values.push(None),
                1 => values.push(Some(read_string_u32(&mut buf)?)),
                _ => return Err(TransportError::CorruptedPayload),
            }
        }

        ensure_empty(buf)?;
        Ok(values)
    }

    pub fn decode_scan(&self) -> Result<ScanPayload> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 12 {
            return Err(TransportError::UnexpectedEof);
        }

        let next_cursor = buf.get_u64();
        let key_count = buf.get_u32() as usize;
        let mut keys = Vec::with_capacity(key_count);

        for _ in 0..key_count {
            keys.push(read_string_u16(&mut buf)?);
        }

        ensure_empty(buf)?;

        Ok(ScanPayload { next_cursor, keys })
    }

    pub fn decode_exec_results(&self) -> Result<Vec<ExecResultPayload>> {
        let mut buf = self.payload.as_slice();

        if buf.remaining() < 4 {
            return Err(TransportError::UnexpectedEof);
        }

        let result_count = buf.get_u32() as usize;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            results.push(decode_exec_result(&mut buf)?);
        }

        ensure_empty(buf)?;
        Ok(results)
    }
}

fn encode_string_u32(value: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u32(&mut buf, value)?;
    Ok(buf.to_vec())
}

fn decode_string_u32(payload: &[u8]) -> Result<String> {
    let mut buf = payload;
    let value = read_string_u32(&mut buf)?;
    ensure_empty(buf)?;
    Ok(value)
}

fn encode_error_payload(code: &str, name: &str, message: &str) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    put_string_u16(&mut buf, code)?;
    put_string_u16(&mut buf, name)?;
    put_string_u32(&mut buf, message)?;
    Ok(buf.to_vec())
}

fn encode_exec_result(buf: &mut BytesMut, result: &ExecResultPayload) -> Result<()> {
    match result {
        ExecResultPayload::Ok => buf.put_u8(0x00),
        ExecResultPayload::NotFound => buf.put_u8(0x01),
        ExecResultPayload::Value(value) => {
            buf.put_u8(0x02);
            put_string_u32(buf, value)?;
        }
        ExecResultPayload::Boolean(value) => {
            buf.put_u8(0x03);
            buf.put_u8(u8::from(*value));
        }
        ExecResultPayload::Count(value) => {
            buf.put_u8(0x04);
            buf.put_u64(*value);
        }
        ExecResultPayload::Integer(value) => {
            buf.put_u8(0x05);
            buf.put_i64(*value);
        }
        ExecResultPayload::Entries(entries) => {
            buf.put_u8(0x06);
            let count =
                u32::try_from(entries.len()).map_err(|_| TransportError::CorruptedPayload)?;
            buf.put_u32(count);
            for (key, value) in entries {
                put_string_u16(buf, key)?;
                put_string_u32(buf, value)?;
            }
        }
        ExecResultPayload::Strings(values) => {
            buf.put_u8(0x07);
            let count =
                u32::try_from(values.len()).map_err(|_| TransportError::CorruptedPayload)?;
            buf.put_u32(count);
            for value in values {
                match value {
                    Some(value) => {
                        buf.put_u8(1);
                        put_string_u32(buf, value)?;
                    }
                    None => buf.put_u8(0),
                }
            }
        }
        ExecResultPayload::Scan(scan) => {
            buf.put_u8(0x08);
            buf.put_u64(scan.next_cursor);
            let count =
                u32::try_from(scan.keys.len()).map_err(|_| TransportError::CorruptedPayload)?;
            buf.put_u32(count);
            for key in &scan.keys {
                put_string_u16(buf, key)?;
            }
        }
    }
    Ok(())
}

fn decode_exec_result(buf: &mut &[u8]) -> Result<ExecResultPayload> {
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    match buf.get_u8() {
        0x00 => Ok(ExecResultPayload::Ok),
        0x01 => Ok(ExecResultPayload::NotFound),
        0x02 => Ok(ExecResultPayload::Value(read_string_u32(buf)?)),
        0x03 => Ok(ExecResultPayload::Boolean(read_bool(buf)?)),
        0x04 => {
            if buf.remaining() < 8 {
                return Err(TransportError::UnexpectedEof);
            }
            Ok(ExecResultPayload::Count(buf.get_u64()))
        }
        0x05 => {
            if buf.remaining() < 8 {
                return Err(TransportError::UnexpectedEof);
            }
            Ok(ExecResultPayload::Integer(buf.get_i64()))
        }
        0x06 => {
            if buf.remaining() < 4 {
                return Err(TransportError::UnexpectedEof);
            }
            let entry_count = buf.get_u32() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                entries.push((read_string_u16(buf)?, read_string_u32(buf)?));
            }
            Ok(ExecResultPayload::Entries(entries))
        }
        0x07 => {
            if buf.remaining() < 4 {
                return Err(TransportError::UnexpectedEof);
            }
            let value_count = buf.get_u32() as usize;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                if buf.remaining() < 1 {
                    return Err(TransportError::UnexpectedEof);
                }
                match buf.get_u8() {
                    0 => values.push(None),
                    1 => values.push(Some(read_string_u32(buf)?)),
                    _ => return Err(TransportError::CorruptedPayload),
                }
            }
            Ok(ExecResultPayload::Strings(values))
        }
        0x08 => {
            if buf.remaining() < 12 {
                return Err(TransportError::UnexpectedEof);
            }
            let next_cursor = buf.get_u64();
            let key_count = buf.get_u32() as usize;
            let mut keys = Vec::with_capacity(key_count);
            for _ in 0..key_count {
                keys.push(read_string_u16(buf)?);
            }
            Ok(ExecResultPayload::Scan(ScanPayload { next_cursor, keys }))
        }
        _ => Err(TransportError::CorruptedPayload),
    }
}

fn decode_error_payload(payload: &[u8]) -> Result<ErrorPayload> {
    let mut buf = payload;
    let code = read_string_u16(&mut buf)?;
    let name = read_string_u16(&mut buf)?;
    let message = read_string_u32(&mut buf)?;
    ensure_empty(buf)?;
    Ok(ErrorPayload {
        code,
        name,
        message,
    })
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

fn ensure_empty(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CorruptedPayload)
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecResultPayload, Response, ScanPayload, Status};
    use crate::TransportError;
    use uuid::Uuid;

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[test]
    fn response_helpers_round_trip() {
        let value = Response::value(id(1), "hello").unwrap();
        assert_eq!(value.decode_value().unwrap(), "hello");

        let boolean = Response::boolean(id(2), true);
        assert!(boolean.decode_bool().unwrap());

        let count = Response::count(id(3), 42);
        assert_eq!(count.decode_count().unwrap(), 42);

        let integer = Response::integer(id(4), -2);
        assert_eq!(integer.decode_integer().unwrap(), -2);

        let entries = Response::entries(
            id(5),
            &[
                ("name".to_string(), "alice".to_string()),
                ("city".to_string(), "paris".to_string()),
            ],
        )
        .unwrap();
        assert_eq!(
            entries.decode_entries().unwrap(),
            vec![
                ("name".to_string(), "alice".to_string()),
                ("city".to_string(), "paris".to_string())
            ]
        );

        let strings = Response::strings(
            id(6),
            &[Some("alice".to_string()), None, Some("paris".to_string())],
        )
        .unwrap();
        assert_eq!(
            strings.decode_strings().unwrap(),
            vec![Some("alice".to_string()), None, Some("paris".to_string())]
        );

        let scan = Response::scan(id(7), 10, &["one".to_string(), "two".to_string()]).unwrap();
        let decoded = scan.decode_scan().unwrap();
        assert_eq!(decoded.next_cursor, 10);
        assert_eq!(decoded.keys, vec!["one".to_string(), "two".to_string()]);

        let exec = Response::exec_results(
            id(8),
            &[
                ExecResultPayload::Ok,
                ExecResultPayload::Value("alpha".to_string()),
                ExecResultPayload::Boolean(true),
                ExecResultPayload::Count(7),
                ExecResultPayload::Integer(-2),
                ExecResultPayload::Entries(vec![("name".to_string(), "alice".to_string())]),
                ExecResultPayload::Strings(vec![Some("one".to_string()), None]),
                ExecResultPayload::Scan(ScanPayload {
                    next_cursor: 22,
                    keys: vec!["k1".to_string(), "k2".to_string()],
                }),
            ],
        )
        .unwrap();
        assert_eq!(
            exec.decode_exec_results().unwrap(),
            vec![
                ExecResultPayload::Ok,
                ExecResultPayload::Value("alpha".to_string()),
                ExecResultPayload::Boolean(true),
                ExecResultPayload::Count(7),
                ExecResultPayload::Integer(-2),
                ExecResultPayload::Entries(vec![("name".to_string(), "alice".to_string())]),
                ExecResultPayload::Strings(vec![Some("one".to_string()), None]),
                ExecResultPayload::Scan(ScanPayload {
                    next_cursor: 22,
                    keys: vec!["k1".to_string(), "k2".to_string()],
                }),
            ]
        );
    }

    #[test]
    fn decodes_error_payload() {
        let response = Response::error(id(9), "SRV-500", "Server Failure", "boom").unwrap();
        assert_eq!(response.status, Status::Error);
        let remote = response.decode_error().unwrap();
        assert_eq!(remote.code, "SRV-500");
        assert_eq!(remote.name, "Server Failure");
        assert_eq!(remote.message, "boom");
    }

    #[test]
    fn rejects_corrupted_response_payloads() {
        let bool_response = Response::new(id(1), Status::Ok, vec![2]);
        assert!(matches!(
            bool_response.decode_bool(),
            Err(TransportError::CorruptedPayload)
        ));

        let count_response = Response::new(id(1), Status::Ok, vec![0, 0, 0, 1, 0]);
        assert!(matches!(
            count_response.decode_count(),
            Err(TransportError::UnexpectedEof)
        ));

        let strings_response = Response::new(id(1), Status::Ok, vec![0, 0, 0, 1, 2]);
        assert!(matches!(
            strings_response.decode_strings(),
            Err(TransportError::CorruptedPayload)
        ));
    }
}
