use bytes::{Buf, BufMut, BytesMut};

use crate::error::{Result, TransportError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorPayload {
    pub code: String,
    pub name: String,
    pub message: String,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub request_id: u32,
    pub status: Status,
    pub payload: Vec<u8>,
}

impl Response {
    pub fn new(request_id: u32, status: Status, payload: Vec<u8>) -> Self {
        Self {
            request_id,
            status,
            payload,
        }
    }

    pub fn ok(request_id: u32) -> Self {
        Self::new(request_id, Status::Ok, Vec::new())
    }

    pub fn not_found(request_id: u32) -> Self {
        Self::new(request_id, Status::NotFound, Vec::new())
    }

    pub fn error(request_id: u32, code: &str, name: &str, message: &str) -> Result<Self> {
        Ok(Self::new(
            request_id,
            Status::Error,
            encode_error_payload(code, name, message)?,
        ))
    }

    pub fn value(request_id: u32, value: &str) -> Result<Self> {
        Ok(Self::new(request_id, Status::Ok, encode_string_u32(value)?))
    }

    pub fn boolean(request_id: u32, value: bool) -> Self {
        Self::new(request_id, Status::Ok, vec![u8::from(value)])
    }

    pub fn count(request_id: u32, count: u64) -> Self {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(count);
        Self::new(request_id, Status::Ok, buf.to_vec())
    }

    pub fn entries(request_id: u32, entries: &[(String, String)]) -> Result<Self> {
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

fn ensure_empty(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CorruptedPayload)
    }
}

#[cfg(test)]
mod tests {
    use super::{Response, Status};
    use crate::TransportError;

    #[test]
    fn response_helpers_round_trip() {
        let value = Response::value(1, "hello").unwrap();
        assert_eq!(value.decode_value().unwrap(), "hello");

        let boolean = Response::boolean(2, true);
        assert!(boolean.decode_bool().unwrap());

        let count = Response::count(3, 42);
        assert_eq!(count.decode_count().unwrap(), 42);

        let entries = Response::entries(
            4,
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
    }

    #[test]
    fn decodes_error_payload() {
        let response = Response::error(9, "SRV-500", "Server Failure", "boom").unwrap();
        assert_eq!(response.status, Status::Error);
        let remote = response.decode_error().unwrap();
        assert_eq!(remote.code, "SRV-500");
        assert_eq!(remote.name, "Server Failure");
        assert_eq!(remote.message, "boom");
    }

    #[test]
    fn rejects_corrupted_response_payloads() {
        let bool_response = Response::new(1, Status::Ok, vec![2]);
        assert!(matches!(
            bool_response.decode_bool(),
            Err(TransportError::CorruptedPayload)
        ));

        let count_response = Response::new(1, Status::Ok, vec![0, 0, 0, 1, 0]);
        assert!(matches!(
            count_response.decode_count(),
            Err(TransportError::UnexpectedEof)
        ));

        let entries_response = Response::new(1, Status::Ok, vec![0, 0, 0, 1, 0, 3, b'f', b'o']);
        assert!(matches!(
            entries_response.decode_entries(),
            Err(TransportError::UnexpectedEof)
        ));
    }
}
