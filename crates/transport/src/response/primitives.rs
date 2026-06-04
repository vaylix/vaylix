use bytes::{Buf, BufMut, BytesMut};

use crate::error::{Result, TransportError};

pub(super) fn put_string_u16(buf: &mut BytesMut, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).map_err(|_| TransportError::CorruptedPayload)?;
    buf.put_u16(length);
    buf.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn put_string_u32(buf: &mut BytesMut, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    put_bytes_u32(buf, bytes)
}

pub(super) fn put_bytes_u32(buf: &mut BytesMut, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| TransportError::CorruptedPayload)?;
    buf.put_u32(length);
    buf.extend_from_slice(value);
    Ok(())
}

pub(super) fn read_string_u16(buf: &mut &[u8]) -> Result<String> {
    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }

    let length = buf.get_u16() as usize;
    read_string(buf, length)
}

pub(super) fn read_string_u32(buf: &mut &[u8]) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(TransportError::UnexpectedEof);
    }

    let length = buf.get_u32() as usize;
    read_string(buf, length)
}

pub(super) fn read_bytes_u32(buf: &mut &[u8]) -> Result<Vec<u8>> {
    if buf.remaining() < 4 {
        return Err(TransportError::UnexpectedEof);
    }

    let length = buf.get_u32() as usize;
    if buf.remaining() < length {
        return Err(TransportError::UnexpectedEof);
    }

    Ok(buf.copy_to_bytes(length).to_vec())
}

fn read_string(buf: &mut &[u8], length: usize) -> Result<String> {
    if buf.remaining() < length {
        return Err(TransportError::UnexpectedEof);
    }

    let bytes = buf.copy_to_bytes(length);
    Ok(String::from_utf8(bytes.to_vec())?)
}

pub(super) fn read_bool(buf: &mut &[u8]) -> Result<bool> {
    if buf.remaining() < 1 {
        return Err(TransportError::UnexpectedEof);
    }

    match buf.get_u8() {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TransportError::CorruptedPayload),
    }
}

pub(super) fn ensure_empty(buf: &[u8]) -> Result<()> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CorruptedPayload)
    }
}
