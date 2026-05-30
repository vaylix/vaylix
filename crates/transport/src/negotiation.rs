use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use uuid::Uuid;

use crate::codec::{
    read_startup_frame_from, read_startup_frame_from_async, write_startup_frame_to,
    write_startup_frame_to_async,
};
use crate::constants::{MAX_FRAME_LEN, VERSION};
use crate::error::{Result, TransportError};
use crate::options::{CodecOptions, CompressionMode};
use crate::response::Status;

const CLIENT_HELLO: u8 = 0xF0;
const SERVER_HELLO: u8 = 0xF1;

pub const CAP_ZSTD: u64 = 1 << 0;
pub const CAP_REQUEST_DEADLINE: u64 = 1 << 1;
pub const CAP_SERVER_METRICS: u64 = 1 << 2;
pub const CAP_PIPELINING: u64 = 1 << 3;
pub const CAP_TRACE_CONTEXT: u64 = 1 << 4;

pub const DEFAULT_CAPABILITIES: u64 =
    CAP_ZSTD | CAP_REQUEST_DEADLINE | CAP_SERVER_METRICS | CAP_PIPELINING | CAP_TRACE_CONTEXT;

/// Startup message sent by a client before command frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHello {
    pub protocol_version: u8,
    pub client_name: String,
    pub client_version: String,
    pub supported_capabilities: u64,
    pub desired_compression: CompressionMode,
    pub max_frame_len: u32,
    pub auth_intent: bool,
}

impl ClientHello {
    pub fn new(client_name: impl Into<String>, client_version: impl Into<String>) -> Self {
        Self {
            protocol_version: VERSION,
            client_name: client_name.into(),
            client_version: client_version.into(),
            supported_capabilities: DEFAULT_CAPABILITIES,
            desired_compression: CompressionMode::Zstd,
            max_frame_len: MAX_FRAME_LEN as u32,
            auth_intent: true,
        }
    }
}

/// Startup message returned by the server after capability negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHello {
    pub protocol_version: u8,
    pub accepted_capabilities: u64,
    pub compression: CompressionMode,
    pub max_frame_len: u32,
    pub server_id: Uuid,
    pub status: Status,
    pub error_code: Option<String>,
    pub error_name: Option<String>,
    pub error_message: Option<String>,
}

impl ServerHello {
    pub fn ok(
        accepted_capabilities: u64,
        compression: CompressionMode,
        max_frame_len: u32,
        server_id: Uuid,
    ) -> Self {
        Self {
            protocol_version: VERSION,
            accepted_capabilities,
            compression,
            max_frame_len,
            server_id,
            status: Status::Ok,
            error_code: None,
            error_name: None,
            error_message: None,
        }
    }

    pub fn error(code: &str, name: &str, message: &str) -> Self {
        Self {
            protocol_version: VERSION,
            accepted_capabilities: 0,
            compression: CompressionMode::None,
            max_frame_len: MAX_FRAME_LEN as u32,
            server_id: Uuid::nil(),
            status: Status::Error,
            error_code: Some(code.to_string()),
            error_name: Some(name.to_string()),
            error_message: Some(message.to_string()),
        }
    }
}

/// Writes the startup client hello over a blocking stream.
pub fn write_client_hello_to<W: std::io::Write>(writer: &mut W, hello: &ClientHello) -> Result<()> {
    write_startup_frame_to(writer, &encode_client_hello(hello)?, startup_options())
}

/// Reads the startup client hello from a blocking stream.
pub fn read_client_hello_from<R: std::io::Read>(reader: &mut R) -> Result<ClientHello> {
    decode_client_hello(&read_startup_frame_from(reader, CodecOptions::default())?)
}

/// Writes the startup server hello over a blocking stream.
pub fn write_server_hello_to<W: std::io::Write>(writer: &mut W, hello: &ServerHello) -> Result<()> {
    write_startup_frame_to(writer, &encode_server_hello(hello)?, startup_options())
}

/// Reads the startup server hello from a blocking stream.
pub fn read_server_hello_from<R: std::io::Read>(reader: &mut R) -> Result<ServerHello> {
    decode_server_hello(&read_startup_frame_from(reader, CodecOptions::default())?)
}

/// Writes the startup client hello over an async stream.
pub async fn write_client_hello_to_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    hello: &ClientHello,
) -> Result<()> {
    write_startup_frame_to_async(writer, &encode_client_hello(hello)?, startup_options()).await
}

/// Reads the startup client hello from an async stream.
pub async fn read_client_hello_from_async<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<ClientHello> {
    decode_client_hello(&read_startup_frame_from_async(reader, CodecOptions::default()).await?)
}

/// Writes the startup server hello over an async stream.
pub async fn write_server_hello_to_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    hello: &ServerHello,
) -> Result<()> {
    write_startup_frame_to_async(writer, &encode_server_hello(hello)?, startup_options()).await
}

/// Reads the startup server hello from an async stream.
pub async fn read_server_hello_from_async<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<ServerHello> {
    decode_server_hello(&read_startup_frame_from_async(reader, CodecOptions::default()).await?)
}

pub fn negotiate_server_options(
    hello: &ClientHello,
    server_options: CodecOptions,
) -> Result<(ServerHello, CodecOptions)> {
    if hello.protocol_version != VERSION {
        return Err(TransportError::NegotiationFailed(
            "unsupported protocol version",
        ));
    }

    let mut capabilities = hello.supported_capabilities & DEFAULT_CAPABILITIES;
    let compression = if server_options.compression == CompressionMode::Zstd
        && hello.desired_compression == CompressionMode::Zstd
        && capabilities & CAP_ZSTD != 0
    {
        CompressionMode::Zstd
    } else {
        capabilities &= !CAP_ZSTD;
        CompressionMode::None
    };
    let max_frame_len = (hello.max_frame_len as usize)
        .min(server_options.max_frame_len)
        .min(MAX_FRAME_LEN);
    let options = CodecOptions {
        compression,
        compression_threshold_bytes: server_options.compression_threshold_bytes,
        max_frame_len,
        max_decompressed_frame_len: max_frame_len,
    };
    let hello = ServerHello::ok(
        capabilities,
        compression,
        max_frame_len as u32,
        Uuid::now_v7(),
    );

    Ok((hello, options))
}

pub fn client_options_from_server_hello(hello: &ServerHello) -> Result<CodecOptions> {
    if hello.status != Status::Ok {
        return Err(TransportError::NegotiationFailed("server rejected startup"));
    }
    if hello.protocol_version != VERSION {
        return Err(TransportError::NegotiationFailed(
            "unsupported server protocol version",
        ));
    }

    Ok(CodecOptions {
        compression: hello.compression,
        compression_threshold_bytes: 0,
        max_frame_len: hello.max_frame_len as usize,
        max_decompressed_frame_len: hello.max_frame_len as usize,
    })
}

fn startup_options() -> CodecOptions {
    CodecOptions {
        compression: CompressionMode::None,
        compression_threshold_bytes: 0,
        ..CodecOptions::default()
    }
}

fn encode_client_hello(hello: &ClientHello) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_u8(CLIENT_HELLO);
    buf.put_u8(hello.protocol_version);
    put_string(&mut buf, &hello.client_name)?;
    put_string(&mut buf, &hello.client_version)?;
    buf.put_u64(hello.supported_capabilities);
    buf.put_u8(compression_to_u8(hello.desired_compression));
    buf.put_u32(hello.max_frame_len);
    buf.put_u8(u8::from(hello.auth_intent));
    Ok(buf.to_vec())
}

fn decode_client_hello(bytes: &[u8]) -> Result<ClientHello> {
    let mut buf = bytes;
    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }
    if buf.get_u8() != CLIENT_HELLO {
        return Err(TransportError::ProtocolStateViolation(
            "expected client hello",
        ));
    }

    let protocol_version = buf.get_u8();
    let client_name = read_string(&mut buf)?;
    let client_version = read_string(&mut buf)?;
    if buf.remaining() < 14 {
        return Err(TransportError::UnexpectedEof);
    }
    let supported_capabilities = buf.get_u64();
    let desired_compression = compression_from_u8(buf.get_u8())?;
    let max_frame_len = buf.get_u32();
    let auth_intent = match buf.get_u8() {
        0 => false,
        1 => true,
        _ => return Err(TransportError::CorruptedPayload),
    };
    if buf.has_remaining() {
        return Err(TransportError::CorruptedPayload);
    }

    Ok(ClientHello {
        protocol_version,
        client_name,
        client_version,
        supported_capabilities,
        desired_compression,
        max_frame_len,
        auth_intent,
    })
}

fn encode_server_hello(hello: &ServerHello) -> Result<Vec<u8>> {
    let mut buf = BytesMut::new();
    buf.put_u8(SERVER_HELLO);
    buf.put_u8(hello.protocol_version);
    buf.put_u64(hello.accepted_capabilities);
    buf.put_u8(compression_to_u8(hello.compression));
    buf.put_u32(hello.max_frame_len);
    buf.extend_from_slice(hello.server_id.as_bytes());
    buf.put_u8(hello.status.into());
    put_optional_string(&mut buf, hello.error_code.as_deref())?;
    put_optional_string(&mut buf, hello.error_name.as_deref())?;
    put_optional_string(&mut buf, hello.error_message.as_deref())?;
    Ok(buf.to_vec())
}

fn decode_server_hello(bytes: &[u8]) -> Result<ServerHello> {
    let mut buf = bytes;
    if buf.remaining() < 31 {
        return Err(TransportError::UnexpectedEof);
    }
    if buf.get_u8() != SERVER_HELLO {
        return Err(TransportError::ProtocolStateViolation(
            "expected server hello",
        ));
    }

    let protocol_version = buf.get_u8();
    let accepted_capabilities = buf.get_u64();
    let compression = compression_from_u8(buf.get_u8())?;
    let max_frame_len = buf.get_u32();
    let server_id =
        Uuid::from_slice(&buf.copy_to_bytes(16)).map_err(|_| TransportError::CorruptedPayload)?;
    let status = Status::try_from(buf.get_u8())?;
    let error_code = read_optional_string(&mut buf)?;
    let error_name = read_optional_string(&mut buf)?;
    let error_message = read_optional_string(&mut buf)?;
    if buf.has_remaining() {
        return Err(TransportError::CorruptedPayload);
    }

    Ok(ServerHello {
        protocol_version,
        accepted_capabilities,
        compression,
        max_frame_len,
        server_id,
        status,
        error_code,
        error_name,
        error_message,
    })
}

fn compression_to_u8(compression: CompressionMode) -> u8 {
    match compression {
        CompressionMode::None => 0,
        CompressionMode::Zstd => 1,
    }
}

fn compression_from_u8(value: u8) -> Result<CompressionMode> {
    match value {
        0 => Ok(CompressionMode::None),
        1 => Ok(CompressionMode::Zstd),
        _ => Err(TransportError::CapabilityMismatch(
            "unknown compression mode",
        )),
    }
}

fn put_string(buf: &mut BytesMut, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| TransportError::CorruptedPayload)?;
    buf.put_u16(len);
    buf.extend_from_slice(bytes);
    Ok(())
}

fn put_optional_string(buf: &mut BytesMut, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            buf.put_u8(1);
            put_string(buf, value)
        }
        None => {
            buf.put_u8(0);
            Ok(())
        }
    }
}

fn read_string(buf: &mut &[u8]) -> Result<String> {
    if buf.remaining() < 2 {
        return Err(TransportError::UnexpectedEof);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(TransportError::UnexpectedEof);
    }
    String::from_utf8(buf.copy_to_bytes(len).to_vec()).map_err(TransportError::from)
}

fn read_optional_string(buf: &mut &[u8]) -> Result<Option<String>> {
    if !buf.has_remaining() {
        return Err(TransportError::UnexpectedEof);
    }
    match buf.get_u8() {
        0 => Ok(None),
        1 => Ok(Some(read_string(buf)?)),
        _ => Err(TransportError::CorruptedPayload),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn client_and_server_hello_round_trip() {
        let client = ClientHello::new("vaylix-client", "0.3.0");
        let mut wire = Cursor::new(Vec::new());
        write_client_hello_to(&mut wire, &client).unwrap();
        assert_eq!(wire.get_ref()[5], 0);
        wire.set_position(0);
        assert_eq!(read_client_hello_from(&mut wire).unwrap(), client);

        let server = ServerHello::ok(
            DEFAULT_CAPABILITIES,
            CompressionMode::Zstd,
            MAX_FRAME_LEN as u32,
            Uuid::from_u128(42),
        );
        let mut wire = Cursor::new(Vec::new());
        write_server_hello_to(&mut wire, &server).unwrap();
        assert_eq!(wire.get_ref()[5], 0);
        wire.set_position(0);
        assert_eq!(read_server_hello_from(&mut wire).unwrap(), server);
    }

    #[test]
    fn rejects_unsupported_protocol_version() {
        let mut hello = ClientHello::new("vaylix-client", "0.3.0");
        hello.protocol_version = VERSION + 1;
        assert!(matches!(
            negotiate_server_options(&hello, CodecOptions::default()),
            Err(TransportError::NegotiationFailed(_))
        ));
    }

    #[test]
    fn negotiates_compression_and_frame_limits() {
        let mut hello = ClientHello::new("vaylix-client", "0.3.0");
        hello.max_frame_len = 1024;
        let (server, options) = negotiate_server_options(&hello, CodecOptions::default()).unwrap();
        assert_eq!(server.compression, CompressionMode::Zstd);
        assert_eq!(options.max_frame_len, 1024);
        assert_ne!(server.accepted_capabilities & CAP_PIPELINING, 0);
    }
}
