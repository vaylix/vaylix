use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use crc32fast::hash;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::constants::HEADER_LEN;
use crate::error::{Result, TransportError};
use crate::frame::FrameHeader;
use crate::opcode::Opcode;
use crate::options::CodecOptions;
use crate::request::{
    REQUEST_META_DEADLINE_MS, REQUEST_META_SEQUENCE, REQUEST_META_TRACE_ID, Request,
    RequestMetadata,
};
use crate::response::{Response, Status};

mod compression;

use compression::{maybe_compress, maybe_decompress};

/// Encodes a request into a framed binary packet.
pub fn encode_request(request: &Request) -> Result<Vec<u8>> {
    encode_request_with_options(request, CodecOptions::default())
}

/// Encodes a request into a framed binary packet using caller-provided write options.
pub fn encode_request_with_options(request: &Request, options: CodecOptions) -> Result<Vec<u8>> {
    let mut body = BytesMut::with_capacity(18 + request.payload.len());
    body.put_u8(metadata_flags(request.metadata));
    body.extend_from_slice(request.request_id.as_bytes());
    body.put_u8(request.opcode.into());
    if let Some(deadline_ms) = request.metadata.deadline_ms {
        body.put_u64(deadline_ms);
    }
    if let Some(trace_id) = request.metadata.trace_id {
        body.extend_from_slice(trace_id.as_bytes());
    }
    if let Some(sequence) = request.metadata.sequence {
        body.put_u64(sequence);
    }
    body.extend_from_slice(&request.payload);

    encode_frame(&body, options)
}

/// Decodes a framed binary packet into a request.
pub fn decode_request(bytes: &[u8]) -> Result<Request> {
    let body = decode_frame_with_options(bytes, CodecOptions::default())?;
    decode_request_body(&body)
}

/// Encodes a response into a framed binary packet.
pub fn encode_response(response: &Response) -> Result<Vec<u8>> {
    encode_response_with_options(response, CodecOptions::default())
}

/// Encodes a response into a framed binary packet using caller-provided write options.
pub fn encode_response_with_options(response: &Response, options: CodecOptions) -> Result<Vec<u8>> {
    let mut body = BytesMut::with_capacity(17 + response.payload.len());
    body.extend_from_slice(response.request_id.as_bytes());
    body.put_u8(response.status.into());
    body.extend_from_slice(&response.payload);

    encode_frame(&body, options)
}

/// Decodes a framed binary packet into a response.
pub fn decode_response(bytes: &[u8]) -> Result<Response> {
    let body = decode_frame_with_options(bytes, CodecOptions::default())?;
    decode_response_body(&body)
}

/// Reads a single framed request from a blocking reader.
pub fn read_request_from<R: Read>(reader: &mut R) -> Result<Request> {
    let body = read_frame_from_with_options(reader, CodecOptions::default())?;
    decode_request_body(&body)
}

/// Reads a single framed request from a blocking reader using negotiated options.
pub fn read_request_from_with_options<R: Read>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Request> {
    let body = read_frame_from_with_options(reader, options)?;
    decode_request_body(&body)
}

/// Reads a single framed request from an async reader.
pub async fn read_request_from_async<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Request> {
    let body = read_frame_from_async_with_options(reader, CodecOptions::default()).await?;
    decode_request_body(&body)
}

/// Reads a single framed request from an async reader using negotiated options.
pub async fn read_request_from_async_with_options<R: AsyncRead + Unpin>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Request> {
    let body = read_frame_from_async_with_options(reader, options).await?;
    decode_request_body(&body)
}

/// Writes a single framed request to a blocking writer.
pub fn write_request_to<W: Write>(writer: &mut W, request: &Request) -> Result<()> {
    write_request_to_with_options(writer, request, CodecOptions::default())
}

/// Writes a single framed request to a blocking writer using caller-provided write options.
pub fn write_request_to_with_options<W: Write>(
    writer: &mut W,
    request: &Request,
    options: CodecOptions,
) -> Result<()> {
    let encoded = encode_request_with_options(request, options)?;
    writer.write_all(&encoded)?;
    Ok(())
}

/// Writes a single framed request to an async writer.
pub async fn write_request_to_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    request: &Request,
) -> Result<()> {
    write_request_to_async_with_options(writer, request, CodecOptions::default()).await
}

/// Writes a single framed request to an async writer using caller-provided write options.
pub async fn write_request_to_async_with_options<W: AsyncWrite + Unpin>(
    writer: &mut W,
    request: &Request,
    options: CodecOptions,
) -> Result<()> {
    let encoded = encode_request_with_options(request, options)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads a single framed response from a blocking reader.
pub fn read_response_from<R: Read>(reader: &mut R) -> Result<Response> {
    let body = read_frame_from_with_options(reader, CodecOptions::default())?;
    decode_response_body(&body)
}

/// Reads a single framed response from a blocking reader using negotiated options.
pub fn read_response_from_with_options<R: Read>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Response> {
    let body = read_frame_from_with_options(reader, options)?;
    decode_response_body(&body)
}

/// Reads a single framed response from an async reader.
pub async fn read_response_from_async<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Response> {
    let body = read_frame_from_async_with_options(reader, CodecOptions::default()).await?;
    decode_response_body(&body)
}

/// Reads a single framed response from an async reader using negotiated options.
pub async fn read_response_from_async_with_options<R: AsyncRead + Unpin>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Response> {
    let body = read_frame_from_async_with_options(reader, options).await?;
    decode_response_body(&body)
}

/// Writes a single framed response to a blocking writer.
pub fn write_response_to<W: Write>(writer: &mut W, response: &Response) -> Result<()> {
    write_response_to_with_options(writer, response, CodecOptions::default())
}

/// Writes a single framed response to a blocking writer using caller-provided write options.
pub fn write_response_to_with_options<W: Write>(
    writer: &mut W,
    response: &Response,
    options: CodecOptions,
) -> Result<()> {
    let encoded = encode_response_with_options(response, options)?;
    writer.write_all(&encoded)?;
    Ok(())
}

/// Writes a single framed response to an async writer.
pub async fn write_response_to_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &Response,
) -> Result<()> {
    write_response_to_async_with_options(writer, response, CodecOptions::default()).await
}

/// Writes a single framed response to an async writer using caller-provided write options.
pub async fn write_response_to_async_with_options<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &Response,
    options: CodecOptions,
) -> Result<()> {
    let encoded = encode_response_with_options(response, options)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

fn encode_frame(body: &[u8], options: CodecOptions) -> Result<Vec<u8>> {
    if body.len() > options.max_decompressed_frame_len {
        return Err(TransportError::FrameTooLarge {
            length: body.len(),
            max: options.max_decompressed_frame_len,
        });
    }

    let (flags, payload) = maybe_compress(body, options)?;
    if payload.len() > options.max_frame_len {
        return Err(TransportError::FrameTooLarge {
            length: payload.len(),
            max: options.max_frame_len,
        });
    }
    let checksum = hash(&payload);
    let mut header = FrameHeader::new(payload.len() as u32, checksum)?;
    header.flags = flags;
    let mut frame = BytesMut::with_capacity(HEADER_LEN + payload.len());
    header.encode(&mut frame);
    frame.extend_from_slice(&payload);

    Ok(frame.to_vec())
}

fn decode_frame_with_options(frame: &[u8], options: CodecOptions) -> Result<Vec<u8>> {
    if frame.len() < HEADER_LEN {
        return Err(TransportError::UnexpectedEof);
    }

    let mut buf = frame;
    let header = FrameHeader::decode(&mut buf)?;
    enforce_frame_limit(header.length as usize, options.max_frame_len)?;
    let payload_length = header.length as usize;

    if buf.remaining() < payload_length {
        return Err(TransportError::UnexpectedEof);
    }

    if buf.remaining() > payload_length {
        return Err(TransportError::InvalidFrame);
    }

    let payload = buf.copy_to_bytes(payload_length).to_vec();
    if hash(&payload) != header.checksum {
        return Err(TransportError::ChecksumMismatch);
    }

    maybe_decompress(&header, payload, options)
}

pub(crate) fn read_startup_frame_from<R: Read>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Vec<u8>> {
    read_frame_from_with_options(reader, options)
}

pub(crate) fn write_startup_frame_to<W: Write>(
    writer: &mut W,
    body: &[u8],
    options: CodecOptions,
) -> Result<()> {
    writer.write_all(&encode_frame(body, options)?)?;
    Ok(())
}

pub(crate) async fn read_startup_frame_from_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Vec<u8>> {
    read_frame_from_async_with_options(reader, options).await
}

pub(crate) async fn write_startup_frame_to_async<W: AsyncWrite + Unpin>(
    writer: &mut W,
    body: &[u8],
    options: CodecOptions,
) -> Result<()> {
    writer.write_all(&encode_frame(body, options)?).await?;
    writer.flush().await?;
    Ok(())
}

fn read_frame_from_with_options<R: Read>(reader: &mut R, options: CodecOptions) -> Result<Vec<u8>> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    read_exact_or_eof(reader, &mut header_bytes)?;

    let mut header_slice = header_bytes.as_slice();
    let header = FrameHeader::decode(&mut header_slice)?;
    enforce_frame_limit(header.length as usize, options.max_frame_len)?;
    let mut payload = vec![0_u8; header.length as usize];

    if !payload.is_empty() {
        read_exact_or_eof(reader, &mut payload)?;
    }

    if hash(&payload) != header.checksum {
        return Err(TransportError::ChecksumMismatch);
    }

    maybe_decompress(&header, payload, options)
}

async fn read_frame_from_async_with_options<R: AsyncRead + Unpin>(
    reader: &mut R,
    options: CodecOptions,
) -> Result<Vec<u8>> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    read_exact_or_eof_async(reader, &mut header_bytes).await?;

    let mut header_slice = header_bytes.as_slice();
    let header = FrameHeader::decode(&mut header_slice)?;
    enforce_frame_limit(header.length as usize, options.max_frame_len)?;
    let mut payload = vec![0_u8; header.length as usize];

    if !payload.is_empty() {
        read_exact_or_eof_async(reader, &mut payload).await?;
    }

    if hash(&payload) != header.checksum {
        return Err(TransportError::ChecksumMismatch);
    }

    maybe_decompress(&header, payload, options)
}

fn decode_request_body(body: &[u8]) -> Result<Request> {
    let mut buf = body;

    if buf.remaining() < 18 {
        return Err(TransportError::UnexpectedEof);
    }

    let flags = buf.get_u8();
    if flags & !(REQUEST_META_DEADLINE_MS | REQUEST_META_TRACE_ID | REQUEST_META_SEQUENCE) != 0 {
        return Err(TransportError::CorruptedPayload);
    }
    let request_id =
        Uuid::from_slice(&buf.copy_to_bytes(16)).map_err(|_| TransportError::CorruptedPayload)?;
    let opcode = Opcode::try_from(buf.get_u8())?;
    let deadline_ms = if flags & REQUEST_META_DEADLINE_MS != 0 {
        if buf.remaining() < 8 {
            return Err(TransportError::UnexpectedEof);
        }
        Some(buf.get_u64())
    } else {
        None
    };
    let trace_id = if flags & REQUEST_META_TRACE_ID != 0 {
        if buf.remaining() < 16 {
            return Err(TransportError::UnexpectedEof);
        }
        Some(
            Uuid::from_slice(&buf.copy_to_bytes(16))
                .map_err(|_| TransportError::CorruptedPayload)?,
        )
    } else {
        None
    };
    let sequence = if flags & REQUEST_META_SEQUENCE != 0 {
        if buf.remaining() < 8 {
            return Err(TransportError::UnexpectedEof);
        }
        Some(buf.get_u64())
    } else {
        None
    };
    let payload = buf.to_vec();

    Ok(
        Request::new(request_id, opcode, payload).with_metadata(RequestMetadata {
            deadline_ms,
            trace_id,
            sequence,
        }),
    )
}

fn metadata_flags(metadata: RequestMetadata) -> u8 {
    let mut flags = 0;
    if metadata.deadline_ms.is_some() {
        flags |= REQUEST_META_DEADLINE_MS;
    }
    if metadata.trace_id.is_some() {
        flags |= REQUEST_META_TRACE_ID;
    }
    if metadata.sequence.is_some() {
        flags |= REQUEST_META_SEQUENCE;
    }
    flags
}

fn enforce_frame_limit(length: usize, max: usize) -> Result<()> {
    if length > max {
        return Err(TransportError::FrameTooLarge { length, max });
    }
    Ok(())
}

fn decode_response_body(body: &[u8]) -> Result<Response> {
    let mut buf = body;

    if buf.remaining() < 17 {
        return Err(TransportError::UnexpectedEof);
    }

    let request_id =
        Uuid::from_slice(&buf.copy_to_bytes(16)).map_err(|_| TransportError::CorruptedPayload)?;
    let status = Status::try_from(buf.get_u8())?;
    let payload = buf.to_vec();

    Ok(Response::new(request_id, status, payload))
}

fn read_exact_or_eof<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<()> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(TransportError::UnexpectedEof)
        }
        Err(err) => Err(TransportError::Io(err)),
    }
}

async fn read_exact_or_eof_async<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<()> {
    match reader.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(TransportError::UnexpectedEof)
        }
        Err(err) => Err(TransportError::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Error, ErrorKind};

    use command::{Command, Expiration, SetCondition, SetOptions};
    use tokio::runtime::Runtime;
    use uuid::Uuid;

    use super::{
        decode_request, decode_response, encode_request, encode_request_with_options,
        encode_response, read_request_from, read_request_from_async,
        read_request_from_with_options, read_response_from, read_response_from_async,
        write_request_to, write_request_to_async, write_response_to, write_response_to_async,
    };
    use crate::constants::{FLAG_COMPRESSED_ZSTD, HEADER_LEN, MAGIC_BYTES, MAX_FRAME_LEN, VERSION};
    use crate::{CodecOptions, CompressionMode, Request, Response, Status, TransportError};

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    struct FailingWriter;

    impl std::io::Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(Error::other("write failed"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct FailingReader;

    impl std::io::Read for FailingReader {
        fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
            Err(Error::new(ErrorKind::BrokenPipe, "read failed"))
        }
    }

    #[test]
    fn request_frame_round_trip_for_all_opcodes() {
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
                value: "Jane".to_string(),
            },
            Command::MGet {
                keys: vec!["one".to_string(), "two".to_string()],
            },
            Command::MSet {
                entries: vec![
                    ("one".to_string(), "1".to_string()),
                    ("two".to_string(), "2".to_string()),
                ],
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
                count: Some(25),
            },
            Command::DbSize,
            Command::Info,
            Command::MetricsProm,
            Command::List,
            Command::Clear,
            Command::Count,
            Command::Save,
            Command::Snapshot,
            Command::Backup,
            Command::BackupTo {
                path: "nightly.json".to_string(),
            },
            Command::BackupVerify {
                dump: "{\"version\":1}".to_string(),
            },
            Command::BackupVerifyFrom {
                path: "nightly.json".to_string(),
            },
            Command::Restore {
                dump: "{\"version\":1}".to_string(),
            },
            Command::RestoreFrom {
                path: "nightly.json".to_string(),
            },
            Command::RestoreCheck {
                dump: "{\"version\":1}".to_string(),
            },
            Command::RestoreCheckFrom {
                path: "nightly.json".to_string(),
            },
            Command::CreateUser {
                username: "alice".to_string(),
                password: "secret".to_string(),
            },
            Command::AlterUserPassword {
                username: "alice".to_string(),
                password: "new-secret".to_string(),
            },
            Command::DropUser {
                username: "alice".to_string(),
            },
            Command::CreateRole {
                role: "readonly".to_string(),
            },
            Command::DropRole {
                role: "readonly".to_string(),
            },
            Command::GrantRole {
                role: "readonly".to_string(),
                username: "alice".to_string(),
            },
            Command::RevokeRole {
                role: "readonly".to_string(),
                username: "alice".to_string(),
            },
            Command::GrantPermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string(),
            },
            Command::RevokePermission {
                permission: "read".to_string(),
                pattern: "app:*".to_string(),
                role: "readonly".to_string(),
            },
            Command::ShowUsers,
            Command::ShowRoles,
            Command::ShowGrants,
            Command::ShowGrantsForUser {
                username: "alice".to_string(),
            },
            Command::ShowGrantsForRole {
                role: "readonly".to_string(),
            },
            Command::WhoAmI,
        ];

        for command in commands {
            let request = Request::from_command(id(11), command.clone()).unwrap();
            let encoded = encode_request(&request).unwrap();
            let decoded = decode_request(&encoded).unwrap();

            assert_eq!(decoded, request);
            assert_eq!(decoded.into_command().unwrap(), command);
        }
    }

    #[test]
    fn response_frame_round_trip_for_payload_types() {
        let responses = vec![
            Response::ok(id(1)),
            Response::not_found(id(2)),
            Response::error(id(3), "SRV-400", "Bad Request", "bad request").unwrap(),
            Response::value(id(4), "alice").unwrap(),
            Response::boolean(id(5), true),
            Response::count(id(6), 42),
            Response::integer(id(7), -2),
            Response::entries(id(8), &[("name".to_string(), "alice".to_string())]).unwrap(),
            Response::strings(id(9), &[Some("alice".to_string()), None]).unwrap(),
            Response::scan(id(10), 3, &["one".to_string(), "two".to_string()]).unwrap(),
            Response::exec_results(
                id(11),
                &[
                    crate::response::ExecResultPayload::Ok,
                    crate::response::ExecResultPayload::Value("alice".to_string()),
                    crate::response::ExecResultPayload::Scan(crate::response::ScanPayload {
                        next_cursor: 7,
                        keys: vec!["one".to_string(), "two".to_string()],
                    }),
                ],
            )
            .unwrap(),
        ];

        for response in responses {
            let encoded = encode_response(&response).unwrap();
            let decoded = decode_response(&encoded).unwrap();
            assert_eq!(decoded, response);
        }
    }

    #[test]
    fn io_helpers_round_trip() {
        let request = Request::from_command(
            id(9),
            Command::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
                options: SetOptions::default(),
            },
        )
        .unwrap();
        let response = Response::value(id(9), "alice").unwrap();
        let mut request_cursor = Cursor::new(Vec::new());
        let mut response_cursor = Cursor::new(Vec::new());

        write_request_to(&mut request_cursor, &request).unwrap();
        write_response_to(&mut response_cursor, &response).unwrap();

        request_cursor.set_position(0);
        response_cursor.set_position(0);

        assert_eq!(read_request_from(&mut request_cursor).unwrap(), request);
        assert_eq!(read_response_from(&mut response_cursor).unwrap(), response);
    }

    #[test]
    fn default_encoding_compresses_frames() {
        let request = Request::from_command(
            id(10),
            Command::Ping {
                message: Some("hello".to_string()),
            },
        )
        .unwrap();

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded[5], FLAG_COMPRESSED_ZSTD);
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }

    #[test]
    fn async_io_helpers_round_trip() {
        let runtime = Runtime::new().unwrap();

        runtime.block_on(async {
            let request = Request::from_command(
                id(9),
                Command::Set {
                    key: "name".to_string(),
                    value: "alice".to_string(),
                    options: SetOptions::default(),
                },
            )
            .unwrap();
            let response = Response::value(id(9), "alice").unwrap();
            let (mut request_reader, mut request_writer) = tokio::io::duplex(1024);
            let (mut response_reader, mut response_writer) = tokio::io::duplex(1024);

            write_request_to_async(&mut request_writer, &request)
                .await
                .unwrap();
            write_response_to_async(&mut response_writer, &response)
                .await
                .unwrap();

            assert_eq!(
                read_request_from_async(&mut request_reader).await.unwrap(),
                request
            );
            assert_eq!(
                read_response_from_async(&mut response_reader)
                    .await
                    .unwrap(),
                response
            );
        });
    }

    #[test]
    fn rejects_bad_magic() {
        let request = Request::from_command(
            id(1),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        let mut encoded = encode_request(&request).unwrap();
        encoded[..4].copy_from_slice(b"NOPE");

        assert!(matches!(
            decode_request(&encoded),
            Err(TransportError::InvalidFrame)
        ));
    }

    #[test]
    fn rejects_wrong_version() {
        let request = Request::from_command(
            id(1),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        let mut encoded = encode_request(&request).unwrap();
        encoded[4] = VERSION + 1;

        assert!(matches!(
            decode_request(&encoded),
            Err(TransportError::VersionMismatch { .. })
        ));
    }

    #[test]
    fn rejects_unknown_opcode() {
        let mut payload = Vec::new();
        payload.push(0);
        payload.extend_from_slice(id(1).as_bytes());
        payload.push(0xff);
        let mut frame = Vec::from(MAGIC_BYTES);
        frame.push(VERSION);
        frame.push(0);
        frame.extend_from_slice(&18_u32.to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);

        assert!(matches!(
            decode_request(&frame),
            Err(TransportError::UnknownOpcode(0xff))
        ));
    }

    #[test]
    fn rejects_unknown_status() {
        let mut payload = Vec::new();
        payload.extend_from_slice(id(1).as_bytes());
        payload.push(0xff);
        let mut frame = Vec::from(MAGIC_BYTES);
        frame.push(VERSION);
        frame.push(0);
        frame.extend_from_slice(&17_u32.to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        frame.extend_from_slice(&payload);

        assert!(matches!(
            decode_response(&frame),
            Err(TransportError::UnknownStatus(0xff))
        ));
    }

    #[test]
    fn rejects_truncated_header() {
        let truncated = vec![0_u8; HEADER_LEN - 1];
        assert!(matches!(
            decode_request(&truncated),
            Err(TransportError::UnexpectedEof)
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let request = Request::from_command(
            id(1),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        let mut encoded = encode_request(&request).unwrap();
        encoded.pop();

        assert!(matches!(
            decode_request(&encoded),
            Err(TransportError::UnexpectedEof)
        ));
    }

    #[test]
    fn rejects_oversized_frame() {
        let mut frame = Vec::from(MAGIC_BYTES);
        frame.push(VERSION);
        frame.push(0);
        frame.extend_from_slice(&((MAX_FRAME_LEN + 1) as u32).to_be_bytes());
        frame.extend_from_slice(&0_u32.to_be_bytes());

        assert!(matches!(
            decode_request(&frame),
            Err(TransportError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_invalid_utf8_payload() {
        let response = Response {
            request_id: id(1),
            status: Status::Ok,
            payload: vec![0, 0, 0, 2, 0xff, 0xfe],
        };

        assert!(matches!(
            response.decode_value(),
            Err(TransportError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn rejects_frame_with_trailing_bytes() {
        let request = Request::from_command(
            id(1),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();
        let mut encoded = encode_request(&request).unwrap();
        encoded.push(0);

        assert!(matches!(
            decode_request(&encoded),
            Err(TransportError::InvalidFrame)
        ));
    }

    #[test]
    fn propagates_reader_and_writer_io_errors() {
        let request = Request::from_command(
            id(1),
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();

        assert!(matches!(
            write_request_to(&mut FailingWriter, &request),
            Err(TransportError::Io(_))
        ));
        assert!(matches!(
            read_request_from(&mut FailingReader),
            Err(TransportError::Io(_))
        ));
    }

    #[test]
    fn round_trips_compressed_frames() {
        let request = Request::from_command(
            id(99),
            Command::Set {
                key: "blob".to_string(),
                value: "x".repeat(4_096),
                options: SetOptions::default(),
            },
        )
        .unwrap();

        let encoded = encode_request_with_options(
            &request,
            CodecOptions {
                compression: CompressionMode::Zstd,
                compression_threshold_bytes: 32,
                ..CodecOptions::default()
            },
        )
        .unwrap();
        let decoded = decode_request(&encoded).unwrap();

        assert_eq!(decoded, request);
    }

    #[test]
    fn rejects_decompressed_payload_above_negotiated_limit() {
        let request = Request::from_command(
            id(100),
            Command::Set {
                key: "blob".to_string(),
                value: "x".repeat(4_096),
                options: SetOptions::default(),
            },
        )
        .unwrap();
        let encoded = encode_request_with_options(
            &request,
            CodecOptions {
                compression: CompressionMode::Zstd,
                compression_threshold_bytes: 0,
                ..CodecOptions::default()
            },
        )
        .unwrap();
        let mut cursor = Cursor::new(encoded);

        assert!(matches!(
            read_request_from_with_options(
                &mut cursor,
                CodecOptions {
                    compression: CompressionMode::Zstd,
                    compression_threshold_bytes: 0,
                    max_frame_len: 8 * 1024 * 1024,
                    max_decompressed_frame_len: 64,
                }
            ),
            Err(TransportError::DecompressedFrameTooLarge { .. })
        ));
    }
}
