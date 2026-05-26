use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};

use crate::constants::{HEADER_LEN, MAX_FRAME_LEN};
use crate::error::{Result, TransportError};
use crate::frame::FrameHeader;
use crate::opcode::Opcode;
use crate::request::Request;
use crate::response::{Response, Status};

pub fn encode_request(request: &Request) -> Result<Vec<u8>> {
    let mut body = BytesMut::with_capacity(5 + request.payload.len());
    body.put_u32(request.request_id);
    body.put_u8(request.opcode.into());
    body.extend_from_slice(&request.payload);

    encode_frame(&body)
}

pub fn decode_request(bytes: &[u8]) -> Result<Request> {
    let body = decode_frame(bytes)?;
    decode_request_body(&body)
}

pub fn encode_response(response: &Response) -> Result<Vec<u8>> {
    let mut body = BytesMut::with_capacity(5 + response.payload.len());
    body.put_u32(response.request_id);
    body.put_u8(response.status.into());
    body.extend_from_slice(&response.payload);

    encode_frame(&body)
}

pub fn decode_response(bytes: &[u8]) -> Result<Response> {
    let body = decode_frame(bytes)?;
    decode_response_body(&body)
}

pub fn read_request_from<R: Read>(reader: &mut R) -> Result<Request> {
    let body = read_frame_from(reader)?;
    decode_request_body(&body)
}

pub fn write_request_to<W: Write>(writer: &mut W, request: &Request) -> Result<()> {
    let encoded = encode_request(request)?;
    writer.write_all(&encoded)?;
    Ok(())
}

pub fn read_response_from<R: Read>(reader: &mut R) -> Result<Response> {
    let body = read_frame_from(reader)?;
    decode_response_body(&body)
}

pub fn write_response_to<W: Write>(writer: &mut W, response: &Response) -> Result<()> {
    let encoded = encode_response(response)?;
    writer.write_all(&encoded)?;
    Ok(())
}

fn encode_frame(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > MAX_FRAME_LEN {
        return Err(TransportError::FrameTooLarge {
            length: body.len(),
            max: MAX_FRAME_LEN,
        });
    }

    let header = FrameHeader::new(body.len() as u32)?;
    let mut frame = BytesMut::with_capacity(HEADER_LEN + body.len());
    header.encode(&mut frame);
    frame.extend_from_slice(body);

    Ok(frame.to_vec())
}

fn decode_frame(frame: &[u8]) -> Result<Vec<u8>> {
    if frame.len() < HEADER_LEN {
        return Err(TransportError::UnexpectedEof);
    }

    let mut buf = frame;
    let header = FrameHeader::decode(&mut buf)?;
    let payload_length = header.length as usize;

    if buf.remaining() < payload_length {
        return Err(TransportError::UnexpectedEof);
    }

    if buf.remaining() > payload_length {
        return Err(TransportError::InvalidFrame);
    }

    Ok(buf.copy_to_bytes(payload_length).to_vec())
}

fn read_frame_from<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut header_bytes = [0_u8; HEADER_LEN];
    read_exact_or_eof(reader, &mut header_bytes)?;

    let mut header_slice = header_bytes.as_slice();
    let header = FrameHeader::decode(&mut header_slice)?;
    let mut payload = vec![0_u8; header.length as usize];

    if !payload.is_empty() {
        read_exact_or_eof(reader, &mut payload)?;
    }

    Ok(payload)
}

fn decode_request_body(body: &[u8]) -> Result<Request> {
    let mut buf = body;

    if buf.remaining() < 5 {
        return Err(TransportError::UnexpectedEof);
    }

    let request_id = buf.get_u32();
    let opcode = Opcode::try_from(buf.get_u8())?;
    let payload = buf.to_vec();

    Ok(Request::new(request_id, opcode, payload))
}

fn decode_response_body(body: &[u8]) -> Result<Response> {
    let mut buf = body;

    if buf.remaining() < 5 {
        return Err(TransportError::UnexpectedEof);
    }

    let request_id = buf.get_u32();
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

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Error, ErrorKind};

    use command::Command;

    use super::{
        decode_request, decode_response, encode_request, encode_response, read_request_from,
        read_response_from, write_request_to, write_response_to,
    };
    use crate::constants::{HEADER_LEN, MAGIC_BYTES, MAX_FRAME_LEN, VERSION};
    use crate::{Request, Response, Status, TransportError};

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
            let request = Request::from_command(11, command.clone()).unwrap();
            let encoded = encode_request(&request).unwrap();
            let decoded = decode_request(&encoded).unwrap();

            assert_eq!(decoded, request);
            assert_eq!(decoded.into_command().unwrap(), command);
        }
    }

    #[test]
    fn response_frame_round_trip_for_payload_types() {
        let responses = vec![
            Response::ok(1),
            Response::not_found(2),
            Response::error(3, "SRV-400", "Bad Request", "bad request").unwrap(),
            Response::value(4, "alice").unwrap(),
            Response::boolean(5, true),
            Response::count(6, 42),
            Response::entries(7, &[("name".to_string(), "alice".to_string())]).unwrap(),
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
            9,
            Command::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
            },
        )
        .unwrap();
        let response = Response::value(9, "alice").unwrap();
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
    fn rejects_bad_magic() {
        let request = Request::from_command(
            1,
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
            1,
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
        let mut frame = Vec::from(MAGIC_BYTES);
        frame.push(VERSION);
        frame.push(0);
        frame.extend_from_slice(&5_u32.to_be_bytes());
        frame.extend_from_slice(&1_u32.to_be_bytes());
        frame.push(0xff);

        assert!(matches!(
            decode_request(&frame),
            Err(TransportError::UnknownOpcode(0xff))
        ));
    }

    #[test]
    fn rejects_unknown_status() {
        let mut frame = Vec::from(MAGIC_BYTES);
        frame.push(VERSION);
        frame.push(0);
        frame.extend_from_slice(&5_u32.to_be_bytes());
        frame.extend_from_slice(&1_u32.to_be_bytes());
        frame.push(0xff);

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
            1,
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

        assert!(matches!(
            decode_request(&frame),
            Err(TransportError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_invalid_utf8_payload() {
        let response = Response {
            request_id: 1,
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
            1,
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
            1,
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
}
