//! Shared transport primitives for framed client/server communication.

pub mod codec;
pub mod constants;
pub mod error;
pub mod frame;
pub mod negotiation;
pub mod opcode;
pub mod options;
pub mod request;
pub mod response;

pub use codec::{
    decode_request, decode_response, encode_request, encode_request_with_options, encode_response,
    encode_response_with_options, read_request_from, read_request_from_async,
    read_request_from_async_with_options, read_request_from_with_options, read_response_from,
    read_response_from_async, read_response_from_async_with_options,
    read_response_from_with_options, write_request_to, write_request_to_async,
    write_request_to_async_with_options, write_request_to_with_options, write_response_to,
    write_response_to_async, write_response_to_async_with_options, write_response_to_with_options,
};
pub use constants::{FLAGS_NONE, MAGIC, MAGIC_BYTES, MAX_FRAME_LEN, VERSION};
pub use error::{Result, TransportError};
pub use frame::FrameHeader;
pub use negotiation::{
    CAP_PIPELINING, CAP_REQUEST_DEADLINE, CAP_SERVER_METRICS, CAP_TRACE_CONTEXT, CAP_ZSTD,
    ClientHello, DEFAULT_CAPABILITIES, ServerHello, client_options_from_server_hello,
    negotiate_server_options, read_client_hello_from, read_client_hello_from_async,
    read_server_hello_from, read_server_hello_from_async, write_client_hello_to,
    write_client_hello_to_async, write_server_hello_to, write_server_hello_to_async,
};
pub use opcode::Opcode;
pub use options::{CodecOptions, CompressionMode};
pub use request::Request;
pub use request::RequestMetadata;
pub use response::{ErrorPayload, Response, ScanPayload, Status};
