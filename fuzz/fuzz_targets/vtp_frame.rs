#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use transport::{
    decode_request, decode_response, read_client_hello_from, read_request_from, read_response_from,
    read_server_hello_from,
};

fuzz_target!(|data: &[u8]| {
    let _ = decode_request(data);
    let _ = decode_response(data);

    let mut cursor = Cursor::new(data);
    let _ = read_request_from(&mut cursor);

    let mut cursor = Cursor::new(data);
    let _ = read_response_from(&mut cursor);

    let mut cursor = Cursor::new(data);
    let _ = read_client_hello_from(&mut cursor);

    let mut cursor = Cursor::new(data);
    let _ = read_server_hello_from(&mut cursor);
});
