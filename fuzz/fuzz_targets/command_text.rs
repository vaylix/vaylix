#![no_main]

use command::{Parser, tokenize};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let input = String::from_utf8_lossy(data);
    let _ = tokenize(&input);
    let _ = Parser::parse(&input);
});
