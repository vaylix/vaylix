pub const MAGIC_BYTES: [u8; 4] = *b"VTP1";
pub const MAGIC: u32 = u32::from_be_bytes(MAGIC_BYTES);
pub const VERSION: u8 = 1;
pub const FLAGS_NONE: u8 = 0;
pub const HEADER_LEN: usize = 10;
pub const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;
