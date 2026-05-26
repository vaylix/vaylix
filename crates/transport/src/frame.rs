use bytes::{Buf, BufMut, BytesMut};

use crate::constants::{
    FLAG_COMPRESSED_ZSTD, FLAGS_NONE, HEADER_LEN, MAGIC, MAX_FRAME_LEN, VERSION,
};
use crate::error::{Result, TransportError};

/// Header metadata for a framed transport packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: u32,
    pub version: u8,
    pub flags: u8,
    pub length: u32,
    pub checksum: u32,
}

impl FrameHeader {
    /// Creates a new frame header for a payload of the given length.
    pub fn new(length: u32, checksum: u32) -> Result<Self> {
        if length as usize > MAX_FRAME_LEN {
            return Err(TransportError::FrameTooLarge {
                length: length as usize,
                max: MAX_FRAME_LEN,
            });
        }

        Ok(Self {
            magic: MAGIC,
            version: VERSION,
            flags: FLAGS_NONE,
            length,
            checksum,
        })
    }

    /// Encodes this header into the provided byte buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.reserve(HEADER_LEN);
        buf.put_u32(self.magic);
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_u32(self.length);
        buf.put_u32(self.checksum);
    }

    /// Decodes a header from the provided byte slice.
    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        if buf.remaining() < HEADER_LEN {
            return Err(TransportError::UnexpectedEof);
        }

        let magic = buf.get_u32();
        let version = buf.get_u8();
        let flags = buf.get_u8();
        let length = buf.get_u32();
        let checksum = buf.get_u32();

        if magic != MAGIC {
            return Err(TransportError::InvalidFrame);
        }

        if version != VERSION {
            return Err(TransportError::VersionMismatch {
                expected: VERSION,
                actual: version,
            });
        }

        if length as usize > MAX_FRAME_LEN {
            return Err(TransportError::FrameTooLarge {
                length: length as usize,
                max: MAX_FRAME_LEN,
            });
        }

        if flags & !FLAG_COMPRESSED_ZSTD != 0 {
            return Err(TransportError::UnsupportedFlags(flags));
        }

        Ok(Self {
            magic,
            version,
            flags,
            length,
            checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::FrameHeader;
    use crate::constants::{FLAGS_NONE, HEADER_LEN, MAGIC, MAX_FRAME_LEN, VERSION};
    use crate::error::TransportError;

    #[test]
    fn encodes_and_decodes_header() {
        let header = FrameHeader::new(32, 1234).unwrap();
        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        assert_eq!(buf.len(), HEADER_LEN);

        let mut slice = buf.as_ref();
        let decoded = FrameHeader::decode(&mut slice).unwrap();

        assert_eq!(
            decoded,
            FrameHeader {
                magic: MAGIC,
                version: VERSION,
                flags: FLAGS_NONE,
                length: 32,
                checksum: 1234,
            }
        );
        assert!(slice.is_empty());
    }

    #[test]
    fn rejects_oversized_header_length() {
        assert!(matches!(
            FrameHeader::new((MAX_FRAME_LEN + 1) as u32, 1),
            Err(TransportError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_truncated_header_decode() {
        let mut bytes = &[0_u8; HEADER_LEN - 1][..];
        assert!(matches!(
            FrameHeader::decode(&mut bytes),
            Err(TransportError::UnexpectedEof)
        ));
    }
}
