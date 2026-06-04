use std::io::Read;

use crate::constants::{FLAG_COMPRESSED_ZSTD, FLAGS_NONE};
use crate::error::{Result, TransportError};
use crate::frame::FrameHeader;
use crate::options::{CodecOptions, CompressionMode};

pub(super) fn should_compress(body_len: usize, options: CodecOptions) -> bool {
    options.compression == CompressionMode::Zstd && body_len >= options.compression_threshold_bytes
}

pub(super) fn maybe_compress(body: &[u8], options: CodecOptions) -> Result<(u8, Vec<u8>)> {
    if !should_compress(body.len(), options) {
        return Ok((FLAGS_NONE, body.to_vec()));
    }

    let compressed =
        zstd::bulk::compress(body, 3).map_err(|_| TransportError::CompressionFailure)?;

    Ok((FLAG_COMPRESSED_ZSTD, compressed))
}

pub(super) fn maybe_decompress(
    header: &FrameHeader,
    payload: Vec<u8>,
    options: CodecOptions,
) -> Result<Vec<u8>> {
    match header.flags {
        FLAGS_NONE => Ok(payload),
        FLAG_COMPRESSED_ZSTD => {
            let decode_limit = options.max_decompressed_frame_len.saturating_add(1);
            let decoder = zstd::stream::read::Decoder::new(payload.as_slice())
                .map_err(|_| TransportError::CompressionFailure)?;
            let mut limited = decoder.take(decode_limit as u64);
            let mut decoded = Vec::new();
            limited
                .read_to_end(&mut decoded)
                .map_err(|_| TransportError::CompressionFailure)?;
            if decoded.len() > options.max_decompressed_frame_len {
                return Err(TransportError::DecompressedFrameTooLarge {
                    length: decoded.len(),
                    max: options.max_decompressed_frame_len,
                });
            }
            Ok(decoded)
        }
        other => Err(TransportError::UnsupportedFlags(other)),
    }
}
