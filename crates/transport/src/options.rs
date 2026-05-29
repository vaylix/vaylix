use crate::constants::MAX_FRAME_LEN;

/// Frame compression modes supported by the transport layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionMode {
    /// Disable transport compression.
    None,
    /// Compress frame payloads with zstd.
    #[default]
    Zstd,
}

impl CompressionMode {
    /// Stable configuration string used by CLI layers and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
        }
    }
}

/// Write-time transport behavior configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecOptions {
    /// Compression mode used when sending frames.
    pub compression: CompressionMode,
    /// Minimum payload size required before compression is attempted.
    pub compression_threshold_bytes: usize,
    /// Maximum on-wire frame payload size accepted or emitted.
    pub max_frame_len: usize,
    /// Maximum decompressed payload size accepted after frame decompression.
    pub max_decompressed_frame_len: usize,
}

impl Default for CodecOptions {
    fn default() -> Self {
        Self {
            compression: CompressionMode::Zstd,
            compression_threshold_bytes: 0,
            max_frame_len: MAX_FRAME_LEN,
            max_decompressed_frame_len: MAX_FRAME_LEN,
        }
    }
}
