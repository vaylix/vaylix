/// Frame compression modes supported by the transport layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionMode {
    /// Disable transport compression.
    #[default]
    None,
    /// Compress frame payloads with zstd.
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
}

impl Default for CodecOptions {
    fn default() -> Self {
        Self {
            compression: CompressionMode::None,
            compression_threshold_bytes: 256,
        }
    }
}
