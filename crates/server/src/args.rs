use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use transport::CompressionMode;

use crate::auth::{DEFAULT_PASSWORD, DEFAULT_USERNAME};

/// CLI-friendly WAL durability modes.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum WalSyncMode {
    /// Leave durability to the operating system page cache.
    Buffered,
    /// Flush userspace buffers after each append.
    Flush,
    /// Force the kernel to sync written data after each append.
    Sync,
}

/// CLI-friendly transport compression modes.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum CompressionModeArg {
    None,
    Zstd,
}

impl From<CompressionModeArg> for CompressionMode {
    fn from(value: CompressionModeArg) -> Self {
        match value {
            CompressionModeArg::None => CompressionMode::None,
            CompressionModeArg::Zstd => CompressionMode::Zstd,
        }
    }
}

/// Command-line arguments for the Vaylix server binary.
#[derive(Parser, Debug)]
#[command(name = "vaylix", about = "Vaylix database server")]
pub struct Args {
    /// Address to bind to
    #[arg(long, env = "VAYLIX_BIND", default_value = "127.0.0.1")]
    pub bind: String,

    /// Port to bind to
    #[arg(long, env = "VAYLIX_PORT", default_value_t = 9173)]
    pub port: u16,

    /// Maximum number of concurrent client sessions.
    #[arg(long, env = "VAYLIX_MAX_CONNECTIONS", default_value_t = 256)]
    pub max_connections: usize,

    /// Background snapshot interval in seconds. Disabled when omitted.
    #[arg(long)]
    pub snapshot_interval_seconds: Option<u64>,

    /// Background expiration sweep interval in seconds. Disabled when omitted.
    #[arg(long)]
    pub expiration_sweep_interval_seconds: Option<u64>,

    /// Disconnect idle clients after this many seconds. Disabled when omitted.
    #[arg(long)]
    pub idle_timeout_seconds: Option<u64>,

    /// Enable TLS for client/server transport.
    #[arg(long, default_value_t = false)]
    pub ssl: bool,

    /// PEM-encoded TLS certificate chain used when SSL is enabled.
    #[arg(long, requires = "ssl")]
    pub tls_cert: Option<PathBuf>,

    /// PEM-encoded PKCS#8 or RSA private key used when SSL is enabled.
    #[arg(long, requires = "ssl")]
    pub tls_key: Option<PathBuf>,

    /// Optional data directory override. This is the directory that should be mounted in containers.
    #[arg(long, env = "VAYLIX_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// WAL durability mode for each committed write.
    #[arg(
        long,
        env = "VAYLIX_WAL_SYNC",
        value_enum,
        default_value_t = WalSyncMode::Flush
    )]
    pub wal_sync: WalSyncMode,

    /// Username required for authenticated access.
    #[arg(long, env = "VAYLIX_USER", default_value = DEFAULT_USERNAME)]
    pub user: String,

    /// Password required for authenticated access.
    #[arg(long, env = "VAYLIX_PASSWORD", default_value = DEFAULT_PASSWORD)]
    pub password: String,

    /// Maximum request payload bytes accepted per command after framing.
    #[arg(long, default_value_t = 1_048_576)]
    pub max_request_payload_bytes: usize,

    /// Maximum key size in bytes accepted by the server.
    #[arg(long, default_value_t = 1_024)]
    pub max_key_bytes: usize,

    /// Maximum string value size in bytes accepted by the server.
    #[arg(long, default_value_t = 262_144)]
    pub max_value_bytes: usize,

    /// Maximum number of keys allowed in a multi-key command.
    #[arg(long, default_value_t = 256)]
    pub max_keys_per_batch: usize,

    /// Maximum queued commands allowed inside a session transaction.
    #[arg(long, default_value_t = 128)]
    pub max_transaction_queue_len: usize,

    /// Sustained request rate per connection.
    #[arg(long, default_value_t = 200)]
    pub requests_per_second: u32,

    /// Burst size for the per-connection request limiter.
    #[arg(long, default_value_t = 400)]
    pub request_burst: u32,

    /// Compression mode used for outbound transport frames.
    #[arg(long, value_enum, default_value_t = CompressionModeArg::None)]
    pub compression: CompressionModeArg,

    /// Minimum payload size before outbound transport compression is attempted.
    #[arg(long, default_value_t = 256)]
    pub compression_threshold_bytes: usize,

    /// Optional audit log path override. Defaults to <data-dir>/audit.log.
    #[arg(long)]
    pub audit_log_path: Option<PathBuf>,
}
