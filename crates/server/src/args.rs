use clap::{Parser, ValueEnum};
use std::path::PathBuf;

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
    #[arg(
        long,
        env = "VAYLIX_SSL",
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub ssl: bool,

    /// PEM-encoded TLS certificate chain used when SSL is enabled.
    #[arg(long, env = "VAYLIX_TLS_CERT", requires = "ssl")]
    pub tls_cert: Option<PathBuf>,

    /// PEM-encoded PKCS#8 or RSA private key used when SSL is enabled.
    #[arg(long, env = "VAYLIX_TLS_KEY", requires = "ssl")]
    pub tls_key: Option<PathBuf>,

    /// PEM-encoded CA bundle used to require and verify client certificates.
    #[arg(long, env = "VAYLIX_TLS_CLIENT_CA", requires = "ssl")]
    pub tls_client_ca: Option<PathBuf>,

    /// Optional data directory override. This is the directory that should be mounted in containers.
    #[arg(long, env = "VAYLIX_DATA_DIR")]
    pub data_dir: Option<PathBuf>,

    /// Directory used for server-side logical backup and restore files.
    #[arg(long, env = "VAYLIX_BACKUP_DIR")]
    pub backup_dir: Option<PathBuf>,

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

    /// Disable authentication. Intended for local development and trusted test networks only.
    #[arg(long, env = "VAYLIX_DISABLE_AUTH", default_value_t = false)]
    pub disable_auth: bool,

    /// Disable outbound transport compression.
    #[arg(long, env = "VAYLIX_DISABLE_COMPRESSION", default_value_t = false)]
    pub disable_compression: bool,

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

    /// Optional audit log path override. Defaults to <data-dir>/audit.log.
    #[arg(long)]
    pub audit_log_path: Option<PathBuf>,

    /// Record slow-command audit events at or above this latency in milliseconds. Use 0 to disable.
    #[arg(long, env = "VAYLIX_SLOW_COMMAND_THRESHOLD_MS", default_value_t = 100)]
    pub slow_command_threshold_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn ssl_flag_accepts_optional_bool_value() {
        let enabled = Args::try_parse_from(["vaylix", "--ssl"]).unwrap();
        assert!(enabled.ssl);

        let explicit_false = Args::try_parse_from(["vaylix", "--ssl", "false"]).unwrap();
        assert!(!explicit_false.ssl);
    }

    #[test]
    fn tls_client_ca_requires_ssl() {
        let result = Args::try_parse_from(["vaylix", "--tls-client-ca", "/tmp/ca.crt"]);
        assert!(result.is_err());

        let parsed =
            Args::try_parse_from(["vaylix", "--ssl", "--tls-client-ca", "/tmp/ca.crt"]).unwrap();
        assert_eq!(
            parsed.tls_client_ca.as_deref(),
            Some(std::path::Path::new("/tmp/ca.crt"))
        );
    }
}
