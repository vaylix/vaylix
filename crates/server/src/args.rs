use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
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

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum ReplicationRoleMode {
    Standalone,
    Leader,
    Follower,
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum WriteAckModeArg {
    Local,
    #[value(alias = "majority")]
    Replica,
    All,
}

/// Command-line arguments for the Vaylix server binary.
#[derive(Parser, Debug)]
#[command(name = "vaylix", about = "Vaylix database server")]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<AdminCommand>,

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
    #[arg(long, env = "VAYLIX_SNAPSHOT_INTERVAL_SECONDS")]
    pub snapshot_interval_seconds: Option<u64>,

    /// Background expiration sweep interval in seconds. Disabled when omitted.
    #[arg(long, env = "VAYLIX_EXPIRATION_SWEEP_INTERVAL_SECONDS")]
    pub expiration_sweep_interval_seconds: Option<u64>,

    /// Disconnect idle clients after this many seconds. Disabled when omitted.
    #[arg(long, env = "VAYLIX_IDLE_TIMEOUT_SECONDS")]
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

    /// Durable server data directory.
    #[arg(long, env = "VAYLIX_DATA_DIR", default_value = engine::DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

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

    /// Emit one connection log line for every validated request.
    #[arg(long, env = "VAYLIX_LOG_REQUESTS", default_value_t = false)]
    pub log_requests: bool,

    /// Maximum request payload bytes accepted per command after framing.
    #[arg(
        long,
        env = "VAYLIX_MAX_REQUEST_PAYLOAD_BYTES",
        default_value_t = 1_048_576
    )]
    pub max_request_payload_bytes: usize,

    /// Maximum key size in bytes accepted by the server.
    #[arg(long, env = "VAYLIX_MAX_KEY_BYTES", default_value_t = 1_024)]
    pub max_key_bytes: usize,

    /// Maximum string value size in bytes accepted by the server.
    #[arg(long, env = "VAYLIX_MAX_VALUE_BYTES", default_value_t = 262_144)]
    pub max_value_bytes: usize,

    /// Maximum number of keys allowed in a multi-key command.
    #[arg(long, env = "VAYLIX_MAX_KEYS_PER_BATCH", default_value_t = 256)]
    pub max_keys_per_batch: usize,

    /// Maximum queued commands allowed inside a session transaction.
    #[arg(long, env = "VAYLIX_MAX_TRANSACTION_QUEUE_LEN", default_value_t = 128)]
    pub max_transaction_queue_len: usize,

    /// Sustained request rate per connection.
    #[arg(long, env = "VAYLIX_REQUESTS_PER_SECOND", default_value_t = 200)]
    pub requests_per_second: u32,

    /// Burst size for the per-connection request limiter.
    #[arg(long, env = "VAYLIX_REQUEST_BURST", default_value_t = 400)]
    pub request_burst: u32,

    /// Optional audit log path override. Defaults to <data-dir>/audit.log.
    #[arg(long, env = "VAYLIX_AUDIT_LOG_PATH")]
    pub audit_log_path: Option<PathBuf>,

    /// Record a generic audit line for every command. Security/operator semantic audit events stay enabled regardless.
    #[arg(long, env = "VAYLIX_AUDIT_COMMANDS", default_value_t = false)]
    pub audit_commands: bool,

    /// Record slow-command audit events at or above this latency in milliseconds. Use 0 to disable.
    #[arg(long, env = "VAYLIX_SLOW_COMMAND_THRESHOLD_MS", default_value_t = 100)]
    pub slow_command_threshold_ms: u64,

    /// Maximum size of one WAL segment before rotation.
    #[arg(long, env = "VAYLIX_WAL_SEGMENT_SIZE_BYTES", default_value_t = engine::DEFAULT_WAL_SEGMENT_SIZE_BYTES)]
    pub wal_segment_size_bytes: u64,

    /// Maximum number of sealed WAL segments retained after snapshot pruning.
    #[arg(long, env = "VAYLIX_WAL_RETAIN_SEGMENTS", default_value_t = engine::DEFAULT_WAL_RETAIN_SEGMENTS)]
    pub wal_retain_segments: usize,

    /// Maximum time a transaction may remain open before automatic discard.
    #[arg(long, env = "VAYLIX_TRANSACTION_MAX_SECONDS", default_value_t = 30)]
    pub transaction_max_seconds: u64,

    /// Rolling authentication failure window in seconds.
    #[arg(
        long,
        env = "VAYLIX_AUTH_FAILURE_WINDOW_SECONDS",
        default_value_t = 300
    )]
    pub auth_failure_window_seconds: u64,

    /// Maximum failed authentication attempts allowed in one failure window before lockout.
    #[arg(long, env = "VAYLIX_AUTH_FAILURE_LIMIT", default_value_t = 5)]
    pub auth_failure_limit: u32,

    /// Lockout duration in seconds after exceeding the auth failure limit.
    #[arg(long, env = "VAYLIX_AUTH_LOCKOUT_SECONDS", default_value_t = 900)]
    pub auth_lockout_seconds: u64,

    /// Replication role for this node.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_ROLE",
        value_enum,
        default_value_t = ReplicationRoleMode::Standalone
    )]
    pub replication_role: ReplicationRoleMode,

    /// Stable node identifier used by replication metadata and follower acknowledgements.
    #[arg(long, env = "VAYLIX_NODE_ID")]
    pub node_id: Option<String>,

    /// Stable replication group identifier shared by leader and followers.
    #[arg(long, env = "VAYLIX_REPLICATION_GROUP_ID", default_value = "default")]
    pub replication_group_id: String,

    /// Address advertised by a leader for replication diagnostics.
    #[arg(long, env = "VAYLIX_REPLICATION_ADVERTISE_ADDR")]
    pub replication_advertise_addr: Option<String>,

    /// Upstream leader address for follower replication, in host:port form.
    #[arg(long, env = "VAYLIX_REPLICATION_UPSTREAM")]
    pub replication_upstream: Option<String>,

    /// Upstream leader username used by followers when authenticating replication requests.
    #[arg(long, env = "VAYLIX_REPLICATION_USER")]
    pub replication_user: Option<String>,

    /// Upstream leader password used by followers when authenticating replication requests.
    #[arg(long, env = "VAYLIX_REPLICATION_PASSWORD")]
    pub replication_password: Option<String>,

    /// Client-visible write acknowledgement mode.
    #[arg(
        long,
        env = "VAYLIX_WRITE_ACK_MODE",
        value_enum,
        default_value_t = WriteAckModeArg::Replica
    )]
    pub write_ack_mode: WriteAckModeArg,

    /// Maximum time in milliseconds to wait for follower acknowledgements.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_ACK_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    pub replication_ack_timeout_ms: u64,

    /// Follower poll interval in milliseconds when syncing from a leader.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_POLL_INTERVAL_MS",
        default_value_t = 250
    )]
    pub replication_poll_interval_ms: u64,

    /// Maximum WAL entries fetched by a follower in one replication round trip.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_FETCH_BATCH_SIZE",
        default_value_t = 256
    )]
    pub replication_fetch_batch_size: usize,

    /// Follower lag threshold in seconds before health degrades to stale.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_STALE_AFTER_SECONDS",
        default_value_t = 15
    )]
    pub replication_stale_after_seconds: u64,

    /// Heartbeat interval in milliseconds for leader-to-peer cluster traffic.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_HEARTBEAT_INTERVAL_MS",
        default_value_t = 300
    )]
    pub replication_heartbeat_interval_ms: u64,

    /// Minimum election timeout in milliseconds.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_ELECTION_TIMEOUT_MIN_MS",
        default_value_t = 1_500
    )]
    pub replication_election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds.
    #[arg(
        long,
        env = "VAYLIX_REPLICATION_ELECTION_TIMEOUT_MAX_MS",
        default_value_t = 3_000
    )]
    pub replication_election_timeout_max_ms: u64,

    /// Static cluster peers in `node_id@host:port` form. Repeated or comma-delimited.
    #[arg(long, env = "VAYLIX_CLUSTER_PEERS", value_delimiter = ',')]
    pub cluster_peers: Vec<String>,
}

#[derive(Subcommand, Debug)]
pub enum AdminCommand {
    Storage(StorageCommand),
    Pitr(PitrCommand),
    Healthcheck(HealthcheckCommand),
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum HealthcheckKind {
    Liveness,
    Readiness,
}

#[derive(ClapArgs, Debug)]
pub struct HealthcheckCommand {
    /// Healthcheck mode. Docker should use liveness; readiness is for traffic gating.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_KIND", value_enum, default_value_t = HealthcheckKind::Liveness)]
    pub kind: HealthcheckKind,

    /// Host used for the local healthcheck probe.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port used for the local healthcheck probe. Defaults to VAYLIX_PORT, then 9173.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_PORT")]
    pub port: Option<u16>,

    /// Username used when the readiness probe must authenticate.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_USER")]
    pub user: Option<String>,

    /// Password used when the readiness probe must authenticate.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_PASSWORD")]
    pub password: Option<String>,

    /// Probe timeout in milliseconds.
    #[arg(long, env = "VAYLIX_HEALTHCHECK_TIMEOUT_MS", default_value_t = 2_000)]
    pub timeout_ms: u64,
}

#[derive(ClapArgs, Debug)]
pub struct StorageCommand {
    #[command(subcommand)]
    pub action: StorageAction,
}

#[derive(Subcommand, Debug)]
pub enum StorageAction {
    Migrate {
        #[arg(long, env = "VAYLIX_DATA_DIR")]
        data_dir: PathBuf,
    },
    Verify {
        #[arg(long, env = "VAYLIX_DATA_DIR")]
        data_dir: PathBuf,
    },
}

#[derive(ClapArgs, Debug)]
pub struct PitrCommand {
    #[command(subcommand)]
    pub action: PitrAction,
}

#[derive(Subcommand, Debug)]
pub enum PitrAction {
    Inspect {
        #[arg(long, env = "VAYLIX_DATA_DIR")]
        data_dir: PathBuf,
    },
    Restore {
        #[arg(long)]
        source_dir: PathBuf,
        #[arg(long)]
        target_dir: PathBuf,
        #[arg(long, conflicts_with = "to_timestamp_ms")]
        to_sequence: Option<u64>,
        #[arg(long, conflicts_with = "to_sequence")]
        to_timestamp_ms: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::{AdminCommand, Args, HealthcheckKind, PitrAction, StorageAction, WriteAckModeArg};
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

    #[test]
    fn parses_storage_and_pitr_subcommands() {
        let parsed =
            Args::try_parse_from(["vaylix", "storage", "verify", "--data-dir", "/tmp/db"]).unwrap();
        assert!(matches!(
            parsed.command,
            Some(AdminCommand::Storage(command))
                if matches!(command.action, StorageAction::Verify { .. })
        ));

        let parsed = Args::try_parse_from([
            "vaylix",
            "pitr",
            "restore",
            "--source-dir",
            "/tmp/source",
            "--target-dir",
            "/tmp/target",
            "--to-sequence",
            "42",
        ])
        .unwrap();
        assert!(matches!(
            parsed.command,
            Some(AdminCommand::Pitr(command))
                if matches!(command.action, PitrAction::Restore { to_sequence: Some(42), .. })
        ));
    }

    #[test]
    fn write_ack_defaults_to_majority_and_accepts_alias() {
        let parsed = Args::try_parse_from(["vaylix"]).unwrap();
        assert_eq!(parsed.write_ack_mode, WriteAckModeArg::Replica);

        let parsed = Args::try_parse_from(["vaylix", "--write-ack-mode", "majority"]).unwrap();
        assert_eq!(parsed.write_ack_mode, WriteAckModeArg::Replica);
    }

    #[test]
    fn parses_healthcheck_subcommand() {
        let parsed = Args::try_parse_from([
            "vaylix",
            "healthcheck",
            "--kind",
            "readiness",
            "--host",
            "127.0.0.1",
            "--port",
            "9174",
            "--user",
            "health",
            "--password",
            "secret",
            "--timeout-ms",
            "500",
        ])
        .unwrap();

        assert!(matches!(
            parsed.command,
                Some(AdminCommand::Healthcheck(command))
                if command.kind == HealthcheckKind::Readiness
                    && command.host == "127.0.0.1"
                    && command.port == Some(9174)
                    && command.user.as_deref() == Some("health")
                    && command.password.as_deref() == Some("secret")
                    && command.timeout_ms == 500
        ));
    }
}
