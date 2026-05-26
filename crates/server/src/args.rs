use clap::{Parser, ValueEnum};

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
    #[arg(long, default_value = "127.0.0.1")]
    pub bind: String,

    /// Port to bind to
    #[arg(long, default_value_t = 9173)]
    pub port: u16,

    /// Maximum number of concurrent client sessions.
    #[arg(long, default_value_t = 256)]
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

    /// WAL durability mode for each committed write.
    #[arg(long, value_enum, default_value_t = WalSyncMode::Flush)]
    pub wal_sync: WalSyncMode,

    /// Username required for authenticated access.
    #[arg(long)]
    pub auth_user: Option<String>,

    /// Password required for authenticated access.
    #[arg(long)]
    pub auth_password: Option<String>,
}
