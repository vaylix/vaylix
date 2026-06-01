use std::path::PathBuf;

use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "vaylix-bench",
    about = "Async load generator for Vaylix benchmarks"
)]
pub struct Args {
    #[command(subcommand)]
    pub command: BenchmarkCommand,
}

#[derive(Subcommand, Debug)]
pub enum BenchmarkCommand {
    Run(RunArgs),
    BaselineSingleNode(BaselineArgs),
    BaselineQuorum(BaselineArgs),
    TransactionFlow(ProfileArgs),
    BackupRestore(ProfileArgs),
    AuthRbacChurn(ProfileArgs),
    QuorumWriteCost(ProfileArgs),
    ManagedSingleNode(ManagedArgs),
    ManagedQuorum(ManagedArgs),
    ManagedTransactionFlow(ManagedArgs),
    ManagedBackupRestore(ManagedArgs),
    ManagedAuthRbacChurn(ManagedArgs),
    ManagedQuorumWriteCost(ManagedArgs),
    ExampleCerts(ExampleCertsArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum, PartialEq, Eq)]
pub enum WorkloadKind {
    Get,
    Set,
    Mixed,
    Ping,
    TransactionFlow,
    BackupRestore,
    AuthRbacChurn,
    QuorumWriteCost,
}

#[derive(ClapArgs, Debug, Clone, Default)]
pub struct TlsArgs {
    #[arg(long, env = "VAYLIX_BENCH_TLS", default_value_t = false)]
    pub tls: bool,

    #[arg(long, env = "VAYLIX_BENCH_TLS_CA_CERT", requires = "tls")]
    pub tls_ca_cert: Option<PathBuf>,

    #[arg(long, env = "VAYLIX_BENCH_TLS_CLIENT_CERT", requires = "tls")]
    pub tls_client_cert: Option<PathBuf>,

    #[arg(long, env = "VAYLIX_BENCH_TLS_CLIENT_KEY", requires = "tls")]
    pub tls_client_key: Option<PathBuf>,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct RunArgs {
    #[arg(long, env = "VAYLIX_BENCH_ADDR")]
    pub addr: String,

    #[arg(long, env = "VAYLIX_BENCH_USERNAME")]
    pub username: Option<String>,

    #[arg(long, env = "VAYLIX_BENCH_PASSWORD")]
    pub password: Option<String>,

    #[command(flatten)]
    pub tls: TlsArgs,

    #[arg(long, env = "VAYLIX_BENCH_CONNECTIONS", default_value_t = 32)]
    pub connections: usize,

    #[arg(long, env = "VAYLIX_BENCH_DURATION_SECONDS", default_value_t = 30)]
    pub duration_seconds: u64,

    #[arg(long, env = "VAYLIX_BENCH_KEYSPACE", default_value_t = 10_000)]
    pub keyspace: usize,

    #[arg(long, env = "VAYLIX_BENCH_VALUE_SIZE", default_value_t = 256)]
    pub value_size: usize,

    #[arg(long, env = "VAYLIX_BENCH_SEED_KEYS", default_value_t = 2_048)]
    pub seed_keys: usize,

    #[arg(long, env = "VAYLIX_BENCH_WORKLOAD", value_enum, default_value_t = WorkloadKind::Mixed)]
    pub workload: WorkloadKind,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct BaselineArgs {
    #[arg(long, env = "VAYLIX_BENCH_ADDR")]
    pub addr: String,

    #[arg(long, env = "VAYLIX_BENCH_USERNAME")]
    pub username: Option<String>,

    #[arg(long, env = "VAYLIX_BENCH_PASSWORD")]
    pub password: Option<String>,

    #[command(flatten)]
    pub tls: TlsArgs,

    #[arg(long, env = "VAYLIX_BENCH_DURATION_SECONDS", default_value_t = 30)]
    pub duration_seconds: u64,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct ProfileArgs {
    #[arg(long, env = "VAYLIX_BENCH_ADDR")]
    pub addr: String,

    #[arg(long, env = "VAYLIX_BENCH_USERNAME")]
    pub username: Option<String>,

    #[arg(long, env = "VAYLIX_BENCH_PASSWORD")]
    pub password: Option<String>,

    #[command(flatten)]
    pub tls: TlsArgs,

    #[arg(long, env = "VAYLIX_BENCH_DURATION_SECONDS", default_value_t = 30)]
    pub duration_seconds: u64,

    #[arg(long, env = "VAYLIX_BENCH_CONNECTIONS")]
    pub connections: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_KEYSPACE")]
    pub keyspace: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_VALUE_SIZE")]
    pub value_size: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_SEED_KEYS")]
    pub seed_keys: Option<usize>,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct ManagedArgs {
    #[arg(
        long,
        env = "VAYLIX_BENCH_SERVER_BIN",
        default_value = "target/debug/vaylix"
    )]
    pub server_bin: PathBuf,

    #[arg(long, env = "VAYLIX_BENCH_DURATION_SECONDS", default_value_t = 30)]
    pub duration_seconds: u64,

    #[arg(long, env = "VAYLIX_BENCH_USERNAME", default_value = "vaylix")]
    pub username: String,

    #[arg(long, env = "VAYLIX_BENCH_PASSWORD", default_value = "vaylix")]
    pub password: String,

    #[arg(long, env = "VAYLIX_BENCH_TLS", default_value_t = false)]
    pub tls: bool,

    #[arg(
        long,
        env = "VAYLIX_BENCH_MTLS",
        default_value_t = false,
        requires = "tls"
    )]
    pub mtls: bool,

    #[arg(long, env = "VAYLIX_BENCH_WORKDIR")]
    pub workdir: Option<PathBuf>,

    #[arg(long, env = "VAYLIX_BENCH_CONNECTIONS")]
    pub connections: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_SEED_KEYS")]
    pub seed_keys: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_KEYSPACE")]
    pub keyspace: Option<usize>,

    #[arg(long, env = "VAYLIX_BENCH_VALUE_SIZE")]
    pub value_size: Option<usize>,
}

#[derive(ClapArgs, Debug, Clone)]
pub struct ExampleCertsArgs {
    #[arg(long)]
    pub out_dir: PathBuf,
}
