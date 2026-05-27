use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputModeArg {
    Plain,
    Table,
    Json,
}

#[derive(Parser, Debug)]
#[command(name = "vaylix-client", about = "Vaylix database client")]
pub struct Args {
    /// URL-style connection string, for example: vaylix://user:password@127.0.0.1:9173
    #[arg(long)]
    pub url: Option<String>,

    /// Address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind to
    #[arg(long, default_value_t = 9173)]
    pub port: u16,

    /// Enable TLS for the client connection.
    #[arg(
        long,
        default_value_t = false,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub ssl: bool,

    /// Optional PEM-encoded CA certificate used to validate the server certificate.
    #[arg(long)]
    pub tls_ca_cert: Option<PathBuf>,

    /// Username used for server authentication.
    #[arg(long)]
    pub user: Option<String>,

    /// Password used for server authentication.
    #[arg(long)]
    pub password: Option<String>,

    /// Disable automatic AUTH on connect.
    #[arg(long, default_value_t = false)]
    pub disable_auth: bool,

    /// Disable outbound transport compression.
    #[arg(long, default_value_t = false)]
    pub disable_compression: bool,

    /// Output rendering mode.
    #[arg(long, value_enum, default_value_t = OutputModeArg::Plain)]
    pub output: OutputModeArg,
}
