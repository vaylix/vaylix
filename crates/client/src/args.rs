use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutputModeArg {
    Plain,
    Table,
    Json,
}

#[derive(Parser, Debug)]
#[command(name = "vaylix", about = "Vaylix database server")]
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

    /// Output rendering mode.
    #[arg(long, value_enum, default_value_t = OutputModeArg::Plain)]
    pub output: OutputModeArg,
}
