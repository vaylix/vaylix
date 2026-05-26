use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "vaylix", about = "Vaylix database server")]
pub struct Args {
    /// Address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind to
    #[arg(long, default_value_t = 9173)]
    pub port: u16,
}
