mod args;
mod client;
mod error;
mod launcher;
mod report;
mod run;

use clap::Parser;

use crate::args::Args;
use crate::error::Result;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn try_main() -> Result<()> {
    let args = Args::parse();
    let report = run::execute(args).await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
