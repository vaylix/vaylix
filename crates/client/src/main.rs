mod args;
mod client;
mod helper;

use anyhow::Result;
use args::Args;
use clap::Parser;
use client::Client;

fn main() -> Result<()> {
    let args = Args::parse();
    let mut client = Client::new(args.host, args.port)?;
    client.run()?;

    Ok(())
}
