mod args;
mod client;
mod helper;

use anyhow::Result;
use args::Args;
use clap::Parser;
use client::Client;

const BANNER: &str = r#"        
        ■ ■ ■
    ████████████
      ████████
         ██
Vaylix Database Client

"#;

fn main() -> Result<()> {
    let args = Args::parse();
    let mut client = Client::new(args.host, args.port)?;

    println!("{BANNER}");

    client.run()?;

    Ok(())
}
