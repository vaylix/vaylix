mod args;
mod client;
mod error;
mod helper;
mod paths;

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

fn main() {
    if let Err(err) = try_main() {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

fn try_main() -> error::Result<()> {
    let args = Args::parse();
    let mut client = Client::new(args.host, args.port)?;

    println!("{BANNER}");

    client.run()?;

    Ok(())
}
