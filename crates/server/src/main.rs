mod args;
mod error;
mod server;

use args::Args;
use clap::Parser;
use server::Server;

const BANNER: &str = r#"        
        ■ ■ ■
    ████████████
      ████████
         ██
Vaylix Database Server

"#;

fn main() {
    if let Err(err) = try_main() {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

fn try_main() -> error::Result<()> {
    let args = Args::parse();
    let mut server = Server::new(args.bind, args.port)?;

    println!("{BANNER}");

    server.start()?;

    Ok(())
}
