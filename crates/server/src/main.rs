mod args;
mod protocol;
mod response;
mod server;

use anyhow::Result;
use args::Args;
use clap::Parser;
use protocol::Protocol;
use response::Response;
use server::Server;

fn main() -> Result<()> {
    let args = Args::parse();
    let mut server = Server::new(args.bind, args.port)?;
    server.start()?;

    Ok(())
}
