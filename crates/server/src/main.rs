pub mod response;
pub mod server;

use anyhow::Result;
pub use response::Response;
pub use server::Server;

fn main() -> Result<()> {
    let mut server = Server::new()?;
    server.start()?;

    Ok(())
}
