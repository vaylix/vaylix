mod client;
mod helper;

use anyhow::Result;
pub use client::Client;

fn main() -> Result<()> {
    let mut client = Client::new()?;
    client.run()?;

    Ok(())
}
