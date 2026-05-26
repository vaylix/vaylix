mod args;
mod client;
mod error;
mod helper;
mod paths;

use args::{Args, OutputModeArg};
use clap::Parser;
use client::{Client, ClientConfig, OutputMode};
use url::Url;

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
    println!("{BANNER}");

    let args = Args::parse();
    let config = parse_client_config(args)?;
    let mut client = Client::new(config)?;
    client.run()?;

    Ok(())
}

fn parse_client_config(args: Args) -> error::Result<ClientConfig> {
    let output = match args.output {
        OutputModeArg::Plain => OutputMode::Plain,
        OutputModeArg::Table => OutputMode::Table,
        OutputModeArg::Json => OutputMode::Json,
    };

    if let Some(url) = args.url {
        let parsed = Url::parse(&url).map_err(std::io::Error::other)?;
        let host = parsed.host_str().unwrap_or("127.0.0.1").to_string();
        let port = parsed.port().unwrap_or(9173);
        let username = if parsed.username().is_empty() {
            None
        } else {
            Some(parsed.username().to_string())
        };
        let password = parsed.password().map(str::to_string);

        Ok(ClientConfig {
            host,
            port,
            username,
            password,
            output,
        })
    } else {
        Ok(ClientConfig {
            host: args.host,
            port: args.port,
            username: None,
            password: None,
            output,
        })
    }
}
