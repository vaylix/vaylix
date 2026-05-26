mod args;
mod client;
mod error;
mod helper;
mod paths;

use args::{Args, OutputModeArg};
use clap::Parser;
use client::{Client, ClientConfig, OutputMode};
use std::path::PathBuf;
use url::Url;

const DEFAULT_USERNAME: &str = "vaylix";
const DEFAULT_PASSWORD: &str = "vaylix";

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
    let cli_output = match args.output {
        OutputModeArg::Plain => OutputMode::Plain,
        OutputModeArg::Table => OutputMode::Table,
        OutputModeArg::Json => OutputMode::Json,
    };

    if let Some(url) = args.url {
        let parsed = Url::parse(&url).map_err(std::io::Error::other)?;
        let host = parsed.host_str().unwrap_or("127.0.0.1").to_string();
        let port = parsed.port().unwrap_or(9173);
        let parsed_username = if parsed.username().is_empty() {
            None
        } else {
            Some(parsed.username().to_string())
        };
        let parsed_password = parsed.password().map(str::to_string);
        let output = parsed
            .query_pairs()
            .find_map(|(key, value)| {
                if key == "output" {
                    match value.as_ref() {
                        "plain" => Some(OutputMode::Plain),
                        "table" => Some(OutputMode::Table),
                        "json" => Some(OutputMode::Json),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .unwrap_or(cli_output);

        Ok(ClientConfig {
            host,
            port,
            ssl: args.ssl
                || parsed
                    .query_pairs()
                    .any(|(key, value)| key == "ssl" && value.eq_ignore_ascii_case("true")),
            tls_ca_cert: args.tls_ca_cert.or_else(|| {
                parsed.query_pairs().find_map(|(key, value)| {
                    (key == "ca_cert").then(|| PathBuf::from(value.into_owned()))
                })
            }),
            username: Some(
                args.user
                    .or(parsed_username)
                    .unwrap_or_else(|| DEFAULT_USERNAME.to_string()),
            ),
            password: Some(
                args.password
                    .or(parsed_password)
                    .unwrap_or_else(|| DEFAULT_PASSWORD.to_string()),
            ),
            output,
        })
    } else {
        Ok(ClientConfig {
            host: args.host,
            port: args.port,
            ssl: args.ssl,
            tls_ca_cert: args.tls_ca_cert,
            username: Some(args.user.unwrap_or_else(|| DEFAULT_USERNAME.to_string())),
            password: Some(
                args.password
                    .unwrap_or_else(|| DEFAULT_PASSWORD.to_string()),
            ),
            output: cli_output,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PASSWORD, DEFAULT_USERNAME, parse_client_config};
    use crate::args::{Args, OutputModeArg};
    use crate::client::OutputMode;

    #[test]
    fn url_credentials_and_output_are_parsed() {
        let config = parse_client_config(Args {
            url: Some("vaylix://alice:secret@db.internal:9999?output=json".to_string()),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            user: None,
            password: None,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert_eq!(config.host, "db.internal");
        assert_eq!(config.port, 9999);
        assert_eq!(config.username.as_deref(), Some("alice"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.output, OutputMode::Json);
    }

    #[test]
    fn explicit_flags_override_url_credentials() {
        let config = parse_client_config(Args {
            url: Some("vaylix://alice:secret@db.internal:9999?ssl=true".to_string()),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            user: Some("override".to_string()),
            password: Some("override-pass".to_string()),
            output: OutputModeArg::Table,
        })
        .unwrap();

        assert_eq!(config.username.as_deref(), Some("override"));
        assert_eq!(config.password.as_deref(), Some("override-pass"));
        assert_eq!(config.output, OutputMode::Table);
        assert!(config.ssl);
    }

    #[test]
    fn defaults_to_local_dev_credentials_without_url() {
        let config = parse_client_config(Args {
            url: None,
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            user: None,
            password: None,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert_eq!(config.username.as_deref(), Some(DEFAULT_USERNAME));
        assert_eq!(config.password.as_deref(), Some(DEFAULT_PASSWORD));
    }
}
