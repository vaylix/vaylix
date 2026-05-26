mod args;
mod client;
mod error;
mod helper;
mod paths;

use args::{Args, OutputModeArg};
use clap::Parser;
use client::{Client, ClientConfig, OutputMode};
use std::path::PathBuf;
use transport::CodecOptions;
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
    let cli_transport = CodecOptions {
        compression: args.compression.into(),
        compression_threshold_bytes: args.compression_threshold_bytes,
    };

    if let Some(url) = args.url {
        let parsed = Url::parse(&url).map_err(std::io::Error::other)?;
        if parsed.scheme() != "vaylix" {
            return Err(error::ClientError::InvalidConfiguration(
                "unsupported URL scheme",
            ));
        }
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
        let url_ssl = parsed
            .query_pairs()
            .any(|(key, value)| key == "ssl" && value.eq_ignore_ascii_case("true"));
        let compression = parsed
            .query_pairs()
            .find_map(|(key, value)| {
                if key == "compression" {
                    match value.as_ref() {
                        "none" => Some(transport::CompressionMode::None),
                        "zstd" => Some(transport::CompressionMode::Zstd),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .unwrap_or(cli_transport.compression);
        let compression_threshold_bytes = parsed
            .query_pairs()
            .find_map(|(key, value)| {
                (key == "compression_threshold_bytes")
                    .then(|| value.parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(cli_transport.compression_threshold_bytes);
        let ssl = args.ssl || url_ssl;
        let tls_ca_cert = args.tls_ca_cert.or_else(|| {
            parsed.query_pairs().find_map(|(key, value)| {
                (key == "ca_cert").then(|| PathBuf::from(value.into_owned()))
            })
        });
        if tls_ca_cert.is_some() && !ssl {
            return Err(error::ClientError::InvalidConfiguration(
                "tls_ca_cert requires ssl=true",
            ));
        }

        Ok(ClientConfig {
            host,
            port,
            ssl,
            tls_ca_cert,
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
            transport: CodecOptions {
                compression,
                compression_threshold_bytes,
            },
        })
    } else {
        if args.tls_ca_cert.is_some() && !args.ssl {
            return Err(error::ClientError::InvalidConfiguration(
                "tls_ca_cert requires --ssl",
            ));
        }
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
            transport: cli_transport,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PASSWORD, DEFAULT_USERNAME, parse_client_config};
    use crate::args::{Args, CompressionModeArg, OutputModeArg};
    use crate::client::OutputMode;
    use transport::CompressionMode;

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
            compression: CompressionModeArg::None,
            compression_threshold_bytes: 256,
        })
        .unwrap();

        assert_eq!(config.host, "db.internal");
        assert_eq!(config.port, 9999);
        assert_eq!(config.username.as_deref(), Some("alice"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.output, OutputMode::Json);
        assert_eq!(config.transport.compression, CompressionMode::None);
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
            compression: CompressionModeArg::None,
            compression_threshold_bytes: 256,
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
            compression: CompressionModeArg::None,
            compression_threshold_bytes: 256,
        })
        .unwrap();

        assert_eq!(config.username.as_deref(), Some(DEFAULT_USERNAME));
        assert_eq!(config.password.as_deref(), Some(DEFAULT_PASSWORD));
    }
}
