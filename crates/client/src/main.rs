mod args;
mod client;
mod error;
mod helper;
mod paths;

use args::{Args, OutputModeArg};
use clap::Parser;
use client::{Client, ClientConfig, OutputMode};
use std::path::PathBuf;
use transport::{CodecOptions, CompressionMode};
use url::Url;

const DEFAULT_USERNAME: &str = "vaylix";
const DEFAULT_PASSWORD: &str = "vaylix";

fn main() {
    if let Err(err) = try_main() {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

fn try_main() -> error::Result<()> {
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
            .find_map(|(key, value)| (key == "ssl").then(|| value.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let ssl = args.ssl || url_ssl;
        let tls_ca_cert = args.tls_ca_cert.or_else(|| {
            parsed.query_pairs().find_map(|(key, value)| {
                (key == "ca_cert").then(|| PathBuf::from(value.into_owned()))
            })
        });
        let tls_client_cert = args.tls_client_cert.or_else(|| {
            parsed.query_pairs().find_map(|(key, value)| {
                (key == "client_cert").then(|| PathBuf::from(value.into_owned()))
            })
        });
        let tls_client_key = args.tls_client_key.or_else(|| {
            parsed.query_pairs().find_map(|(key, value)| {
                (key == "client_key").then(|| PathBuf::from(value.into_owned()))
            })
        });
        if tls_ca_cert.is_some() && !ssl {
            return Err(error::ClientError::InvalidConfiguration(
                "tls_ca_cert requires ssl=true or --ssl",
            ));
        }
        validate_mtls_config(ssl, tls_client_cert.as_ref(), tls_client_key.as_ref())?;
        let disable_auth = args.disable_auth
            || parsed
                .query_pairs()
                .any(|(key, value)| key == "auth" && value.eq_ignore_ascii_case("false"));
        let disable_compression = args.disable_compression
            || parsed
                .query_pairs()
                .any(|(key, value)| key == "compression" && value.eq_ignore_ascii_case("none"));

        Ok(ClientConfig {
            host,
            port,
            ssl,
            tls_ca_cert,
            tls_client_cert,
            tls_client_key,
            username: (!disable_auth).then(|| {
                args.user
                    .or(parsed_username)
                    .unwrap_or_else(|| DEFAULT_USERNAME.to_string())
            }),
            password: (!disable_auth).then(|| {
                args.password
                    .or(parsed_password)
                    .unwrap_or_else(|| DEFAULT_PASSWORD.to_string())
            }),
            output,
            transport: transport_options(disable_compression),
        })
    } else {
        if args.tls_ca_cert.is_some() && !args.ssl {
            return Err(error::ClientError::InvalidConfiguration(
                "tls_ca_cert requires --ssl",
            ));
        }
        validate_mtls_config(
            args.ssl,
            args.tls_client_cert.as_ref(),
            args.tls_client_key.as_ref(),
        )?;
        Ok(ClientConfig {
            host: args.host,
            port: args.port,
            ssl: args.ssl,
            tls_ca_cert: args.tls_ca_cert,
            tls_client_cert: args.tls_client_cert,
            tls_client_key: args.tls_client_key,
            username: (!args.disable_auth)
                .then(|| args.user.unwrap_or_else(|| DEFAULT_USERNAME.to_string())),
            password: (!args.disable_auth).then(|| {
                args.password
                    .unwrap_or_else(|| DEFAULT_PASSWORD.to_string())
            }),
            output: cli_output,
            transport: transport_options(args.disable_compression),
        })
    }
}

fn validate_mtls_config(
    ssl: bool,
    tls_client_cert: Option<&PathBuf>,
    tls_client_key: Option<&PathBuf>,
) -> error::Result<()> {
    if (tls_client_cert.is_some() || tls_client_key.is_some()) && !ssl {
        return Err(error::ClientError::InvalidConfiguration(
            "tls client certificate options require ssl=true or --ssl",
        ));
    }
    if tls_client_cert.is_some() != tls_client_key.is_some() {
        return Err(error::ClientError::InvalidConfiguration(
            "tls_client_cert and tls_client_key must be provided together",
        ));
    }

    Ok(())
}

fn transport_options(disable_compression: bool) -> CodecOptions {
    if disable_compression {
        CodecOptions {
            compression: CompressionMode::None,
            compression_threshold_bytes: 0,
            ..CodecOptions::default()
        }
    } else {
        CodecOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PASSWORD, DEFAULT_USERNAME, parse_client_config};
    use crate::args::{Args, OutputModeArg};
    use crate::client::OutputMode;
    use clap::Parser;
    use transport::CompressionMode;

    #[test]
    fn url_credentials_and_output_are_parsed() {
        let config = parse_client_config(Args {
            url: Some("vaylix://alice:secret@db.internal:9999?output=json".to_string()),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            user: None,
            password: None,
            disable_auth: false,
            disable_compression: false,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert_eq!(config.host, "db.internal");
        assert_eq!(config.port, 9999);
        assert_eq!(config.username.as_deref(), Some("alice"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.output, OutputMode::Json);
        assert!(!config.ssl);
        assert_eq!(config.transport.compression, CompressionMode::Zstd);
    }

    #[test]
    fn explicit_flags_override_url_credentials() {
        let config = parse_client_config(Args {
            url: Some("vaylix://alice:secret@db.internal:9999?ssl=true".to_string()),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            user: Some("override".to_string()),
            password: Some("override-pass".to_string()),
            disable_auth: false,
            disable_compression: false,
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
            tls_client_cert: None,
            tls_client_key: None,
            user: None,
            password: None,
            disable_auth: false,
            disable_compression: false,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert_eq!(config.username.as_deref(), Some(DEFAULT_USERNAME));
        assert_eq!(config.password.as_deref(), Some(DEFAULT_PASSWORD));
    }

    #[test]
    fn can_disable_auth_and_compression() {
        let config = parse_client_config(Args {
            url: Some(
                "vaylix://alice:secret@db.internal:9999?auth=false&compression=none".to_string(),
            ),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            user: None,
            password: None,
            disable_auth: false,
            disable_compression: false,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert_eq!(config.transport.compression, CompressionMode::None);
    }

    #[test]
    fn ssl_flag_accepts_optional_bool_value() {
        let enabled = Args::try_parse_from(["vaylix-client", "--ssl"]).unwrap();
        assert!(enabled.ssl);

        let explicit_false = Args::try_parse_from(["vaylix-client", "--ssl", "false"]).unwrap();
        assert!(!explicit_false.ssl);
    }

    #[test]
    fn url_can_configure_mutual_tls_paths() {
        let config = parse_client_config(Args {
            url: Some(
                "vaylix://alice:secret@db.internal:9999?ssl=true&client_cert=/tmp/client.crt&client_key=/tmp/client.key"
                    .to_string(),
            ),
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            user: None,
            password: None,
            disable_auth: false,
            disable_compression: false,
            output: OutputModeArg::Plain,
        })
        .unwrap();

        assert!(config.ssl);
        assert_eq!(
            config.tls_client_cert.as_deref(),
            Some(std::path::Path::new("/tmp/client.crt"))
        );
        assert_eq!(
            config.tls_client_key.as_deref(),
            Some(std::path::Path::new("/tmp/client.key"))
        );
    }

    #[test]
    fn rejects_partial_mutual_tls_configuration() {
        let err = parse_client_config(Args {
            url: None,
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: true,
            tls_ca_cert: None,
            tls_client_cert: Some("/tmp/client.crt".into()),
            tls_client_key: None,
            user: None,
            password: None,
            disable_auth: false,
            disable_compression: false,
            output: OutputModeArg::Plain,
        })
        .unwrap_err();

        assert_eq!(err.code(), "CLI-008");
    }
}
