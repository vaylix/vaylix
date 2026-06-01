use std::path::PathBuf;

use url::Url;

use crate::args::{Args, OutputModeArg};
use crate::client::{ClientConfig, OutputMode};
use crate::error::{ClientError, Result};
use transport::{CodecOptions, CompressionMode};

pub(crate) const DEFAULT_USERNAME: &str = "vaylix";
pub(crate) const DEFAULT_PASSWORD: &str = "vaylix";

pub(crate) fn parse_client_config(args: Args) -> Result<ClientConfig> {
    let cli_output = match args.output {
        OutputModeArg::Plain => OutputMode::Plain,
        OutputModeArg::Table => OutputMode::Table,
        OutputModeArg::Json => OutputMode::Json,
    };
    if let Some(url) = args.url {
        let parsed = Url::parse(&url).map_err(std::io::Error::other)?;
        if parsed.scheme() != "vaylix" {
            return Err(ClientError::InvalidConfiguration("unsupported URL scheme"));
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
            return Err(ClientError::InvalidConfiguration(
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
            return Err(ClientError::InvalidConfiguration(
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
) -> Result<()> {
    if (tls_client_cert.is_some() || tls_client_key.is_some()) && !ssl {
        return Err(ClientError::InvalidConfiguration(
            "tls client certificate options require ssl=true or --ssl",
        ));
    }
    if tls_client_cert.is_some() != tls_client_key.is_some() {
        return Err(ClientError::InvalidConfiguration(
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
