mod args;
mod client;
mod config;
mod error;
mod helper;
mod paths;

use args::Args;
use clap::Parser;
use client::Client;
use config::parse_client_config;

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

#[cfg(test)]
mod tests {
    use crate::args::{Args, OutputModeArg};
    use crate::client::OutputMode;
    use crate::config::{DEFAULT_PASSWORD, DEFAULT_USERNAME, parse_client_config};
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
