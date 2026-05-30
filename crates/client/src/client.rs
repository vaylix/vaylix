use crate::helper::ClientHelper;
use crate::paths::Paths;
use command::{COMMANDS, Command, Parser};
use rustls::pki_types::ServerName;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::{ClientConfig as TlsClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use serde_json::json;
use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use transport::{
    ClientHello, CodecOptions, Request, Response, Status, client_options_from_server_hello,
    read_response_from_with_options, read_server_hello_from, write_client_hello_to,
    write_request_to_with_options,
};
use uuid::Uuid;

use crate::error::{ClientError, Result};

/// Output formatting modes supported by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Plain,
    Table,
    Json,
}

/// Connection parameters resolved from CLI flags or a connection URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub ssl: bool,
    pub tls_ca_cert: Option<PathBuf>,
    pub tls_client_cert: Option<PathBuf>,
    pub tls_client_key: Option<PathBuf>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub output: OutputMode,
    pub transport: CodecOptions,
}

enum ClientStream {
    Tcp(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl std::io::Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl std::io::Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

/// Interactive CLI client for talking to a Vaylix server.
pub struct Client {
    editor: Editor<ClientHelper, rustyline::history::DefaultHistory>,
    stream: ClientStream,
    paths: Paths,
    output: OutputMode,
    transport: CodecOptions,
}

const PROMPT: &str = "vaylix> ";

impl Client {
    /// Connects to a server and prepares the REPL state.
    pub fn new(config: ClientConfig) -> Result<Self> {
        let rustyline_config = Builder::new()
            .completion_type(CompletionType::List)
            .edit_mode(EditMode::Emacs)
            .auto_add_history(true)
            .build();

        let addr = format!("{}:{}", config.host, config.port);

        let helper = ClientHelper::new();
        let paths = Paths::new()?;
        log_event(
            "INFO",
            "client.startup",
            &format!("connecting to {addr} ssl={}", config.ssl),
        );
        let tcp_stream = TcpStream::connect(addr)?;
        let mut stream = if config.ssl {
            ClientStream::Tls(Box::new(connect_tls(
                tcp_stream,
                &config.host,
                config.tls_ca_cert.as_deref(),
                config.tls_client_cert.as_deref(),
                config.tls_client_key.as_deref(),
            )?))
        } else {
            ClientStream::Tcp(tcp_stream)
        };
        let client_hello = ClientHello {
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            desired_compression: config.transport.compression,
            max_frame_len: config.transport.max_frame_len as u32,
            auth_intent: config.username.is_some(),
            ..ClientHello::new("vaylix-client", env!("CARGO_PKG_VERSION"))
        };
        write_client_hello_to(&mut stream, &client_hello)?;
        let server_hello = read_server_hello_from(&mut stream)?;
        let transport = client_options_from_server_hello(&server_hello)?;
        log_event("INFO", "client.startup", "connection established");

        let mut editor = Editor::<ClientHelper, DefaultHistory>::with_config(rustyline_config)?;
        editor.set_helper(Some(helper));
        editor.load_history(&paths.history_path).ok();

        let mut client = Self {
            editor,
            stream,
            paths,
            output: config.output,
            transport,
        };

        if let (Some(username), Some(password)) = (config.username, config.password) {
            client.execute(Command::Auth { username, password })?;
        }

        Ok(client)
    }

    /// Runs the interactive REPL loop until the user exits.
    pub fn run(&mut self) -> Result<()> {
        loop {
            let readline = self.editor.readline(PROMPT);

            match readline {
                Ok(line) => {
                    let line = line.trim();

                    if line.is_empty() {
                        continue;
                    }

                    let command = match Parser::parse(line) {
                        Ok(command) => command,
                        Err(err) => {
                            println!("[{}] {}: {err}", err.code(), err.name());
                            continue;
                        }
                    };

                    match command {
                        Command::Help => println!("{}", help_text()),
                        Command::Exit => {
                            log_event("INFO", "client.session", "received local exit command");
                            self.editor.save_history(&self.paths.history_path)?;
                            break;
                        }
                        command => self.execute(command)?,
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    log_event("INFO", "client.session", "readline interrupted");
                    println!("Exiting...");
                    self.editor.save_history(&self.paths.history_path)?;
                    break;
                }
                Err(ReadlineError::Eof) => {
                    log_event("INFO", "client.session", "reached end of input");
                    println!("Exiting...");
                    self.editor.save_history(&self.paths.history_path)?;
                    break;
                }
                Err(err) => {
                    let err = ClientError::Readline(err);
                    println!("[{}] {}: {err}", err.code(), err.name());
                }
            }
        }

        Ok(())
    }

    fn execute(&mut self, command: Command) -> Result<()> {
        let request_id = Uuid::now_v7();
        let request = Request::from_command(request_id, command.clone())?;
        write_request_to_with_options(&mut self.stream, &request, self.transport)?;
        self.stream.flush()?;

        let response = read_response_from_with_options(&mut self.stream, self.transport)?;

        if response.request_id != request_id {
            return Err(ClientError::ResponseIdMismatch {
                expected: request_id,
                actual: response.request_id,
            });
        }

        println!("{}", render_response(&command, &response, self.output)?);

        Ok(())
    }
}

fn connect_tls(
    stream: TcpStream,
    host: &str,
    ca_cert: Option<&std::path::Path>,
    client_cert: Option<&std::path::Path>,
    client_key: Option<&std::path::Path>,
) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let tls_config = build_tls_client_config(ca_cert, client_cert, client_key)?;
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|_| std::io::Error::other("invalid TLS server name"))?;
    let connection =
        ClientConnection::new(tls_config, server_name).map_err(std::io::Error::other)?;
    Ok(StreamOwned::new(connection, stream))
}

fn build_tls_client_config(
    ca_cert: Option<&std::path::Path>,
    client_cert: Option<&std::path::Path>,
    client_key: Option<&std::path::Path>,
) -> Result<Arc<TlsClientConfig>> {
    let mut roots = RootCertStore::empty();

    if let Some(ca_cert) = ca_cert {
        for cert in CertificateDer::pem_file_iter(ca_cert).map_err(std::io::Error::other)? {
            roots
                .add(cert.map_err(std::io::Error::other)?)
                .map_err(std::io::Error::other)?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        for cert in native.certs {
            roots.add(cert).map_err(std::io::Error::other)?;
        }
        if !native.errors.is_empty() && roots.is_empty() {
            return Err(std::io::Error::other("no native root certificates available").into());
        }
    }

    let builder = TlsClientConfig::builder().with_root_certificates(roots);
    let config = match (client_cert, client_key) {
        (Some(client_cert), Some(client_key)) => {
            let certs = load_cert_chain(client_cert)?;
            let key = load_private_key(client_key)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(std::io::Error::other)?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(std::io::Error::other(
                "tls_client_cert and tls_client_key must be provided together",
            )
            .into());
        }
    };

    Ok(Arc::new(config))
}

fn load_cert_chain(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(std::io::Error::other)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(std::io::Error::other)?;
    if certs.is_empty() {
        return Err(std::io::Error::other("client certificate file is empty").into());
    }

    Ok(certs)
}

fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    let key_bytes = std::fs::read(path)?;
    Ok(PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(std::io::Error::other)?
        .clone_key())
}

fn help_text() -> String {
    let name_width = COMMANDS
        .iter()
        .map(|command| command.name.len())
        .max()
        .unwrap_or(7)
        .max(7);
    let usage_width = COMMANDS
        .iter()
        .map(|command| command.usage.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let mut lines = vec![
        "Vaylix command help".to_string(),
        "".to_string(),
        format!(
            "{:<name_width$} | {:<usage_width$}",
            "command",
            "usage",
            name_width = name_width,
            usage_width = usage_width
        ),
        format!("{}-+-{}", "-".repeat(name_width), "-".repeat(usage_width)),
    ];
    for command in COMMANDS {
        lines.push(format!(
            "{:<name_width$} | {:<usage_width$}",
            command.name,
            command.usage,
            name_width = name_width,
            usage_width = usage_width
        ));
    }

    lines.join("\n")
}

fn render_response(command: &Command, response: &Response, output: OutputMode) -> Result<String> {
    if let Ok(value) = response.decode_value()
        && value == "QUEUED"
    {
        return Ok(value);
    }

    match response.status {
        Status::NotFound => Ok("NOT_FOUND".to_string()),
        Status::Error => {
            let remote = response.decode_error()?;
            Ok(format!(
                "ERROR [{}] {}: {}",
                remote.code, remote.name, remote.message
            ))
        }
        Status::Ok => match command {
            Command::Auth { .. } => Ok("OK".to_string()),
            Command::Ping { .. }
            | Command::Get { .. }
            | Command::GetDel { .. }
            | Command::GetEx { .. }
            | Command::Backup
            | Command::MetricsProm => Ok(response.decode_value()?),
            Command::Set { options, .. } => {
                if options.return_previous {
                    Ok(response.decode_value()?)
                } else if options.condition.is_some() {
                    Ok(response.decode_bool()?.to_string())
                } else {
                    Ok("OK".to_string())
                }
            }
            Command::MSet { .. }
            | Command::BackupTo { .. }
            | Command::Clear
            | Command::Save
            | Command::Snapshot
            | Command::AlterUserPassword { .. }
            | Command::CreateUser { .. }
            | Command::DropUser { .. }
            | Command::CreateRole { .. }
            | Command::DropRole { .. }
            | Command::GrantRole { .. }
            | Command::RevokeRole { .. }
            | Command::GrantPermission { .. }
            | Command::RevokePermission { .. }
            | Command::Multi
            | Command::MaintenanceOn
            | Command::MaintenanceOff
            | Command::Discard => Ok("OK".to_string()),
            Command::Exec => Ok(response
                .decode_exec_results()?
                .into_iter()
                .map(render_exec_result)
                .collect::<Vec<_>>()
                .join("\n")),
            Command::SetNx { .. }
            | Command::Exists { .. }
            | Command::Expire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. } => Ok(response.decode_bool()?.to_string()),
            Command::MGet { .. } => Ok(response
                .decode_strings()?
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
                .collect::<Vec<_>>()
                .join(", ")),
            Command::Delete { .. }
            | Command::DbSize
            | Command::Count
            | Command::Restore { .. }
            | Command::RestoreFrom { .. }
            | Command::RestoreCheck { .. }
            | Command::RestoreCheckFrom { .. } => Ok(response.decode_count()?.to_string()),
            Command::Incr { .. } | Command::Decr { .. } | Command::Ttl { .. } => {
                Ok(response.decode_integer()?.to_string())
            }
            Command::Scan { .. } => {
                let payload = response.decode_scan()?;
                let keys = if payload.keys.is_empty() {
                    "(empty)".to_string()
                } else {
                    payload.keys.join(", ")
                };

                Ok(format!("cursor={}, keys=[{}]", payload.next_cursor, keys))
            }
            Command::Info
            | Command::Metrics
            | Command::List
            | Command::BackupVerify { .. }
            | Command::BackupVerifyFrom { .. }
            | Command::ShowUsers
            | Command::ShowRoles
            | Command::ShowGrants
            | Command::ShowGrantsForUser { .. }
            | Command::ShowGrantsForRole { .. }
            | Command::WhoAmI
            | Command::MaintenanceStatus
            | Command::Health
            | Command::ShowReplication => {
                let entries = response.decode_entries()?;
                render_entries(&entries, output)
            }
            Command::PromoteFollower | Command::PauseReplication | Command::ResumeReplication => {
                Ok("OK".to_string())
            }
            Command::Help | Command::Exit => Err(ClientError::LocalCommandResponse),
        },
    }
}

fn render_exec_result(result: transport::ExecResultPayload) -> String {
    match result {
        transport::ExecResultPayload::Ok => "OK".to_string(),
        transport::ExecResultPayload::NotFound => "NOT_FOUND".to_string(),
        transport::ExecResultPayload::Value(value) => value,
        transport::ExecResultPayload::Boolean(value) => value.to_string(),
        transport::ExecResultPayload::Count(value) => value.to_string(),
        transport::ExecResultPayload::Integer(value) => value.to_string(),
        transport::ExecResultPayload::Entries(entries) => entries
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", "),
        transport::ExecResultPayload::Strings(values) => values
            .into_iter()
            .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
            .collect::<Vec<_>>()
            .join(", "),
        transport::ExecResultPayload::Scan(scan) => {
            format!(
                "cursor={}, keys=[{}]",
                scan.next_cursor,
                scan.keys.join(", ")
            )
        }
    }
}

fn render_entries(entries: &[(String, String)], output: OutputMode) -> Result<String> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

    match output {
        OutputMode::Plain => Ok(sorted
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")),
        OutputMode::Table => Ok(render_table(&sorted)),
        OutputMode::Json => Ok(serde_json::to_string_pretty(
            &sorted
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
        )
        .map_err(std::io::Error::other)?),
    }
}

fn render_table(entries: &[(String, String)]) -> String {
    let key_width = entries
        .iter()
        .map(|(key, _)| key.len())
        .max()
        .unwrap_or(3)
        .max(3);
    let value_width = entries
        .iter()
        .map(|(_, value)| value.len())
        .max()
        .unwrap_or(5)
        .max(5);

    let mut lines = Vec::with_capacity(entries.len() + 2);
    lines.push(format!(
        "{:<key_width$} | {:<value_width$}",
        "key",
        "value",
        key_width = key_width,
        value_width = value_width
    ));
    lines.push(format!(
        "{}-+-{}",
        "-".repeat(key_width),
        "-".repeat(value_width)
    ));
    for (key, value) in entries {
        lines.push(format!(
            "{:<key_width$} | {:<value_width$}",
            key,
            value,
            key_width = key_width,
            value_width = value_width
        ));
    }
    lines.join("\n")
}

fn log_event(level: &str, component: &str, message: &str) {
    println!("[{level}] [{component}] {message}");
}

#[cfg(test)]
mod tests {
    use command::{Command, Expiration, SetCondition, SetOptions};
    use transport::{CodecOptions, ExecResultPayload, Response, Status};
    use uuid::Uuid;

    use super::{ClientConfig, OutputMode, help_text, render_response, render_table};

    fn id(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    #[test]
    fn renders_serious_v1_response_types() {
        assert_eq!(
            render_response(
                &Command::Ping { message: None },
                &Response::value(id(1), "PONG").unwrap(),
                OutputMode::Plain,
            )
            .unwrap(),
            "PONG"
        );

        assert_eq!(
            render_response(
                &Command::Exec,
                &Response::exec_results(
                    id(2),
                    &[ExecResultPayload::Ok, ExecResultPayload::Count(1)],
                )
                .unwrap(),
                OutputMode::Plain,
            )
            .unwrap(),
            "OK\n1"
        );

        assert_eq!(
            render_response(
                &Command::Set {
                    key: "cache".to_string(),
                    value: "item".to_string(),
                    options: SetOptions {
                        condition: Some(SetCondition::Nx),
                        expiration: Some(Expiration::Px(100)),
                        keep_ttl: false,
                        return_previous: false,
                    },
                },
                &Response::boolean(id(7), true),
                OutputMode::Plain,
            )
            .unwrap(),
            "true"
        );
    }

    #[test]
    fn renders_table_and_json_for_entries() {
        let response = Response::entries(
            id(1),
            &[
                ("name".to_string(), "alice".to_string()),
                ("city".to_string(), "paris".to_string()),
            ],
        )
        .unwrap();

        let table = render_response(&Command::Info, &response, OutputMode::Table).unwrap();
        assert!(table.contains("name"));
        assert!(table.contains("alice"));

        let json = render_response(&Command::List, &response, OutputMode::Json).unwrap();
        assert!(json.contains("\"key\": \"name\""));
    }

    #[test]
    fn renders_ok_not_found_and_error_responses() {
        assert_eq!(
            render_response(
                &Command::Set {
                    key: "name".to_string(),
                    value: "alice".to_string(),
                    options: SetOptions::default(),
                },
                &Response::ok(id(1)),
                OutputMode::Plain,
            )
            .unwrap(),
            "OK"
        );

        assert_eq!(
            render_response(
                &Command::Get {
                    key: "missing".to_string()
                },
                &Response::not_found(id(2)),
                OutputMode::Plain,
            )
            .unwrap(),
            "NOT_FOUND"
        );

        assert_eq!(
            render_response(
                &Command::Count,
                &Response::new(
                    id(3),
                    Status::Error,
                    Response::error(id(3), "SRV-400", "Bad Request", "bad request")
                        .unwrap()
                        .payload,
                ),
                OutputMode::Plain,
            )
            .unwrap(),
            "ERROR [SRV-400] Bad Request: bad request"
        );
    }

    #[test]
    fn rejects_rendering_for_local_commands() {
        assert!(render_response(&Command::Help, &Response::ok(id(1)), OutputMode::Plain).is_err());
        assert!(render_response(&Command::Exit, &Response::ok(id(1)), OutputMode::Plain).is_err());
    }

    #[test]
    fn help_text_lists_supported_commands() {
        let help = help_text();
        assert!(help.contains("command"));
        assert!(help.contains("auth"));
        assert!(help.contains("auth <username> <password>"));
        assert!(help.contains("backup verify from <path>"));
        assert!(help.contains("create user <username> password <password>"));
        assert!(help.contains("show grants for user <username>"));
        assert!(help.contains("maintenance status"));
        assert!(!help.contains("Examples:"));
    }

    #[test]
    fn renders_basic_table() {
        let table = render_table(&[("a".to_string(), "1".to_string())]);
        assert!(table.contains("key"));
        assert!(table.contains("a"));
    }

    #[test]
    fn client_config_keeps_output_mode() {
        let config = ClientConfig {
            host: "127.0.0.1".to_string(),
            port: 9173,
            ssl: false,
            tls_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            output: OutputMode::Json,
            transport: CodecOptions::default(),
        };
        assert_eq!(config.output, OutputMode::Json);
    }
}
