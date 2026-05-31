use crate::helper::ClientHelper;
use crate::paths::Paths;
use command::{Command, Parser};
use rustls::{ClientConnection, StreamOwned};
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use std::io::Write;
use std::net::TcpStream;
use std::path::PathBuf;
use transport::{
    ClientHello, CodecOptions, Request, client_options_from_server_hello,
    read_response_from_with_options, read_server_hello_from, write_client_hello_to,
    write_request_to_with_options,
};
use uuid::Uuid;

use crate::error::{ClientError, Result};

mod help;
mod render;
mod tls;

use help::help_text;
use render::render_response;
#[cfg(test)]
use render::render_table;
use tls::connect_tls;

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
