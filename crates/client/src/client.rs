use crate::helper::ClientHelper;
use crate::paths::Paths;
use command::{COMMANDS, Command, Parser};
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use serde_json::json;
use std::io::Write;
use std::net::TcpStream;
use transport::{Request, Response, Status, read_response_from, write_request_to};

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
    pub username: Option<String>,
    pub password: Option<String>,
    pub output: OutputMode,
}

/// Interactive CLI client for talking to a Vaylix server.
pub struct Client {
    editor: Editor<ClientHelper, rustyline::history::DefaultHistory>,
    stream: TcpStream,
    paths: Paths,
    next_request_id: u32,
    output: OutputMode,
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
        log_event("INFO", "client.startup", &format!("connecting to {addr}"));
        let stream = TcpStream::connect(addr)?;
        log_event("INFO", "client.startup", "connection established");

        let mut editor = Editor::<ClientHelper, DefaultHistory>::with_config(rustyline_config)?;
        editor.set_helper(Some(helper));
        editor.load_history(&paths.history_path).ok();

        let mut client = Self {
            editor,
            stream,
            paths,
            next_request_id: 1,
            output: config.output,
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
        let request_id = self.next_request_id();
        let request = Request::from_command(request_id, command.clone())?;
        log_event(
            "INFO",
            "client.request",
            &format!(
                "sending request_id={request_id} opcode={:?}",
                request.opcode
            ),
        );
        write_request_to(&mut self.stream, &request)?;
        self.stream.flush()?;

        let response = read_response_from(&mut self.stream)?;

        if response.request_id != request_id {
            return Err(ClientError::ResponseIdMismatch {
                expected: request_id,
                actual: response.request_id,
            });
        }

        log_event(
            "INFO",
            "client.response",
            &format!(
                "received response request_id={} status={:?}",
                response.request_id, response.status
            ),
        );
        println!("{}", render_response(&command, &response, self.output)?);

        Ok(())
    }

    fn next_request_id(&mut self) -> u32 {
        let current = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);

        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }

        current
    }
}

fn help_text() -> String {
    let commands = COMMANDS
        .iter()
        .map(|command| format!("{} ({})", command.name, command.usage))
        .collect::<Vec<_>>()
        .join(", ");

    format!("Available commands: {commands}")
}

fn render_response(command: &Command, response: &Response, output: OutputMode) -> Result<String> {
    if let Ok(value) = response.decode_value() {
        if value == "QUEUED" {
            return Ok(value);
        }
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
            | Command::GetEx { .. } => Ok(response.decode_value()?),
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
            | Command::Clear
            | Command::Save
            | Command::Snapshot
            | Command::Multi
            | Command::Discard => Ok("OK".to_string()),
            Command::Exec => Ok(response
                .decode_strings()?
                .into_iter()
                .map(|value| value.unwrap_or_else(|| "(nil)".to_string()))
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
            Command::Delete { .. } | Command::DbSize | Command::Count => {
                Ok(response.decode_count()?.to_string())
            }
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
            Command::Info | Command::Metrics | Command::List => {
                let entries = response.decode_entries()?;
                render_entries(&entries, output)
            }
            Command::Help | Command::Exit => Err(ClientError::LocalCommandResponse),
        },
    }
}

fn render_entries(entries: &[(String, String)], output: OutputMode) -> Result<String> {
    match output {
        OutputMode::Plain => Ok(entries
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")),
        OutputMode::Table => Ok(render_table(entries)),
        OutputMode::Json => Ok(serde_json::to_string_pretty(
            &entries
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
    use transport::{Response, Status};

    use super::{ClientConfig, OutputMode, help_text, render_response, render_table};

    #[test]
    fn renders_serious_v1_response_types() {
        assert_eq!(
            render_response(
                &Command::Ping { message: None },
                &Response::value(1, "PONG").unwrap(),
                OutputMode::Plain,
            )
            .unwrap(),
            "PONG"
        );

        assert_eq!(
            render_response(
                &Command::Exec,
                &Response::strings(2, &[Some("OK".to_string()), Some("1".to_string())]).unwrap(),
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
                &Response::boolean(7, true),
                OutputMode::Plain,
            )
            .unwrap(),
            "true"
        );
    }

    #[test]
    fn renders_table_and_json_for_entries() {
        let response = Response::entries(
            1,
            &[("name".to_string(), "alice".to_string()), ("city".to_string(), "paris".to_string())],
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
                &Response::ok(1),
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
                &Response::not_found(2),
                OutputMode::Plain,
            )
            .unwrap(),
            "NOT_FOUND"
        );

        assert_eq!(
            render_response(
                &Command::Count,
                &Response::new(
                    3,
                    Status::Error,
                    Response::error(3, "SRV-400", "Bad Request", "bad request")
                        .unwrap()
                        .payload
                ),
                OutputMode::Plain,
            )
            .unwrap(),
            "ERROR [SRV-400] Bad Request: bad request"
        );
    }

    #[test]
    fn rejects_rendering_for_local_commands() {
        assert!(render_response(&Command::Help, &Response::ok(1), OutputMode::Plain).is_err());
        assert!(render_response(&Command::Exit, &Response::ok(1), OutputMode::Plain).is_err());
    }

    #[test]
    fn help_text_lists_supported_commands() {
        let help = help_text();
        assert!(help.contains("auth (auth <username> <password>)"));
        assert!(help.contains("multi (multi)"));
        assert!(help.contains("exec (exec)"));
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
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            output: OutputMode::Json,
        };
        assert_eq!(config.output, OutputMode::Json);
    }
}
