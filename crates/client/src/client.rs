use crate::helper::ClientHelper;
use crate::paths::Paths;
use command::{COMMANDS, Command, Parser};
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use std::io::Write;
use std::net::TcpStream;
use transport::{Request, Response, Status, read_response_from, write_request_to};

use crate::error::{ClientError, Result};

pub struct Client {
    editor: Editor<ClientHelper, rustyline::history::DefaultHistory>,
    stream: TcpStream,
    paths: Paths,
    next_request_id: u32,
}

const PROMPT: &str = "vaylix> ";

impl Client {
    pub fn new(host: String, port: u16) -> Result<Self> {
        let config = Builder::new()
            .completion_type(CompletionType::List)
            .edit_mode(EditMode::Emacs)
            .auto_add_history(true)
            .build();

        let addr = format!("{}:{}", host, port);

        let helper = ClientHelper::new();
        let paths = Paths::new()?;
        let stream = TcpStream::connect(addr)?;

        let mut editor = Editor::<ClientHelper, DefaultHistory>::with_config(config)?;

        editor.set_helper(Some(helper));
        editor.load_history(&paths.history_path).ok();

        Ok(Self {
            editor,
            stream,
            paths,
            next_request_id: 1,
        })
    }

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
                            self.editor.save_history(&self.paths.history_path)?;
                            break;
                        }
                        command => self.execute(command)?,
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("Exiting...");
                    self.editor.save_history(&self.paths.history_path)?;
                    break;
                }
                Err(ReadlineError::Eof) => {
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
        write_request_to(&mut self.stream, &request)?;
        self.stream.flush()?;

        let response = read_response_from(&mut self.stream)?;

        if response.request_id != request_id {
            return Err(ClientError::ResponseIdMismatch {
                expected: request_id,
                actual: response.request_id,
            });
        }

        println!("{}", render_response(&command, &response)?);

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
        .map(|command| command.name)
        .collect::<Vec<_>>()
        .join(", ");

    format!("Available commands: {commands}")
}

fn render_response(command: &Command, response: &Response) -> Result<String> {
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
            Command::Get { .. } => Ok(response.decode_value()?),
            Command::Set { .. } | Command::Delete { .. } | Command::Clear | Command::Snapshot => {
                Ok("OK".to_string())
            }
            Command::Exists { .. } => Ok(response.decode_bool()?.to_string()),
            Command::List => Ok(response
                .decode_entries()?
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(", ")),
            Command::Count => Ok(response.decode_count()?.to_string()),
            Command::Help | Command::Exit => Err(ClientError::LocalCommandResponse),
        },
    }
}

#[cfg(test)]
mod tests {
    use command::Command;
    use transport::{Response, Status};

    use super::{help_text, render_response};

    #[test]
    fn renders_value_not_found_bool_count_and_list() {
        assert_eq!(
            render_response(
                &Command::Get {
                    key: "name".to_string()
                },
                &Response::value(1, "alice").unwrap(),
            )
            .unwrap(),
            "alice"
        );

        assert_eq!(
            render_response(
                &Command::Get {
                    key: "missing".to_string()
                },
                &Response::not_found(2),
            )
            .unwrap(),
            "NOT_FOUND"
        );

        assert_eq!(
            render_response(
                &Command::Exists {
                    key: "name".to_string()
                },
                &Response::boolean(3, true),
            )
            .unwrap(),
            "true"
        );

        assert_eq!(
            render_response(&Command::Count, &Response::count(4, 42)).unwrap(),
            "42"
        );

        assert_eq!(
            render_response(
                &Command::List,
                &Response::entries(
                    5,
                    &[
                        ("name".to_string(), "alice".to_string()),
                        ("city".to_string(), "paris".to_string())
                    ],
                )
                .unwrap(),
            )
            .unwrap(),
            "name=alice, city=paris"
        );
    }

    #[test]
    fn renders_ok_and_error_responses() {
        assert_eq!(
            render_response(
                &Command::Set {
                    key: "name".to_string(),
                    value: "alice".to_string()
                },
                &Response::ok(1),
            )
            .unwrap(),
            "OK"
        );

        assert_eq!(
            render_response(
                &Command::Count,
                &Response::new(
                    2,
                    Status::Error,
                    Response::error(2, "SRV-400", "Bad Request", "bad request")
                        .unwrap()
                        .payload
                ),
            )
            .unwrap(),
            "ERROR [SRV-400] Bad Request: bad request"
        );
    }

    #[test]
    fn rejects_rendering_for_local_commands() {
        assert!(render_response(&Command::Help, &Response::ok(1)).is_err());
        assert!(render_response(&Command::Exit, &Response::ok(1)).is_err());
    }

    #[test]
    fn help_text_lists_supported_commands() {
        let help = help_text();
        assert!(help.contains("get"));
        assert!(help.contains("set"));
        assert!(help.contains("snapshot"));
    }
}
