use crate::Response;
use veyra_core::{Command, Engine, StorageEngine};

use anyhow::{Result, bail};

use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
};

pub struct Server {
    listener: TcpListener,
    engine: Engine,
}

impl Server {
    pub fn new() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:6379")?;
        let engine = Engine::new()?;
        println!("Veyra ready for connections on 6379");

        Ok(Self { listener, engine })
    }

    pub fn start(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    println!("Client connected");

                    if let Err(err) = self.handle_client(stream) {
                        eprintln!("Client error: {err}");
                    }
                }

                Err(err) => {
                    eprintln!("Connection failed: {err}");
                }
            }
        }
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        let mut reader = BufReader::new(stream.try_clone()?);

        loop {
            let mut line = String::new();

            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                break;
            }

            let command = self.parse_command(&line)?;

            let should_exit = matches!(command, Command::Exit);

            let response = self.execute_command(command)?;

            self.write_response(&mut stream, response)?;

            if should_exit {
                break;
            }
        }

        Ok(())
    }

    fn parse_command(&self, line: &str) -> Result<Command> {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();

        if parts.is_empty() {
            bail!("Empty command")
        }

        match parts[0].to_lowercase().as_str() {
            "get" => {
                if parts.len() != 2 {
                    bail!("Usage: get <key>")
                }

                Ok(Command::Get {
                    key: parts[1].to_string(),
                })
            }

            "set" => {
                if parts.len() < 3 {
                    bail!("Usage: set <key> <value>")
                }

                Ok(Command::Set {
                    key: parts[1].to_string(),
                    value: parts[2..].join(" "),
                })
            }

            "delete" => {
                if parts.len() < 2 {
                    bail!("Usage: delete <key> [key...]")
                }

                Ok(Command::Delete {
                    keys: parts[1..].iter().map(|key| key.to_string()).collect(),
                })
            }

            "exists" => {
                if parts.len() != 2 {
                    bail!("Usage: exists <key>")
                }

                Ok(Command::Exists {
                    key: parts[1].to_string(),
                })
            }

            "list" => Ok(Command::List),

            "clear" => Ok(Command::Clear),

            "count" => Ok(Command::Count),

            "help" => Ok(Command::Help),

            "exit" => Ok(Command::Exit),

            "snapshot" => Ok(Command::Snapshot),

            _ => bail!("Unknown command"),
        }
    }

    fn execute_command(&mut self, command: Command) -> Result<Response> {
        match command {
        Command::Get { key } => match self.engine.get(&key)? {
            Some(value) => Ok(Response::Value(value)),

            None => Ok(Response::NotFound),
        },

        Command::Set { key, value } => {
            self.engine.set(key, value)?;

            Ok(Response::Ok)
        }

        Command::Delete { keys } => {
            self.engine.delete_many(&keys)?;

            Ok(Response::Ok)
        }

        Command::Exists { key } => {
            let exists = self.engine.exists(&key)?;

            Ok(Response::Value(exists.to_string()))
        }

        Command::List => {
            let entries = self.engine.list()?;

            let formatted = entries
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<String>>()
                .join(", ");

            Ok(Response::Value(formatted))
        }

        Command::Clear => {
            self.engine.clear()?;

            Ok(Response::Ok)
        }

        Command::Count => {
            let count = self.engine.count()?;

            Ok(Response::Count(count))
        }

        Command::Help => Ok(Response::Value(
            "Available commands: get, set, delete, exists, list, clear, count, snapshot, help, exit"
                .to_string(),
        )),

        Command::Exit => Ok(Response::Ok),

        Command::Snapshot => {
            self.engine.snapshot()?;

            Ok(Response::Ok)
        }
    }
    }

    fn write_response(&self, stream: &mut TcpStream, response: Response) -> Result<()> {
        let output = match response {
            Response::Ok => "OK\n".to_string(),

            Response::Value(value) => {
                format!("{value}\n")
            }

            Response::Count(count) => {
                format!("{count}\n")
            }

            Response::NotFound => "NOT_FOUND\n".to_string(),

            Response::Error { code, message } => {
                format!("ERROR [{code}]: {message}\n")
            }
        };

        stream.write_all(output.as_bytes())?;

        Ok(())
    }
}
