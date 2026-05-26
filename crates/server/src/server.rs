use crate::Protocol;
use crate::Response;
use anyhow::{Result, bail};
use engine::{Engine, StorageEngine};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
};

pub struct Server {
    listener: TcpListener,
    engine: Engine,
}

impl Server {
    pub fn new(bind: String, port: u16) -> Result<Self> {
        let addr = format!("{}:{}", bind, port);
        let listener = TcpListener::bind(addr)?;
        let engine = Engine::new()?;
        println!("Ready for connections on {}", port);

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

            let should_exit = matches!(command, Protocol::Exit);

            let response = self.execute_command(command)?;

            self.write_response(&mut stream, response)?;

            if should_exit {
                break;
            }
        }

        Ok(())
    }

    fn parse_command(&self, line: &str) -> Result<Protocol> {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();

        if parts.is_empty() {
            bail!("Empty command")
        }

        match parts[0].to_lowercase().as_str() {
            "get" => {
                if parts.len() != 2 {
                    bail!("Usage: get <key>")
                }

                Ok(Protocol::Get {
                    key: parts[1].to_string(),
                })
            }

            "set" => {
                if parts.len() < 3 {
                    bail!("Usage: set <key> <value>")
                }

                Ok(Protocol::Set {
                    key: parts[1].to_string(),
                    value: parts[2..].join(" "),
                })
            }

            "delete" => {
                if parts.len() < 2 {
                    bail!("Usage: delete <key> [key...]")
                }

                Ok(Protocol::Delete {
                    keys: parts[1..].iter().map(|key| key.to_string()).collect(),
                })
            }

            "exists" => {
                if parts.len() != 2 {
                    bail!("Usage: exists <key>")
                }

                Ok(Protocol::Exists {
                    key: parts[1].to_string(),
                })
            }

            "list" => Ok(Protocol::List),

            "clear" => Ok(Protocol::Clear),

            "count" => Ok(Protocol::Count),

            "help" => Ok(Protocol::Help),

            "exit" => Ok(Protocol::Exit),

            "snapshot" => Ok(Protocol::Snapshot),

            _ => bail!("Unknown command"),
        }
    }

    fn execute_command(&mut self, command: Protocol) -> Result<Response> {
        match command {
        Protocol::Get { key } => match self.engine.get(&key)? {
            Some(value) => Ok(Response::Value(value)),

            None => Ok(Response::NotFound),
        },

        Protocol::Set { key, value } => {
            self.engine.set(key, value)?;

            Ok(Response::Ok)
        }

        Protocol::Delete { keys } => {
            self.engine.delete_many(&keys)?;

            Ok(Response::Ok)
        }

        Protocol::Exists { key } => {
            let exists = self.engine.exists(&key)?;

            Ok(Response::Value(exists.to_string()))
        }

        Protocol::List => {
            let entries = self.engine.list()?;

            let formatted = entries
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<String>>()
                .join(", ");

            Ok(Response::Value(formatted))
        }

        Protocol::Clear => {
            self.engine.clear()?;

            Ok(Response::Ok)
        }

        Protocol::Count => {
            let count = self.engine.count()?;

            Ok(Response::Count(count))
        }

        Protocol::Help => Ok(Response::Value(
            "Available commands: get, set, delete, exists, list, clear, count, snapshot, help, exit"
                .to_string(),
        )),

        Protocol::Exit => Ok(Response::Ok),

        Protocol::Snapshot => {
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
