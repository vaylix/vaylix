use command::Command;
use engine::{Engine, StorageEngine};
use std::net::{TcpListener, TcpStream};
use transport::{Response, TransportError, read_request_from, write_response_to};

use crate::error::{Result, ServerError};

pub struct Server {
    listener: TcpListener,
    engine: Engine,
}

impl Server {
    pub fn new(bind: String, port: u16) -> Result<Self> {
        let addr = format!("{}:{}", bind, port);
        let listener = TcpListener::bind(addr).map_err(ServerError::Bind)?;
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
                        eprintln!("Client error [{}] {}: {err}", err.code(), err.name());
                    }
                }
                Err(err) => {
                    let err = ServerError::Accept(err);
                    eprintln!("Connection failed [{}] {}: {err}", err.code(), err.name());
                }
            }
        }
    }

    fn handle_client(&mut self, mut stream: TcpStream) -> Result<()> {
        loop {
            let request = match read_request_from(&mut stream) {
                Ok(request) => request,
                Err(TransportError::UnexpectedEof) => break,
                Err(err) => return Err(err.into()),
            };

            let request_id = request.request_id;

            let response = match request.into_command() {
                Ok(command) => self
                    .execute_command(request_id, command)
                    .unwrap_or_else(|err| {
                        error_response(request_id, err.code(), err.name(), &err.to_string())
                    }),
                Err(err) => error_response(request_id, err.code(), err.name(), &err.to_string()),
            };

            write_response_to(&mut stream, &response)?;
        }

        Ok(())
    }

    fn execute_command(&mut self, request_id: u32, command: Command) -> Result<Response> {
        execute_command(&mut self.engine, request_id, command)
    }
}

fn error_response(request_id: u32, code: &str, name: &str, message: &str) -> Response {
    Response::error(request_id, code, name, message).unwrap_or_else(|_| {
        Response::error(
            request_id,
            "TRN-011",
            "Remote Error Encoding Failure",
            "failed to encode structured error payload",
        )
        .expect("static remote error encoding should never fail")
    })
}

fn execute_command<E>(engine: &mut E, request_id: u32, command: Command) -> Result<Response>
where
    E: StorageEngine,
{
    match command {
        Command::Get { key } => match engine.get(&key)? {
            Some(value) => Ok(Response::value(request_id, &value)?),
            None => Ok(Response::not_found(request_id)),
        },
        Command::Set { key, value } => {
            engine.set(key, value)?;
            Ok(Response::ok(request_id))
        }
        Command::Delete { keys } => {
            engine.delete_many(&keys)?;
            Ok(Response::ok(request_id))
        }
        Command::Exists { key } => {
            let exists = engine.exists(&key)?;
            Ok(Response::boolean(request_id, exists))
        }
        Command::List => {
            let entries = engine.list()?;
            Ok(Response::entries(request_id, &entries)?)
        }
        Command::Clear => {
            engine.clear()?;
            Ok(Response::ok(request_id))
        }
        Command::Count => {
            let count = engine.count()?;
            Ok(Response::count(request_id, count as u64))
        }
        Command::Snapshot => {
            engine.snapshot()?;
            Ok(Response::ok(request_id))
        }
        Command::Help | Command::Exit => Err(ServerError::UnsupportedRemoteCommand),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{error_response, execute_command};
    use command::Command;
    use engine::{Result, StorageEngine};
    use transport::{Response, Status};

    #[derive(Default)]
    struct FakeEngine {
        data: BTreeMap<String, String>,
    }

    impl StorageEngine for FakeEngine {
        fn get(&self, key: &str) -> Result<Option<String>> {
            Ok(self.data.get(key).cloned())
        }

        fn set(&mut self, key: String, value: String) -> Result<()> {
            self.data.insert(key, value);
            Ok(())
        }

        fn delete(&mut self, key: &str) -> Result<()> {
            self.data.remove(key);
            Ok(())
        }

        fn delete_many(&mut self, keys: &[String]) -> Result<()> {
            for key in keys {
                self.data.remove(key);
            }

            Ok(())
        }

        fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.data.contains_key(key))
        }

        fn count(&self) -> Result<usize> {
            Ok(self.data.len())
        }

        fn clear(&mut self) -> Result<()> {
            self.data.clear();
            Ok(())
        }

        fn list(&self) -> Result<Vec<(String, String)>> {
            Ok(self
                .data
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        fn snapshot(&self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn routes_get_request_to_value_response() {
        let mut engine = FakeEngine::default();
        engine.set("name".to_string(), "alice".to_string()).unwrap();

        let response = execute_command(
            &mut engine,
            41,
            Command::Get {
                key: "name".to_string(),
            },
        )
        .unwrap();

        assert_eq!(response.request_id, 41);
        assert_eq!(response.status, Status::Ok);
        assert_eq!(response.decode_value().unwrap(), "alice");
    }

    #[test]
    fn routes_exists_count_and_list_responses() {
        let mut engine = FakeEngine::default();
        engine.set("name".to_string(), "alice".to_string()).unwrap();

        let exists = execute_command(
            &mut engine,
            1,
            Command::Exists {
                key: "name".to_string(),
            },
        )
        .unwrap();
        let count = execute_command(&mut engine, 2, Command::Count).unwrap();
        let list = execute_command(&mut engine, 3, Command::List).unwrap();

        assert!(exists.decode_bool().unwrap());
        assert_eq!(count.decode_count().unwrap(), 1);
        assert_eq!(
            list.decode_entries().unwrap(),
            vec![("name".to_string(), "alice".to_string())]
        );
    }

    #[test]
    fn routes_missing_key_to_not_found() {
        let mut engine = FakeEngine::default();

        let response = execute_command(
            &mut engine,
            9,
            Command::Get {
                key: "missing".to_string(),
            },
        )
        .unwrap();

        assert_eq!(response, Response::not_found(9));
    }

    #[test]
    fn routes_mutating_commands_and_remote_local_command_errors() {
        let mut engine = FakeEngine::default();

        assert_eq!(
            execute_command(
                &mut engine,
                1,
                Command::Set {
                    key: "name".to_string(),
                    value: "alice".to_string()
                }
            )
            .unwrap(),
            Response::ok(1)
        );

        assert_eq!(engine.get("name").unwrap().as_deref(), Some("alice"));

        assert_eq!(
            execute_command(
                &mut engine,
                2,
                Command::Delete {
                    keys: vec!["name".to_string()]
                }
            )
            .unwrap(),
            Response::ok(2)
        );

        assert_eq!(engine.get("name").unwrap(), None);
        assert_eq!(
            execute_command(&mut engine, 3, Command::Clear).unwrap(),
            Response::ok(3)
        );
        assert_eq!(
            execute_command(&mut engine, 4, Command::Snapshot).unwrap(),
            Response::ok(4)
        );
        assert!(execute_command(&mut engine, 5, Command::Help).is_err());
        assert!(execute_command(&mut engine, 6, Command::Exit).is_err());
    }

    #[test]
    fn builds_error_responses() {
        let response = error_response(99, "SRV-999", "Boom", "boom");
        assert_eq!(response.request_id, 99);
        assert_eq!(response.status, Status::Error);
        let remote = response.decode_error().unwrap();
        assert_eq!(remote.code, "SRV-999");
        assert_eq!(remote.name, "Boom");
        assert_eq!(remote.message, "boom");
    }
}
