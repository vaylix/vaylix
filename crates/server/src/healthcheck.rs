use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use command::Command;
use transport::{
    ClientHello, CodecOptions, Request, Status, client_options_from_server_hello,
    read_response_from_with_options, read_server_hello_from, write_client_hello_to,
    write_request_to_with_options,
};
use uuid::Uuid;

use crate::args::{HealthcheckCommand, HealthcheckKind};
use crate::error::{Result, ServerError};

/// Runs the in-binary container/operator healthcheck without opening storage directly.
///
/// The probe talks to the running server through the normal framed protocol. This keeps runtime
/// health semantics owned by the server process instead of racing the engine from a second process.
pub fn run_healthcheck(command: HealthcheckCommand) -> Result<()> {
    let timeout = Duration::from_millis(command.timeout_ms);
    let port = command.port.unwrap_or(resolve_server_port()?);
    let credentials = match command.kind {
        HealthcheckKind::Liveness => None,
        HealthcheckKind::Readiness => resolve_credentials(&command)?,
    };
    let addr = format!("{}:{port}", command.host);
    let socket_addr = addr
        .to_socket_addrs()
        .map_err(|err| healthcheck_error(format!("failed to resolve {addr}: {err}")))?
        .next()
        .ok_or_else(|| {
            ServerError::InvalidArguments(format!("invalid healthcheck address: {addr}"))
        })?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, timeout)
        .map_err(|err| healthcheck_error(format!("failed to connect to {socket_addr}: {err}")))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|err| healthcheck_error(format!("failed to set read timeout: {err}")))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|err| healthcheck_error(format!("failed to set write timeout: {err}")))?;

    let client_hello = ClientHello {
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        desired_compression: CodecOptions::default().compression,
        max_frame_len: CodecOptions::default().max_frame_len as u32,
        auth_intent: credentials.is_some(),
        ..ClientHello::new("vaylix-healthcheck", env!("CARGO_PKG_VERSION"))
    };
    write_client_hello_to(&mut stream, &client_hello)?;
    let server_hello = read_server_hello_from(&mut stream)?;
    let transport = client_options_from_server_hello(&server_hello)?;

    if let Some((username, password)) = credentials {
        authenticate(&mut stream, transport, username, password)?;
    }

    match command.kind {
        HealthcheckKind::Liveness => check_liveness(&mut stream, transport),
        HealthcheckKind::Readiness => check_readiness(&mut stream, transport),
    }
}

fn authenticate(
    stream: &mut TcpStream,
    transport: CodecOptions,
    username: String,
    password: String,
) -> Result<()> {
    let request_id = Uuid::now_v7();
    let request = Request::from_command(request_id, Command::Auth { username, password })?;
    write_request_to_with_options(stream, &request, transport)?;
    let response = read_response_from_with_options(stream, transport)?;
    ensure_response_id(request_id, response.request_id)?;
    ensure_ok(response.status, "authentication")?;
    Ok(())
}

fn check_liveness(stream: &mut TcpStream, transport: CodecOptions) -> Result<()> {
    let request_id = Uuid::now_v7();
    let request = Request::from_command(request_id, Command::Ping { message: None })?;
    write_request_to_with_options(stream, &request, transport)?;
    let response = read_response_from_with_options(stream, transport)?;
    ensure_response_id(request_id, response.request_id)?;
    ensure_ok(response.status, "liveness")?;
    Ok(())
}

fn check_readiness(stream: &mut TcpStream, transport: CodecOptions) -> Result<()> {
    let request_id = Uuid::now_v7();
    let request = Request::from_command(request_id, Command::Health)?;
    write_request_to_with_options(stream, &request, transport)?;
    let response = read_response_from_with_options(stream, transport)?;
    ensure_response_id(request_id, response.request_id)?;
    ensure_ok(response.status, "readiness")?;

    let ready = response
        .decode_entries()?
        .into_iter()
        .any(|(key, value)| key == "ready" && value == "true");

    if ready {
        Ok(())
    } else {
        Err(ServerError::HealthcheckFailed(
            "readiness probe returned ready=false".to_string(),
        ))
    }
}

fn ensure_response_id(expected: Uuid, actual: Uuid) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(ServerError::HealthcheckFailed(format!(
            "mismatched healthcheck response id: expected {expected}, got {actual}"
        )))
    }
}

fn ensure_ok(status: Status, kind: &str) -> Result<()> {
    if status == Status::Ok {
        Ok(())
    } else {
        Err(ServerError::HealthcheckFailed(format!(
            "{kind} probe returned status {status:?}"
        )))
    }
}

fn healthcheck_error(message: String) -> ServerError {
    ServerError::HealthcheckFailed(message)
}

fn resolve_server_port() -> Result<u16> {
    match std::env::var("VAYLIX_PORT") {
        Ok(value) => value.parse().map_err(|_| {
            ServerError::InvalidArguments(format!("invalid VAYLIX_PORT for healthcheck: {value}"))
        }),
        Err(std::env::VarError::NotPresent) => Ok(9173),
        Err(err) => Err(ServerError::InvalidArguments(format!(
            "invalid VAYLIX_PORT for healthcheck: {err}"
        ))),
    }
}

fn resolve_credentials(command: &HealthcheckCommand) -> Result<Option<(String, String)>> {
    let username = command
        .user
        .clone()
        .or_else(|| std::env::var("VAYLIX_USER").ok());
    let password = command
        .password
        .clone()
        .or_else(|| std::env::var("VAYLIX_PASSWORD").ok());

    match (username, password) {
        (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => {
            Ok(Some((username, password)))
        }
        (None, None) => Ok(None),
        _ => Err(ServerError::InvalidArguments(
            "healthcheck authentication requires both username and password".to_string(),
        )),
    }
}
