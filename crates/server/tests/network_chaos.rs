#![cfg(feature = "chaos-tests")]

use command::Command;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use transport::{
    ClientHello, Request, Status, read_response_from_async, read_server_hello_from_async,
    write_client_hello_to_async, write_request_to_async,
};
use uuid::Uuid;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_process_proxy_latency_and_disconnect_preserve_committed_state() {
    let seed = test_seed();
    eprintln!("VAYLIX_TEST_SEED={seed}");

    let server_port = free_tcp_port();
    let data_dir = temp_dir(seed);
    let mut server = ServerProcess::start(server_port, data_dir.clone());
    let server_addr = SocketAddr::from(([127, 0, 0, 1], server_port));
    wait_for_tcp_accept(server_addr).await;

    let latency_proxy = Proxy::start(server_addr, Duration::from_millis(15), None).await;
    let mut stream = connect_via_proxy(latency_proxy.addr()).await;
    round_trip(
        &mut stream,
        1,
        Command::Set {
            key: "chaos:key".to_string(),
            value: b"committed".to_vec(),
            options: Default::default(),
        },
    )
    .await;
    let value = round_trip(
        &mut stream,
        2,
        Command::Get {
            key: "chaos:key".to_string(),
        },
    )
    .await
    .decode_value()
    .expect("GET should return committed value");
    assert_eq!(value, "committed");

    let disconnect_proxy = Proxy::start(server_addr, Duration::from_millis(0), Some(1)).await;
    let disrupted = timeout(
        Duration::from_secs(2),
        try_connect_via_proxy(disconnect_proxy.addr()),
    )
    .await;
    assert!(
        disrupted.is_err()
            || disrupted
                .expect("disconnect attempt should complete")
                .is_err(),
        "forced proxy disconnect should surface as a bounded connection failure"
    );

    let mut stream = connect_via_proxy(latency_proxy.addr()).await;
    let value = round_trip(
        &mut stream,
        3,
        Command::Get {
            key: "chaos:key".to_string(),
        },
    )
    .await
    .decode_value()
    .expect("committed value should survive disrupted connection");
    assert_eq!(value, "committed");

    drop(latency_proxy);
    drop(disconnect_proxy);
    server.stop();
    std::fs::remove_dir_all(data_dir).ok();
}

async fn connect_via_proxy(addr: SocketAddr) -> TcpStream {
    try_connect_via_proxy(addr)
        .await
        .expect("proxy handshake should succeed")
}

async fn try_connect_via_proxy(addr: SocketAddr) -> std::result::Result<TcpStream, String> {
    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .map_err(|_| "proxy connect timed out".to_string())?
        .map_err(|err| format!("proxy connect failed: {err}"))?;
    let hello = ClientHello::new("network-chaos-test", "0.10.0");
    write_client_hello_to_async(&mut stream, &hello)
        .await
        .map_err(|err| format!("client hello failed: {err}"))?;
    let response = read_server_hello_from_async(&mut stream)
        .await
        .map_err(|err| format!("server hello failed: {err}"))?;
    assert_eq!(response.status, Status::Ok);
    Ok(stream)
}

async fn round_trip(stream: &mut TcpStream, id: u128, command: Command) -> transport::Response {
    let request = Request::from_command(Uuid::from_u128(id), command)
        .expect("command should encode as request");
    write_request_to_async(stream, &request)
        .await
        .expect("request should write");
    let response = timeout(Duration::from_secs(2), read_response_from_async(stream))
        .await
        .expect("response should not hang")
        .expect("response should decode");
    assert_eq!(response.status, Status::Ok);
    response
}

struct Proxy {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Proxy {
    async fn start(
        upstream: SocketAddr,
        latency: Duration,
        drop_after_client_bytes: Option<usize>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("proxy should bind");
        let addr = listener
            .local_addr()
            .expect("proxy local addr should exist");
        let task = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(handle_proxy_connection(
                    client,
                    upstream,
                    latency,
                    drop_after_client_bytes,
                ));
            }
        });
        Self { addr, task }
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_proxy_connection(
    mut client: TcpStream,
    upstream: SocketAddr,
    latency: Duration,
    drop_after_client_bytes: Option<usize>,
) {
    let Ok(mut server) = TcpStream::connect(upstream).await else {
        return;
    };
    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();
    let client_to_server = relay(
        &mut client_read,
        &mut server_write,
        latency,
        drop_after_client_bytes,
    );
    let server_to_client = relay(&mut server_read, &mut client_write, latency, None);
    tokio::select! {
        _ = client_to_server => {}
        _ = server_to_client => {}
    }
}

async fn relay<R, W>(
    reader: &mut R,
    writer: &mut W,
    latency: Duration,
    drop_after_bytes: Option<usize>,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0u8; 1024];
    let mut seen = 0usize;
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        seen = seen.saturating_add(n);
        if drop_after_bytes.is_some_and(|limit| seen >= limit) {
            return Ok(());
        }
        if !latency.is_zero() {
            sleep(latency).await;
        }
        writer.write_all(&buf[..n]).await?;
    }
}

struct ServerProcess {
    child: Child,
}

impl ServerProcess {
    fn start(port: u16, data_dir: PathBuf) -> Self {
        let child = ProcessCommand::new(server_binary())
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--disable-auth")
            .arg("--wal-sync")
            .arg("flush")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("server process should start");
        Self { child }
    }

    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn server_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_vaylix") {
        return PathBuf::from(path);
    }

    let mut path = std::env::current_exe().expect("current test executable should be known");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(format!("vaylix{}", std::env::consts::EXE_SUFFIX));
    path
}

async fn wait_for_tcp_accept(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "server did not accept TCP connections at {addr}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

fn free_tcp_port() -> u16 {
    StdTcpListener::bind("127.0.0.1:0")
        .expect("free port probe should bind")
        .local_addr()
        .expect("free port local addr should exist")
        .port()
}

fn test_seed() -> u64 {
    std::env::var("VAYLIX_TEST_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| 0xa5a5_5a5a_1234_5678 ^ TEST_COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn temp_dir(seed: u64) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("vaylix-chaos-{seed}-{unique}"))
}
