use std::fs;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use command::{Command, Expiration, SetCondition, SetOptions};
use engine::{Engine, EngineOptions, Paths, WalSyncPolicy};
use server::Server;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use transport::{Request, Status, read_response_from_async, write_request_to_async};

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("veyra-server-test-{name}-{unique}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handles_real_tcp_round_trip_for_extended_commands() {
    let root = temp_dir("tcp-round-trip");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        Some(Duration::from_millis(50)),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();

    let set = Request::from_command(
        1,
        Command::Set {
            key: "user:1".to_string(),
            value: "alice".to_string(),
            options: SetOptions {
                condition: Some(SetCondition::Nx),
                expiration: Some(Expiration::Px(500)),
                keep_ttl: false,
                return_previous: false,
            },
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &set).await.unwrap();
    let set_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(set_response.status, Status::Ok);
    assert!(set_response.decode_bool().unwrap());

    let getex = Request::from_command(
        2,
        Command::GetEx {
            key: "user:1".to_string(),
            expiration: Some(Expiration::Ex(1)),
            persist: false,
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &getex).await.unwrap();
    let getex_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(getex_response.decode_value().unwrap(), "alice");

    let scan = Request::from_command(
        3,
        Command::Scan {
            cursor: 0,
            pattern: Some("user:*".to_string()),
            count: Some(10),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &scan).await.unwrap();
    let scan_response = read_response_from_async(&mut stream).await.unwrap();
    let scan_payload = scan_response.decode_scan().unwrap();
    assert_eq!(scan_payload.keys, vec!["user:1".to_string()]);

    let getdel = Request::from_command(
        4,
        Command::GetDel {
            key: "user:1".to_string(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &getdel).await.unwrap();
    let getdel_response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(getdel_response.decode_value().unwrap(), "alice");

    server_task.abort();
    fs::remove_dir_all(root).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_snapshotter_writes_snapshot_and_flushes_wal() {
    let root = temp_dir("snapshotter");
    let paths = Paths::from_data_dir(&root).unwrap();
    let engine = Engine::from_paths_with_options(
        paths,
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
        },
    )
    .unwrap();

    let server = Server::with_engine(
        "127.0.0.1".to_string(),
        0,
        16,
        engine,
        Some(Duration::from_millis(50)),
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    let server_task = tokio::spawn(async move { server.start().await });

    let mut stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
        .await
        .unwrap()
        .unwrap();

    let set = Request::from_command(
        1,
        Command::Set {
            key: "snapshot:key".to_string(),
            value: "value".to_string(),
            options: SetOptions::default(),
        },
    )
    .unwrap();
    write_request_to_async(&mut stream, &set).await.unwrap();
    let response = read_response_from_async(&mut stream).await.unwrap();
    assert_eq!(response.status, Status::Ok);

    sleep(Duration::from_millis(150)).await;

    assert!(root.join("snapshot.bin").exists());
    assert!(root.join("manifest.bin").exists());
    let wal_len = fs::metadata(root.join("wal.log")).unwrap().len();
    assert_eq!(wal_len, 0);

    server_task.abort();
    fs::remove_dir_all(root).ok();
}
