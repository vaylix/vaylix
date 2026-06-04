use std::io::Cursor;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use transport::{
    CodecOptions, CompressionMode, Opcode, Request, Response, Status, decode_request,
    decode_response, encode_request_with_options, encode_response_with_options,
    read_request_from_with_options,
};
use uuid::Uuid;

struct RequestCase {
    name: &'static str,
    opcode: Opcode,
    payload: Vec<u8>,
}

struct ResponseCase {
    name: &'static str,
    status: Status,
    payload: Vec<u8>,
}

fn text(size: usize, byte: u8) -> String {
    std::iter::repeat_n(char::from(byte), size).collect()
}

fn request_cases() -> Vec<RequestCase> {
    vec![
        RequestCase {
            name: "auth",
            opcode: Opcode::Auth,
            payload: br#"{"username":"bench","password":"secret"}"#.to_vec(),
        },
        RequestCase {
            name: "ping",
            opcode: Opcode::Ping,
            payload: br#"{"message":"PING"}"#.to_vec(),
        },
        RequestCase {
            name: "get",
            opcode: Opcode::Get,
            payload: br#"{"key":"bench-key"}"#.to_vec(),
        },
        RequestCase {
            name: "getdel",
            opcode: Opcode::GetDel,
            payload: br#"{"key":"bench-key"}"#.to_vec(),
        },
        RequestCase {
            name: "getex",
            opcode: Opcode::GetEx,
            payload: br#"{"key":"bench-key","expiration":{"ex":60}}"#.to_vec(),
        },
        RequestCase {
            name: "set",
            opcode: Opcode::Set,
            payload: format!(
                r#"{{"key":"bench-key","value":"{}"}}"#,
                text(256, b'v')
            )
            .into_bytes(),
        },
        RequestCase {
            name: "setnx",
            opcode: Opcode::SetNx,
            payload: format!(
                r#"{{"key":"bench-key","value":"{}"}}"#,
                text(256, b'v')
            )
            .into_bytes(),
        },
        RequestCase {
            name: "delete",
            opcode: Opcode::Delete,
            payload: br#"{"keys":["key-001","key-002","key-003","key-004"]}"#.to_vec(),
        },
        RequestCase {
            name: "exists",
            opcode: Opcode::Exists,
            payload: br#"{"key":"bench-key"}"#.to_vec(),
        },
        RequestCase {
            name: "mget",
            opcode: Opcode::MGet,
            payload: br#"{"keys":["key-001","key-002","key-003","key-004","key-005","key-006","key-007","key-008"]}"#.to_vec(),
        },
        RequestCase {
            name: "mset",
            opcode: Opcode::MSet,
            payload: format!(
                r#"{{"entries":[["key-001","{}"],["key-002","{}"],["key-003","{}"],["key-004","{}"]]}}"#,
                text(128, b'a'),
                text(128, b'b'),
                text(128, b'c'),
                text(128, b'd'),
            )
            .into_bytes(),
        },
        RequestCase {
            name: "incr",
            opcode: Opcode::Incr,
            payload: br#"{"key":"counter"}"#.to_vec(),
        },
        RequestCase {
            name: "decr",
            opcode: Opcode::Decr,
            payload: br#"{"key":"counter"}"#.to_vec(),
        },
        RequestCase {
            name: "expire",
            opcode: Opcode::Expire,
            payload: br#"{"key":"bench-key","ttl_seconds":60}"#.to_vec(),
        },
        RequestCase {
            name: "ttl",
            opcode: Opcode::Ttl,
            payload: br#"{"key":"bench-key"}"#.to_vec(),
        },
        RequestCase {
            name: "persist",
            opcode: Opcode::Persist,
            payload: br#"{"key":"bench-key"}"#.to_vec(),
        },
        RequestCase {
            name: "rename",
            opcode: Opcode::Rename,
            payload: br#"{"source":"old-key","destination":"new-key"}"#.to_vec(),
        },
        RequestCase {
            name: "renamenx",
            opcode: Opcode::RenameNx,
            payload: br#"{"source":"old-key","destination":"new-key"}"#.to_vec(),
        },
        RequestCase {
            name: "scan",
            opcode: Opcode::Scan,
            payload: br#"{"cursor":0,"match":"session:*","count":128}"#.to_vec(),
        },
        RequestCase {
            name: "dbsize",
            opcode: Opcode::DbSize,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "info",
            opcode: Opcode::Info,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "metrics",
            opcode: Opcode::Metrics,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "list",
            opcode: Opcode::List,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "clear",
            opcode: Opcode::Clear,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "count",
            opcode: Opcode::Count,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "save",
            opcode: Opcode::Save,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "snapshot",
            opcode: Opcode::Snapshot,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "multi",
            opcode: Opcode::Multi,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "exec",
            opcode: Opcode::Exec,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "discard",
            opcode: Opcode::Discard,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "backup",
            opcode: Opcode::Backup,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "restore",
            opcode: Opcode::Restore,
            payload: format!(
                r#"{{"entries":[["restore-001","{}"],["restore-002","{}"],["restore-003","{}"]]}}"#,
                text(128, b'r'),
                text(128, b's'),
                text(128, b't'),
            )
            .into_bytes(),
        },
        RequestCase {
            name: "create_user",
            opcode: Opcode::CreateUser,
            payload: br#"{"username":"bench-user","password":"secret"}"#.to_vec(),
        },
        RequestCase {
            name: "drop_user",
            opcode: Opcode::DropUser,
            payload: br#"{"username":"bench-user"}"#.to_vec(),
        },
        RequestCase {
            name: "create_role",
            opcode: Opcode::CreateRole,
            payload: br#"{"role":"bench-role"}"#.to_vec(),
        },
        RequestCase {
            name: "drop_role",
            opcode: Opcode::DropRole,
            payload: br#"{"role":"bench-role"}"#.to_vec(),
        },
        RequestCase {
            name: "grant_role",
            opcode: Opcode::GrantRole,
            payload: br#"{"role":"bench-role","username":"bench-user"}"#.to_vec(),
        },
        RequestCase {
            name: "revoke_role",
            opcode: Opcode::RevokeRole,
            payload: br#"{"role":"bench-role","username":"bench-user"}"#.to_vec(),
        },
        RequestCase {
            name: "grant_permission",
            opcode: Opcode::GrantPermission,
            payload: br#"{"role":"bench-role","permission":"read","pattern":"tenant:*"}"#.to_vec(),
        },
        RequestCase {
            name: "revoke_permission",
            opcode: Opcode::RevokePermission,
            payload: br#"{"role":"bench-role","permission":"read","pattern":"tenant:*"}"#.to_vec(),
        },
        RequestCase {
            name: "show_users",
            opcode: Opcode::ShowUsers,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "show_roles",
            opcode: Opcode::ShowRoles,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "whoami",
            opcode: Opcode::WhoAmI,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "backup_to",
            opcode: Opcode::BackupTo,
            payload: br#"{"path":"/var/lib/vaylix/backups/bench.dump"}"#.to_vec(),
        },
        RequestCase {
            name: "restore_from",
            opcode: Opcode::RestoreFrom,
            payload: br#"{"path":"/var/lib/vaylix/backups/bench.dump"}"#.to_vec(),
        },
        RequestCase {
            name: "restore_check",
            opcode: Opcode::RestoreCheck,
            payload: format!(r#"{{"dump":"{}"}}"#, text(2048, b'c')).into_bytes(),
        },
        RequestCase {
            name: "restore_check_from",
            opcode: Opcode::RestoreCheckFrom,
            payload: br#"{"path":"/var/lib/vaylix/backups/bench.dump"}"#.to_vec(),
        },
        RequestCase {
            name: "alter_user_password",
            opcode: Opcode::AlterUserPassword,
            payload: br#"{"username":"bench-user","password":"rotated-secret"}"#.to_vec(),
        },
        RequestCase {
            name: "metrics_prom",
            opcode: Opcode::MetricsProm,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "backup_verify",
            opcode: Opcode::BackupVerify,
            payload: format!(r#"{{"dump":"{}"}}"#, text(2048, b'v')).into_bytes(),
        },
        RequestCase {
            name: "backup_verify_from",
            opcode: Opcode::BackupVerifyFrom,
            payload: br#"{"path":"/var/lib/vaylix/backups/bench.dump"}"#.to_vec(),
        },
        RequestCase {
            name: "show_grants",
            opcode: Opcode::ShowGrants,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "show_grants_for_user",
            opcode: Opcode::ShowGrantsForUser,
            payload: br#"{"username":"bench-user"}"#.to_vec(),
        },
        RequestCase {
            name: "show_grants_for_role",
            opcode: Opcode::ShowGrantsForRole,
            payload: br#"{"role":"bench-role"}"#.to_vec(),
        },
        RequestCase {
            name: "maintenance_on",
            opcode: Opcode::MaintenanceOn,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "maintenance_off",
            opcode: Opcode::MaintenanceOff,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "maintenance_status",
            opcode: Opcode::MaintenanceStatus,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "health",
            opcode: Opcode::Health,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "show_cluster",
            opcode: Opcode::ShowCluster,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "cluster_join",
            opcode: Opcode::ClusterJoin,
            payload: br#"{"node_id":"node-2","address":"127.0.0.1:9274"}"#.to_vec(),
        },
        RequestCase {
            name: "cluster_remove",
            opcode: Opcode::ClusterRemove,
            payload: br#"{"node_id":"node-2"}"#.to_vec(),
        },
        RequestCase {
            name: "show_replication",
            opcode: Opcode::ShowReplication,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "promote_follower",
            opcode: Opcode::PromoteFollower,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "pause_replication",
            opcode: Opcode::PauseReplication,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "resume_replication",
            opcode: Opcode::ResumeReplication,
            payload: br#"{}"#.to_vec(),
        },
        RequestCase {
            name: "replication_status",
            opcode: Opcode::ReplicationStatus,
            payload: br#"{"node_id":"node-2","last_applied_lsn":8192}"#.to_vec(),
        },
        RequestCase {
            name: "replication_snapshot",
            opcode: Opcode::ReplicationSnapshot,
            payload: format!(
                r#"{{"snapshot_id":"snap-01","last_included_lsn":8192,"manifest":"{}"}}"#,
                text(2048, b'm')
            )
            .into_bytes(),
        },
        RequestCase {
            name: "replication_fetch",
            opcode: Opcode::ReplicationFetch,
            payload: br#"{"from_lsn":8192,"max_entries":128}"#.to_vec(),
        },
        RequestCase {
            name: "replication_ack",
            opcode: Opcode::ReplicationAck,
            payload: br#"{"node_id":"node-2","durable_lsn":8256}"#.to_vec(),
        },
        RequestCase {
            name: "replication_vote",
            opcode: Opcode::ReplicationVote,
            payload: br#"{"term":7,"candidate_id":"node-2","last_log_index":1024,"last_log_term":7}"#.to_vec(),
        },
        RequestCase {
            name: "replication_heartbeat",
            opcode: Opcode::ReplicationHeartbeat,
            payload: br#"{"term":7,"leader_id":"node-1","commit_index":1024}"#.to_vec(),
        },
        RequestCase {
            name: "replication_append",
            opcode: Opcode::ReplicationAppend,
            payload: format!(
                r#"{{"term":7,"leader_id":"node-1","prev_log_index":1023,"prev_log_term":7,"entries":"{}","leader_commit":1024}}"#,
                text(4096, b'a')
            )
            .into_bytes(),
        },
        RequestCase {
            name: "replication_install_snapshot",
            opcode: Opcode::ReplicationInstallSnapshot,
            payload: format!(
                r#"{{"term":7,"leader_id":"node-1","last_included_index":1024,"last_included_term":7,"chunk":"{}","done":true}}"#,
                text(8192, b's')
            )
            .into_bytes(),
        },
    ]
}

fn response_cases() -> Vec<ResponseCase> {
    vec![
        ResponseCase {
            name: "ok_empty",
            status: Status::Ok,
            payload: Vec::new(),
        },
        ResponseCase {
            name: "ok_small_string",
            status: Status::Ok,
            payload: br#""PONG""#.to_vec(),
        },
        ResponseCase {
            name: "ok_get_value",
            status: Status::Ok,
            payload: format!(r#""{}""#, text(256, b'v')).into_bytes(),
        },
        ResponseCase {
            name: "ok_scan_page",
            status: Status::Ok,
            payload: format!(
                r#"{{"cursor":128,"keys":["{}","{}","{}","{}","{}","{}","{}","{}"]}}"#,
                "session:0001",
                "session:0002",
                "session:0003",
                "session:0004",
                "session:0005",
                "session:0006",
                "session:0007",
                "session:0008",
            )
            .into_bytes(),
        },
        ResponseCase {
            name: "ok_exec_results",
            status: Status::Ok,
            payload: br#"{"results":[{"status":"ok","value":"1"},{"status":"ok","value":"2"},{"status":"ok","value":"3"},{"status":"ok","value":"4"}]}"#.to_vec(),
        },
        ResponseCase {
            name: "ok_info_json",
            status: Status::Ok,
            payload: format!(
                r#"{{"server":"vaylix","role":"leader","commit_index":8192,"metrics":"{}"}}"#,
                text(4096, b'i')
            )
            .into_bytes(),
        },
        ResponseCase {
            name: "ok_large_backup",
            status: Status::Ok,
            payload: format!(r#"{{"dump":"{}"}}"#, text(32 * 1024, b'b')).into_bytes(),
        },
        ResponseCase {
            name: "error_json",
            status: Status::Error,
            payload: br#"{"code":"SRV-008","message":"client authentication failed"}"#.to_vec(),
        },
    ]
}

fn request_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport/request_by_opcode");
    for case in request_cases() {
        group.bench_with_input(BenchmarkId::from_parameter(case.name), &case, |b, case| {
            b.iter_batched(
                || {
                    (
                        Request::new(Uuid::now_v7(), case.opcode, case.payload.clone()),
                        CodecOptions::default(),
                    )
                },
                |(request, options)| {
                    let bytes =
                        encode_request_with_options(&request, options).expect("encode request");
                    let decoded = decode_request(&bytes).expect("decode request");
                    assert_eq!(decoded.request_id, request.request_id);
                    assert_eq!(decoded.opcode, request.opcode);
                    assert_eq!(decoded.payload, request.payload);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn response_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport/response_shapes");
    for case in response_cases() {
        for compression in [CompressionMode::None, CompressionMode::Zstd] {
            let bench_id =
                BenchmarkId::new(case.name, format!("compression={}", compression.as_str()));
            group.bench_with_input(bench_id, &(case.name, compression), |b, (_, mode)| {
                b.iter_batched(
                    || {
                        let response =
                            Response::new(Uuid::now_v7(), case.status, case.payload.clone());
                        let options = CodecOptions {
                            compression: *mode,
                            ..CodecOptions::default()
                        };
                        (response, options)
                    },
                    |(response, options)| {
                        let bytes = encode_response_with_options(&response, options)
                            .expect("encode response");
                        let decoded = decode_response(&bytes).expect("decode response");
                        assert_eq!(decoded.request_id, response.request_id);
                        assert_eq!(decoded.status, response.status);
                        assert_eq!(decoded.payload, response.payload);
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn frame_parse_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport/frame_parse_latency");
    for case in request_cases()
        .into_iter()
        .filter(|case| matches!(case.opcode, Opcode::Get | Opcode::Set | Opcode::MGet))
    {
        let request = Request::new(Uuid::from_u128(42), case.opcode, case.payload);
        let encoded =
            encode_request_with_options(&request, CodecOptions::default()).expect("encode request");
        group.bench_with_input(
            BenchmarkId::from_parameter(case.name),
            &encoded,
            |b, bytes| {
                b.iter(|| {
                    let decoded = decode_request(bytes).expect("decode request");
                    assert_eq!(decoded.request_id, request.request_id);
                });
            },
        );
    }
    group.finish();
}

fn encode_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport/encode_latency");
    for case in request_cases()
        .into_iter()
        .filter(|case| matches!(case.opcode, Opcode::Get | Opcode::Set | Opcode::MSet))
    {
        let request = Request::new(Uuid::from_u128(7), case.opcode, case.payload);
        group.bench_with_input(
            BenchmarkId::from_parameter(case.name),
            &request,
            |b, request| {
                b.iter(|| {
                    let encoded = encode_request_with_options(request, CodecOptions::default())
                        .expect("encode");
                    assert!(!encoded.is_empty());
                });
            },
        );
    }
    group.finish();
}

fn pipelined_request_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("transport/pipelined_request_throughput");
    let options = CodecOptions {
        compression: CompressionMode::None,
        ..CodecOptions::default()
    };
    let request = Request::new(
        Uuid::from_u128(99),
        Opcode::Get,
        br#"{"key":"bench-key"}"#.to_vec(),
    );
    let encoded = encode_request_with_options(&request, options).expect("encode request");

    for pipeline_depth in [16_usize, 64, 256] {
        let mut stream = Vec::with_capacity(encoded.len() * pipeline_depth);
        for _ in 0..pipeline_depth {
            stream.extend_from_slice(&encoded);
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(pipeline_depth),
            &stream,
            |b, stream| {
                b.iter(|| {
                    let mut cursor = Cursor::new(stream.as_slice());
                    for _ in 0..pipeline_depth {
                        let decoded =
                            read_request_from_with_options(&mut cursor, options).expect("decode");
                        assert_eq!(decoded.opcode, Opcode::Get);
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    transport_benches,
    request_round_trip,
    response_round_trip,
    frame_parse_latency,
    encode_latency,
    pipelined_request_throughput
);
criterion_main!(transport_benches);
