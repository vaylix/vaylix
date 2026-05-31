use clap::Parser;
use server::{
    AdminCommand, Args, PitrAction, ReplicationRoleMode, Server, StorageAction, WalSyncMode,
    WriteAckModeArg,
};
use transport::{CodecOptions, CompressionMode};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

async fn try_main() -> server::Result<()> {
    let mut args = Args::parse();
    if let Some(command) = args.command.take() {
        return run_admin_command(&args, command);
    }
    if !args.ssl
        && (args.tls_cert.is_some() || args.tls_key.is_some() || args.tls_client_ca.is_some())
    {
        return Err(server::ServerError::TlsConfiguration);
    }
    let paths = match args.data_dir.as_ref() {
        Some(data_dir) => engine::Paths::from_data_dir(data_dir)?,
        None => engine::Paths::new()?,
    };
    let keyring = engine::load_or_create_keyring(&paths.keyring_path, &paths.keyring_tmp_path)?;
    let insecure_default_credentials = args.user == server::auth::DEFAULT_USERNAME
        && args.password == server::auth::DEFAULT_PASSWORD;
    let auth_config = if args.disable_auth {
        None
    } else {
        Some(server::auth::AuthConfig::load_or_bootstrap(
            paths.auth_path.clone(),
            paths.auth_tmp_path.clone(),
            keyring.clone(),
            args.user.clone(),
            args.password.clone(),
        )?)
    };
    let engine_options = engine_options(&args, Some(keyring));
    let node_id = args
        .node_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
    let cluster_members = parse_cluster_peers(&args.cluster_peers)?;
    let tls_state = if args.ssl {
        let cert = args
            .tls_cert
            .as_deref()
            .ok_or(server::ServerError::TlsConfiguration)?;
        let key = args
            .tls_key
            .as_deref()
            .ok_or(server::ServerError::TlsConfiguration)?;
        Some(server::tls::TlsState::load(
            cert,
            key,
            args.tls_client_ca.as_deref(),
        )?)
    } else {
        None
    };
    let transport = if args.disable_compression {
        CodecOptions {
            compression: CompressionMode::None,
            compression_threshold_bytes: 0,
            ..CodecOptions::default()
        }
    } else {
        CodecOptions::default()
    };
    let audit_log_path = args
        .audit_log_path
        .clone()
        .unwrap_or_else(|| paths.data_dir.join("audit.log"));
    let audit_logger = server::audit::AuditLogger::open(&audit_log_path)?;
    let backup_dir = args
        .backup_dir
        .clone()
        .unwrap_or_else(|| paths.data_dir.join("backups"));
    std::fs::create_dir_all(&backup_dir)?;
    let mtls_enabled = args.tls_client_ca.is_some();
    let runtime = server::server::ServerRuntimeConfig {
        snapshot_interval: args
            .snapshot_interval_seconds
            .map(std::time::Duration::from_secs),
        expiration_sweep_interval: args
            .expiration_sweep_interval_seconds
            .map(std::time::Duration::from_secs),
        idle_timeout: args
            .idle_timeout_seconds
            .map(std::time::Duration::from_secs),
        auth_config,
        guards: server::server::ServerGuards {
            max_request_payload_bytes: args.max_request_payload_bytes,
            max_key_bytes: args.max_key_bytes,
            max_value_bytes: args.max_value_bytes,
            max_keys_per_batch: args.max_keys_per_batch,
            max_transaction_queue_len: args.max_transaction_queue_len,
            requests_per_second: args.requests_per_second,
            request_burst: args.request_burst,
        },
        tls_state,
        transport,
        audit_logger: std::sync::Arc::new(audit_logger),
        backup_dir,
        mtls_enabled,
        slow_command_threshold: if args.slow_command_threshold_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(
                args.slow_command_threshold_ms,
            ))
        },
        auth_lockouts: std::sync::Arc::new(tokio::sync::Mutex::new(
            server::server::AuthLockoutState::default(),
        )),
        wal_segment_size_bytes: args.wal_segment_size_bytes,
        wal_retain_segments: args.wal_retain_segments,
        auth_failure_window: std::time::Duration::from_secs(args.auth_failure_window_seconds),
        auth_failure_limit: args.auth_failure_limit,
        auth_lockout: std::time::Duration::from_secs(args.auth_lockout_seconds),
        transaction_max_duration: std::time::Duration::from_secs(args.transaction_max_seconds),
        maintenance: std::sync::Arc::new(server::server::MaintenanceMode::load(
            paths.maintenance_path.clone(),
        )?),
        insecure_auth_disabled: args.disable_auth,
        insecure_default_credentials,
        replication: std::sync::Arc::new(server::replication::ReplicationRuntime::new(
            server::replication::ReplicationConfig {
                node_id: node_id.clone(),
                group_id: args.replication_group_id.clone(),
                advertise_addr: args.replication_advertise_addr.clone(),
                role: match args.replication_role {
                    ReplicationRoleMode::Standalone => {
                        server::replication::ReplicationRole::Standalone
                    }
                    ReplicationRoleMode::Leader => server::replication::ReplicationRole::Leader,
                    ReplicationRoleMode::Follower => server::replication::ReplicationRole::Follower,
                },
                upstream: args.replication_upstream.clone(),
                upstream_username: args
                    .replication_user
                    .clone()
                    .or_else(|| Some(args.user.clone())),
                upstream_password: args
                    .replication_password
                    .clone()
                    .or_else(|| Some(args.password.clone())),
                write_ack_mode: match args.write_ack_mode {
                    WriteAckModeArg::Local => server::replication::WriteAckMode::Local,
                    WriteAckModeArg::Replica => server::replication::WriteAckMode::Replica,
                    WriteAckModeArg::All => server::replication::WriteAckMode::All,
                },
                ack_timeout: std::time::Duration::from_millis(args.replication_ack_timeout_ms),
                poll_interval: std::time::Duration::from_millis(args.replication_poll_interval_ms),
                fetch_batch_size: args.replication_fetch_batch_size,
                stale_after: std::time::Duration::from_secs(args.replication_stale_after_seconds),
                heartbeat_interval: std::time::Duration::from_millis(
                    args.replication_heartbeat_interval_ms,
                ),
                election_timeout_min: std::time::Duration::from_millis(
                    args.replication_election_timeout_min_ms,
                ),
                election_timeout_max: std::time::Duration::from_millis(
                    args.replication_election_timeout_max_ms,
                ),
                state_path: paths.cluster_state_path.clone(),
                state_tmp_path: paths.cluster_state_tmp_path.clone(),
                initial_members: cluster_members,
            },
        )?),
        replication_fanout_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        replication_apply_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };
    let server = Server::new(
        args.bind,
        args.port,
        args.max_connections,
        paths,
        engine_options,
        runtime,
    )
    .await?;
    server.start().await?;

    Ok(())
}

fn parse_cluster_peers(
    peers: &[String],
) -> server::Result<Vec<server::replication::ClusterMember>> {
    peers
        .iter()
        .map(|peer| {
            let Some((node_id, advertise_addr)) = peer.split_once('@') else {
                return Err(server::ServerError::InvalidArguments(
                    "cluster peers must use node_id@host:port form".to_string(),
                ));
            };
            if node_id.is_empty() || advertise_addr.is_empty() {
                return Err(server::ServerError::InvalidArguments(
                    "cluster peer entries must include both node_id and host:port".to_string(),
                ));
            }
            Ok(server::replication::ClusterMember {
                node_id: node_id.to_string(),
                advertise_addr: advertise_addr.to_string(),
                voter: true,
            })
        })
        .collect()
}

fn engine_options(args: &Args, keyring: Option<engine::StorageKeyring>) -> engine::EngineOptions {
    engine::EngineOptions {
        wal_sync: match args.wal_sync {
            WalSyncMode::Buffered => engine::WalSyncPolicy::Buffered,
            WalSyncMode::Flush => engine::WalSyncPolicy::Flush,
            WalSyncMode::Sync => engine::WalSyncPolicy::SyncData,
        },
        keyring,
        wal_segment_size_bytes: args.wal_segment_size_bytes,
        wal_retain_segments: args.wal_retain_segments,
    }
}

fn run_admin_command(args: &Args, command: AdminCommand) -> server::Result<()> {
    match command {
        AdminCommand::Storage(command) => match command.action {
            StorageAction::Migrate { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection =
                    engine::Engine::migrate_storage(&paths, &engine_options(args, keyring))?;
                print_storage_inspection(&inspection);
            }
            StorageAction::Verify { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection =
                    engine::Engine::verify_storage(&paths, engine_options(args, keyring))?;
                print_storage_inspection(&inspection);
            }
        },
        AdminCommand::Pitr(command) => match command.action {
            PitrAction::Inspect { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection = engine::Engine::inspect_storage(&paths, keyring.as_ref())?;
                print_storage_inspection(&inspection);
            }
            PitrAction::Restore {
                source_dir,
                target_dir,
                to_sequence,
                to_timestamp_ms,
            } => {
                let source_paths = engine::Paths::from_data_dir(source_dir)?;
                let target_paths = engine::Paths::from_data_dir(target_dir)?;
                let target = if let Some(sequence) = to_sequence {
                    engine::PointInTimeTarget::Sequence(sequence)
                } else if let Some(timestamp_ms) = to_timestamp_ms {
                    engine::PointInTimeTarget::TimestampMs(timestamp_ms)
                } else {
                    return Err(server::ServerError::InvalidArguments(
                        "pitr restore requires --to-sequence or --to-timestamp-ms".to_string(),
                    ));
                };
                let keyring = engine::load_keyring(&source_paths.keyring_path)?;
                let inspection = engine::Engine::restore_to_point(
                    &source_paths,
                    &target_paths,
                    engine_options(args, keyring),
                    target,
                )?;
                print_storage_inspection(&inspection);
            }
        },
    }
    Ok(())
}

fn print_storage_inspection(inspection: &engine::StorageInspection) {
    for (key, value) in [
        ("snapshot_present", inspection.snapshot_present.to_string()),
        (
            "storage_format_version",
            inspection.storage_format_version.to_string(),
        ),
        (
            "snapshot_size_bytes",
            inspection.snapshot_size_bytes.to_string(),
        ),
        (
            "last_snapshot_sequence",
            inspection.last_snapshot_sequence.to_string(),
        ),
        (
            "last_snapshot_at_ms",
            inspection
                .last_snapshot_at_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "wal_segment_count",
            inspection.wal_segment_count.to_string(),
        ),
        (
            "sealed_wal_segment_count",
            inspection.sealed_wal_segment_count.to_string(),
        ),
        (
            "active_wal_segment_count",
            inspection.active_wal_segment_count.to_string(),
        ),
        (
            "active_wal_start_sequence",
            inspection.active_wal_start_sequence.to_string(),
        ),
        (
            "oldest_retained_sequence",
            inspection
                .oldest_retained_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "newest_sequence",
            inspection
                .newest_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        ("wal_size_bytes", inspection.wal_size_bytes.to_string()),
    ] {
        println!("{key}={value}");
    }
}
