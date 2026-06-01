use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use transport::{CodecOptions, CompressionMode};

use crate::args::{Args, ReplicationRoleMode, WalSyncMode, WriteAckModeArg};
use crate::audit::AuditLogger;
use crate::auth::{AuthConfig, DEFAULT_PASSWORD, DEFAULT_USERNAME};
use crate::error::{Result, ServerError};
use crate::replication::{
    ClusterMember, ReplicationConfig, ReplicationRole, ReplicationRuntime, WriteAckMode,
};
use crate::server::{AuthLockoutState, MaintenanceMode, ServerGuards, ServerRuntimeConfig};

/// Fully resolved dependencies required to start a server instance.
///
/// This keeps command-line parsing separate from server construction: callers
/// can test environment/config mapping without binding a TCP listener.
pub struct ServerLaunchConfig {
    pub bind: String,
    pub port: u16,
    pub max_connections: usize,
    pub paths: engine::Paths,
    pub engine_options: engine::EngineOptions,
    pub runtime: ServerRuntimeConfig,
}

/// Converts parsed CLI/env arguments into concrete server dependencies.
pub fn build_server_launch_config(args: &Args) -> Result<ServerLaunchConfig> {
    validate_tls_args(args)?;

    let paths = match args.data_dir.as_ref() {
        Some(data_dir) => engine::Paths::from_data_dir(data_dir)?,
        None => engine::Paths::new()?,
    };
    let keyring = engine::load_or_create_keyring(&paths.keyring_path, &paths.keyring_tmp_path)?;
    let auth_config = build_auth_config(args, &paths, keyring.clone())?;
    let engine_options = engine_options(args, Some(keyring));
    let cluster_members = parse_cluster_peers(&args.cluster_peers)?;
    let tls_state = build_tls_state(args)?;
    let transport = transport_options(args);
    let audit_logger = AuditLogger::open(&audit_log_path(args, &paths))?;
    let backup_dir = args
        .backup_dir
        .clone()
        .unwrap_or_else(|| paths.data_dir.join("backups"));
    std::fs::create_dir_all(&backup_dir)?;

    let runtime = ServerRuntimeConfig {
        snapshot_interval: args.snapshot_interval_seconds.map(Duration::from_secs),
        expiration_sweep_interval: args
            .expiration_sweep_interval_seconds
            .map(Duration::from_secs),
        idle_timeout: args.idle_timeout_seconds.map(Duration::from_secs),
        auth_config,
        guards: ServerGuards {
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
        audit_logger: Arc::new(audit_logger),
        backup_dir,
        mtls_enabled: args.tls_client_ca.is_some(),
        slow_command_threshold: if args.slow_command_threshold_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(args.slow_command_threshold_ms))
        },
        auth_lockouts: Arc::new(Mutex::new(AuthLockoutState::default())),
        wal_segment_size_bytes: args.wal_segment_size_bytes,
        wal_retain_segments: args.wal_retain_segments,
        auth_failure_window: Duration::from_secs(args.auth_failure_window_seconds),
        auth_failure_limit: args.auth_failure_limit,
        auth_lockout: Duration::from_secs(args.auth_lockout_seconds),
        transaction_max_duration: Duration::from_secs(args.transaction_max_seconds),
        maintenance: Arc::new(MaintenanceMode::load(paths.maintenance_path.clone())?),
        insecure_auth_disabled: args.disable_auth,
        insecure_default_credentials: uses_default_credentials(args),
        replication: Arc::new(ReplicationRuntime::new(replication_config(
            args,
            cluster_members,
            &paths,
        ))?),
        replication_fanout_lock: Arc::new(Mutex::new(())),
        replication_apply_lock: Arc::new(Mutex::new(())),
    };

    Ok(ServerLaunchConfig {
        bind: args.bind.clone(),
        port: args.port,
        max_connections: args.max_connections,
        paths,
        engine_options,
        runtime,
    })
}

/// Builds engine options shared by server startup and offline admin commands.
pub fn engine_options(
    args: &Args,
    keyring: Option<engine::StorageKeyring>,
) -> engine::EngineOptions {
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

/// Parses static cluster peers from `node_id@host:port` CLI/env entries.
pub fn parse_cluster_peers(peers: &[String]) -> Result<Vec<ClusterMember>> {
    peers
        .iter()
        .map(|peer| {
            let Some((node_id, advertise_addr)) = peer.split_once('@') else {
                return Err(ServerError::InvalidArguments(
                    "cluster peers must use node_id@host:port form".to_string(),
                ));
            };
            if node_id.is_empty() || advertise_addr.is_empty() {
                return Err(ServerError::InvalidArguments(
                    "cluster peer entries must include both node_id and host:port".to_string(),
                ));
            }
            Ok(ClusterMember {
                node_id: node_id.to_string(),
                advertise_addr: advertise_addr.to_string(),
                voter: true,
            })
        })
        .collect()
}

fn validate_tls_args(args: &Args) -> Result<()> {
    if !args.ssl
        && (args.tls_cert.is_some() || args.tls_key.is_some() || args.tls_client_ca.is_some())
    {
        return Err(ServerError::TlsConfiguration);
    }
    Ok(())
}

fn build_auth_config(
    args: &Args,
    paths: &engine::Paths,
    keyring: engine::StorageKeyring,
) -> Result<Option<AuthConfig>> {
    if args.disable_auth {
        return Ok(None);
    }
    Ok(Some(AuthConfig::load_or_bootstrap(
        paths.auth_path.clone(),
        paths.auth_tmp_path.clone(),
        keyring,
        args.user.clone(),
        args.password.clone(),
    )?))
}

fn build_tls_state(args: &Args) -> Result<Option<Arc<crate::tls::TlsState>>> {
    if !args.ssl {
        return Ok(None);
    }
    let cert = args
        .tls_cert
        .as_deref()
        .ok_or(ServerError::TlsConfiguration)?;
    let key = args
        .tls_key
        .as_deref()
        .ok_or(ServerError::TlsConfiguration)?;
    Ok(Some(crate::tls::TlsState::load(
        cert,
        key,
        args.tls_client_ca.as_deref(),
    )?))
}

fn transport_options(args: &Args) -> CodecOptions {
    if args.disable_compression {
        CodecOptions {
            compression: CompressionMode::None,
            compression_threshold_bytes: 0,
            ..CodecOptions::default()
        }
    } else {
        CodecOptions::default()
    }
}

fn audit_log_path(args: &Args, paths: &engine::Paths) -> std::path::PathBuf {
    args.audit_log_path
        .clone()
        .unwrap_or_else(|| paths.data_dir.join("audit.log"))
}

fn replication_config(
    args: &Args,
    initial_members: Vec<ClusterMember>,
    paths: &engine::Paths,
) -> ReplicationConfig {
    ReplicationConfig {
        node_id: args
            .node_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
        group_id: args.replication_group_id.clone(),
        advertise_addr: args.replication_advertise_addr.clone(),
        role: match args.replication_role {
            ReplicationRoleMode::Standalone => ReplicationRole::Standalone,
            ReplicationRoleMode::Leader => ReplicationRole::Leader,
            ReplicationRoleMode::Follower => ReplicationRole::Follower,
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
            WriteAckModeArg::Local => WriteAckMode::Local,
            WriteAckModeArg::Replica => WriteAckMode::Replica,
            WriteAckModeArg::All => WriteAckMode::All,
        },
        ack_timeout: Duration::from_millis(args.replication_ack_timeout_ms),
        poll_interval: Duration::from_millis(args.replication_poll_interval_ms),
        fetch_batch_size: args.replication_fetch_batch_size,
        stale_after: Duration::from_secs(args.replication_stale_after_seconds),
        heartbeat_interval: Duration::from_millis(args.replication_heartbeat_interval_ms),
        election_timeout_min: Duration::from_millis(args.replication_election_timeout_min_ms),
        election_timeout_max: Duration::from_millis(args.replication_election_timeout_max_ms),
        state_path: paths.cluster_state_path.clone(),
        state_tmp_path: paths.cluster_state_tmp_path.clone(),
        initial_members,
    }
}

fn uses_default_credentials(args: &Args) -> bool {
    args.user == DEFAULT_USERNAME && args.password == DEFAULT_PASSWORD
}

#[cfg(test)]
mod tests {
    use super::{parse_cluster_peers, transport_options};
    use crate::args::Args;
    use clap::Parser;
    use transport::CompressionMode;

    #[test]
    fn parses_cluster_peer_entries() {
        let peers = parse_cluster_peers(&[
            "node-a@127.0.0.1:9173".to_string(),
            "node-b@127.0.0.1:9174".to_string(),
        ])
        .unwrap();

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id, "node-a");
        assert_eq!(peers[1].advertise_addr, "127.0.0.1:9174");
        assert!(peers.iter().all(|peer| peer.voter));
    }

    #[test]
    fn rejects_malformed_cluster_peer_entries() {
        let err = parse_cluster_peers(&["node-a".to_string()]).unwrap_err();
        assert_eq!(err.code(), "SRV-030");
    }

    #[test]
    fn compression_can_be_disabled_from_args() {
        let args = Args::try_parse_from(["vaylix", "--disable-compression"]).unwrap();
        let transport = transport_options(&args);
        assert_eq!(transport.compression, CompressionMode::None);
        assert_eq!(transport.compression_threshold_bytes, 0);
    }
}
