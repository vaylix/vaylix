use clap::Parser;
use server::{Args, Server, WalSyncMode};
use transport::{CodecOptions, CompressionMode};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

async fn try_main() -> server::Result<()> {
    let args = Args::parse();
    if !args.ssl
        && (args.tls_cert.is_some() || args.tls_key.is_some() || args.tls_client_ca.is_some())
    {
        return Err(server::ServerError::TlsConfiguration);
    }
    let paths = match args.data_dir {
        Some(data_dir) => engine::Paths::from_data_dir(data_dir)?,
        None => engine::Paths::new()?,
    };
    let keyring = engine::load_or_create_keyring(&paths.keyring_path, &paths.keyring_tmp_path)?;
    let auth_config = if args.disable_auth {
        None
    } else {
        Some(server::auth::AuthConfig::load_or_bootstrap(
            paths.auth_path.clone(),
            paths.auth_tmp_path.clone(),
            keyring.clone(),
            args.user,
            args.password,
        )?)
    };
    let engine_options = engine::EngineOptions {
        wal_sync: match args.wal_sync {
            WalSyncMode::Buffered => engine::WalSyncPolicy::Buffered,
            WalSyncMode::Flush => engine::WalSyncPolicy::Flush,
            WalSyncMode::Sync => engine::WalSyncPolicy::SyncData,
        },
        keyring: Some(keyring),
    };
    let tls_config = if args.ssl {
        let cert = args
            .tls_cert
            .as_deref()
            .ok_or(server::ServerError::TlsConfiguration)?;
        let key = args
            .tls_key
            .as_deref()
            .ok_or(server::ServerError::TlsConfiguration)?;
        Some(server::tls::load_server_config(
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
        tls_config,
        transport,
        audit_logger: std::sync::Arc::new(audit_logger),
        backup_dir,
        mtls_enabled,
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
