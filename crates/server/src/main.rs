use clap::Parser;
use server::{Args, Server, WalSyncMode};

const BANNER: &str = r#"        
        ■ ■ ■
    ████████████
      ████████
         ██
Vaylix Database Server

"#;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = try_main().await {
        eprintln!("[{}] {}: {err}", err.code(), err.name());
        std::process::exit(1);
    }
}

async fn try_main() -> server::Result<()> {
    println!("{BANNER}");

    let args = Args::parse();
    let auth_config = match (args.auth_user, args.auth_password) {
        (Some(username), Some(password)) => Some(server::auth::AuthConfig::new(username, password)?),
        (None, None) => None,
        _ => return Err(server::ServerError::AuthenticationConfiguration),
    };
    let engine_options = engine::EngineOptions {
        wal_sync: match args.wal_sync {
            WalSyncMode::Buffered => engine::WalSyncPolicy::Buffered,
            WalSyncMode::Flush => engine::WalSyncPolicy::Flush,
            WalSyncMode::Sync => engine::WalSyncPolicy::SyncData,
        },
    };
    let server = Server::new(
        args.bind,
        args.port,
        args.max_connections,
        engine_options,
        args.snapshot_interval_seconds
            .map(std::time::Duration::from_secs),
        args.expiration_sweep_interval_seconds
            .map(std::time::Duration::from_secs),
        args.idle_timeout_seconds.map(std::time::Duration::from_secs),
        auth_config,
    )
    .await?;
    server.start().await?;

    Ok(())
}
