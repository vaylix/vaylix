use clap::Parser;
use server::admin::run_admin_command;
use server::bootstrap::build_server_launch_config;
use server::{Args, Server};

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

    let launch = build_server_launch_config(&args)?;
    let server = Server::new(
        launch.bind,
        launch.port,
        launch.max_connections,
        launch.paths,
        launch.engine_options,
        launch.runtime,
    )
    .await?;
    server.start().await
}
