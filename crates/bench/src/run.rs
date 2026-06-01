use std::time::{Duration, Instant};

use command::Command;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

use crate::args::{
    Args, BaselineArgs, BenchmarkCommand, ExampleCertsArgs, ManagedArgs, ProfileArgs, RunArgs,
    TlsArgs, WorkloadKind,
};
use crate::client::{BenchmarkClient, ConnectionConfig, TlsConfig};
use crate::error::{BenchError, Result};
use crate::launcher::{ManagedCluster, write_example_certs};
use crate::report::{BenchmarkReport, LatencySummary};

pub async fn execute(args: Args) -> Result<BenchmarkReport> {
    match args.command {
        BenchmarkCommand::Run(args) => run_profile("custom".to_string(), run_config(&args)).await,
        BenchmarkCommand::BaselineSingleNode(args) => {
            let run = single_node_profile(args);
            run_profile("baseline-single-node".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::BaselineQuorum(args) => {
            let run = quorum_profile(args);
            run_profile("baseline-quorum".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::TransactionFlow(args) => {
            let run = command_profile(args, WorkloadKind::TransactionFlow);
            run_profile("transaction-flow".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::BackupRestore(args) => {
            let run = command_profile(args, WorkloadKind::BackupRestore);
            run_profile("backup-restore".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::AuthRbacChurn(args) => {
            let run = command_profile(args, WorkloadKind::AuthRbacChurn);
            run_profile("auth-rbac-churn".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::QuorumWriteCost(args) => {
            let run = command_profile(args, WorkloadKind::QuorumWriteCost);
            run_profile("quorum-write-cost".to_string(), run_config(&run)).await
        }
        BenchmarkCommand::ManagedSingleNode(args) => managed_single_node(args).await,
        BenchmarkCommand::ManagedQuorum(args) => managed_quorum(args).await,
        BenchmarkCommand::ManagedTransactionFlow(args) => {
            managed_single_node_profile(
                args,
                "managed-transaction-flow",
                WorkloadKind::TransactionFlow,
            )
            .await
        }
        BenchmarkCommand::ManagedBackupRestore(args) => {
            managed_single_node_profile(args, "managed-backup-restore", WorkloadKind::BackupRestore)
                .await
        }
        BenchmarkCommand::ManagedAuthRbacChurn(args) => {
            managed_single_node_profile(
                args,
                "managed-auth-rbac-churn",
                WorkloadKind::AuthRbacChurn,
            )
            .await
        }
        BenchmarkCommand::ManagedQuorumWriteCost(args) => managed_quorum_write_cost(args).await,
        BenchmarkCommand::ExampleCerts(args) => example_certs(args),
    }
}

fn example_certs(args: ExampleCertsArgs) -> Result<BenchmarkReport> {
    write_example_certs(&args.out_dir)?;
    Ok(BenchmarkReport {
        profile: "example-certs".to_string(),
        addr: args.out_dir.display().to_string(),
        connections: 0,
        duration_seconds: 0,
        keyspace: 0,
        value_size: 0,
        seed_keys: 0,
        completed_operations: 0,
        failed_operations: 0,
        operations_per_second: 0.0,
        latency_us: LatencySummary::zero(),
    })
}

async fn managed_single_node(args: ManagedArgs) -> Result<BenchmarkReport> {
    let cluster = ManagedCluster::single_node(&args).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let connections = args.connections.unwrap_or(64);
    let seed_keys = args.seed_keys.unwrap_or(8_192);
    let keyspace = args.keyspace.unwrap_or(25_000);
    let value_size = args.value_size.unwrap_or(512);
    let run = RunArgs {
        addr: cluster.addr.clone(),
        username: Some(args.username),
        password: Some(args.password),
        tls: TlsArgs {
            tls: cluster.connection.tls.enabled,
            tls_ca_cert: cluster.connection.tls.ca_cert.clone(),
            tls_client_cert: cluster.connection.tls.client_cert.clone(),
            tls_client_key: cluster.connection.tls.client_key.clone(),
        },
        connections,
        duration_seconds: args.duration_seconds,
        keyspace,
        value_size,
        seed_keys,
        workload: WorkloadKind::Mixed,
    };
    run_profile("managed-single-node".to_string(), run_config(&run)).await
}

async fn managed_quorum(args: ManagedArgs) -> Result<BenchmarkReport> {
    let mut cluster = ManagedCluster::quorum(&args).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.addr = select_writable_addr(&cluster).await?;
    let connections = args.connections.unwrap_or(32);
    let seed_keys = args.seed_keys.unwrap_or(4_096);
    let keyspace = args.keyspace.unwrap_or(10_000);
    let value_size = args.value_size.unwrap_or(256);
    let run = RunArgs {
        addr: cluster.addr.clone(),
        username: Some(args.username),
        password: Some(args.password),
        tls: TlsArgs {
            tls: cluster.connection.tls.enabled,
            tls_ca_cert: cluster.connection.tls.ca_cert.clone(),
            tls_client_cert: cluster.connection.tls.client_cert.clone(),
            tls_client_key: cluster.connection.tls.client_key.clone(),
        },
        connections,
        duration_seconds: args.duration_seconds,
        keyspace,
        value_size,
        seed_keys,
        workload: WorkloadKind::Set,
    };
    run_profile("managed-quorum".to_string(), run_config(&run)).await
}

async fn managed_single_node_profile(
    args: ManagedArgs,
    profile: &'static str,
    workload: WorkloadKind,
) -> Result<BenchmarkReport> {
    let cluster = ManagedCluster::single_node(&args).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let run = managed_profile_run_args(&args, &cluster, workload);
    run_profile(profile.to_string(), run_config(&run)).await
}

async fn managed_quorum_write_cost(args: ManagedArgs) -> Result<BenchmarkReport> {
    let mut cluster = ManagedCluster::quorum(&args).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.addr = select_writable_addr(&cluster).await?;
    let mut run = managed_profile_run_args(&args, &cluster, WorkloadKind::QuorumWriteCost);
    run.addr = cluster.addr.clone();
    run_profile("managed-quorum-write-cost".to_string(), run_config(&run)).await
}

fn single_node_profile(args: BaselineArgs) -> RunArgs {
    RunArgs {
        addr: args.addr,
        username: args.username,
        password: args.password,
        tls: args.tls,
        connections: 64,
        duration_seconds: args.duration_seconds,
        keyspace: 25_000,
        value_size: 512,
        seed_keys: 8_192,
        workload: WorkloadKind::Mixed,
    }
}

fn quorum_profile(args: BaselineArgs) -> RunArgs {
    RunArgs {
        addr: args.addr,
        username: args.username,
        password: args.password,
        tls: args.tls,
        connections: 32,
        duration_seconds: args.duration_seconds,
        keyspace: 10_000,
        value_size: 256,
        seed_keys: 4_096,
        workload: WorkloadKind::Set,
    }
}

fn command_profile(args: ProfileArgs, workload: WorkloadKind) -> RunArgs {
    let defaults = profile_defaults(workload);
    RunArgs {
        addr: args.addr,
        username: args.username,
        password: args.password,
        tls: args.tls,
        connections: args.connections.unwrap_or(defaults.connections),
        duration_seconds: args.duration_seconds,
        keyspace: args.keyspace.unwrap_or(defaults.keyspace),
        value_size: args.value_size.unwrap_or(defaults.value_size),
        seed_keys: args.seed_keys.unwrap_or(defaults.seed_keys),
        workload,
    }
}

fn managed_profile_run_args(
    args: &ManagedArgs,
    cluster: &ManagedCluster,
    workload: WorkloadKind,
) -> RunArgs {
    let defaults = profile_defaults(workload);
    RunArgs {
        addr: cluster.addr.clone(),
        username: Some(args.username.clone()),
        password: Some(args.password.clone()),
        tls: TlsArgs {
            tls: cluster.connection.tls.enabled,
            tls_ca_cert: cluster.connection.tls.ca_cert.clone(),
            tls_client_cert: cluster.connection.tls.client_cert.clone(),
            tls_client_key: cluster.connection.tls.client_key.clone(),
        },
        connections: args.connections.unwrap_or(defaults.connections),
        duration_seconds: args.duration_seconds,
        keyspace: args.keyspace.unwrap_or(defaults.keyspace),
        value_size: args.value_size.unwrap_or(defaults.value_size),
        seed_keys: args.seed_keys.unwrap_or(defaults.seed_keys),
        workload,
    }
}

#[derive(Clone, Copy)]
struct ProfileDefaults {
    connections: usize,
    keyspace: usize,
    value_size: usize,
    seed_keys: usize,
}

fn profile_defaults(workload: WorkloadKind) -> ProfileDefaults {
    match workload {
        WorkloadKind::TransactionFlow => ProfileDefaults {
            connections: 16,
            keyspace: 10_000,
            value_size: 256,
            seed_keys: 0,
        },
        WorkloadKind::BackupRestore => ProfileDefaults {
            connections: 1,
            keyspace: 1_000,
            value_size: 256,
            seed_keys: 128,
        },
        WorkloadKind::AuthRbacChurn => ProfileDefaults {
            connections: 4,
            keyspace: 10_000,
            value_size: 64,
            seed_keys: 0,
        },
        WorkloadKind::QuorumWriteCost => ProfileDefaults {
            connections: 32,
            keyspace: 10_000,
            value_size: 256,
            seed_keys: 0,
        },
        WorkloadKind::Get | WorkloadKind::Set | WorkloadKind::Mixed | WorkloadKind::Ping => {
            ProfileDefaults {
                connections: 32,
                keyspace: 10_000,
                value_size: 256,
                seed_keys: 2_048,
            }
        }
    }
}

fn run_config(args: &RunArgs) -> RunConfig {
    RunConfig {
        profile_addr: args.addr.clone(),
        connection: ConnectionConfig {
            addr: args.addr.clone(),
            host_for_tls: "localhost".to_string(),
            username: args.username.clone(),
            password: args.password.clone(),
            tls: TlsConfig {
                enabled: args.tls.tls,
                ca_cert: args.tls.tls_ca_cert.clone(),
                client_cert: args.tls.tls_client_cert.clone(),
                client_key: args.tls.tls_client_key.clone(),
            },
        },
        connections: args.connections,
        duration_seconds: args.duration_seconds,
        keyspace: args.keyspace,
        value_size: args.value_size,
        seed_keys: args.seed_keys,
        workload: args.workload,
    }
}

#[derive(Clone)]
struct RunConfig {
    profile_addr: String,
    connection: ConnectionConfig,
    connections: usize,
    duration_seconds: u64,
    keyspace: usize,
    value_size: usize,
    seed_keys: usize,
    workload: WorkloadKind,
}

async fn run_profile(profile: String, args: RunConfig) -> Result<BenchmarkReport> {
    if args.connections == 0 {
        return Err(BenchError::InvalidConfiguration(
            "connections must be greater than zero".to_string(),
        ));
    }
    if args.keyspace == 0 {
        return Err(BenchError::InvalidConfiguration(
            "keyspace must be greater than zero".to_string(),
        ));
    }
    if args.duration_seconds == 0 {
        return Err(BenchError::InvalidConfiguration(
            "duration-seconds must be greater than zero".to_string(),
        ));
    }

    let seed_client = connect_with_retry(&args.connection, Duration::from_secs(10)).await?;
    seed_client
        .wait_until_ready(Duration::from_secs(10))
        .await?;
    seed_dataset(&seed_client, args.seed_keys, args.value_size).await?;

    let mut clients = Vec::with_capacity(args.connections);
    for _ in 0..args.connections {
        clients.push(connect_with_retry(&args.connection, Duration::from_secs(10)).await?);
    }

    let deadline = Instant::now() + Duration::from_secs(args.duration_seconds);
    let mut tasks = Vec::with_capacity(args.connections);
    for (worker_id, client) in clients.into_iter().enumerate() {
        let workload = args.workload;
        let keyspace = args.keyspace;
        let value_size = args.value_size;
        tasks.push(tokio::spawn(async move {
            run_worker(worker_id, client, workload, deadline, keyspace, value_size).await
        }));
    }

    let mut completed_operations = 0u64;
    let mut failed_operations = 0u64;
    let mut histogram = Histogram::<u64>::new(3).expect("histogram");
    for task in tasks {
        let WorkerReport {
            completed,
            failed,
            histogram: worker_histogram,
        } = task.await??;
        completed_operations += completed;
        failed_operations += failed;
        histogram
            .add(worker_histogram)
            .expect("merge worker histogram");
    }

    let latency_us = LatencySummary {
        min: histogram.min(),
        p50: histogram.value_at_quantile(0.50),
        p95: histogram.value_at_quantile(0.95),
        p99: histogram.value_at_quantile(0.99),
        max: histogram.max(),
        mean: histogram.mean(),
    };

    Ok(BenchmarkReport {
        profile,
        addr: args.profile_addr,
        connections: args.connections,
        duration_seconds: args.duration_seconds,
        keyspace: args.keyspace,
        value_size: args.value_size,
        seed_keys: args.seed_keys,
        completed_operations,
        failed_operations,
        operations_per_second: completed_operations as f64 / args.duration_seconds as f64,
        latency_us,
    })
}

async fn connect_with_retry(
    config: &ConnectionConfig,
    timeout: Duration,
) -> Result<BenchmarkClient> {
    let started = tokio::time::Instant::now();
    loop {
        match BenchmarkClient::connect(config).await {
            Ok(client) => return Ok(client),
            Err(err) if started.elapsed() < timeout => {
                let _ = err;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn select_writable_addr(cluster: &ManagedCluster) -> Result<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        for addr in &cluster.candidate_addrs {
            let mut config = cluster.connection.clone();
            config.addr = addr.clone();
            if let Ok(client) = BenchmarkClient::connect(&config).await
                && let Ok(response) = client
                    .send(Command::Set {
                        key: "__bench-leader-probe__".to_string(),
                        value: "1".to_string(),
                        options: Default::default(),
                    })
                    .await
                && response.status == transport::Status::Ok
            {
                return Ok(addr.clone());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(BenchError::InvalidConfiguration(
                "could not identify a writable quorum leader".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn seed_dataset(client: &BenchmarkClient, seed_keys: usize, value_size: usize) -> Result<()> {
    let value = "x".repeat(value_size);
    for idx in 0..seed_keys {
        let response = client
            .send(Command::Set {
                key: format!("seed-{idx:06}"),
                value: value.clone(),
                options: Default::default(),
            })
            .await?;
        if response.status != transport::Status::Ok {
            return Err(BenchError::InvalidConfiguration(format!(
                "failed to seed benchmark key {idx}"
            )));
        }
    }
    Ok(())
}

struct WorkerReport {
    completed: u64,
    failed: u64,
    histogram: Histogram<u64>,
}

async fn run_worker(
    worker_id: usize,
    client: BenchmarkClient,
    workload: WorkloadKind,
    deadline: Instant,
    keyspace: usize,
    value_size: usize,
) -> Result<WorkerReport> {
    let mut rng = SmallRng::seed_from_u64(0x5eed_u64 + worker_id as u64);
    let mut completed = 0u64;
    let mut failed = 0u64;
    let mut operation_index = 0u64;
    let mut histogram = Histogram::<u64>::new(3).expect("histogram");
    let value = "v".repeat(value_size);

    while Instant::now() < deadline {
        let started = Instant::now();
        let response = run_operation(
            worker_id,
            operation_index,
            &client,
            &mut rng,
            workload,
            keyspace,
            &value,
        )
        .await;
        let latency_us = started.elapsed().as_micros() as u64;
        histogram.record(latency_us.max(1)).expect("record latency");
        match response {
            Ok(()) => completed += 1,
            Err(_) => failed += 1,
        }
        operation_index += 1;
    }

    Ok(WorkerReport {
        completed,
        failed,
        histogram,
    })
}

async fn run_operation(
    worker_id: usize,
    operation_index: u64,
    client: &BenchmarkClient,
    rng: &mut SmallRng,
    workload: WorkloadKind,
    keyspace: usize,
    value: &str,
) -> Result<()> {
    match workload {
        WorkloadKind::TransactionFlow => {
            run_transaction_flow(worker_id, operation_index, client, rng, keyspace, value).await
        }
        WorkloadKind::BackupRestore => {
            run_backup_restore(worker_id, operation_index, client, value).await
        }
        WorkloadKind::AuthRbacChurn => {
            run_auth_rbac_churn(worker_id, operation_index, client).await
        }
        WorkloadKind::QuorumWriteCost => {
            let command = next_command(rng, WorkloadKind::Set, keyspace, value);
            send_ok(client, command).await
        }
        WorkloadKind::Get | WorkloadKind::Set | WorkloadKind::Mixed | WorkloadKind::Ping => {
            let command = next_command(rng, workload, keyspace, value);
            send_ok(client, command).await
        }
    }
}

async fn run_transaction_flow(
    worker_id: usize,
    operation_index: u64,
    client: &BenchmarkClient,
    rng: &mut SmallRng,
    keyspace: usize,
    value: &str,
) -> Result<()> {
    send_ok(client, Command::Multi).await?;
    for item in 0..4u8 {
        let key_index = rng.random_range(0..keyspace);
        send_ok(
            client,
            Command::Set {
                key: format!("tx:{worker_id}:{key_index:06}:{operation_index}:{item}"),
                value: value.to_string(),
                options: Default::default(),
            },
        )
        .await?;
    }
    send_ok(client, Command::Exec).await
}

async fn run_backup_restore(
    worker_id: usize,
    operation_index: u64,
    client: &BenchmarkClient,
    value: &str,
) -> Result<()> {
    send_ok(
        client,
        Command::Set {
            key: format!("backup:{worker_id}:stable"),
            value: format!("{value}:{operation_index}"),
            options: Default::default(),
        },
    )
    .await?;
    let dump = send_value(client, Command::Backup).await?;
    send_ok(client, Command::RestoreCheck { dump: dump.clone() }).await?;
    send_ok(client, Command::Restore { dump }).await
}

async fn run_auth_rbac_churn(
    worker_id: usize,
    operation_index: u64,
    client: &BenchmarkClient,
) -> Result<()> {
    let suffix = format!("{worker_id}-{operation_index}");
    let username = format!("bench-user-{suffix}");
    let role = format!("bench-role-{suffix}");
    send_ok(
        client,
        Command::CreateUser {
            username: username.clone(),
            password: "password1234".to_string(),
        },
    )
    .await?;
    send_ok(client, Command::CreateRole { role: role.clone() }).await?;
    send_ok(
        client,
        Command::GrantPermission {
            permission: "read".to_string(),
            pattern: format!("bench:{worker_id}:*"),
            role: role.clone(),
        },
    )
    .await?;
    send_ok(
        client,
        Command::GrantRole {
            role: role.clone(),
            username: username.clone(),
        },
    )
    .await?;
    send_ok(
        client,
        Command::ShowGrantsForUser {
            username: username.clone(),
        },
    )
    .await?;
    send_ok(
        client,
        Command::RevokeRole {
            role: role.clone(),
            username: username.clone(),
        },
    )
    .await?;
    send_ok(
        client,
        Command::RevokePermission {
            permission: "read".to_string(),
            pattern: format!("bench:{worker_id}:*"),
            role: role.clone(),
        },
    )
    .await?;
    send_ok(client, Command::DropRole { role }).await?;
    send_ok(client, Command::DropUser { username }).await
}

async fn send_ok(client: &BenchmarkClient, command: Command) -> Result<()> {
    let response = client.send(command).await?;
    if response.status == transport::Status::Ok {
        return Ok(());
    }
    Err(BenchError::InvalidConfiguration(
        response
            .decode_error_message()
            .unwrap_or_else(|_| "benchmark command returned non-OK status".to_string()),
    ))
}

async fn send_value(client: &BenchmarkClient, command: Command) -> Result<String> {
    let response = client.send(command).await?;
    if response.status != transport::Status::Ok {
        return Err(BenchError::InvalidConfiguration(
            response
                .decode_error_message()
                .unwrap_or_else(|_| "benchmark command returned non-OK status".to_string()),
        ));
    }
    Ok(response.decode_value()?)
}

fn next_command(
    rng: &mut SmallRng,
    workload: WorkloadKind,
    keyspace: usize,
    value: &str,
) -> Command {
    let index = rng.random_range(0..keyspace);
    let key = format!("bench-{index:06}");
    match workload {
        WorkloadKind::Get => Command::Get { key },
        WorkloadKind::Set => Command::Set {
            key,
            value: value.to_string(),
            options: Default::default(),
        },
        WorkloadKind::Ping => Command::Ping { message: None },
        WorkloadKind::Mixed => {
            if rng.random_ratio(7, 10) {
                Command::Get { key }
            } else {
                Command::Set {
                    key,
                    value: value.to_string(),
                    options: Default::default(),
                }
            }
        }
        WorkloadKind::TransactionFlow
        | WorkloadKind::BackupRestore
        | WorkloadKind::AuthRbacChurn
        | WorkloadKind::QuorumWriteCost => {
            unreachable!("profile workloads are executed by run_operation")
        }
    }
}
