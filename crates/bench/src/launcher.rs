use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use tokio::process::{Child, Command};

use crate::args::ManagedArgs;
use crate::client::{ConnectionConfig, TlsConfig};
use crate::error::{BenchError, Result};

pub struct ManagedCluster {
    children: Vec<Child>,
    pub addr: String,
    pub candidate_addrs: Vec<String>,
    pub connection: ConnectionConfig,
    _workdir: PathBuf,
}

impl ManagedCluster {
    pub async fn single_node(args: &ManagedArgs) -> Result<Self> {
        let workdir = managed_root(args.workdir.as_deref(), "single-node")?;
        let port = free_port()?;
        let tls = if args.tls {
            Some(generate_example_certs(&workdir.join("certs"), args.mtls)?)
        } else {
            None
        };
        let node_root = workdir.join("node-1");
        let child = spawn_server(SpawnServerConfig {
            server_bin: &args.server_bin,
            root: &node_root,
            node_id: "node-1",
            port,
            username: &args.username,
            password: &args.password,
            tls: tls.as_ref(),
            peers: &[],
            role: "standalone",
            wal_sync: args.wal_sync.as_cli_value(),
            write_ack_mode: args.write_ack_mode.as_cli_value(),
        })
        .await?;
        let addr = format!("127.0.0.1:{port}");
        Ok(Self {
            children: vec![child],
            addr: addr.clone(),
            candidate_addrs: vec![addr.clone()],
            connection: ConnectionConfig {
                addr,
                host_for_tls: "localhost".to_string(),
                username: Some(args.username.clone()),
                password: Some(args.password.clone()),
                tls: tls
                    .as_ref()
                    .map(|certs| TlsConfig {
                        enabled: true,
                        ca_cert: Some(certs.ca_cert.clone()),
                        client_cert: certs.client_cert.clone(),
                        client_key: certs.client_key.clone(),
                    })
                    .unwrap_or_default(),
            },
            _workdir: workdir,
        })
    }

    pub async fn quorum(args: &ManagedArgs) -> Result<Self> {
        let workdir = managed_root(args.workdir.as_deref(), "quorum")?;
        let tls = if args.tls {
            Some(generate_example_certs(&workdir.join("certs"), args.mtls)?)
        } else {
            None
        };
        let ports = [free_port()?, free_port()?, free_port()?];
        let peers = vec![
            format!("node-1@127.0.0.1:{}", ports[0]),
            format!("node-2@127.0.0.1:{}", ports[1]),
            format!("node-3@127.0.0.1:{}", ports[2]),
        ];

        let mut children = Vec::with_capacity(3);
        let node_1_root = workdir.join("node-1");
        let node_2_root = workdir.join("node-2");
        let node_3_root = workdir.join("node-3");
        children.push(
            spawn_server(SpawnServerConfig {
                server_bin: &args.server_bin,
                root: &node_1_root,
                node_id: "node-1",
                port: ports[0],
                username: &args.username,
                password: &args.password,
                tls: tls.as_ref(),
                peers: &peers,
                role: "leader",
                wal_sync: args.wal_sync.as_cli_value(),
                write_ack_mode: args.write_ack_mode.as_cli_value(),
            })
            .await?,
        );
        children.push(
            spawn_server(SpawnServerConfig {
                server_bin: &args.server_bin,
                root: &node_2_root,
                node_id: "node-2",
                port: ports[1],
                username: &args.username,
                password: &args.password,
                tls: tls.as_ref(),
                peers: &peers,
                role: "follower",
                wal_sync: args.wal_sync.as_cli_value(),
                write_ack_mode: args.write_ack_mode.as_cli_value(),
            })
            .await?,
        );
        children.push(
            spawn_server(SpawnServerConfig {
                server_bin: &args.server_bin,
                root: &node_3_root,
                node_id: "node-3",
                port: ports[2],
                username: &args.username,
                password: &args.password,
                tls: tls.as_ref(),
                peers: &peers,
                role: "follower",
                wal_sync: args.wal_sync.as_cli_value(),
                write_ack_mode: args.write_ack_mode.as_cli_value(),
            })
            .await?,
        );

        let addr = format!("127.0.0.1:{}", ports[0]);
        let candidate_addrs = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect::<Vec<_>>();
        Ok(Self {
            children,
            addr: addr.clone(),
            candidate_addrs,
            connection: ConnectionConfig {
                addr,
                host_for_tls: "localhost".to_string(),
                username: Some(args.username.clone()),
                password: Some(args.password.clone()),
                tls: tls
                    .as_ref()
                    .map(|certs| TlsConfig {
                        enabled: true,
                        ca_cert: Some(certs.ca_cert.clone()),
                        client_cert: certs.client_cert.clone(),
                        client_key: certs.client_key.clone(),
                    })
                    .unwrap_or_default(),
            },
            _workdir: workdir,
        })
    }
}

impl Drop for ManagedCluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.start_kill();
        }
    }
}

pub fn write_example_certs(out_dir: &Path) -> Result<()> {
    generate_example_certs(out_dir, true)?;
    Ok(())
}

struct GeneratedCerts {
    ca_cert: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
}

fn generate_example_certs(out_dir: &Path, mtls: bool) -> Result<GeneratedCerts> {
    fs::create_dir_all(out_dir)?;

    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let mut ca_dn = DistinguishedName::new();
    ca_dn.push(DnType::CommonName, "Vaylix Bench CA");
    ca_params.distinguished_name = ca_dn;
    let ca_issuer = CertifiedIssuer::self_signed(ca_params, ca_key)?;

    let server_key = KeyPair::generate()?;
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let mut server_dn = DistinguishedName::new();
    server_dn.push(DnType::CommonName, "localhost");
    server_params.distinguished_name = server_dn;
    let server_cert = server_params.signed_by(&server_key, &ca_issuer)?;

    let ca_pem = ca_issuer.pem();
    let server_cert_pem = server_cert.pem();
    let server_key_pem = server_key.serialize_pem();
    let ca_cert_path = out_dir.join("ca.crt");
    let server_cert_path = out_dir.join("server.crt");
    let server_key_path = out_dir.join("server.key");
    fs::write(&ca_cert_path, ca_pem)?;
    fs::write(&server_cert_path, server_cert_pem)?;
    fs::write(&server_key_path, server_key_pem)?;

    let (client_cert_path, client_key_path) = if mtls {
        let client_key = KeyPair::generate()?;
        let mut client_params = CertificateParams::new(vec!["vaylix-bench-client".to_string()])?;
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let mut client_dn = DistinguishedName::new();
        client_dn.push(DnType::CommonName, "vaylix-bench-client");
        client_params.distinguished_name = client_dn;
        let client_cert = client_params.signed_by(&client_key, &ca_issuer)?;
        let client_cert_path = out_dir.join("client.crt");
        let client_key_path = out_dir.join("client.key");
        fs::write(&client_cert_path, client_cert.pem())?;
        fs::write(&client_key_path, client_key.serialize_pem())?;
        (Some(client_cert_path), Some(client_key_path))
    } else {
        (None, None)
    };

    Ok(GeneratedCerts {
        ca_cert: ca_cert_path,
        server_cert: server_cert_path,
        server_key: server_key_path,
        client_cert: client_cert_path,
        client_key: client_key_path,
    })
}

struct SpawnServerConfig<'a> {
    server_bin: &'a Path,
    root: &'a Path,
    node_id: &'a str,
    port: u16,
    username: &'a str,
    password: &'a str,
    tls: Option<&'a GeneratedCerts>,
    peers: &'a [String],
    role: &'a str,
    wal_sync: &'a str,
    write_ack_mode: &'a str,
}

async fn spawn_server(config: SpawnServerConfig<'_>) -> Result<Child> {
    fs::create_dir_all(config.root)?;
    let stdout = fs::File::create(config.root.join("stdout.log"))?;
    let stderr = fs::File::create(config.root.join("stderr.log"))?;
    let mut command = Command::new(config.server_bin);
    command
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(config.port.to_string())
        .arg("--data-dir")
        .arg(config.root)
        .arg("--user")
        .arg(config.username)
        .arg("--password")
        .arg(config.password)
        .arg("--requests-per-second")
        .arg("1000000")
        .arg("--request-burst")
        .arg("1000000")
        .arg("--slow-command-threshold-ms")
        .arg("0")
        .arg("--node-id")
        .arg(config.node_id)
        .arg("--replication-role")
        .arg(config.role)
        .arg("--replication-advertise-addr")
        .arg(format!("127.0.0.1:{}", config.port))
        .arg("--wal-sync")
        .arg(config.wal_sync)
        .arg("--write-ack-mode")
        .arg(config.write_ack_mode)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if !config.peers.is_empty() {
        command.arg("--cluster-peers").arg(config.peers.join(","));
    }
    if let Some(tls) = config.tls {
        command
            .arg("--ssl")
            .arg("--tls-cert")
            .arg(&tls.server_cert)
            .arg("--tls-key")
            .arg(&tls.server_key);
        if tls.client_cert.is_some() {
            command.arg("--tls-client-ca").arg(&tls.ca_cert);
        }
    }
    command.spawn().map_err(BenchError::from)
}

fn managed_root(base: Option<&Path>, label: &str) -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BenchError::InvalidConfiguration("system clock before epoch".to_string()))?
        .as_nanos();
    let root = match base {
        Some(base) => base.join(format!("vaylix-bench-{label}-{unique}")),
        None => std::env::temp_dir().join(format!("vaylix-bench-{label}-{unique}")),
    };
    fs::create_dir_all(&root)?;
    Ok(root)
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
