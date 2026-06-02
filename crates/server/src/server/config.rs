use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use transport::CodecOptions;

use crate::audit::AuditLogger;
use crate::auth::AuthConfig;
use crate::replication::ReplicationRuntime;
use crate::runtime_state::{AuthLockoutState, MaintenanceMode};

/// Runtime guardrails for request validation, quotas, and abuse controls.
#[derive(Debug, Clone)]
pub struct ServerGuards {
    pub max_request_payload_bytes: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_keys_per_batch: usize,
    pub max_transaction_queue_len: usize,
    pub requests_per_second: u32,
    pub request_burst: u32,
}

/// Runtime configuration for the async server.
#[derive(Clone)]
pub struct ServerRuntimeConfig {
    pub snapshot_interval: Option<Duration>,
    pub expiration_sweep_interval: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub auth_config: Option<AuthConfig>,
    pub guards: ServerGuards,
    pub tls_state: Option<Arc<crate::tls::TlsState>>,
    pub transport: CodecOptions,
    pub log_requests: bool,
    pub audit_logger: Arc<AuditLogger>,
    pub backup_dir: PathBuf,
    pub mtls_enabled: bool,
    pub slow_command_threshold: Option<Duration>,
    pub wal_segment_size_bytes: u64,
    pub wal_retain_segments: usize,
    pub auth_failure_window: Duration,
    pub auth_failure_limit: u32,
    pub auth_lockout: Duration,
    pub transaction_max_duration: Duration,
    pub maintenance: Arc<MaintenanceMode>,
    pub auth_lockouts: Arc<Mutex<AuthLockoutState>>,
    pub insecure_auth_disabled: bool,
    pub insecure_default_credentials: bool,
    pub replication: Arc<ReplicationRuntime>,
    pub replication_fanout_lock: Arc<Mutex<()>>,
    pub replication_apply_lock: Arc<Mutex<()>>,
}
