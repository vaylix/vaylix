use std::collections::BTreeMap;
use std::net::SocketAddr;

use command::Command;
use transport::Status;
use uuid::Uuid;

use super::{AuditContext, ServerRuntimeConfig};
use crate::audit::{AuditEvent, AuditLogger};

pub(crate) fn log_event(level: &str, component: &str, message: &str) {
    println!("[{level}] [{component}] {message}");
}

pub(super) fn log_connection_event(
    level: &str,
    connection_id: u64,
    peer_addr: Option<SocketAddr>,
    message: &str,
) {
    match peer_addr {
        Some(peer_addr) => log_event(
            level,
            "server.connection",
            &format!("connection_id={connection_id} peer={peer_addr} {message}"),
        ),
        None => log_event(
            level,
            "server.connection",
            &format!("connection_id={connection_id} peer=unknown {message}"),
        ),
    }
}

pub(super) fn auth_lockout_key(username: &str, peer_addr: Option<SocketAddr>) -> String {
    format!(
        "{username}|{}",
        peer_addr
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    )
}

pub(super) fn auth_lockout_username_key(username: &str) -> String {
    format!("{username}|*")
}

pub(super) fn opcode_name(command: &Command) -> &'static str {
    match command {
        Command::Auth { .. } => "AUTH",
        Command::Ping { .. } => "PING",
        Command::Get { .. } => "GET",
        Command::GetDel { .. } => "GETDEL",
        Command::GetEx { .. } => "GETEX",
        Command::Set { .. } => "SET",
        Command::SetNx { .. } => "SETNX",
        Command::MGet { .. } => "MGET",
        Command::MSet { .. } => "MSET",
        Command::Delete { .. } => "DEL",
        Command::Exists { .. } => "EXISTS",
        Command::Incr { .. } => "INCR",
        Command::Decr { .. } => "DECR",
        Command::Expire { .. } => "EXPIRE",
        Command::Ttl { .. } => "TTL",
        Command::Persist { .. } => "PERSIST",
        Command::Rename { .. } => "RENAME",
        Command::RenameNx { .. } => "RENAMENX",
        Command::Scan { .. } => "SCAN",
        Command::DbSize => "DBSIZE",
        Command::Info => "INFO",
        Command::Metrics => "METRICS",
        Command::MetricsProm => "METRICS_PROM",
        Command::List => "LIST",
        Command::Clear => "CLEAR",
        Command::Count => "COUNT",
        Command::Save => "SAVE",
        Command::Snapshot => "SNAPSHOT",
        Command::Backup => "BACKUP",
        Command::BackupTo { .. } => "BACKUP_TO",
        Command::BackupVerify { .. } => "BACKUP_VERIFY",
        Command::BackupVerifyFrom { .. } => "BACKUP_VERIFY_FROM",
        Command::Restore { .. } => "RESTORE",
        Command::RestoreFrom { .. } => "RESTORE_FROM",
        Command::RestoreCheck { .. } => "RESTORE_CHECK",
        Command::RestoreCheckFrom { .. } => "RESTORE_CHECK_FROM",
        Command::AlterUserPassword { .. } => "ALTER_USER_PASSWORD",
        Command::CreateUser { .. } => "CREATE_USER",
        Command::DropUser { .. } => "DROP_USER",
        Command::CreateRole { .. } => "CREATE_ROLE",
        Command::DropRole { .. } => "DROP_ROLE",
        Command::GrantRole { .. } => "GRANT_ROLE",
        Command::RevokeRole { .. } => "REVOKE_ROLE",
        Command::GrantPermission { .. } => "GRANT_PERMISSION",
        Command::RevokePermission { .. } => "REVOKE_PERMISSION",
        Command::ShowUsers => "SHOW_USERS",
        Command::ShowRoles => "SHOW_ROLES",
        Command::ShowGrants => "SHOW_GRANTS",
        Command::ShowGrantsForUser { .. } => "SHOW_GRANTS_FOR_USER",
        Command::ShowGrantsForRole { .. } => "SHOW_GRANTS_FOR_ROLE",
        Command::WhoAmI => "WHOAMI",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::MaintenanceOn => "MAINTENANCE_ON",
        Command::MaintenanceOff => "MAINTENANCE_OFF",
        Command::MaintenanceStatus => "MAINTENANCE_STATUS",
        Command::Health => "HEALTH",
        Command::ShowCluster => "SHOW_CLUSTER",
        Command::ClusterJoin { .. } => "CLUSTER_JOIN",
        Command::ClusterRemove { .. } => "CLUSTER_REMOVE",
        Command::ShowReplication => "SHOW_REPLICATION",
        Command::PromoteFollower => "PROMOTE_FOLLOWER",
        Command::PauseReplication => "PAUSE_REPLICATION",
        Command::ResumeReplication => "RESUME_REPLICATION",
        Command::Help => "HELP",
        Command::Exit => "EXIT",
    }
}

pub(super) fn record_audit_event(logger: &AuditLogger, context: AuditContext<'_>) {
    record_audit_event_with(logger, context, "command", BTreeMap::new());
}

pub(super) fn record_command_audit_event(runtime: &ServerRuntimeConfig, context: AuditContext<'_>) {
    if runtime.audit_commands {
        record_audit_event(&runtime.audit_logger, context);
    }
}

pub(super) fn record_runtime_event(
    logger: &AuditLogger,
    event_type: &str,
    details: BTreeMap<String, String>,
) {
    let _ = logger.record(&AuditEvent {
        timestamp_ms: current_time_millis(),
        connection_id: 0,
        peer: None,
        username: None,
        request_id: Uuid::nil().to_string(),
        opcode: "RUNTIME".to_string(),
        status: "ok".to_string(),
        error_code: None,
        latency_ms: 0,
        event_type: event_type.to_string(),
        details,
    });
}

pub(super) fn record_audit_event_with(
    logger: &AuditLogger,
    context: AuditContext<'_>,
    event_type: &str,
    details: BTreeMap<String, String>,
) {
    let _ = logger.record(&AuditEvent {
        timestamp_ms: current_time_millis(),
        connection_id: context.connection_id,
        peer: context.peer_addr.map(|addr| addr.to_string()),
        username: context
            .session
            .identity
            .as_ref()
            .map(|identity| identity.username.clone()),
        request_id: context.request_id.to_string(),
        opcode: context.opcode.to_string(),
        status: match context.status {
            Status::Ok => "ok".to_string(),
            Status::Error => "error".to_string(),
            Status::NotFound => "not_found".to_string(),
        },
        error_code: context.error_code,
        latency_ms: context.latency_ms.min(u128::from(u64::MAX)) as u64,
        event_type: event_type.to_string(),
        details,
    });
}

pub(super) fn record_semantic_audit_event(
    logger: &AuditLogger,
    context: AuditContext<'_>,
    command: &Command,
) {
    let Some((event_type, mut details)) = semantic_audit_details(command) else {
        return;
    };
    details.insert("result".to_string(), audit_status(context.status));
    record_audit_event_with(logger, context, event_type, details);
}

pub(super) fn record_slow_command_event(
    logger: &AuditLogger,
    runtime: &ServerRuntimeConfig,
    context: AuditContext<'_>,
) {
    let Some(threshold) = runtime.slow_command_threshold else {
        return;
    };
    if context.latency_ms < threshold.as_millis() {
        return;
    }
    let mut details = BTreeMap::new();
    details.insert("opcode".to_string(), context.opcode.to_string());
    details.insert("latency_ms".to_string(), context.latency_ms.to_string());
    details.insert(
        "threshold_ms".to_string(),
        threshold.as_millis().to_string(),
    );
    record_audit_event_with(logger, context, "slow_command", details);
}

fn semantic_audit_details(command: &Command) -> Option<(&'static str, BTreeMap<String, String>)> {
    let mut details = BTreeMap::new();
    let event_type = match command {
        Command::Auth { username, .. } => {
            details.insert("username".to_string(), username.clone());
            "auth"
        }
        Command::CreateUser { username, .. } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_create_user"
        }
        Command::AlterUserPassword { username, .. } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_alter_user_password"
        }
        Command::DropUser { username } => {
            details.insert("target_user".to_string(), username.clone());
            "rbac_drop_user"
        }
        Command::CreateRole { role } => {
            details.insert("role".to_string(), role.clone());
            "rbac_create_role"
        }
        Command::DropRole { role } => {
            details.insert("role".to_string(), role.clone());
            "rbac_drop_role"
        }
        Command::GrantRole { role, username } => {
            details.insert("role".to_string(), role.clone());
            details.insert("target_user".to_string(), username.clone());
            "rbac_grant_role"
        }
        Command::RevokeRole { role, username } => {
            details.insert("role".to_string(), role.clone());
            details.insert("target_user".to_string(), username.clone());
            "rbac_revoke_role"
        }
        Command::GrantPermission {
            permission,
            pattern,
            role,
        } => {
            details.insert("permission".to_string(), permission.clone());
            details.insert("pattern".to_string(), pattern.clone());
            details.insert("role".to_string(), role.clone());
            "rbac_grant_permission"
        }
        Command::RevokePermission {
            permission,
            pattern,
            role,
        } => {
            details.insert("permission".to_string(), permission.clone());
            details.insert("pattern".to_string(), pattern.clone());
            details.insert("role".to_string(), role.clone());
            "rbac_revoke_permission"
        }
        _ => return None,
    };
    Some((event_type, details))
}

fn audit_status(status: Status) -> String {
    match status {
        Status::Ok => "ok",
        Status::Error => "error",
        Status::NotFound => "not_found",
    }
    .to_string()
}

pub(super) fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}
