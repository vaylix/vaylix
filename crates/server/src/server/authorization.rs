use command::Command;

use super::SessionState;
use crate::auth::Permission;
use crate::error::{Result, ServerError};

pub(super) fn is_allowed_during_maintenance(command: &Command) -> bool {
    matches!(
        command,
        Command::Ping { .. }
            | Command::Get { .. }
            | Command::Exists { .. }
            | Command::GetEx { .. }
            | Command::MGet { .. }
            | Command::Ttl { .. }
            | Command::Scan { .. }
            | Command::DbSize
            | Command::Count
            | Command::List
            | Command::Info
            | Command::Metrics
            | Command::MetricsProm
            | Command::Backup
            | Command::BackupVerify { .. }
            | Command::BackupVerifyFrom { .. }
            | Command::ShowUsers
            | Command::ShowRoles
            | Command::ShowGrants
            | Command::ShowGrantsForUser { .. }
            | Command::ShowGrantsForRole { .. }
            | Command::WhoAmI
            | Command::MaintenanceStatus
            | Command::MaintenanceOff
            | Command::Health
            | Command::ShowReplication
    )
}

pub(super) fn authorize_command(command: &Command, session: &SessionState) -> Result<()> {
    let Some(permission) = command_permission(command) else {
        return Ok(());
    };
    let Some(identity) = &session.identity else {
        return Err(ServerError::AuthenticationRequired);
    };
    let keys = command_keys(command);
    if keys.is_empty() {
        if let Some(pattern) = command_pattern(command) {
            if identity.allows_pattern(permission, pattern) {
                return Ok(());
            }
        } else if identity.has(permission) {
            return Ok(());
        }
    } else if keys.iter().all(|key| identity.allows_key(permission, key)) {
        return Ok(());
    }
    Err(ServerError::PermissionDenied)
}

fn command_permission(command: &Command) -> Option<Permission> {
    match command {
        Command::Ping { .. }
        | Command::Auth { .. }
        | Command::Multi
        | Command::Exec
        | Command::Discard
        | Command::MaintenanceStatus
        | Command::Health
        | Command::WhoAmI => None,
        Command::Get { .. }
        | Command::Exists { .. }
        | Command::MGet { .. }
        | Command::Ttl { .. }
        | Command::Scan { .. }
        | Command::DbSize
        | Command::Count
        | Command::List => Some(Permission::Read),
        Command::Clear => Some(Permission::Clear),
        Command::GetDel { .. }
        | Command::GetEx { .. }
        | Command::Set { .. }
        | Command::SetNx { .. }
        | Command::MSet { .. }
        | Command::Delete { .. }
        | Command::Incr { .. }
        | Command::Decr { .. }
        | Command::Expire { .. }
        | Command::Persist { .. }
        | Command::Rename { .. }
        | Command::RenameNx { .. } => Some(Permission::Write),
        Command::Info | Command::Metrics | Command::MetricsProm => Some(Permission::Metrics),
        Command::Save | Command::Snapshot => Some(Permission::Snapshot),
        Command::Backup
        | Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. } => Some(Permission::Backup),
        Command::Restore { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. } => Some(Permission::Restore),
        Command::CreateUser { .. }
        | Command::AlterUserPassword { .. }
        | Command::DropUser { .. } => Some(Permission::UserAdmin),
        Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowRoles => Some(Permission::RoleAdmin),
        Command::ShowGrants => None,
        Command::ShowGrantsForUser { .. } => Some(Permission::UserAdmin),
        Command::ShowGrantsForRole { .. } => Some(Permission::RoleAdmin),
        Command::ShowUsers => Some(Permission::UserAdmin),
        Command::MaintenanceOn | Command::MaintenanceOff => Some(Permission::Admin),
        Command::ShowReplication
        | Command::ShowCluster
        | Command::PromoteFollower
        | Command::PauseReplication
        | Command::ResumeReplication
        | Command::ClusterJoin { .. }
        | Command::ClusterRemove { .. } => Some(Permission::Admin),
        Command::Help | Command::Exit => None,
    }
}

fn command_keys(command: &Command) -> Vec<&str> {
    match command {
        Command::Get { key }
        | Command::GetDel { key }
        | Command::GetEx { key, .. }
        | Command::Set { key, .. }
        | Command::SetNx { key, .. }
        | Command::Exists { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::Expire { key, .. }
        | Command::Ttl { key }
        | Command::Persist { key } => vec![key.as_str()],
        Command::MGet { keys } | Command::Delete { keys } => {
            keys.iter().map(String::as_str).collect()
        }
        Command::MSet { entries } => entries.iter().map(|(key, _)| key.as_str()).collect(),
        Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        } => vec![source.as_str(), destination.as_str()],
        _ => Vec::new(),
    }
}

fn command_pattern(command: &Command) -> Option<&str> {
    match command {
        Command::Scan { pattern, .. } => Some(pattern.as_deref().unwrap_or("*")),
        _ => None,
    }
}
