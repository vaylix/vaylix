use command::Command;
use transport::Request;

use super::ServerGuards;
use crate::error::{Result, ServerError};

pub(super) fn is_internal_replication_opcode(opcode: transport::Opcode) -> bool {
    matches!(
        opcode,
        transport::Opcode::ReplicationStatus
            | transport::Opcode::ReplicationSnapshot
            | transport::Opcode::ReplicationFetch
            | transport::Opcode::ReplicationAck
            | transport::Opcode::ReplicationAppend
            | transport::Opcode::ReplicationInstallSnapshot
            | transport::Opcode::ReplicationVote
            | transport::Opcode::ReplicationHeartbeat
    )
}

pub(super) fn validate_request(request: &Request, guards: &ServerGuards) -> Result<()> {
    if request.payload.len() > guards.max_request_payload_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_key(key: &str, guards: &ServerGuards) -> Result<()> {
    if key.len() > guards.max_key_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_value(value: &str, guards: &ServerGuards) -> Result<()> {
    if value.len() > guards.max_value_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

fn validate_value_bytes(value: &[u8], guards: &ServerGuards) -> Result<()> {
    if value.len() > guards.max_value_bytes {
        return Err(ServerError::QuotaExceeded);
    }
    Ok(())
}

pub(super) fn validate_command(command: &Command, guards: &ServerGuards) -> Result<()> {
    match command {
        Command::Auth { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::Get { key }
        | Command::GetDel { key }
        | Command::Exists { key }
        | Command::Incr { key }
        | Command::Decr { key }
        | Command::Ttl { key }
        | Command::Persist { key } => validate_key(key, guards)?,
        Command::GetEx { key, .. } => validate_key(key, guards)?,
        Command::Set { key, value, .. } | Command::SetNx { key, value } => {
            validate_key(key, guards)?;
            validate_value_bytes(value, guards)?;
        }
        Command::MGet { keys } | Command::Delete { keys } => {
            if keys.len() > guards.max_keys_per_batch {
                return Err(ServerError::QuotaExceeded);
            }
            for key in keys {
                validate_key(key, guards)?;
            }
        }
        Command::MSet { entries } => {
            if entries.len() > guards.max_keys_per_batch {
                return Err(ServerError::QuotaExceeded);
            }
            for (key, value) in entries {
                validate_key(key, guards)?;
                validate_value_bytes(value, guards)?;
            }
        }
        Command::Expire { key, .. } => validate_key(key, guards)?,
        Command::Rename {
            source,
            destination,
        }
        | Command::RenameNx {
            source,
            destination,
        } => {
            validate_key(source, guards)?;
            validate_key(destination, guards)?;
        }
        Command::Scan {
            pattern: Some(pattern),
            ..
        } => validate_key(pattern, guards)?,
        Command::Scan { pattern: None, .. } => {}
        Command::BackupVerify { dump } => validate_value(dump, guards)?,
        Command::Restore { dump } => validate_value(dump, guards)?,
        Command::BackupTo { path }
        | Command::BackupVerifyFrom { path }
        | Command::RestoreFrom { path }
        | Command::RestoreCheckFrom { path } => validate_value(path, guards)?,
        Command::RestoreCheck { dump } => validate_value(dump, guards)?,
        Command::CreateUser { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::AlterUserPassword { username, password } => {
            validate_key(username, guards)?;
            validate_value(password, guards)?;
        }
        Command::DropUser { username } => validate_key(username, guards)?,
        Command::ClusterJoin { node_id, address } => {
            validate_key(node_id, guards)?;
            validate_value(address, guards)?;
        }
        Command::ClusterRemove { node_id } => validate_key(node_id, guards)?,
        Command::CreateRole { role } | Command::DropRole { role } => validate_key(role, guards)?,
        Command::ShowGrantsForUser { username } => validate_key(username, guards)?,
        Command::ShowGrantsForRole { role } => validate_key(role, guards)?,
        Command::GrantRole { role, username } | Command::RevokeRole { role, username } => {
            validate_key(role, guards)?;
            validate_key(username, guards)?;
        }
        Command::GrantPermission {
            permission,
            pattern,
            role,
        }
        | Command::RevokePermission {
            permission,
            pattern,
            role,
        } => {
            validate_key(permission, guards)?;
            validate_key(pattern, guards)?;
            validate_key(role, guards)?;
        }
        _ => {}
    }

    Ok(())
}
