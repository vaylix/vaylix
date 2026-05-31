use command::{
    Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
    SetOptions as CommandSetOptions,
};
use engine::{
    Expiration, ScanPage, SetCondition, SetOptions, SetOutcome, StorageEngine, TransactionResult,
};
use transport::{ExecResultPayload, Response};
use uuid::Uuid;

use crate::error::{Result, ServerError};

pub(super) fn error_response(request_id: Uuid, code: &str, name: &str, message: &str) -> Response {
    Response::error(request_id, code, name, message).unwrap_or_else(|_| {
        Response::error(
            request_id,
            "TRN-011",
            "Remote Error Encoding Failure",
            "failed to encode structured error payload",
        )
        .expect("static remote error encoding should never fail")
    })
}

pub(super) fn execute_command<E>(
    engine: &mut E,
    request_id: Uuid,
    command: Command,
) -> Result<Response>
where
    E: StorageEngine,
{
    match command {
        Command::Auth { .. } => Err(ServerError::UnsupportedRemoteCommand),
        Command::Ping { message } => {
            let payload = message.unwrap_or_else(|| "PONG".to_string());
            Ok(Response::value(request_id, &payload)?)
        }
        Command::Get { key } => value_or_not_found(request_id, engine.get(&key)?),
        Command::GetDel { key } => value_or_not_found(request_id, engine.get_del(&key)?),
        Command::GetEx {
            key,
            expiration,
            persist,
        } => value_or_not_found(
            request_id,
            engine.get_ex(&key, map_expiration(expiration), persist)?,
        ),
        Command::Set {
            key,
            value,
            options,
        } => render_set_response(
            request_id,
            options.return_previous,
            options.condition.is_some(),
            engine.set_with_options(key, value, map_set_options(options))?,
        ),
        Command::SetNx { key, value } => {
            Ok(Response::boolean(request_id, engine.set_nx(key, value)?))
        }
        Command::MGet { keys } => Ok(Response::strings(request_id, &engine.mget(&keys)?)?),
        Command::MSet { entries } => {
            engine.mset(&entries)?;
            Ok(Response::ok(request_id))
        }
        Command::Delete { keys } => Ok(Response::count(
            request_id,
            engine.delete_many(&keys)? as u64,
        )),
        Command::Exists { key } => Ok(Response::boolean(request_id, engine.exists(&key)?)),
        Command::Incr { key } => Ok(Response::integer(request_id, engine.incr(&key)?)),
        Command::Decr { key } => Ok(Response::integer(request_id, engine.decr(&key)?)),
        Command::Expire { key, seconds } => {
            Ok(Response::boolean(request_id, engine.expire(&key, seconds)?))
        }
        Command::Ttl { key } => Ok(Response::integer(request_id, engine.ttl(&key)?)),
        Command::Persist { key } => Ok(Response::boolean(request_id, engine.persist(&key)?)),
        Command::Rename {
            source,
            destination,
        } => Ok(Response::boolean(
            request_id,
            engine.rename(&source, destination)?,
        )),
        Command::RenameNx {
            source,
            destination,
        } => Ok(Response::boolean(
            request_id,
            engine.rename_nx(&source, destination)?,
        )),
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let ScanPage { next_cursor, keys } = engine.scan(cursor, pattern.as_deref(), count)?;
            Ok(Response::scan(request_id, next_cursor, &keys)?)
        }
        Command::DbSize | Command::Count => {
            Ok(Response::count(request_id, engine.db_size()? as u64))
        }
        Command::Info => Ok(Response::entries(request_id, &engine.info()?)?),
        Command::Metrics | Command::MetricsProm => Err(ServerError::UnsupportedRemoteCommand),
        Command::List => Ok(Response::entries(request_id, &engine.list()?)?),
        Command::Clear => {
            engine.clear()?;
            Ok(Response::ok(request_id))
        }
        Command::Save | Command::Snapshot => {
            engine.snapshot()?;
            Ok(Response::ok(request_id))
        }
        Command::Backup => Ok(Response::value(request_id, &engine.logical_backup()?)?),
        Command::Restore { dump } => Ok(Response::count(
            request_id,
            engine.restore_logical_backup(&dump)? as u64,
        )),
        Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. }
        | Command::AlterUserPassword { .. }
        | Command::Health
        | Command::ShowCluster
        | Command::ClusterJoin { .. }
        | Command::ClusterRemove { .. }
        | Command::ShowReplication
        | Command::PromoteFollower
        | Command::PauseReplication
        | Command::ResumeReplication
        | Command::MaintenanceOn
        | Command::MaintenanceOff
        | Command::MaintenanceStatus => Err(ServerError::UnsupportedRemoteCommand),
        Command::Multi | Command::Exec | Command::Discard => {
            Err(ServerError::UnsupportedRemoteCommand)
        }
        Command::CreateUser { .. }
        | Command::DropUser { .. }
        | Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowUsers
        | Command::ShowRoles
        | Command::ShowGrants
        | Command::ShowGrantsForUser { .. }
        | Command::ShowGrantsForRole { .. }
        | Command::WhoAmI => Err(ServerError::UnsupportedRemoteCommand),
        Command::Help | Command::Exit => Err(ServerError::UnsupportedRemoteCommand),
    }
}

pub(super) fn map_transaction_result_payload(result: TransactionResult) -> ExecResultPayload {
    match result {
        TransactionResult::Ok => ExecResultPayload::Ok,
        TransactionResult::NotFound => ExecResultPayload::NotFound,
        TransactionResult::Value(value) => ExecResultPayload::Value(value),
        TransactionResult::Boolean(value) => ExecResultPayload::Boolean(value),
        TransactionResult::Count(value) => ExecResultPayload::Count(value),
        TransactionResult::Integer(value) => ExecResultPayload::Integer(value),
        TransactionResult::Entries(entries) => ExecResultPayload::Entries(entries),
        TransactionResult::Strings(values) => ExecResultPayload::Strings(values),
        TransactionResult::Scan(scan) => ExecResultPayload::Scan(transport::ScanPayload {
            next_cursor: scan.next_cursor,
            keys: scan.keys,
        }),
    }
}

pub(super) fn validate_transaction_command(command: &Command) -> Result<()> {
    match command {
        Command::Info
        | Command::Metrics
        | Command::MetricsProm
        | Command::Save
        | Command::Snapshot
        | Command::Backup
        | Command::BackupTo { .. }
        | Command::BackupVerify { .. }
        | Command::BackupVerifyFrom { .. }
        | Command::Restore { .. }
        | Command::RestoreFrom { .. }
        | Command::RestoreCheck { .. }
        | Command::RestoreCheckFrom { .. }
        | Command::AlterUserPassword { .. }
        | Command::CreateUser { .. }
        | Command::DropUser { .. }
        | Command::CreateRole { .. }
        | Command::DropRole { .. }
        | Command::GrantRole { .. }
        | Command::RevokeRole { .. }
        | Command::GrantPermission { .. }
        | Command::RevokePermission { .. }
        | Command::ShowUsers
        | Command::ShowRoles
        | Command::ShowGrants
        | Command::ShowGrantsForUser { .. }
        | Command::ShowGrantsForRole { .. }
        | Command::WhoAmI
        | Command::MaintenanceOn
        | Command::MaintenanceOff
        | Command::MaintenanceStatus
        | Command::Health
        | Command::ShowCluster
        | Command::ShowReplication
        | Command::PromoteFollower
        | Command::PauseReplication
        | Command::ResumeReplication
        | Command::Auth { .. }
        | Command::Help
        | Command::Exit
        | Command::Multi
        | Command::Exec
        | Command::Discard => Err(ServerError::UnsupportedRemoteCommand),
        _ => Ok(()),
    }
}

fn value_or_not_found(request_id: Uuid, value: Option<String>) -> Result<Response> {
    match value {
        Some(value) => Ok(Response::value(request_id, &value)?),
        None => Ok(Response::not_found(request_id)),
    }
}

pub(super) fn render_set_response(
    request_id: Uuid,
    return_previous: bool,
    conditional_write: bool,
    outcome: SetOutcome,
) -> Result<Response> {
    if return_previous {
        return value_or_not_found(request_id, outcome.previous);
    }
    if conditional_write {
        return Ok(Response::boolean(request_id, outcome.applied));
    }
    Ok(Response::ok(request_id))
}

fn map_expiration(expiration: Option<CommandExpiration>) -> Option<Expiration> {
    expiration.map(|expiration| match expiration {
        CommandExpiration::Ex(value) => Expiration::Seconds(value),
        CommandExpiration::Px(value) => Expiration::Milliseconds(value),
    })
}

fn map_set_options(options: CommandSetOptions) -> SetOptions {
    SetOptions {
        condition: options.condition.map(|condition| match condition {
            CommandSetCondition::Nx => SetCondition::Nx,
            CommandSetCondition::Xx => SetCondition::Xx,
        }),
        expiration: map_expiration(options.expiration),
        keep_ttl: options.keep_ttl,
    }
}
