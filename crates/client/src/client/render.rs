use command::Command;
use serde_json::json;
use transport::{Response, Status};

use super::OutputMode;
use crate::error::{ClientError, Result};

pub(super) fn render_response(
    command: &Command,
    response: &Response,
    output: OutputMode,
) -> Result<String> {
    if let Ok(value) = response.decode_value()
        && value == "QUEUED"
    {
        return Ok(value);
    }

    match response.status {
        Status::NotFound => Ok("NOT_FOUND".to_string()),
        Status::Error => {
            let remote = response.decode_error()?;
            Ok(format!(
                "ERROR [{}] {}: {}",
                remote.code, remote.name, remote.message
            ))
        }
        Status::Ok => match command {
            Command::Auth { .. } => Ok("OK".to_string()),
            Command::Ping { .. }
            | Command::Get { .. }
            | Command::GetDel { .. }
            | Command::GetEx { .. }
            | Command::Backup
            | Command::MetricsProm => Ok(render_value_bytes(response.decode_value_bytes()?)),
            Command::Set { options, .. } => {
                if options.return_previous {
                    Ok(render_value_bytes(response.decode_value_bytes()?))
                } else if options.condition.is_some() || options.if_version.is_some() {
                    Ok(response.decode_bool()?.to_string())
                } else {
                    Ok("OK".to_string())
                }
            }
            Command::MSet { .. }
            | Command::BackupTo { .. }
            | Command::Clear
            | Command::Save
            | Command::Snapshot
            | Command::AlterUserPassword { .. }
            | Command::CreateUser { .. }
            | Command::DropUser { .. }
            | Command::CreateRole { .. }
            | Command::DropRole { .. }
            | Command::GrantRole { .. }
            | Command::RevokeRole { .. }
            | Command::GrantPermission { .. }
            | Command::RevokePermission { .. }
            | Command::Multi
            | Command::MaintenanceOn
            | Command::MaintenanceOff
            | Command::Discard => Ok("OK".to_string()),
            Command::Exec => Ok(response
                .decode_exec_results()?
                .into_iter()
                .map(render_exec_result)
                .collect::<Vec<_>>()
                .join("\n")),
            Command::SetNx { .. }
            | Command::Exists { .. }
            | Command::Expire { .. }
            | Command::Persist { .. }
            | Command::Rename { .. }
            | Command::RenameNx { .. } => Ok(response.decode_bool()?.to_string()),
            Command::MGet { .. } => Ok(response
                .decode_byte_strings()?
                .into_iter()
                .map(|value| {
                    value
                        .map(render_value_bytes)
                        .unwrap_or_else(|| "(nil)".to_string())
                })
                .collect::<Vec<_>>()
                .join(", ")),
            Command::Delete { .. }
            | Command::DbSize
            | Command::Count
            | Command::Restore { .. }
            | Command::RestoreFrom { .. }
            | Command::RestoreCheck { .. }
            | Command::RestoreCheckFrom { .. } => Ok(response.decode_count()?.to_string()),
            Command::Incr { .. } | Command::Decr { .. } | Command::Ttl { .. } => {
                Ok(response.decode_integer()?.to_string())
            }
            Command::Scan { .. } => {
                let payload = response.decode_scan()?;
                let keys = if payload.keys.is_empty() {
                    "(empty)".to_string()
                } else {
                    payload.keys.join(", ")
                };

                Ok(format!("cursor={}, keys=[{}]", payload.next_cursor, keys))
            }
            Command::Info
            | Command::Metrics
            | Command::List
            | Command::BackupVerify { .. }
            | Command::BackupVerifyFrom { .. }
            | Command::ShowUsers
            | Command::ShowRoles
            | Command::ShowGrants
            | Command::ShowGrantsForUser { .. }
            | Command::ShowGrantsForRole { .. }
            | Command::WhoAmI
            | Command::MaintenanceStatus
            | Command::Health
            | Command::ShowCluster
            | Command::ShowReplication => {
                let entries = response.decode_entries()?;
                render_entries(&entries, output)
            }
            Command::ClusterJoin { .. }
            | Command::ClusterRemove { .. }
            | Command::PromoteFollower
            | Command::PauseReplication
            | Command::ResumeReplication => Ok("OK".to_string()),
            Command::Help | Command::Exit => Err(ClientError::LocalCommandResponse),
        },
    }
}

fn render_exec_result(result: transport::ExecResultPayload) -> String {
    match result {
        transport::ExecResultPayload::Ok => "OK".to_string(),
        transport::ExecResultPayload::NotFound => "NOT_FOUND".to_string(),
        transport::ExecResultPayload::Value(value) => render_value_bytes(value),
        transport::ExecResultPayload::Boolean(value) => value.to_string(),
        transport::ExecResultPayload::Count(value) => value.to_string(),
        transport::ExecResultPayload::Integer(value) => value.to_string(),
        transport::ExecResultPayload::Entries(entries) => entries
            .into_iter()
            .map(|(key, value)| format!("{key}={}", render_value_bytes(value)))
            .collect::<Vec<_>>()
            .join(", "),
        transport::ExecResultPayload::Strings(values) => values
            .into_iter()
            .map(|value| {
                value
                    .map(render_value_bytes)
                    .unwrap_or_else(|| "(nil)".to_string())
            })
            .collect::<Vec<_>>()
            .join(", "),
        transport::ExecResultPayload::Scan(scan) => {
            format!(
                "cursor={}, keys=[{}]",
                scan.next_cursor,
                scan.keys.join(", ")
            )
        }
    }
}

fn render_value_bytes(value: Vec<u8>) -> String {
    match String::from_utf8(value) {
        Ok(value) => value,
        Err(err) => {
            let bytes = err.into_bytes();
            let mut rendered = String::with_capacity(2 + bytes.len() * 2);
            rendered.push_str("0x");
            for byte in bytes {
                use std::fmt::Write as _;
                let _ = write!(&mut rendered, "{byte:02x}");
            }
            rendered
        }
    }
}

fn render_entries(entries: &[(String, String)], output: OutputMode) -> Result<String> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));

    match output {
        OutputMode::Plain => Ok(sorted
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ")),
        OutputMode::Table => Ok(render_table(&sorted)),
        OutputMode::Json => Ok(serde_json::to_string_pretty(
            &sorted
                .iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
        )
        .map_err(std::io::Error::other)?),
    }
}

pub(super) fn render_table(entries: &[(String, String)]) -> String {
    let key_width = entries
        .iter()
        .map(|(key, _)| key.len())
        .max()
        .unwrap_or(3)
        .max(3);
    let value_width = entries
        .iter()
        .map(|(_, value)| value.len())
        .max()
        .unwrap_or(5)
        .max(5);

    let mut lines = Vec::with_capacity(entries.len() + 2);
    lines.push(format!(
        "{:<key_width$} | {:<value_width$}",
        "key",
        "value",
        key_width = key_width,
        value_width = value_width
    ));
    lines.push(format!(
        "{}-+-{}",
        "-".repeat(key_width),
        "-".repeat(value_width)
    ));
    for (key, value) in entries {
        lines.push(format!(
            "{:<key_width$} | {:<value_width$}",
            key,
            value,
            key_width = key_width,
            value_width = value_width
        ));
    }
    lines.join("\n")
}
