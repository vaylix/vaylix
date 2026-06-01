use crate::args::{AdminCommand, Args, PitrAction, StorageAction};
use crate::bootstrap::engine_options;
use crate::error::{Result, ServerError};

/// Executes offline administrative commands that do not start the TCP server.
pub fn run_admin_command(args: &Args, command: AdminCommand) -> Result<()> {
    match command {
        AdminCommand::Storage(command) => match command.action {
            StorageAction::Migrate { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection =
                    engine::Engine::migrate_storage(&paths, &engine_options(args, keyring))?;
                print_storage_inspection(&inspection);
            }
            StorageAction::Verify { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection =
                    engine::Engine::verify_storage(&paths, engine_options(args, keyring))?;
                print_storage_inspection(&inspection);
            }
        },
        AdminCommand::Pitr(command) => match command.action {
            PitrAction::Inspect { data_dir } => {
                let paths = engine::Paths::from_data_dir(data_dir)?;
                let keyring = engine::load_keyring(&paths.keyring_path)?;
                let inspection = engine::Engine::inspect_storage(&paths, keyring.as_ref())?;
                print_storage_inspection(&inspection);
            }
            PitrAction::Restore {
                source_dir,
                target_dir,
                to_sequence,
                to_timestamp_ms,
            } => {
                let source_paths = engine::Paths::from_data_dir(source_dir)?;
                let target_paths = engine::Paths::from_data_dir(target_dir)?;
                let target = if let Some(sequence) = to_sequence {
                    engine::PointInTimeTarget::Sequence(sequence)
                } else if let Some(timestamp_ms) = to_timestamp_ms {
                    engine::PointInTimeTarget::TimestampMs(timestamp_ms)
                } else {
                    return Err(ServerError::InvalidArguments(
                        "pitr restore requires --to-sequence or --to-timestamp-ms".to_string(),
                    ));
                };
                let keyring = engine::load_keyring(&source_paths.keyring_path)?;
                let inspection = engine::Engine::restore_to_point(
                    &source_paths,
                    &target_paths,
                    engine_options(args, keyring),
                    target,
                )?;
                print_storage_inspection(&inspection);
            }
        },
    }
    Ok(())
}

fn print_storage_inspection(inspection: &engine::StorageInspection) {
    for (key, value) in [
        ("snapshot_present", inspection.snapshot_present.to_string()),
        (
            "storage_format_version",
            inspection.storage_format_version.to_string(),
        ),
        (
            "snapshot_size_bytes",
            inspection.snapshot_size_bytes.to_string(),
        ),
        (
            "last_snapshot_sequence",
            inspection.last_snapshot_sequence.to_string(),
        ),
        (
            "last_snapshot_at_ms",
            inspection
                .last_snapshot_at_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "wal_segment_count",
            inspection.wal_segment_count.to_string(),
        ),
        (
            "sealed_wal_segment_count",
            inspection.sealed_wal_segment_count.to_string(),
        ),
        (
            "active_wal_segment_count",
            inspection.active_wal_segment_count.to_string(),
        ),
        (
            "active_wal_start_sequence",
            inspection.active_wal_start_sequence.to_string(),
        ),
        (
            "oldest_retained_sequence",
            inspection
                .oldest_retained_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        (
            "newest_sequence",
            inspection
                .newest_sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
        ("wal_size_bytes", inspection.wal_size_bytes.to_string()),
    ] {
        println!("{key}={value}");
    }
}
