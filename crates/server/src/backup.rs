use std::{
    io::Write,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result, ServerError};

const BACKUP_MANIFEST_VERSION: u32 = 1;
const BACKUP_HASH_ALGORITHM: &str = "sha256";

/// Sidecar metadata used to verify a logical backup dump before restore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BackupManifest {
    pub(crate) manifest_version: u32,
    pub(crate) backup_version: u32,
    pub(crate) created_at_ms: u64,
    pub(crate) source_engine_version: u32,
    pub(crate) source_sequence: u64,
    pub(crate) entry_count: u64,
    pub(crate) byte_len: u64,
    pub(crate) hash_algorithm: String,
    pub(crate) sha256: String,
}

#[derive(Debug, Deserialize)]
struct BackupDocumentHeader {
    version: u32,
    created_at_ms: u64,
    source_engine_version: u32,
    source_sequence: u64,
    entries: Vec<serde_json::Value>,
}

/// Resolves a requested backup path under the configured backup directory.
///
/// Absolute paths are allowed only when they canonicalize back under
/// `base_dir`. Parent traversal is rejected before any filesystem operation.
pub(crate) fn resolve_backup_path(
    base_dir: &Path,
    requested: &str,
    must_exist: bool,
) -> Result<PathBuf> {
    let requested_path = Path::new(requested);
    if requested_path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }

    std::fs::create_dir_all(base_dir)?;
    let base = base_dir.canonicalize()?;
    let candidate = if requested_path.is_absolute() {
        requested_path.to_path_buf()
    } else {
        base.join(requested_path)
    };

    if must_exist {
        let canonical = candidate.canonicalize()?;
        if canonical.starts_with(&base) {
            return Ok(canonical);
        }
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }

    let parent = candidate
        .parent()
        .ok_or_else(|| ServerError::BackupPathRejected(requested.to_string()))?;
    std::fs::create_dir_all(parent)?;
    let canonical_parent = parent.canonicalize()?;
    if !canonical_parent.starts_with(&base) {
        return Err(ServerError::BackupPathRejected(requested.to_string()));
    }
    reject_final_symlink(&candidate, requested)?;
    Ok(candidate)
}

pub(crate) fn backup_manifest_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("backup");
    path.with_file_name(format!("{file_name}.manifest.json"))
}

pub(crate) fn reject_final_symlink(path: &Path, requested: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(ServerError::BackupPathRejected(requested.to_string()))
        }
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Writes a backup-owned file without following a final-path symlink.
///
/// `resolve_backup_path` already verifies the parent directory boundary. This
/// helper closes the final-component symlink race for backup and manifest
/// creation on Unix by opening with `O_NOFOLLOW`.
pub(crate) fn write_backup_file(path: &Path, requested: &str, bytes: &[u8]) -> Result<()> {
    reject_final_symlink(path, requested)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .map_err(|err| {
                if err.raw_os_error() == Some(libc::ELOOP) {
                    ServerError::BackupPathRejected(requested.to_string())
                } else {
                    err.into()
                }
            })?;
        file.write_all(bytes)?;
        Ok(())
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

pub(crate) fn load_backup_manifest(path: &Path) -> Result<BackupManifest> {
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|err| ServerError::BackupVerification(err.to_string()))
}

pub(crate) fn build_backup_manifest(dump: &str) -> Result<BackupManifest> {
    let backup = parse_backup_document_header(dump)?;
    Ok(BackupManifest {
        manifest_version: BACKUP_MANIFEST_VERSION,
        backup_version: backup.version,
        created_at_ms: backup.created_at_ms,
        source_engine_version: backup.source_engine_version,
        source_sequence: backup.source_sequence,
        entry_count: backup.entries.len() as u64,
        byte_len: dump.len() as u64,
        hash_algorithm: BACKUP_HASH_ALGORITHM.to_string(),
        sha256: sha256_hex(dump.as_bytes()),
    })
}

pub(crate) fn verify_backup_manifest(dump: &str, manifest: &BackupManifest) -> Result<()> {
    let expected = build_backup_manifest(dump)?;
    if manifest.manifest_version != BACKUP_MANIFEST_VERSION {
        return Err(ServerError::BackupVerification(format!(
            "unsupported manifest version {}",
            manifest.manifest_version
        )));
    }
    if manifest.hash_algorithm != BACKUP_HASH_ALGORITHM {
        return Err(ServerError::BackupVerification(format!(
            "unsupported hash algorithm {}",
            manifest.hash_algorithm
        )));
    }
    if manifest.backup_version != expected.backup_version
        || manifest.created_at_ms != expected.created_at_ms
        || manifest.source_engine_version != expected.source_engine_version
        || manifest.source_sequence != expected.source_sequence
        || manifest.entry_count != expected.entry_count
        || manifest.byte_len != expected.byte_len
        || manifest.sha256 != expected.sha256
    {
        return Err(ServerError::BackupVerification(
            "backup manifest does not match dump".to_string(),
        ));
    }
    Ok(())
}

fn parse_backup_document_header(dump: &str) -> Result<BackupDocumentHeader> {
    let backup: BackupDocumentHeader = serde_json::from_str(dump)
        .map_err(|err| ServerError::BackupVerification(err.to_string()))?;
    if !matches!(backup.version, 1 | 2) {
        return Err(ServerError::BackupVerification(format!(
            "unsupported backup version {}",
            backup.version
        )));
    }
    Ok(backup)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes).to_vec();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in &digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{
        backup_manifest_path, build_backup_manifest, resolve_backup_path, verify_backup_manifest,
        write_backup_file,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-backup-{name}-{unique}"))
    }

    fn backup_dump() -> String {
        serde_json::json!({
            "version": 2,
            "created_at_ms": 10,
            "source_engine_version": 1,
            "source_sequence": 2,
            "entries": [
                {
                    "key": "alpha",
                    "value_base64": "b25l",
                    "expires_at_ms": null,
                    "version": 1
                }
            ]
        })
        .to_string()
    }

    #[test]
    fn resolves_relative_backup_paths_under_base_dir() {
        let base = temp_dir("paths");
        let resolved = resolve_backup_path(&base, "nested/backup.json", false).unwrap();
        assert!(resolved.starts_with(base.canonicalize().unwrap()));
        assert!(resolved.ends_with("nested/backup.json"));
        std::fs::remove_dir_all(base).ok();
    }

    #[test]
    fn rejects_parent_traversal() {
        let base = temp_dir("traversal");
        let err = resolve_backup_path(&base, "../escape.json", false).unwrap_err();
        assert_eq!(err.code(), "SRV-027");
        std::fs::remove_dir_all(base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_output_symlink_escape() {
        use std::os::unix::fs::symlink;

        let base = temp_dir("symlink");
        let outside = temp_dir("outside");
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, base.join("backup.json")).unwrap();

        let err = write_backup_file(&base.join("backup.json"), "backup.json", b"new").unwrap_err();
        assert_eq!(err.code(), "SRV-027");
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

        std::fs::remove_dir_all(base).ok();
        std::fs::remove_file(outside).ok();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_manifest_sidecar_symlink_escape() {
        use std::os::unix::fs::symlink;

        let base = temp_dir("manifest-symlink");
        let outside = temp_dir("manifest-outside");
        std::fs::create_dir_all(&base).unwrap();
        let backup = base.join("backup.json");
        std::fs::write(&outside, b"outside").unwrap();
        symlink(&outside, backup_manifest_path(&backup)).unwrap();

        let manifest = backup_manifest_path(&backup);
        let err = write_backup_file(&manifest, &manifest.display().to_string(), b"{}").unwrap_err();
        assert_eq!(err.code(), "SRV-027");
        assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

        std::fs::remove_dir_all(base).ok();
        std::fs::remove_file(outside).ok();
    }

    #[test]
    fn manifest_verification_rejects_mutated_dump() {
        let dump = backup_dump();
        let manifest = build_backup_manifest(&dump).unwrap();
        verify_backup_manifest(&dump, &manifest).unwrap();

        let mutated = dump.replace("alpha", "beta");
        assert!(verify_backup_manifest(&mutated, &manifest).is_err());
    }

    #[test]
    fn manifest_path_adds_sidecar_suffix() {
        let path = std::path::Path::new("/tmp/backup.json");
        assert_eq!(
            backup_manifest_path(path),
            std::path::Path::new("/tmp/backup.json.manifest.json")
        );
    }
}
