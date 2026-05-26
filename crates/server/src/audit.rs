use serde::Serialize;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::Result;

/// Structured audit event recorded by the server.
#[derive(Debug, Clone, Serialize)]
pub struct AuditEvent {
    pub timestamp_ms: u64,
    pub connection_id: u64,
    pub peer: Option<String>,
    pub username: Option<String>,
    pub request_id: String,
    pub opcode: String,
    pub status: String,
    pub error_code: Option<String>,
    pub latency_ms: u128,
}

/// Append-only audit logger used for command-level accountability.
pub struct AuditLogger {
    path: PathBuf,
    file: Mutex<File>,
}

impl AuditLogger {
    /// Opens or creates the audit log file at the provided path.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(file),
        })
    }

    /// Returns the backing audit log path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persists one structured audit event as a JSON line.
    pub fn record(&self, event: &AuditEvent) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("audit log mutex poisoned"))?;
        serde_json::to_writer(&mut *file, event).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AuditEvent, AuditLogger};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("veyra-audit-{unique}.log"))
    }

    #[test]
    fn records_json_lines() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger
            .record(&AuditEvent {
                timestamp_ms: 1,
                connection_id: 2,
                peer: Some("127.0.0.1:1".to_string()),
                username: Some("alice".to_string()),
                request_id: "id".to_string(),
                opcode: "GET".to_string(),
                status: "ok".to_string(),
                error_code: None,
                latency_ms: 3,
            })
            .unwrap();

        let body = fs::read_to_string(&path).unwrap();
        assert!(body.contains("\"opcode\":\"GET\""));
        fs::remove_file(path).ok();
    }
}
