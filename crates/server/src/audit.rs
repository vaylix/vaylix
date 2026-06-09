use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Result, ServerError};

const AUDIT_VERSION: u32 = 1;
const HASH_ALGORITHM: &str = "sha256";
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Structured audit event recorded by the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub timestamp_ms: u64,
    pub connection_id: u64,
    pub peer: Option<String>,
    pub username: Option<String>,
    pub request_id: String,
    pub opcode: String,
    pub status: String,
    pub error_code: Option<String>,
    pub latency_ms: u64,
    pub event_type: String,
    pub details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashPayload {
    audit_version: u32,
    sequence: u64,
    previous_hash: String,
    hash_algorithm: String,
    timestamp_ms: u64,
    connection_id: u64,
    peer: Option<String>,
    username: Option<String>,
    request_id: String,
    opcode: String,
    status: String,
    error_code: Option<String>,
    latency_ms: u64,
    event_type: String,
    details: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainedAuditEvent {
    audit_version: u32,
    sequence: u64,
    previous_hash: String,
    event_hash: String,
    hash_algorithm: String,
    timestamp_ms: u64,
    connection_id: u64,
    peer: Option<String>,
    username: Option<String>,
    request_id: String,
    opcode: String,
    status: String,
    error_code: Option<String>,
    latency_ms: u64,
    event_type: String,
    details: BTreeMap<String, String>,
}

struct AuditChainState {
    next_sequence: u64,
    previous_hash: String,
}

/// Append-only audit logger used for command-level accountability.
pub struct AuditLogger {
    path: PathBuf,
    file: Mutex<File>,
    chain: Mutex<AuditChainState>,
}

impl AuditLogger {
    /// Opens or creates the audit log file and verifies any existing hash chain.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }

        let chain = verify_existing_chain(path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Mutex::new(file),
            chain: Mutex::new(chain),
        })
    }

    /// Returns the backing audit log path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persists one structured audit event as a hash-chained JSON line.
    pub fn record(&self, event: &AuditEvent) -> Result<()> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("audit log mutex poisoned"))?;
        let mut chain = self
            .chain
            .lock()
            .map_err(|_| std::io::Error::other("audit chain mutex poisoned"))?;

        let sequence = chain.next_sequence;
        let previous_hash = chain.previous_hash.clone();
        let event_hash = compute_hash(sequence, &previous_hash, event)?;
        let chained = ChainedAuditEvent {
            audit_version: AUDIT_VERSION,
            sequence,
            previous_hash,
            event_hash: event_hash.clone(),
            hash_algorithm: HASH_ALGORITHM.to_string(),
            timestamp_ms: event.timestamp_ms,
            connection_id: event.connection_id,
            peer: event.peer.clone(),
            username: event.username.clone(),
            request_id: event.request_id.clone(),
            opcode: event.opcode.clone(),
            status: event.status.clone(),
            error_code: event.error_code.clone(),
            latency_ms: event.latency_ms,
            event_type: event.event_type.clone(),
            details: event.details.clone(),
        };

        serde_json::to_writer(&mut *file, &chained).map_err(std::io::Error::other)?;
        file.write_all(b"\n")?;
        file.flush()?;

        chain.next_sequence = sequence + 1;
        chain.previous_hash = event_hash;
        Ok(())
    }
}

fn verify_existing_chain(path: &Path) -> Result<AuditChainState> {
    if !path.exists() {
        return Ok(AuditChainState {
            next_sequence: 1,
            previous_hash: GENESIS_HASH.to_string(),
        });
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut expected_sequence = 1;
    let mut expected_previous_hash = GENESIS_HASH.to_string();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line?;
        if line.trim().is_empty() {
            return Err(chain_error(line_number, "empty audit log line"));
        }

        let event: ChainedAuditEvent =
            serde_json::from_str(&line).map_err(|err| chain_error(line_number, err.to_string()))?;
        if event.audit_version != AUDIT_VERSION {
            return Err(chain_error(
                line_number,
                format!("unsupported audit version {}", event.audit_version),
            ));
        }
        if event.hash_algorithm != HASH_ALGORITHM {
            return Err(chain_error(
                line_number,
                format!("unsupported hash algorithm {}", event.hash_algorithm),
            ));
        }
        if event.sequence != expected_sequence {
            return Err(chain_error(
                line_number,
                format!(
                    "expected sequence {expected_sequence}, found {}",
                    event.sequence
                ),
            ));
        }
        if event.previous_hash != expected_previous_hash {
            return Err(chain_error(line_number, "previous hash mismatch"));
        }

        let computed_hash = compute_hash(
            event.sequence,
            &event.previous_hash,
            &AuditEvent {
                timestamp_ms: event.timestamp_ms,
                connection_id: event.connection_id,
                peer: event.peer.clone(),
                username: event.username.clone(),
                request_id: event.request_id.clone(),
                opcode: event.opcode.clone(),
                status: event.status.clone(),
                error_code: event.error_code.clone(),
                latency_ms: event.latency_ms,
                event_type: event.event_type.clone(),
                details: event.details.clone(),
            },
        )?;
        if event.event_hash != computed_hash {
            return Err(chain_error(line_number, "event hash mismatch"));
        }

        expected_sequence += 1;
        expected_previous_hash = event.event_hash;
    }

    Ok(AuditChainState {
        next_sequence: expected_sequence,
        previous_hash: expected_previous_hash,
    })
}

fn compute_hash(sequence: u64, previous_hash: &str, event: &AuditEvent) -> Result<String> {
    let payload = HashPayload {
        audit_version: AUDIT_VERSION,
        sequence,
        previous_hash: previous_hash.to_string(),
        hash_algorithm: HASH_ALGORITHM.to_string(),
        timestamp_ms: event.timestamp_ms,
        connection_id: event.connection_id,
        peer: event.peer.clone(),
        username: event.username.clone(),
        request_id: event.request_id.clone(),
        opcode: event.opcode.clone(),
        status: event.status.clone(),
        error_code: event.error_code.clone(),
        latency_ms: event.latency_ms,
        event_type: event.event_type.clone(),
        details: event.details.clone(),
    };
    let bytes = serde_json::to_vec(&payload).map_err(std::io::Error::other)?;
    let digest = Sha256::digest(bytes).to_vec();
    Ok(hex_encode(&digest))
}

fn chain_error(line: usize, message: impl Into<String>) -> ServerError {
    ServerError::AuditChainVerification(format!("line {line}: {}", message.into()))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{AUDIT_VERSION, AuditEvent, AuditLogger, GENESIS_HASH, HASH_ALGORITHM};
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = TEST_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("vaylix-audit-{unique}-{suffix}.log"))
    }

    fn event(opcode: &str) -> AuditEvent {
        AuditEvent {
            timestamp_ms: 1,
            connection_id: 2,
            peer: Some("127.0.0.1:1".to_string()),
            username: Some("alice".to_string()),
            request_id: "id".to_string(),
            opcode: opcode.to_string(),
            status: "ok".to_string(),
            error_code: None,
            latency_ms: 3,
            event_type: "command".to_string(),
            details: BTreeMap::new(),
        }
    }

    fn read_lines(path: &std::path::Path) -> Vec<Value> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn records_json_lines_with_genesis_hash() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("GET")).unwrap();

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["audit_version"], AUDIT_VERSION);
        assert_eq!(lines[0]["sequence"], 1);
        assert_eq!(lines[0]["previous_hash"], GENESIS_HASH);
        assert_eq!(lines[0]["hash_algorithm"], HASH_ALGORITHM);
        assert_eq!(lines[0]["opcode"], "GET");
        assert_eq!(lines[0]["event_type"], "command");
        assert_eq!(lines[0]["event_hash"].as_str().unwrap().len(), 64);
        fs::remove_file(path).ok();
    }

    #[test]
    fn chains_multiple_events_and_reopens_from_tail() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("GET")).unwrap();
        logger.record(&event("SET")).unwrap();
        drop(logger);

        let lines = read_lines(&path);
        assert_eq!(lines[0]["sequence"], 1);
        assert_eq!(lines[1]["sequence"], 2);
        assert_eq!(lines[1]["previous_hash"], lines[0]["event_hash"]);

        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("DEL")).unwrap();
        let lines = read_lines(&path);
        assert_eq!(lines[2]["sequence"], 3);
        assert_eq!(lines[2]["previous_hash"], lines[1]["event_hash"]);
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_modified_event_fields() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("GET")).unwrap();
        drop(logger);

        let mut line: Value = read_lines(&path).remove(0);
        line["opcode"] = Value::String("SET".to_string());
        fs::write(
            &path,
            format!("{}\n", serde_json::to_string(&line).unwrap()),
        )
        .unwrap();

        assert!(AuditLogger::open(&path).is_err());
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_malformed_json_lines() {
        let path = temp_path();
        fs::write(&path, "{not-json}\n").unwrap();

        let err = match AuditLogger::open(&path) {
            Ok(_) => panic!("malformed audit log must fail chain verification"),
            Err(err) => err,
        };
        assert_eq!(err.code(), "SRV-028");
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_removed_or_reordered_lines() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("GET")).unwrap();
        logger.record(&event("SET")).unwrap();
        drop(logger);

        let lines = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        fs::write(&path, format!("{}\n", lines[1])).unwrap();
        assert!(AuditLogger::open(&path).is_err());

        fs::write(&path, format!("{}\n{}\n", lines[1], lines[0])).unwrap();
        assert!(AuditLogger::open(&path).is_err());
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_modified_final_line_in_chain() {
        let path = temp_path();
        let logger = AuditLogger::open(&path).unwrap();
        logger.record(&event("GET")).unwrap();
        logger.record(&event("SET")).unwrap();
        drop(logger);

        let mut lines = read_lines(&path);
        lines[1]["status"] = Value::String("error".to_string());
        fs::write(
            &path,
            lines
                .iter()
                .map(|line| serde_json::to_string(line).unwrap())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();

        assert!(AuditLogger::open(&path).is_err());
        fs::remove_file(path).ok();
    }

    #[test]
    fn rejects_corrupted_chain_metadata() {
        for field in ["previous_hash", "event_hash", "hash_algorithm"] {
            let path = temp_path();
            let logger = AuditLogger::open(&path).unwrap();
            logger.record(&event("GET")).unwrap();
            drop(logger);

            let mut line: Value = read_lines(&path).remove(0);
            line[field] = Value::String("corrupted".to_string());
            fs::write(
                &path,
                format!("{}\n", serde_json::to_string(&line).unwrap()),
            )
            .unwrap();

            assert!(AuditLogger::open(&path).is_err());
            fs::remove_file(path).ok();
        }
    }

    #[test]
    fn concurrent_records_preserve_a_verifiable_chain() {
        let path = temp_path();
        let logger = Arc::new(AuditLogger::open(&path).unwrap());
        let threads = (0..4)
            .map(|worker| {
                let logger = Arc::clone(&logger);
                std::thread::spawn(move || {
                    for index in 0..25 {
                        logger
                            .record(&event(&format!("SET-{worker}-{index}")))
                            .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap();
        }
        drop(logger);

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 100);
        for (index, line) in lines.iter().enumerate() {
            assert_eq!(line["sequence"], (index + 1) as u64);
            if index == 0 {
                assert_eq!(line["previous_hash"], GENESIS_HASH);
            } else {
                assert_eq!(line["previous_hash"], lines[index - 1]["event_hash"]);
            }
        }
        AuditLogger::open(&path).unwrap();
        fs::remove_file(path).ok();
    }
}
