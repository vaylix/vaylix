use crate::config::StorageKeyring;
use crate::store::crypto::{decrypt, encrypt};
use crate::{EngineError, Result, WalSyncPolicy};
use crc32fast::hash;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use super::binary;

const MAX_WAL_ENTRY_SIZE: u32 = 4 * 1024 * 1024;

/// A single operation captured by the write-ahead log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOperation {
    /// Insert or replace a key/value pair.
    Set {
        key: String,
        #[serde(
            serialize_with = "crate::value::serialize_bytes",
            deserialize_with = "crate::value::deserialize_bytes"
        )]
        value: Vec<u8>,
        #[serde(default = "default_value_version")]
        version: u64,
    },
    /// Delete a key and any associated expiration.
    Delete { key: String },
    /// Attach an absolute expiration timestamp to a key.
    Expire { key: String, expires_at_ms: u64 },
    /// Remove any expiration from a key.
    Persist { key: String },
    /// Clear the entire database.
    Clear,
    /// Validate and update a numeric string value atomically.
    CheckInteger { key: String, delta: i64 },
}

fn default_value_version() -> u64 {
    1
}

/// A durable atomic batch of storage mutations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalEntry {
    /// Monotonic sequence number assigned by the engine.
    pub sequence: u64,
    /// Consensus term associated with the entry when it was created.
    #[serde(default)]
    pub term: u64,
    /// Entry creation time in unix milliseconds.
    pub created_at_ms: u64,
    /// Mutations applied atomically as one logical unit.
    pub operations: Vec<WalOperation>,
}

impl WalEntry {
    /// Builds a new WAL entry.
    pub fn new(
        sequence: u64,
        term: u64,
        created_at_ms: u64,
        operations: Vec<WalOperation>,
    ) -> Self {
        Self {
            sequence,
            term,
            created_at_ms,
            operations,
        }
    }

    /// Returns a stable checksum for replication identity checks.
    pub fn checksum(&self) -> Result<u32> {
        let encoded =
            binary::encode(self).map_err(|err| EngineError::WalSerialize(err.to_string()))?;
        Ok(hash(&encoded))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplay {
    pub entries: Vec<WalEntry>,
    pub segment_count: usize,
    pub oldest_retained_sequence: Option<u64>,
    pub newest_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalReplayTarget {
    Sequence(u64),
    TimestampMs(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSegmentReport {
    pub segment_count: usize,
    pub sealed_segment_count: usize,
    pub active_segment_count: usize,
    pub oldest_retained_sequence: Option<u64>,
    pub active_start_sequence: u64,
    pub newest_sequence: Option<u64>,
    pub total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentFile {
    path: PathBuf,
    start_sequence: u64,
    end_sequence: Option<u64>,
    active: bool,
}

impl SegmentFile {
    fn sort_key(&self) -> (u64, bool) {
        (self.start_sequence, self.active)
    }
}

/// Stateful WAL appender that keeps the active segment open across writes.
///
/// The entry format is identical to [`append`]. The only behavioral difference is
/// that callers can append several entries and pay the configured flush/sync
/// cost once per batch, while still rotating segments only at entry boundaries.
pub struct WalWriter {
    wal_dir: PathBuf,
    sync_policy: WalSyncPolicy,
    max_segment_size_bytes: u64,
    active: SegmentFile,
    file: Option<File>,
    file_len: u64,
}

/// Dedicated WAL I/O worker handle.
///
/// The engine remains the authority for sequence assignment and state
/// application, but append/flush/sync filesystem work is performed on this
/// worker thread. Callers still wait for the durability result before
/// acknowledging writes, preserving existing `buffered`/`flush`/`sync`
/// semantics.
pub struct WalWriterHandle {
    sender: mpsc::Sender<WalWriterCommand>,
}

enum WalWriterCommand {
    AppendBatch {
        entries: Vec<WalEntry>,
        keyring: Option<StorageKeyring>,
        respond_to: mpsc::Sender<Result<()>>,
    },
    CloseActive {
        respond_to: mpsc::Sender<Result<()>>,
    },
    Reset {
        start_sequence: u64,
        respond_to: mpsc::Sender<Result<()>>,
    },
    Shutdown,
}

impl WalWriterHandle {
    /// Starts a WAL worker with an open active segment.
    pub fn open(
        wal_dir: &Path,
        sync_policy: WalSyncPolicy,
        max_segment_size_bytes: u64,
        start_sequence: u64,
    ) -> Result<Self> {
        let writer = WalWriter::open(wal_dir, sync_policy, max_segment_size_bytes, start_sequence)?;
        let wal_dir = wal_dir.to_path_buf();
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("vaylix-wal-io".to_string())
            .spawn(move || {
                run_wal_writer_worker(
                    receiver,
                    writer,
                    wal_dir,
                    sync_policy,
                    max_segment_size_bytes,
                );
            })
            .map_err(EngineError::Io)?;
        Ok(Self { sender })
    }

    /// Appends a batch and waits for the configured durability boundary.
    pub fn append_batch(
        &self,
        entries: &[WalEntry],
        keyring: Option<&StorageKeyring>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let (respond_to, response) = mpsc::channel();
        self.sender
            .send(WalWriterCommand::AppendBatch {
                entries: entries.to_vec(),
                keyring: keyring.cloned(),
                respond_to,
            })
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?;
        response
            .recv()
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?
    }

    /// Closes the active file after applying the configured durability boundary.
    pub fn close_active(&self) -> Result<()> {
        let (respond_to, response) = mpsc::channel();
        self.sender
            .send(WalWriterCommand::CloseActive { respond_to })
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?;
        response
            .recv()
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?
    }

    /// Reopens the active writer at a new sequence after snapshot or suffix replacement.
    pub fn reset(&self, start_sequence: u64) -> Result<()> {
        let (respond_to, response) = mpsc::channel();
        self.sender
            .send(WalWriterCommand::Reset {
                start_sequence,
                respond_to,
            })
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?;
        response
            .recv()
            .map_err(|_| EngineError::Io(std::io::Error::other("wal writer worker closed")))?
    }
}

impl Drop for WalWriterHandle {
    fn drop(&mut self) {
        let _ = self.sender.send(WalWriterCommand::Shutdown);
    }
}

fn run_wal_writer_worker(
    receiver: mpsc::Receiver<WalWriterCommand>,
    mut writer: WalWriter,
    wal_dir: PathBuf,
    sync_policy: WalSyncPolicy,
    max_segment_size_bytes: u64,
) {
    for command in receiver {
        match command {
            WalWriterCommand::AppendBatch {
                entries,
                keyring,
                respond_to,
            } => {
                let result = writer.append_batch(&entries, keyring.as_ref());
                let _ = respond_to.send(result);
            }
            WalWriterCommand::CloseActive { respond_to } => {
                let result = writer.close_active();
                let _ = respond_to.send(result);
            }
            WalWriterCommand::Reset {
                start_sequence,
                respond_to,
            } => {
                let result = WalWriter::open(
                    &wal_dir,
                    sync_policy,
                    max_segment_size_bytes,
                    start_sequence,
                )
                .map(|next| {
                    writer = next;
                });
                let _ = respond_to.send(result);
            }
            WalWriterCommand::Shutdown => break,
        }
    }
}

impl WalWriter {
    /// Opens the active segment, creating one at `start_sequence` when missing.
    pub fn open(
        wal_dir: &Path,
        sync_policy: WalSyncPolicy,
        max_segment_size_bytes: u64,
        start_sequence: u64,
    ) -> Result<Self> {
        fs::create_dir_all(wal_dir)?;
        let active = active_segment_file(wal_dir)?.unwrap_or_else(|| {
            let path = wal_dir.join(format!("active-{start_sequence}.wal"));
            SegmentFile {
                path,
                start_sequence,
                end_sequence: None,
                active: true,
            }
        });
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active.path)?;
        let file_len = file.metadata()?.len();

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            sync_policy,
            max_segment_size_bytes,
            active,
            file: Some(file),
            file_len,
        })
    }

    /// Appends one entry and applies the configured durability boundary.
    #[cfg(test)]
    pub fn append(&mut self, entry: &WalEntry, keyring: Option<&StorageKeyring>) -> Result<()> {
        self.append_batch(std::slice::from_ref(entry), keyring)
    }

    /// Appends a batch of entries and applies one durability boundary per touched segment.
    pub fn append_batch(
        &mut self,
        entries: &[WalEntry],
        keyring: Option<&StorageKeyring>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for entry in entries {
            let written = append_encoded_entry(self.file_mut()?, entry, keyring)?;
            self.file_len = self.file_len.saturating_add(written);
            if self.max_segment_size_bytes > 0 && self.file_len >= self.max_segment_size_bytes {
                let sync_policy = self.sync_policy;
                sync_file(self.file_mut()?, sync_policy)?;
                self.rotate_after(entry.sequence)?;
            }
        }

        let sync_policy = self.sync_policy;
        sync_file(self.file_mut()?, sync_policy)
    }

    /// Closes the active file after applying the configured durability boundary.
    ///
    /// Snapshotting may rename or remove the visible active segment. The writer
    /// must release its file handle before that happens so future appends cannot
    /// continue writing to a stale unlinked file.
    pub fn close_active(&mut self) -> Result<()> {
        let sync_policy = self.sync_policy;
        if let Some(mut file) = self.file.take() {
            sync_file(&mut file, sync_policy)?;
        }
        Ok(())
    }

    fn file_mut(&mut self) -> Result<&mut File> {
        self.file.as_mut().ok_or_else(|| {
            EngineError::Io(std::io::Error::other("wal writer active file is closed"))
        })
    }

    fn rotate_after(&mut self, last_sequence: u64) -> Result<()> {
        let active_path = self.active.path.clone();
        let sealed_path =
            sealed_segment_path(&self.wal_dir, self.active.start_sequence, last_sequence);
        drop(self.file.take());
        fs::rename(active_path, sealed_path)?;
        sync_dir(&self.wal_dir)?;

        let next_start = last_sequence.saturating_add(1);
        let next_path = self.wal_dir.join(format!("active-{next_start}.wal"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_path)?;
        file.sync_all()?;
        sync_dir(&self.wal_dir)?;
        self.active = SegmentFile {
            path: next_path,
            start_sequence: next_start,
            end_sequence: None,
            active: true,
        };
        self.file = Some(file);
        self.file_len = 0;
        Ok(())
    }
}

/// Appends a WAL entry durably to the active segment on disk.
#[cfg(test)]
pub fn append(
    entry: &WalEntry,
    wal_dir: &Path,
    sync_policy: WalSyncPolicy,
    keyring: Option<&StorageKeyring>,
    max_segment_size_bytes: u64,
) -> Result<()> {
    let mut writer = WalWriter::open(wal_dir, sync_policy, max_segment_size_bytes, entry.sequence)?;
    writer.append(entry, keyring)
}

/// Creates a fresh empty active WAL segment starting at the provided sequence.
pub fn create_active_segment(wal_dir: &Path, start_sequence: u64) -> Result<()> {
    fs::create_dir_all(wal_dir)?;
    let path = wal_dir.join(format!("active-{start_sequence}.wal"));
    if !path.exists() {
        File::create(path)?.sync_all()?;
        sync_dir(wal_dir)?;
    }
    Ok(())
}

/// Replays all retained WAL segments in sequence order.
pub fn replay(wal_dir: &Path, keyring: Option<&StorageKeyring>) -> Result<WalReplay> {
    let segments = list_segments(wal_dir)?;
    replay_segments(&segments, keyring)
}

/// Replays WAL entries up to an inclusive sequence or timestamp target.
pub fn replay_until(
    wal_dir: &Path,
    keyring: Option<&StorageKeyring>,
    target: WalReplayTarget,
) -> Result<WalReplay> {
    let replay = replay(wal_dir, keyring)?;
    let filtered = replay
        .entries
        .into_iter()
        .take_while(|entry| match target {
            WalReplayTarget::Sequence(sequence) => entry.sequence <= sequence,
            WalReplayTarget::TimestampMs(timestamp_ms) => entry.created_at_ms <= timestamp_ms,
        })
        .collect::<Vec<_>>();

    let newest_sequence = filtered.last().map(|entry| entry.sequence);
    Ok(WalReplay {
        entries: filtered,
        segment_count: replay.segment_count,
        oldest_retained_sequence: replay.oldest_retained_sequence,
        newest_sequence,
    })
}

/// Returns a summary of the current segmented WAL layout.
pub fn inspect(wal_dir: &Path) -> Result<WalSegmentReport> {
    let segments = list_segments(wal_dir)?;
    let segment_count = segments.len();
    let sealed_segment_count = segments.iter().filter(|segment| !segment.active).count();
    let active_segment_count = segments.iter().filter(|segment| segment.active).count();
    let oldest_retained_sequence = segments.first().map(|segment| segment.start_sequence);
    let active_start_sequence = segments
        .iter()
        .find(|segment| segment.active)
        .map(|segment| segment.start_sequence)
        .unwrap_or_else(|| {
            segments
                .last()
                .and_then(|segment| segment.end_sequence.map(|end| end + 1))
                .unwrap_or(1)
        });
    let newest_sequence = segments
        .iter()
        .filter_map(|segment| segment.end_sequence)
        .max();
    let total_size_bytes = segments
        .iter()
        .map(|segment| {
            fs::metadata(&segment.path)
                .map(|metadata| metadata.len())
                .unwrap_or(0)
        })
        .sum();

    Ok(WalSegmentReport {
        segment_count,
        sealed_segment_count,
        active_segment_count,
        oldest_retained_sequence,
        active_start_sequence,
        newest_sequence,
        total_size_bytes,
    })
}

/// Seals the current active segment, returning its retained range if it contained entries.
pub fn seal_active(wal_dir: &Path, keyring: Option<&StorageKeyring>) -> Result<Option<(u64, u64)>> {
    let Some(active) = active_segment_file(wal_dir)? else {
        return Ok(None);
    };
    let entries = read_segment_entries(&active.path, keyring)?;
    if entries.is_empty() {
        fs::remove_file(&active.path).ok();
        return Ok(None);
    }

    let end_sequence = entries
        .last()
        .map(|entry| entry.sequence)
        .unwrap_or(active.start_sequence);
    let sealed_path = sealed_segment_path(wal_dir, active.start_sequence, end_sequence);
    fs::rename(&active.path, sealed_path)?;
    sync_dir(wal_dir)?;
    Ok(Some((active.start_sequence, end_sequence)))
}

/// Removes oldest sealed segments so that at most `retain_segments` sealed segments remain.
pub fn prune_sealed_segments(wal_dir: &Path, retain_segments: usize) -> Result<usize> {
    prune_sealed_segments_with_floor(wal_dir, retain_segments, None)
}

/// Removes oldest sealed segments so that at most `retain_segments` sealed segments remain, while
/// preserving any segment range that may contain `minimum_sequence_to_keep`.
pub fn prune_sealed_segments_with_floor(
    wal_dir: &Path,
    retain_segments: usize,
    minimum_sequence_to_keep: Option<u64>,
) -> Result<usize> {
    let segments = list_segments(wal_dir)?;
    let sealed = segments
        .into_iter()
        .filter(|segment| !segment.active)
        .collect::<Vec<_>>();
    if sealed.len() <= retain_segments {
        return Ok(0);
    }

    let remove_count = sealed.len() - retain_segments;
    let mut removed = 0;
    for segment in sealed.into_iter().take(remove_count) {
        if let Some(minimum_sequence_to_keep) = minimum_sequence_to_keep {
            let end_sequence = segment.end_sequence.unwrap_or(segment.start_sequence);
            if minimum_sequence_to_keep >= segment.start_sequence
                && minimum_sequence_to_keep <= end_sequence
            {
                continue;
            }
        }
        fs::remove_file(segment.path)?;
        removed += 1;
    }
    Ok(removed)
}

/// Writes a sequence of WAL entries into a fresh segmented WAL directory.
pub fn write_entries(
    entries: &[WalEntry],
    wal_dir: &Path,
    sync_policy: WalSyncPolicy,
    keyring: Option<&StorageKeyring>,
    max_segment_size_bytes: u64,
) -> Result<()> {
    if wal_dir.exists() {
        fs::remove_dir_all(wal_dir)?;
    }
    fs::create_dir_all(wal_dir)?;
    if let Some(first) = entries.first() {
        let mut writer =
            WalWriter::open(wal_dir, sync_policy, max_segment_size_bytes, first.sequence)?;
        writer.append_batch(entries, keyring)?;
    }
    Ok(())
}

/// Migrates a legacy monolithic `wal.log` into the segmented WAL directory.
pub fn migrate_legacy(
    legacy_path: &Path,
    wal_dir: &Path,
    sync_policy: WalSyncPolicy,
    keyring: Option<&StorageKeyring>,
    max_segment_size_bytes: u64,
) -> Result<WalSegmentReport> {
    let entries = replay_legacy(legacy_path, keyring)?;
    write_entries(
        &entries,
        wal_dir,
        sync_policy,
        keyring,
        max_segment_size_bytes,
    )?;
    fs::remove_file(legacy_path).ok();
    inspect(wal_dir)
}

fn append_encoded_entry(
    file: &mut File,
    entry: &WalEntry,
    keyring: Option<&StorageKeyring>,
) -> Result<u64> {
    let bytes = binary::encode(entry).map_err(|err| EngineError::WalSerialize(err.to_string()))?;
    let durable_bytes = match keyring {
        Some(keyring) => encrypt(keyring.active(), "wal entry", &bytes)?,
        None => bytes,
    };
    let length = u32::try_from(durable_bytes.len())
        .map_err(|_| EngineError::Io(std::io::Error::other("wal entry too large")))?;
    let checksum = hash(&durable_bytes);
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(&durable_bytes)?;
    Ok(8_u64.saturating_add(durable_bytes.len() as u64))
}

fn sync_file(file: &mut File, sync_policy: WalSyncPolicy) -> Result<()> {
    match sync_policy {
        WalSyncPolicy::Buffered => {}
        WalSyncPolicy::Flush => {
            file.flush()?;
        }
        WalSyncPolicy::SyncData => {
            file.sync_data()?;
        }
    }
    Ok(())
}

fn sealed_segment_path(wal_dir: &Path, start_sequence: u64, end_sequence: u64) -> PathBuf {
    wal_dir.join(format!("{start_sequence}-{end_sequence}.wal"))
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_: &Path) -> Result<()> {
    Ok(())
}

fn active_segment_file(wal_dir: &Path) -> Result<Option<SegmentFile>> {
    let mut active = list_segments(wal_dir)?
        .into_iter()
        .filter(|segment| segment.active)
        .collect::<Vec<_>>();
    active.sort_by_key(SegmentFile::sort_key);
    Ok(active.into_iter().next())
}

fn list_segments(wal_dir: &Path) -> Result<Vec<SegmentFile>> {
    let mut segments = Vec::new();
    if !wal_dir.exists() {
        return Ok(segments);
    }

    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if let Some(start_sequence) = name
            .strip_prefix("active-")
            .and_then(|value| value.strip_suffix(".wal"))
            .and_then(|value| value.parse::<u64>().ok())
        {
            segments.push(SegmentFile {
                path,
                start_sequence,
                end_sequence: None,
                active: true,
            });
            continue;
        }
        if let Some((start_sequence, end_sequence)) = parse_sealed_segment_name(name) {
            segments.push(SegmentFile {
                path,
                start_sequence,
                end_sequence: Some(end_sequence),
                active: false,
            });
        }
    }

    segments.sort_by_key(SegmentFile::sort_key);
    Ok(segments)
}

fn parse_sealed_segment_name(name: &str) -> Option<(u64, u64)> {
    let value = name.strip_suffix(".wal")?;
    let (start, end) = value.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    Some((start, end))
}

fn replay_segments(
    segments: &[SegmentFile],
    keyring: Option<&StorageKeyring>,
) -> Result<WalReplay> {
    let active_count = segments.iter().filter(|segment| segment.active).count();
    if active_count > 1 {
        return Err(EngineError::WalDeserialize(format!(
            "ambiguous WAL layout: found {active_count} active segments"
        )));
    }

    let mut entries = Vec::new();
    let mut expected_sequence = None;

    for segment in segments {
        let segment_entries = read_segment_entries(&segment.path, keyring)?;
        if segment_entries.is_empty() {
            if segment.active {
                continue;
            }
            return Err(EngineError::WalDeserialize(
                "sealed WAL segment is empty".to_string(),
            ));
        }

        let first_sequence = segment_entries
            .first()
            .map(|entry| entry.sequence)
            .unwrap_or(0);
        if first_sequence != segment.start_sequence {
            return Err(EngineError::WalDeserialize(format!(
                "segment start sequence mismatch for {}",
                segment.path.display()
            )));
        }

        if let Some(end_sequence) = segment.end_sequence {
            let actual_end = segment_entries
                .last()
                .map(|entry| entry.sequence)
                .unwrap_or(0);
            if actual_end != end_sequence {
                return Err(EngineError::WalDeserialize(format!(
                    "segment end sequence mismatch for {}",
                    segment.path.display()
                )));
            }
        }

        for entry in segment_entries {
            if let Some(expected) = expected_sequence
                && entry.sequence != expected
            {
                return Err(EngineError::WalDeserialize(format!(
                    "non-contiguous WAL sequence: expected {expected}, found {}",
                    entry.sequence
                )));
            }
            expected_sequence = Some(entry.sequence + 1);
            entries.push(entry);
        }
    }

    Ok(WalReplay {
        oldest_retained_sequence: entries.first().map(|entry| entry.sequence),
        newest_sequence: entries.last().map(|entry| entry.sequence),
        segment_count: segments.len(),
        entries,
    })
}

fn replay_legacy(path: &Path, keyring: Option<&StorageKeyring>) -> Result<Vec<WalEntry>> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    read_entries_from_reader(&mut file, keyring)
}

fn read_segment_entries(path: &Path, keyring: Option<&StorageKeyring>) -> Result<Vec<WalEntry>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    read_entries_from_reader(&mut file, keyring)
}

fn read_entries_from_reader<R: Read>(
    reader: &mut R,
    keyring: Option<&StorageKeyring>,
) -> Result<Vec<WalEntry>> {
    let mut entries = Vec::new();

    loop {
        let mut length_buf = [0u8; 4];
        match reader.read(&mut length_buf[..1]) {
            Ok(0) => break,
            Ok(_) => {}
            Err(err) => return Err(err.into()),
        }
        match reader.read_exact(&mut length_buf[1..]) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Err(EngineError::WalDeserialize(
                    "truncated WAL length header".to_string(),
                ));
            }
            Err(err) => return Err(err.into()),
        }

        let length = u32::from_le_bytes(length_buf);
        if length == 0 || length > MAX_WAL_ENTRY_SIZE {
            return Err(EngineError::WalDeserialize(format!(
                "invalid WAL entry length {length}"
            )));
        }

        let mut checksum_buf = [0u8; 4];
        match reader.read_exact(&mut checksum_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Err(EngineError::WalDeserialize(
                    "truncated WAL checksum".to_string(),
                ));
            }
            Err(err) => return Err(err.into()),
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let mut entry_buf = vec![0u8; length as usize];
        match reader.read_exact(&mut entry_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                return Err(EngineError::WalDeserialize(
                    "truncated WAL payload".to_string(),
                ));
            }
            Err(err) => return Err(err.into()),
        }

        if hash(&entry_buf) != expected_checksum {
            return Err(EngineError::ChecksumMismatch {
                resource: "wal entry",
            });
        }

        let plain_bytes = match keyring {
            Some(keyring) => decrypt(keyring, "wal entry", &entry_buf)?,
            None => entry_buf,
        };

        let entry: WalEntry = binary::decode(&plain_bytes)
            .map_err(|err| EngineError::WalDeserialize(err.to_string()))?;
        entries.push(entry);
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use crate::store::crypto::encrypt;
    use crate::{StorageKey, StorageKeyring};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    use super::{
        WalEntry, WalOperation, WalReplayTarget, WalWriter, append, append_encoded_entry, inspect,
        migrate_legacy, replay, replay_until, seal_active, sealed_segment_path,
    };
    use crate::WalSyncPolicy;

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-{name}-{unique}"))
    }

    fn legacy_path(root: &Path) -> PathBuf {
        root.join("wal.log")
    }

    fn keyring(secret: &str) -> StorageKeyring {
        StorageKeyring {
            active: StorageKey {
                id: Uuid::from_u128(1),
                secret: secret.to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        }
    }

    fn sample_entry(sequence: u64) -> WalEntry {
        WalEntry::new(
            sequence,
            0,
            100 + sequence,
            vec![WalOperation::Set {
                key: format!("name:{sequence}"),
                value: b"alice".to_vec(),
                version: 1,
            }],
        )
    }

    #[test]
    fn appends_and_replays_segmented_entries() {
        let root = temp_dir("wal-round-trip");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");

        append(
            &sample_entry(1),
            &wal_dir,
            WalSyncPolicy::Flush,
            Some(&keyring),
            1024,
        )
        .unwrap();
        append(
            &sample_entry(2),
            &wal_dir,
            WalSyncPolicy::Flush,
            Some(&keyring),
            1024,
        )
        .unwrap();

        let replay = replay(&wal_dir, Some(&keyring)).unwrap();
        assert_eq!(replay.entries.len(), 2);
        assert_eq!(replay.entries[0].sequence, 1);
        assert_eq!(replay.entries[1].sequence, 2);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rotates_and_seals_segments_when_threshold_is_hit() {
        let root = temp_dir("wal-rotate");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");

        append(
            &sample_entry(1),
            &wal_dir,
            WalSyncPolicy::Flush,
            Some(&keyring),
            1,
        )
        .unwrap();

        let report = inspect(&wal_dir).unwrap();
        assert_eq!(report.sealed_segment_count, 1);
        assert_eq!(report.active_start_sequence, 2);
        assert!(sealed_segment_path(&wal_dir, 1, 1).exists());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn writer_appends_batches_and_rotates_at_entry_boundaries() {
        let root = temp_dir("wal-writer-batch");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");
        let entries = [sample_entry(1), sample_entry(2), sample_entry(3)];

        let mut writer = WalWriter::open(&wal_dir, WalSyncPolicy::Flush, 1, 1).unwrap();
        writer.append_batch(&entries, Some(&keyring)).unwrap();
        drop(writer);

        let replay = replay(&wal_dir, Some(&keyring)).unwrap();
        assert_eq!(replay.entries, entries);

        let report = inspect(&wal_dir).unwrap();
        assert_eq!(report.sealed_segment_count, 3);
        assert_eq!(report.active_start_sequence, 4);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn seal_active_returns_range_for_non_empty_segment() {
        let root = temp_dir("wal-seal");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");

        append(
            &sample_entry(1),
            &wal_dir,
            WalSyncPolicy::Flush,
            Some(&keyring),
            1024,
        )
        .unwrap();

        let range = seal_active(&wal_dir, Some(&keyring)).unwrap().unwrap();
        assert_eq!(range, (1, 1));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn replays_until_sequence_or_timestamp() {
        let root = temp_dir("wal-replay-until");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");

        for sequence in 1..=3 {
            append(
                &sample_entry(sequence),
                &wal_dir,
                WalSyncPolicy::Flush,
                Some(&keyring),
                1024,
            )
            .unwrap();
        }

        let by_sequence =
            replay_until(&wal_dir, Some(&keyring), WalReplayTarget::Sequence(2)).unwrap();
        assert_eq!(by_sequence.entries.len(), 2);

        let by_time =
            replay_until(&wal_dir, Some(&keyring), WalReplayTarget::TimestampMs(102)).unwrap();
        assert_eq!(by_time.entries.len(), 2);

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn migrates_legacy_wal_into_segmented_layout() {
        let root = temp_dir("wal-migrate");
        fs::create_dir_all(&root).unwrap();
        let path = legacy_path(&root);
        let keyring = keyring("wal-key");
        let mut legacy = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        for entry in [sample_entry(1), sample_entry(2)] {
            let bytes = crate::store::binary::encode(&entry).unwrap();
            let durable_bytes = encrypt(keyring.active(), "wal entry", &bytes).unwrap();
            let length = u32::try_from(durable_bytes.len()).unwrap();
            legacy.write_all(&length.to_le_bytes()).unwrap();
            legacy
                .write_all(&crc32fast::hash(&durable_bytes).to_le_bytes())
                .unwrap();
            legacy.write_all(&durable_bytes).unwrap();
        }
        legacy.flush().unwrap();

        let report = migrate_legacy(
            &path,
            &root.join("wal"),
            WalSyncPolicy::Flush,
            Some(&keyring),
            1024,
        )
        .unwrap();
        assert_eq!(report.segment_count, 1);
        assert!(!path.exists());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_corrupt_segment_payloads() {
        let root = temp_dir("wal-corrupt");
        let wal_dir = root.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        let corrupt_path = wal_dir.join("active-1.wal");
        fs::write(&corrupt_path, [1, 2, 3, 4, 5]).unwrap();

        assert!(replay(&wal_dir, None).is_err());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_segment_payload_checksum_mismatch() {
        let root = temp_dir("wal-checksum");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");

        append(
            &sample_entry(1),
            &wal_dir,
            WalSyncPolicy::Flush,
            Some(&keyring),
            1024,
        )
        .unwrap();
        let active_path = wal_dir.join("active-1.wal");
        let mut bytes = fs::read(&active_path).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 0xff;
        fs::write(&active_path, bytes).unwrap();

        assert!(replay(&wal_dir, Some(&keyring)).is_err());

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn rejects_multiple_active_segments_as_ambiguous_recovery_state() {
        let root = temp_dir("wal-multiple-active");
        let wal_dir = root.join("wal");
        let keyring = keyring("wal-key");
        fs::create_dir_all(&wal_dir).unwrap();

        let mut first = WalWriter::open(&wal_dir, WalSyncPolicy::Flush, 1024, 1).unwrap();
        first.append(&sample_entry(1), Some(&keyring)).unwrap();
        drop(first);

        let mut second = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_dir.join("active-2.wal"))
            .unwrap();
        append_encoded_entry(&mut second, &sample_entry(2), Some(&keyring)).unwrap();
        second.flush().unwrap();

        let err = replay(&wal_dir, Some(&keyring)).unwrap_err();
        assert!(
            err.to_string().contains("ambiguous WAL layout"),
            "unexpected error: {err}"
        );

        fs::remove_dir_all(root).ok();
    }
}
