use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::error::Result;

/// Durable maintenance-mode switch backed by a sentinel file.
///
/// The server reads the sentinel on startup and rewrites it whenever operators
/// change maintenance state through the command surface. This keeps restarts
/// from accidentally re-enabling writes during planned maintenance.
pub struct MaintenanceMode {
    path: PathBuf,
    enabled: AtomicBool,
}

impl MaintenanceMode {
    /// Loads maintenance mode from the configured sentinel path.
    pub fn load(path: PathBuf) -> Result<Self> {
        Ok(Self {
            enabled: AtomicBool::new(path.exists()),
            path,
        })
    }

    /// Returns the current in-memory maintenance state.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Persists and publishes the maintenance state.
    pub fn set(&self, enabled: bool) -> Result<()> {
        if enabled {
            std::fs::write(&self.path, b"maintenance=on\n")?;
        } else if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        self.enabled.store(enabled, Ordering::Relaxed);
        Ok(())
    }

    /// Returns the sentinel path used for persistence.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Default)]
pub struct AuthLockoutState {
    records: BTreeMap<String, AuthLockoutRecord>,
}

struct AuthLockoutRecord {
    first_failure_at_ms: u64,
    failure_count: u32,
    locked_until_ms: Option<u64>,
}

impl AuthLockoutState {
    /// Counts currently active lockouts and drops expired records.
    pub fn active_lockout_count(&mut self, now_ms: u64) -> usize {
        self.records
            .retain(|_, record| record.locked_until_ms.unwrap_or(now_ms) > now_ms);
        self.records
            .values()
            .filter(|record| record.locked_until_ms.unwrap_or(0) > now_ms)
            .count()
    }

    /// Returns the remaining lockout duration for a user/source key.
    pub fn remaining_lockout_seconds(&mut self, key: &str, now_ms: u64) -> Option<u64> {
        let remaining_ms = self
            .records
            .get(key)
            .and_then(|record| record.locked_until_ms)
            .and_then(|locked_until_ms| locked_until_ms.checked_sub(now_ms))?;
        Some(remaining_ms.div_ceil(1_000))
    }

    /// Clears failure history after successful authentication.
    pub fn clear_success(&mut self, key: &str) {
        self.records.remove(key);
    }

    /// Records a failed authentication attempt and returns true when it locks.
    pub fn record_failure(
        &mut self,
        key: &str,
        now_ms: u64,
        failure_window: Duration,
        failure_limit: u32,
        lockout: Duration,
    ) -> bool {
        let record = self
            .records
            .entry(key.to_string())
            .or_insert(AuthLockoutRecord {
                first_failure_at_ms: now_ms,
                failure_count: 0,
                locked_until_ms: None,
            });

        if now_ms.saturating_sub(record.first_failure_at_ms) > failure_window.as_millis() as u64 {
            record.first_failure_at_ms = now_ms;
            record.failure_count = 0;
            record.locked_until_ms = None;
        }

        record.failure_count = record.failure_count.saturating_add(1);
        if record.failure_count >= failure_limit {
            record.locked_until_ms = Some(now_ms.saturating_add(lockout.as_millis() as u64));
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthLockoutState, MaintenanceMode};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-runtime-state-{name}-{unique}.mode"))
    }

    #[test]
    fn maintenance_mode_persists_to_sentinel_file() {
        let path = temp_path("maintenance");
        let mode = MaintenanceMode::load(path.clone()).unwrap();
        assert!(!mode.is_enabled());

        mode.set(true).unwrap();
        assert!(mode.is_enabled());
        assert!(path.exists());

        let reloaded = MaintenanceMode::load(path.clone()).unwrap();
        assert!(reloaded.is_enabled());

        reloaded.set(false).unwrap();
        assert!(!reloaded.is_enabled());
        assert!(!path.exists());
    }

    #[test]
    fn auth_lockout_counts_only_active_records() {
        let mut lockouts = AuthLockoutState::default();
        assert!(!lockouts.record_failure(
            "alice@127.0.0.1",
            1_000,
            Duration::from_secs(60),
            2,
            Duration::from_secs(30),
        ));
        assert!(lockouts.record_failure(
            "alice@127.0.0.1",
            1_100,
            Duration::from_secs(60),
            2,
            Duration::from_secs(30),
        ));
        assert_eq!(lockouts.active_lockout_count(1_200), 1);
        assert_eq!(
            lockouts.remaining_lockout_seconds("alice@127.0.0.1", 1_200),
            Some(30)
        );
        assert_eq!(lockouts.active_lockout_count(31_200), 0);
    }

    #[test]
    fn auth_lockout_expires_and_failure_window_resets() {
        let mut lockouts = AuthLockoutState::default();
        let key = "alice@127.0.0.1";

        assert!(!lockouts.record_failure(
            key,
            1_000,
            Duration::from_secs(1),
            2,
            Duration::from_secs(5),
        ));
        assert!(!lockouts.record_failure(
            key,
            3_000,
            Duration::from_secs(1),
            2,
            Duration::from_secs(5),
        ));
        assert_eq!(lockouts.remaining_lockout_seconds(key, 3_000), None);

        assert!(lockouts.record_failure(
            key,
            3_100,
            Duration::from_secs(1),
            2,
            Duration::from_secs(5),
        ));
        assert_eq!(lockouts.remaining_lockout_seconds(key, 8_200), None);
        assert_eq!(lockouts.active_lockout_count(8_200), 0);
    }
}
