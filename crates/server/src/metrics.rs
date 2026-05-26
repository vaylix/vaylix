use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide counters collected by the server runtime.
#[derive(Default)]
pub struct Metrics {
    pub accepted_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub completed_connections: AtomicU64,
    pub requests_total: AtomicU64,
    pub auth_successes: AtomicU64,
    pub auth_failures: AtomicU64,
    pub transactions_started: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_discarded: AtomicU64,
    pub idle_disconnects: AtomicU64,
    pub snapshots_completed: AtomicU64,
    pub expiration_sweeps: AtomicU64,
    pub expired_keys_removed: AtomicU64,
}

impl Metrics {
    /// Renders the metrics as key/value pairs suitable for `INFO` or `METRICS`.
    pub fn snapshot(&self) -> Vec<(String, String)> {
        [
            (
                "accepted_connections",
                self.accepted_connections.load(Ordering::Relaxed),
            ),
            (
                "active_connections",
                self.active_connections.load(Ordering::Relaxed),
            ),
            (
                "completed_connections",
                self.completed_connections.load(Ordering::Relaxed),
            ),
            ("requests_total", self.requests_total.load(Ordering::Relaxed)),
            ("auth_successes", self.auth_successes.load(Ordering::Relaxed)),
            ("auth_failures", self.auth_failures.load(Ordering::Relaxed)),
            (
                "transactions_started",
                self.transactions_started.load(Ordering::Relaxed),
            ),
            (
                "transactions_committed",
                self.transactions_committed.load(Ordering::Relaxed),
            ),
            (
                "transactions_discarded",
                self.transactions_discarded.load(Ordering::Relaxed),
            ),
            ("idle_disconnects", self.idle_disconnects.load(Ordering::Relaxed)),
            (
                "snapshots_completed",
                self.snapshots_completed.load(Ordering::Relaxed),
            ),
            (
                "expiration_sweeps",
                self.expiration_sweeps.load(Ordering::Relaxed),
            ),
            (
                "expired_keys_removed",
                self.expired_keys_removed.load(Ordering::Relaxed),
            ),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }
}
