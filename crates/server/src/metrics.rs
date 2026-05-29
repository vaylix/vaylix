use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricKind {
    Counter,
    Gauge,
}

impl MetricKind {
    fn as_prometheus_type(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricId {
    AcceptedConnections,
    ActiveConnections,
    CompletedConnections,
    RequestCount,
    AuthSuccessCount,
    AuthFailureCount,
    AuthLockedAttemptCount,
    TransactionBeginCount,
    TransactionCommitCount,
    TransactionDiscardCount,
    TransactionTimeoutCount,
    IdleDisconnectCount,
    SnapshotCompletedCount,
    ExpirationSweepCount,
    ExpiredKeyRemovedCount,
    SlowCommandCount,
    WalEntryReplayedCount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetricDescriptor {
    id: MetricId,
    otel_name: &'static str,
    unit: &'static str,
    help: &'static str,
    kind: MetricKind,
}

impl MetricDescriptor {
    fn prometheus_name(self) -> String {
        self.otel_name.replace('.', "_")
    }
}

const METRIC_DESCRIPTORS: &[MetricDescriptor] = &[
    MetricDescriptor {
        id: MetricId::AcceptedConnections,
        otel_name: "vaylix.server.connection.accepted.count",
        unit: "{connection}",
        help: "Number of client connections accepted by the Vaylix server.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::ActiveConnections,
        otel_name: "vaylix.server.connection.active",
        unit: "{connection}",
        help: "Current number of active client connections handled by the Vaylix server.",
        kind: MetricKind::Gauge,
    },
    MetricDescriptor {
        id: MetricId::CompletedConnections,
        otel_name: "vaylix.server.connection.completed.count",
        unit: "{connection}",
        help: "Number of client connections completed by the Vaylix server.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::RequestCount,
        otel_name: "vaylix.server.request.count",
        unit: "{request}",
        help: "Number of transport requests processed by the Vaylix server.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::AuthSuccessCount,
        otel_name: "vaylix.server.auth.success.count",
        unit: "{authentication}",
        help: "Number of successful authentication attempts.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::AuthFailureCount,
        otel_name: "vaylix.server.auth.failure.count",
        unit: "{authentication}",
        help: "Number of failed authentication attempts.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::AuthLockedAttemptCount,
        otel_name: "vaylix.server.auth.locked.attempt.count",
        unit: "{authentication}",
        help: "Number of authentication attempts rejected due to an active lockout.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::TransactionBeginCount,
        otel_name: "vaylix.server.transaction.begin.count",
        unit: "{transaction}",
        help: "Number of transactions started with MULTI.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::TransactionCommitCount,
        otel_name: "vaylix.server.transaction.commit.count",
        unit: "{transaction}",
        help: "Number of transactions committed with EXEC.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::TransactionDiscardCount,
        otel_name: "vaylix.server.transaction.discard.count",
        unit: "{transaction}",
        help: "Number of transactions discarded explicitly or due to state changes.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::TransactionTimeoutCount,
        otel_name: "vaylix.server.transaction.timeout.count",
        unit: "{transaction}",
        help: "Number of transactions discarded after exceeding the configured lifetime.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::IdleDisconnectCount,
        otel_name: "vaylix.server.connection.idle.disconnect.count",
        unit: "{disconnect}",
        help: "Number of connections closed after exceeding the idle timeout.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::SnapshotCompletedCount,
        otel_name: "vaylix.persistence.snapshot.completed.count",
        unit: "{snapshot}",
        help: "Number of completed physical snapshots.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::ExpirationSweepCount,
        otel_name: "vaylix.storage.expiration.sweep.count",
        unit: "{sweep}",
        help: "Number of expiration sweeps completed by the background reaper.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::ExpiredKeyRemovedCount,
        otel_name: "vaylix.storage.expired.key.removed.count",
        unit: "{key}",
        help: "Number of expired keys removed by expiration sweeps.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::SlowCommandCount,
        otel_name: "vaylix.server.command.slow.count",
        unit: "{command}",
        help: "Number of commands recorded as slow relative to the configured threshold.",
        kind: MetricKind::Counter,
    },
    MetricDescriptor {
        id: MetricId::WalEntryReplayedCount,
        otel_name: "vaylix.persistence.wal.entry.replayed.count",
        unit: "{entry}",
        help: "Number of WAL entries replayed during recovery.",
        kind: MetricKind::Counter,
    },
];

/// Process-wide counters collected by the server runtime.
#[derive(Default)]
pub struct Metrics {
    pub accepted_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub completed_connections: AtomicU64,
    pub requests_total: AtomicU64,
    pub auth_successes: AtomicU64,
    pub auth_failures: AtomicU64,
    pub locked_auth_attempts_total: AtomicU64,
    pub transactions_started: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_discarded: AtomicU64,
    pub transactions_timed_out: AtomicU64,
    pub idle_disconnects: AtomicU64,
    pub snapshots_completed: AtomicU64,
    pub expiration_sweeps: AtomicU64,
    pub expired_keys_removed: AtomicU64,
    pub slow_commands_total: AtomicU64,
    pub wal_entries_replayed_total: AtomicU64,
}

impl Metrics {
    fn load(&self, metric_id: MetricId) -> u64 {
        match metric_id {
            MetricId::AcceptedConnections => self.accepted_connections.load(Ordering::Relaxed),
            MetricId::ActiveConnections => self.active_connections.load(Ordering::Relaxed),
            MetricId::CompletedConnections => self.completed_connections.load(Ordering::Relaxed),
            MetricId::RequestCount => self.requests_total.load(Ordering::Relaxed),
            MetricId::AuthSuccessCount => self.auth_successes.load(Ordering::Relaxed),
            MetricId::AuthFailureCount => self.auth_failures.load(Ordering::Relaxed),
            MetricId::AuthLockedAttemptCount => {
                self.locked_auth_attempts_total.load(Ordering::Relaxed)
            }
            MetricId::TransactionBeginCount => self.transactions_started.load(Ordering::Relaxed),
            MetricId::TransactionCommitCount => self.transactions_committed.load(Ordering::Relaxed),
            MetricId::TransactionDiscardCount => {
                self.transactions_discarded.load(Ordering::Relaxed)
            }
            MetricId::TransactionTimeoutCount => {
                self.transactions_timed_out.load(Ordering::Relaxed)
            }
            MetricId::IdleDisconnectCount => self.idle_disconnects.load(Ordering::Relaxed),
            MetricId::SnapshotCompletedCount => self.snapshots_completed.load(Ordering::Relaxed),
            MetricId::ExpirationSweepCount => self.expiration_sweeps.load(Ordering::Relaxed),
            MetricId::ExpiredKeyRemovedCount => self.expired_keys_removed.load(Ordering::Relaxed),
            MetricId::SlowCommandCount => self.slow_commands_total.load(Ordering::Relaxed),
            MetricId::WalEntryReplayedCount => {
                self.wal_entries_replayed_total.load(Ordering::Relaxed)
            }
        }
    }

    /// Renders the metrics as key/value pairs suitable for `INFO` or `METRICS`.
    pub fn snapshot(&self) -> Vec<(String, String)> {
        METRIC_DESCRIPTORS
            .iter()
            .map(|descriptor| {
                (
                    descriptor.otel_name.to_string(),
                    self.load(descriptor.id).to_string(),
                )
            })
            .collect()
    }

    /// Renders counters and gauges in Prometheus text exposition format.
    pub fn prometheus(&self) -> String {
        let mut lines = Vec::new();
        for descriptor in METRIC_DESCRIPTORS {
            let metric = descriptor.prometheus_name();
            let value = self.load(descriptor.id);
            lines.push(format!(
                "# HELP {metric} {} Unit: {}.",
                descriptor.help, descriptor.unit
            ));
            lines.push(format!(
                "# TYPE {metric} {}",
                descriptor.kind.as_prometheus_type()
            ));
            lines.push(format!("{metric} {value}"));
        }
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::{METRIC_DESCRIPTORS, Metrics};
    use std::collections::HashSet;

    #[test]
    fn metric_contract_uses_otel_style_names() {
        let mut seen = HashSet::new();
        for descriptor in METRIC_DESCRIPTORS {
            assert!(descriptor.otel_name.starts_with("vaylix."));
            assert!(!descriptor.otel_name.contains('_'));
            assert!(!descriptor.otel_name.ends_with(".total"));
            assert!(seen.insert(descriptor.otel_name));
        }
    }

    #[test]
    fn prometheus_export_translates_otel_names() {
        let metrics = Metrics::default();
        let body = metrics.prometheus();
        assert!(body.contains("# HELP vaylix_server_request_count "));
        assert!(body.contains("# TYPE vaylix_server_request_count counter"));
        assert!(body.contains("# TYPE vaylix_server_connection_active gauge"));
        assert!(!body.contains("_total"));
    }
}
