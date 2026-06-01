use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub profile: String,
    pub addr: String,
    pub connections: usize,
    pub duration_seconds: u64,
    pub keyspace: usize,
    pub value_size: usize,
    pub seed_keys: usize,
    pub completed_operations: u64,
    pub failed_operations: u64,
    pub operations_per_second: f64,
    pub latency_us: LatencySummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
    pub mean: f64,
}

impl LatencySummary {
    pub fn zero() -> Self {
        Self {
            min: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
            mean: 0.0,
        }
    }
}
