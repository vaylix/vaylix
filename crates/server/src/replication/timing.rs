use rand::RngExt;
use std::time::{Duration, Instant};

use super::{ReplicationConfig, ReplicationState};

pub(super) fn leader_lease_duration(config: &ReplicationConfig) -> Duration {
    config
        .stale_after
        .max(config.election_timeout_max.saturating_mul(4))
}

pub(super) fn recently_heard_from_known_leader(
    config: &ReplicationConfig,
    state: &ReplicationState,
) -> bool {
    state.leader_node_id.is_some()
        && Instant::now().duration_since(state.last_heartbeat_at) < leader_lease_duration(config)
}

pub(super) fn random_election_timeout(config: &ReplicationConfig) -> Duration {
    if config.election_timeout_max <= config.election_timeout_min {
        return config.election_timeout_min;
    }
    let min = config.election_timeout_min.as_millis() as u64;
    let max = config.election_timeout_max.as_millis() as u64;
    let jitter = rand::rng().random_range(min..=max);
    Duration::from_millis(jitter)
}
