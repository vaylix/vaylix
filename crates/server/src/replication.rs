use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::{Result, ServerError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationRole {
    Standalone,
    Leader,
    Follower,
}

impl ReplicationRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Leader => "leader",
            Self::Follower => "follower",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAckMode {
    Local,
    Replica,
    All,
}

impl WriteAckMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Replica => "replica",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FollowerPhase {
    Bootstrap,
    SnapshotSync,
    CatchingUp,
    Streaming,
    Stale,
    Paused,
}

impl FollowerPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::SnapshotSync => "snapshot_sync",
            Self::CatchingUp => "catching_up",
            Self::Streaming => "streaming",
            Self::Stale => "stale",
            Self::Paused => "paused",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub node_id: String,
    pub group_id: String,
    pub advertise_addr: Option<String>,
    pub role: ReplicationRole,
    pub upstream: Option<String>,
    pub upstream_username: Option<String>,
    pub upstream_password: Option<String>,
    pub write_ack_mode: WriteAckMode,
    pub ack_timeout: Duration,
    pub poll_interval: Duration,
    pub fetch_batch_size: usize,
    pub stale_after: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatusSnapshot {
    pub node_id: String,
    pub group_id: String,
    pub role: String,
    pub advertise_addr: Option<String>,
    pub leader_node_id: Option<String>,
    pub upstream: Option<String>,
    pub write_ack_mode: String,
    pub paused: bool,
    pub health: String,
    pub reason: Option<String>,
    pub local_last_applied_sequence: u64,
    pub commit_sequence: u64,
    pub retention_floor_sequence: Option<u64>,
    pub follower_phase: Option<String>,
    pub follower_lag_entries: Option<u64>,
    pub follower_lag_ms: Option<u64>,
    pub known_followers: usize,
    pub followers: Vec<ReplicationFollowerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationFollowerSnapshot {
    pub node_id: String,
    pub applied_sequence: u64,
    pub lag_entries: u64,
    pub lag_ms: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationFetchRequest {
    pub after_sequence: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationAckRequest {
    pub follower_node_id: String,
    pub applied_sequence: u64,
}

#[derive(Debug, Clone)]
struct FollowerProgress {
    applied_sequence: u64,
    last_ack_at: Instant,
}

#[derive(Debug, Clone)]
struct ReplicationState {
    role: ReplicationRole,
    paused: bool,
    leader_node_id: Option<String>,
    local_last_applied_sequence: u64,
    follower_phase: Option<FollowerPhase>,
    follower_lag_entries: Option<u64>,
    follower_lag_ms: Option<u64>,
    health: String,
    reason: Option<String>,
    followers: BTreeMap<String, FollowerProgress>,
}

#[derive(Clone)]
pub struct ReplicationRuntime {
    config: ReplicationConfig,
    state: Arc<Mutex<ReplicationState>>,
}

impl ReplicationRuntime {
    pub fn new(config: ReplicationConfig) -> Self {
        let role = config.role;
        let follower_phase = match role {
            ReplicationRole::Follower => Some(FollowerPhase::Bootstrap),
            _ => None,
        };
        Self {
            config,
            state: Arc::new(Mutex::new(ReplicationState {
                role,
                paused: false,
                leader_node_id: None,
                local_last_applied_sequence: 0,
                follower_phase,
                follower_lag_entries: None,
                follower_lag_ms: None,
                health: "ready".to_string(),
                reason: None,
                followers: BTreeMap::new(),
            })),
        }
    }

    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    pub async fn role(&self) -> ReplicationRole {
        self.state.lock().await.role
    }

    pub async fn is_paused(&self) -> bool {
        self.state.lock().await.paused
    }

    pub async fn set_paused(&self, paused: bool) {
        let mut state = self.state.lock().await;
        state.paused = paused;
        if state.role == ReplicationRole::Follower {
            state.follower_phase = Some(if paused {
                FollowerPhase::Paused
            } else {
                FollowerPhase::Bootstrap
            });
        }
    }

    pub async fn set_leader_node_id(&self, node_id: Option<String>) {
        self.state.lock().await.leader_node_id = node_id;
    }

    pub async fn set_local_last_applied_sequence(&self, sequence: u64) {
        self.state.lock().await.local_last_applied_sequence = sequence;
    }

    pub async fn update_follower_phase(
        &self,
        phase: FollowerPhase,
        lag_entries: Option<u64>,
        lag_ms: Option<u64>,
    ) {
        let mut state = self.state.lock().await;
        state.follower_phase = Some(phase);
        state.follower_lag_entries = lag_entries;
        state.follower_lag_ms = lag_ms;
        if phase == FollowerPhase::Stale {
            state.health = "degraded".to_string();
            state.reason = Some("replication_stale".to_string());
        } else if phase == FollowerPhase::Paused {
            state.health = "degraded".to_string();
            state.reason = Some("replication_paused".to_string());
        } else {
            state.health = "ready".to_string();
            state.reason = None;
        }
    }

    pub async fn register_follower_ack(&self, follower_node_id: String, applied_sequence: u64) {
        let mut state = self.state.lock().await;
        state.followers.insert(
            follower_node_id,
            FollowerProgress {
                applied_sequence,
                last_ack_at: Instant::now(),
            },
        );
    }

    pub async fn wait_for_write_ack(&self, sequence: u64) -> Result<()> {
        if self.config.write_ack_mode == WriteAckMode::Local {
            return Ok(());
        }

        let deadline = Instant::now() + self.config.ack_timeout;
        loop {
            {
                let state = self.state.lock().await;
                let follower_count = state.followers.len();
                if follower_count == 0 {
                    if Instant::now() >= deadline {
                        return Err(ServerError::ReplicationAckUnavailable);
                    }
                } else {
                    let matched = state
                        .followers
                        .values()
                        .filter(|progress| progress.applied_sequence >= sequence)
                        .count();
                    let satisfied = match self.config.write_ack_mode {
                        WriteAckMode::Local => true,
                        WriteAckMode::Replica => matched >= 1,
                        WriteAckMode::All => matched == follower_count,
                    };
                    if satisfied {
                        return Ok(());
                    }
                }
            }

            if Instant::now() >= deadline {
                return Err(ServerError::ReplicationAckTimeout {
                    sequence,
                    mode: self.config.write_ack_mode.as_str().to_string(),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn retention_floor_sequence(&self) -> Option<u64> {
        let state = self.state.lock().await;
        state
            .followers
            .values()
            .map(|progress| progress.applied_sequence.saturating_add(1))
            .min()
    }

    pub async fn promote_follower(&self, maintenance_enabled: bool) -> Result<()> {
        if !maintenance_enabled {
            return Err(ServerError::ReplicationPromotionDenied(
                "promotion requires maintenance mode".to_string(),
            ));
        }
        let mut state = self.state.lock().await;
        if state.role != ReplicationRole::Follower {
            return Err(ServerError::ReplicationPromotionDenied(
                "only followers may be promoted".to_string(),
            ));
        }
        state.role = ReplicationRole::Leader;
        state.follower_phase = None;
        state.leader_node_id = None;
        state.health = "ready".to_string();
        state.reason = None;
        Ok(())
    }

    pub async fn snapshot(&self) -> ReplicationStatusSnapshot {
        let state = self.state.lock().await;
        let now = Instant::now();
        let followers = state
            .followers
            .iter()
            .map(|(node_id, progress)| ReplicationFollowerSnapshot {
                node_id: node_id.clone(),
                applied_sequence: progress.applied_sequence,
                lag_entries: state
                    .local_last_applied_sequence
                    .saturating_sub(progress.applied_sequence),
                lag_ms: now.duration_since(progress.last_ack_at).as_millis() as u64,
                stale: now.duration_since(progress.last_ack_at) > self.config.stale_after,
            })
            .collect::<Vec<_>>();
        ReplicationStatusSnapshot {
            node_id: self.config.node_id.clone(),
            group_id: self.config.group_id.clone(),
            role: state.role.as_str().to_string(),
            advertise_addr: self.config.advertise_addr.clone(),
            leader_node_id: state.leader_node_id.clone(),
            upstream: self.config.upstream.clone(),
            write_ack_mode: self.config.write_ack_mode.as_str().to_string(),
            paused: state.paused,
            health: state.health.clone(),
            reason: state.reason.clone(),
            local_last_applied_sequence: state.local_last_applied_sequence,
            commit_sequence: state.local_last_applied_sequence,
            retention_floor_sequence: followers
                .iter()
                .map(|follower| follower.applied_sequence.saturating_add(1))
                .min(),
            follower_phase: state.follower_phase.map(|phase| phase.as_str().to_string()),
            follower_lag_entries: state.follower_lag_entries,
            follower_lag_ms: state.follower_lag_ms,
            known_followers: followers.len(),
            followers,
        }
    }
}
