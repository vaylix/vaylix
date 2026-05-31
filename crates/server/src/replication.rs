use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::error::{Result, ServerError};
use crate::server::log_event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationRole {
    Standalone,
    Leader,
    Follower,
    Candidate,
}

impl ReplicationRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Leader => "leader",
            Self::Follower => "follower",
            Self::Candidate => "candidate",
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
            Self::Replica => "majority",
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMember {
    pub node_id: String,
    pub advertise_addr: String,
    #[serde(default = "default_true")]
    pub voter: bool,
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
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub state_path: PathBuf,
    pub state_tmp_path: PathBuf,
    pub initial_members: Vec<ClusterMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatusSnapshot {
    pub node_id: String,
    pub group_id: String,
    pub role: String,
    pub advertise_addr: Option<String>,
    pub leader_node_id: Option<String>,
    pub leader_advertise_addr: Option<String>,
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
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub quorum_size: usize,
    pub members: Vec<ClusterMember>,
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
    pub term: u64,
    pub leader_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_node_id: String,
    pub candidate_addr: String,
    pub last_log_index: u64,
    pub last_log_term: Option<u64>,
    #[serde(default)]
    pub prevote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub term: u64,
    pub leader_node_id: String,
    pub leader_addr: String,
    pub commit_sequence: u64,
    pub leader_frontier_sequence: u64,
    pub members: Vec<ClusterMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub term: u64,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_node_id: String,
    pub leader_addr: String,
    pub commit_sequence: u64,
    pub leader_frontier_sequence: u64,
    pub prev_sequence: u64,
    pub prev_term: Option<u64>,
    pub prev_entry_checksum: Option<u32>,
    pub entries: Vec<engine::WalEntry>,
    pub members: Vec<ClusterMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub accepted: bool,
    pub match_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInstallRequest {
    pub term: u64,
    pub leader_node_id: String,
    pub leader_addr: String,
    pub commit_sequence: u64,
    pub members: Vec<ClusterMember>,
    pub snapshot: engine::ReplicationSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInstallResponse {
    pub term: u64,
    pub accepted: bool,
    pub applied_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedReplicationState {
    current_term: u64,
    voted_for: Option<String>,
    members: Vec<ClusterMember>,
}

#[derive(Debug, Clone)]
struct FollowerProgress {
    match_index: u64,
    next_sequence: u64,
    last_ack_at: Instant,
    last_ack_term: u64,
}

#[derive(Debug, Clone)]
struct ReplicationState {
    role: ReplicationRole,
    paused: bool,
    leader_node_id: Option<String>,
    leader_advertise_addr: Option<String>,
    local_last_applied_sequence: u64,
    local_last_applied_term: Option<u64>,
    local_last_applied_checksum: Option<u32>,
    commit_sequence: u64,
    leader_target_sequence: u64,
    follower_phase: Option<FollowerPhase>,
    follower_lag_entries: Option<u64>,
    follower_lag_ms: Option<u64>,
    health: String,
    reason: Option<String>,
    followers: BTreeMap<String, FollowerProgress>,
    current_term: u64,
    voted_for: Option<String>,
    bootstrap_preferred: bool,
    bootstrap_release_at: Option<Instant>,
    last_heartbeat_at: Instant,
    next_election_at: Instant,
    election_suppressed_until: Option<Instant>,
    members: BTreeMap<String, ClusterMember>,
}

#[derive(Clone)]
pub struct ReplicationRuntime {
    config: ReplicationConfig,
    state: Arc<Mutex<ReplicationState>>,
}

impl ReplicationRuntime {
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        let persisted = load_persisted_state(&config.state_path)?;
        let now = Instant::now();
        let next_election_at = now + random_election_timeout(&config);
        let mut members = BTreeMap::new();
        for member in persisted
            .as_ref()
            .map(|state| state.members.clone())
            .unwrap_or_else(|| config.initial_members.clone())
        {
            members.insert(member.node_id.clone(), member);
        }
        if let Some(advertise_addr) = &config.advertise_addr {
            members
                .entry(config.node_id.clone())
                .or_insert(ClusterMember {
                    node_id: config.node_id.clone(),
                    advertise_addr: advertise_addr.clone(),
                    voter: true,
                });
        }
        let has_cluster_peers = members.values().filter(|member| member.voter).count() > 1;
        let preferred_bootstrap_candidate =
            persisted.is_none() && has_cluster_peers && config.role == ReplicationRole::Leader;
        let role = if has_cluster_peers && config.role != ReplicationRole::Standalone {
            ReplicationRole::Follower
        } else {
            config.role
        };
        let follower_phase = match role {
            ReplicationRole::Follower | ReplicationRole::Candidate
                if has_cluster_peers && persisted.is_some() =>
            {
                Some(FollowerPhase::CatchingUp)
            }
            ReplicationRole::Follower | ReplicationRole::Candidate => {
                Some(FollowerPhase::Bootstrap)
            }
            _ => None,
        };
        let election_suppressed_until = if role == ReplicationRole::Follower && has_cluster_peers {
            Some(if preferred_bootstrap_candidate {
                now + config.election_timeout_min
            } else if persisted.is_some() {
                now + config.stale_after.saturating_mul(2)
            } else {
                now + config.election_timeout_max.saturating_mul(4)
            })
        } else {
            None
        };
        let bootstrap_release_at = if role == ReplicationRole::Follower && has_cluster_peers {
            Some(if preferred_bootstrap_candidate {
                now + config.election_timeout_min
            } else if persisted.is_some() {
                now + config.stale_after.saturating_mul(4)
            } else {
                now + config.election_timeout_max.saturating_mul(8)
            })
        } else {
            None
        };

        Ok(Self {
            config,
            state: Arc::new(Mutex::new(ReplicationState {
                role,
                paused: false,
                leader_node_id: None,
                leader_advertise_addr: None,
                local_last_applied_sequence: 0,
                local_last_applied_term: None,
                local_last_applied_checksum: None,
                commit_sequence: 0,
                leader_target_sequence: 0,
                follower_phase,
                follower_lag_entries: None,
                follower_lag_ms: None,
                health: if role == ReplicationRole::Standalone {
                    "ready".to_string()
                } else {
                    "degraded".to_string()
                },
                reason: if role == ReplicationRole::Standalone {
                    None
                } else {
                    Some("awaiting_leader".to_string())
                },
                followers: BTreeMap::new(),
                current_term: persisted.as_ref().map_or(0, |state| state.current_term),
                voted_for: persisted.and_then(|state| state.voted_for),
                bootstrap_preferred: preferred_bootstrap_candidate,
                bootstrap_release_at,
                last_heartbeat_at: now,
                next_election_at,
                election_suppressed_until,
                members,
            })),
        })
    }

    pub fn config(&self) -> &ReplicationConfig {
        &self.config
    }

    pub async fn role(&self) -> ReplicationRole {
        self.state.lock().await.role
    }

    pub async fn current_term(&self) -> u64 {
        self.state.lock().await.current_term
    }

    pub async fn leader_term(&self) -> Option<u64> {
        let state = self.state.lock().await;
        (state.role == ReplicationRole::Leader).then_some(state.current_term)
    }

    pub async fn is_leader_for_term(&self, term: u64) -> bool {
        let state = self.state.lock().await;
        state.role == ReplicationRole::Leader && state.current_term == term
    }

    pub async fn election_probe(&self) -> (u64, Vec<ClusterMember>, u64, Option<u64>) {
        let state = self.state.lock().await;
        (
            state.current_term.saturating_add(1),
            state.members.values().cloned().collect::<Vec<_>>(),
            state.local_last_applied_sequence,
            state.local_last_applied_term,
        )
    }

    pub async fn commit_sequence(&self) -> u64 {
        self.state.lock().await.commit_sequence
    }

    pub async fn local_last_applied_sequence(&self) -> u64 {
        self.state.lock().await.local_last_applied_sequence
    }

    pub async fn is_paused(&self) -> bool {
        self.state.lock().await.paused
    }

    pub async fn write_window_available(&self) -> bool {
        let state = self.state.lock().await;
        match self.config.write_ack_mode {
            WriteAckMode::Local => true,
            WriteAckMode::Replica | WriteAckMode::All => {
                let pending_entries = state
                    .local_last_applied_sequence
                    .saturating_sub(state.commit_sequence);
                pending_entries < self.config.fetch_batch_size as u64
            }
        }
    }

    pub async fn write_quorum_fresh(&self) -> bool {
        let state = self.state.lock().await;
        let freshness_window = self.config.stale_after;
        match self.config.write_ack_mode {
            WriteAckMode::Local => true,
            WriteAckMode::Replica => {
                let needed = quorum_size_from_members(&state.members).saturating_sub(1);
                if needed == 0 {
                    return true;
                }
                let now = Instant::now();
                let fresh = state
                    .members
                    .values()
                    .filter(|member| member.voter && member.node_id != self.config.node_id)
                    .filter(|member| {
                        state
                            .followers
                            .get(&member.node_id)
                            .is_some_and(|progress| {
                                progress.last_ack_term == state.current_term
                                    && now.duration_since(progress.last_ack_at) < freshness_window
                            })
                    })
                    .count();
                fresh >= needed
            }
            WriteAckMode::All => {
                let now = Instant::now();
                state
                    .members
                    .values()
                    .filter(|member| member.voter && member.node_id != self.config.node_id)
                    .all(|member| {
                        state
                            .followers
                            .get(&member.node_id)
                            .is_some_and(|progress| {
                                progress.last_ack_term == state.current_term
                                    && now.duration_since(progress.last_ack_at) < freshness_window
                            })
                    })
            }
        }
    }

    pub async fn set_advertise_addr(&self, advertise_addr: String) -> Result<()> {
        let mut state = self.state.lock().await;
        state
            .members
            .entry(self.config.node_id.clone())
            .and_modify(|member| member.advertise_addr = advertise_addr.clone())
            .or_insert(ClusterMember {
                node_id: self.config.node_id.clone(),
                advertise_addr,
                voter: true,
            });
        persist_state(&self.config, &state)?;
        Ok(())
    }

    pub async fn leader_hint(&self) -> Option<String> {
        self.state.lock().await.leader_advertise_addr.clone()
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

    pub async fn set_local_last_applied_state(
        &self,
        sequence: u64,
        term: Option<u64>,
        checksum: Option<u32>,
    ) {
        let mut state = self.state.lock().await;
        state.local_last_applied_sequence = sequence;
        state.local_last_applied_term = term;
        state.local_last_applied_checksum = checksum;
        if state.role == ReplicationRole::Standalone {
            state.commit_sequence = sequence;
        } else if state.role == ReplicationRole::Leader {
            recompute_commit_sequence(&self.config, &mut state);
        }
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
        } else if state.role != ReplicationRole::Candidate {
            state.health = "ready".to_string();
            state.reason = None;
        }
    }

    pub async fn register_follower_ack(
        &self,
        follower_node_id: String,
        applied_sequence: u64,
        term: u64,
        leader_node_id: &str,
    ) {
        let mut state = self.state.lock().await;
        if state.role != ReplicationRole::Leader
            || state.current_term != term
            || leader_node_id != self.config.node_id
        {
            return;
        }
        state.followers.insert(
            follower_node_id,
            FollowerProgress {
                match_index: applied_sequence,
                next_sequence: applied_sequence.saturating_add(1),
                last_ack_at: Instant::now(),
                last_ack_term: term,
            },
        );
        if state.role == ReplicationRole::Leader {
            recompute_commit_sequence(&self.config, &mut state);
        }
    }

    pub async fn wait_for_write_ack(&self, sequence: u64) -> Result<()> {
        if self.config.write_ack_mode == WriteAckMode::Local {
            return Ok(());
        }

        let deadline = Instant::now() + self.config.ack_timeout;
        loop {
            {
                let state = self.state.lock().await;
                let total_voters = voter_count(&state.members);
                if state.commit_sequence >= sequence {
                    return Ok(());
                }
                let matched_followers = state
                    .followers
                    .values()
                    .filter(|progress| {
                        progress.last_ack_term == state.current_term
                            && progress.match_index >= sequence
                    })
                    .count();
                if total_voters > 1 && matched_followers == 0 && Instant::now() >= deadline {
                    return Err(ServerError::ReplicationAckUnavailable);
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
            .map(|progress| progress.match_index.saturating_add(1))
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
        state.current_term = state.current_term.saturating_add(1);
        state.voted_for = Some(self.config.node_id.clone());
        state.role = ReplicationRole::Leader;
        state.follower_phase = None;
        state.leader_node_id = Some(self.config.node_id.clone());
        state.leader_advertise_addr = self.config.advertise_addr.clone().or_else(|| {
            state
                .members
                .get(&self.config.node_id)
                .map(|m| m.advertise_addr.clone())
        });
        state.health = "ready".to_string();
        state.reason = None;
        persist_state(&self.config, &state)?;
        Ok(())
    }

    pub async fn heartbeat_due(&self) -> bool {
        let state = self.state.lock().await;
        state.role == ReplicationRole::Leader
    }

    pub async fn election_due(&self) -> bool {
        let state = self.state.lock().await;
        if matches!(state.follower_phase, Some(FollowerPhase::Bootstrap))
            && !state.bootstrap_preferred
            && state
                .bootstrap_release_at
                .is_some_and(|until| Instant::now() < until)
        {
            return false;
        }
        if state
            .election_suppressed_until
            .is_some_and(|until| Instant::now() < until)
        {
            return false;
        }
        if recently_heard_from_known_leader(&self.config, &state) {
            return false;
        }
        let role_allows_election = match state.role {
            ReplicationRole::Candidate => true,
            ReplicationRole::Follower => matches!(
                state.follower_phase,
                Some(FollowerPhase::Bootstrap)
                    | Some(FollowerPhase::Streaming)
                    | Some(FollowerPhase::Stale)
                    | None
            ),
            _ => false,
        };
        let caught_up_to_leader_frontier =
            state.local_last_applied_sequence >= state.leader_target_sequence;
        role_allows_election
            && caught_up_to_leader_frontier
            && Instant::now() >= state.next_election_at
    }

    pub async fn defer_election(&self) {
        let mut state = self.state.lock().await;
        state.next_election_at = Instant::now() + random_election_timeout(&self.config);
    }

    pub async fn begin_election(&self) -> Result<(u64, Vec<ClusterMember>, u64, Option<u64>)> {
        let mut state = self.state.lock().await;
        state.role = ReplicationRole::Candidate;
        state.current_term = state.current_term.saturating_add(1);
        state.voted_for = Some(self.config.node_id.clone());
        state.bootstrap_preferred = false;
        state.bootstrap_release_at = None;
        state.followers.clear();
        state.leader_target_sequence = 0;
        state.leader_node_id = None;
        state.leader_advertise_addr = None;
        state.health = "degraded".to_string();
        state.reason = Some("election_in_progress".to_string());
        state.next_election_at = Instant::now() + random_election_timeout(&self.config);
        let term = state.current_term;
        let members = state.members.values().cloned().collect::<Vec<_>>();
        let last_log_index = state.local_last_applied_sequence;
        let last_log_term = state.local_last_applied_term;
        persist_state(&self.config, &state)?;
        Ok((term, members, last_log_index, last_log_term))
    }

    pub async fn finalize_election(
        &self,
        term: u64,
        votes_granted: usize,
        members: usize,
    ) -> Result<bool> {
        let mut state = self.state.lock().await;
        if state.current_term != term || state.role != ReplicationRole::Candidate {
            return Ok(false);
        }
        if votes_granted < quorum_size(members) {
            return Ok(false);
        }
        state.role = ReplicationRole::Leader;
        state.followers.clear();
        state.leader_target_sequence = 0;
        state.bootstrap_preferred = false;
        state.bootstrap_release_at = None;
        state.leader_node_id = Some(self.config.node_id.clone());
        state.leader_advertise_addr = self.config.advertise_addr.clone().or_else(|| {
            state
                .members
                .get(&self.config.node_id)
                .map(|m| m.advertise_addr.clone())
        });
        state.health = "ready".to_string();
        state.reason = None;
        state.next_election_at = Instant::now() + random_election_timeout(&self.config);
        persist_state(&self.config, &state)?;
        Ok(true)
    }

    pub async fn observe_remote_term(&self, term: u64) -> Result<()> {
        let mut state = self.state.lock().await;
        if term > state.current_term {
            state.current_term = term;
            state.voted_for = None;
            state.bootstrap_preferred = false;
            state.bootstrap_release_at = None;
            if state.role != ReplicationRole::Standalone {
                state.role = ReplicationRole::Follower;
            }
            state.followers.clear();
            state.leader_target_sequence = 0;
            state.leader_node_id = None;
            state.leader_advertise_addr = None;
            state.health = "degraded".to_string();
            state.reason = Some("awaiting_leader".to_string());
            state.next_election_at = Instant::now() + random_election_timeout(&self.config);
            persist_state(&self.config, &state)?;
        }
        Ok(())
    }

    pub async fn handle_vote_request(&self, request: VoteRequest) -> Result<VoteResponse> {
        let mut state = self.state.lock().await;
        let local_log_term = state.local_last_applied_term.unwrap_or(0);
        let request_log_term = request.last_log_term.unwrap_or(0);
        let up_to_date = request_log_term > local_log_term
            || (request_log_term == local_log_term
                && request.last_log_index >= state.local_last_applied_sequence);
        let has_uncommitted_local = state.local_last_applied_sequence > state.commit_sequence;
        let recently_heard_from_leader = recently_heard_from_known_leader(&self.config, &state);
        if request.term < state.current_term {
            log_event(
                "INFO",
                "server.consensus",
                &format!(
                    "vote reject node={} candidate={} request_term={} local_term={} reason=stale_term",
                    self.config.node_id,
                    request.candidate_node_id,
                    request.term,
                    state.current_term
                ),
            );
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }
        if request.prevote {
            let same_term_candidate_tie_break = state.current_term == request.term
                && state.role == ReplicationRole::Candidate
                && state.voted_for.as_deref() == Some(self.config.node_id.as_str())
                && ((request_log_term > local_log_term)
                    || (request_log_term == local_log_term
                        && request.last_log_index > state.local_last_applied_sequence)
                    || (request_log_term == local_log_term
                        && request.last_log_index == state.local_last_applied_sequence
                        && request.candidate_node_id.as_str() > self.config.node_id.as_str()));
            let active_leader_blocks_prevote = state.role == ReplicationRole::Leader
                && state.leader_node_id.as_deref() != Some(request.candidate_node_id.as_str());
            let active_candidate_blocks_prevote = state.role == ReplicationRole::Candidate
                && state.current_term >= request.term
                && state.voted_for.as_deref() == Some(self.config.node_id.as_str())
                && !same_term_candidate_tie_break;
            let equal_log_candidate_has_lower_priority = request.term > 1
                && request_log_term == local_log_term
                && request.last_log_index == state.local_last_applied_sequence
                && request.candidate_node_id.as_str() < self.config.node_id.as_str();
            let uncommitted_tail_blocks_prevote = has_uncommitted_local && !up_to_date;
            let vote_granted = up_to_date
                && !active_leader_blocks_prevote
                && !active_candidate_blocks_prevote
                && !equal_log_candidate_has_lower_priority
                && !uncommitted_tail_blocks_prevote
                && (!recently_heard_from_leader
                    || state.leader_node_id.as_deref() == Some(request.candidate_node_id.as_str()));
            log_event(
                "INFO",
                "server.consensus",
                &format!(
                    "prevote {} node={} candidate={} request_term={} local_term={} up_to_date={} recently_heard_from_leader={} active_leader_blocks_prevote={} active_candidate_blocks_prevote={}",
                    if vote_granted { "grant" } else { "reject" },
                    self.config.node_id,
                    request.candidate_node_id,
                    request.term,
                    state.current_term,
                    up_to_date,
                    recently_heard_from_leader,
                    active_leader_blocks_prevote,
                    active_candidate_blocks_prevote
                ),
            );
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted,
            });
        }
        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            state.bootstrap_preferred = false;
            state.bootstrap_release_at = None;
            if state.role != ReplicationRole::Standalone {
                state.role = ReplicationRole::Follower;
            }
            state.followers.clear();
            state.leader_node_id = None;
            state.leader_advertise_addr = None;
            state.health = "degraded".to_string();
            state.reason = Some("awaiting_leader".to_string());
            state.next_election_at = Instant::now() + random_election_timeout(&self.config);
            persist_state(&self.config, &state)?;
        }
        let recently_heard_from_leader = recently_heard_from_known_leader(&self.config, &state);
        if recently_heard_from_leader
            && state.leader_node_id.as_deref() != Some(request.candidate_node_id.as_str())
        {
            log_event(
                "INFO",
                "server.consensus",
                &format!(
                    "vote reject node={} candidate={} request_term={} local_term={} reason=recent_leader leader={:?}",
                    self.config.node_id,
                    request.candidate_node_id,
                    request.term,
                    state.current_term,
                    state.leader_node_id
                ),
            );
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }
        if !up_to_date {
            log_event(
                "INFO",
                "server.consensus",
                &format!(
                    "vote reject node={} candidate={} request_term={} local_term={} reason=out_of_date candidate_log=({:?},{}) local_log=({:?},{})",
                    self.config.node_id,
                    request.candidate_node_id,
                    request.term,
                    state.current_term,
                    request.last_log_term,
                    request.last_log_index,
                    state.local_last_applied_term,
                    state.local_last_applied_sequence
                ),
            );
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }
        if has_uncommitted_local && !up_to_date {
            log_event(
                "INFO",
                "server.consensus",
                &format!(
                    "vote reject node={} candidate={} request_term={} local_term={} reason=uncommitted_tail local_commit={} local_applied={}",
                    self.config.node_id,
                    request.candidate_node_id,
                    request.term,
                    state.current_term,
                    state.commit_sequence,
                    state.local_last_applied_sequence
                ),
            );
            return Ok(VoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }
        let vote_granted = up_to_date
            && (state.voted_for.is_none()
                || state.voted_for.as_deref() == Some(request.candidate_node_id.as_str()));
        if vote_granted {
            state.voted_for = Some(request.candidate_node_id.clone());
            state.bootstrap_preferred = false;
            state.bootstrap_release_at = None;
            if state.role != ReplicationRole::Standalone {
                state.role = ReplicationRole::Follower;
            }
            state.next_election_at = Instant::now() + leader_lease_duration(&self.config);
            state.election_suppressed_until =
                Some(Instant::now() + leader_lease_duration(&self.config));
            state
                .members
                .entry(request.candidate_node_id.clone())
                .and_modify(|member| member.advertise_addr = request.candidate_addr.clone())
                .or_insert(ClusterMember {
                    node_id: request.candidate_node_id.clone(),
                    advertise_addr: request.candidate_addr,
                    voter: true,
                });
            persist_state(&self.config, &state)?;
        }
        log_event(
            "INFO",
            "server.consensus",
            &format!(
                "vote {} node={} candidate={} request_term={} local_term={} voted_for={:?}",
                if vote_granted { "grant" } else { "reject" },
                self.config.node_id,
                request.candidate_node_id,
                request.term,
                state.current_term,
                state.voted_for
            ),
        );
        Ok(VoteResponse {
            term: state.current_term,
            vote_granted,
        })
    }

    pub async fn handle_heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse> {
        let mut state = self.state.lock().await;
        if request.term < state.current_term {
            return Ok(HeartbeatResponse {
                term: state.current_term,
                accepted: false,
            });
        }
        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            state.followers.clear();
        }
        if state.role != ReplicationRole::Standalone {
            state.role = ReplicationRole::Follower;
        }
        state.voted_for = Some(request.leader_node_id.clone());
        state.bootstrap_preferred = false;
        state.bootstrap_release_at = None;
        state.leader_node_id = Some(request.leader_node_id.clone());
        state.leader_advertise_addr = Some(request.leader_addr.clone());
        state.commit_sequence = state.commit_sequence.max(request.commit_sequence);
        state.leader_target_sequence = state.leader_target_sequence.max(
            request
                .leader_frontier_sequence
                .max(request.commit_sequence),
        );
        if !state.paused {
            state.follower_phase = Some(
                if state.local_last_applied_sequence != state.leader_target_sequence {
                    FollowerPhase::CatchingUp
                } else {
                    FollowerPhase::Streaming
                },
            );
        }
        state.last_heartbeat_at = Instant::now();
        state.next_election_at = Instant::now() + leader_lease_duration(&self.config);
        state.election_suppressed_until =
            Some(Instant::now() + leader_lease_duration(&self.config));
        state.health = "ready".to_string();
        state.reason = None;
        state.members = request
            .members
            .into_iter()
            .map(|member| (member.node_id.clone(), member))
            .collect();
        persist_state(&self.config, &state)?;
        Ok(HeartbeatResponse {
            term: state.current_term,
            accepted: true,
        })
    }

    pub async fn observe_leader_status(
        &self,
        leader_node_id: String,
        leader_addr: Option<String>,
        term: u64,
        commit_sequence: u64,
        leader_frontier_sequence: u64,
        members: Vec<ClusterMember>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        if term < state.current_term {
            return Ok(());
        }
        if term > state.current_term {
            state.current_term = term;
            state.voted_for = None;
            state.followers.clear();
        }
        if state.role != ReplicationRole::Standalone {
            state.role = ReplicationRole::Follower;
        }
        state.voted_for = Some(leader_node_id.clone());
        state.bootstrap_preferred = false;
        state.bootstrap_release_at = None;
        state.leader_node_id = Some(leader_node_id);
        state.leader_advertise_addr = leader_addr;
        state.commit_sequence = state.commit_sequence.max(commit_sequence);
        state.leader_target_sequence = state
            .leader_target_sequence
            .max(leader_frontier_sequence.max(commit_sequence));
        if !state.paused {
            state.follower_phase = Some(
                if state.local_last_applied_sequence != state.leader_target_sequence {
                    FollowerPhase::CatchingUp
                } else {
                    FollowerPhase::Streaming
                },
            );
        }
        state.last_heartbeat_at = Instant::now();
        state.next_election_at = Instant::now() + leader_lease_duration(&self.config);
        state.election_suppressed_until =
            Some(Instant::now() + leader_lease_duration(&self.config));
        state.health = "ready".to_string();
        state.reason = None;
        if !members.is_empty() {
            state.members = members
                .into_iter()
                .map(|member| (member.node_id.clone(), member))
                .collect();
        }
        persist_state(&self.config, &state)?;
        Ok(())
    }

    pub async fn leader_heartbeat_payload(
        &self,
    ) -> Option<(u64, String, String, u64, u64, Vec<ClusterMember>)> {
        let state = self.state.lock().await;
        if state.role != ReplicationRole::Leader {
            return None;
        }
        Some((
            state.current_term,
            self.config.node_id.clone(),
            self.config
                .advertise_addr
                .clone()
                .or_else(|| {
                    state
                        .members
                        .get(&self.config.node_id)
                        .map(|m| m.advertise_addr.clone())
                })
                .unwrap_or_default(),
            state.commit_sequence,
            state.local_last_applied_sequence.max(state.commit_sequence),
            state.members.values().cloned().collect(),
        ))
    }

    pub async fn note_leader_activity(&self) {
        let mut state = self.state.lock().await;
        if state.role == ReplicationRole::Leader {
            state.last_heartbeat_at = Instant::now();
        }
    }

    pub async fn note_leader_frontier(&self, sequence: u64) {
        let mut state = self.state.lock().await;
        if state.role == ReplicationRole::Follower {
            state.leader_target_sequence = state.leader_target_sequence.max(sequence);
            if !state.paused {
                state.follower_phase = Some(
                    if state.local_last_applied_sequence != state.leader_target_sequence {
                        FollowerPhase::CatchingUp
                    } else {
                        FollowerPhase::Streaming
                    },
                );
            }
        }
    }

    pub async fn current_members(&self) -> Vec<ClusterMember> {
        self.state.lock().await.members.values().cloned().collect()
    }

    pub async fn follower_next_sequence(&self, follower_node_id: &str) -> u64 {
        let state = self.state.lock().await;
        state
            .followers
            .get(follower_node_id)
            .map(|progress| progress.next_sequence)
            .unwrap_or(1)
    }

    pub async fn record_append_result(
        &self,
        follower_node_id: String,
        accepted: bool,
        match_sequence: u64,
        term: u64,
        leader_node_id: &str,
    ) {
        let mut state = self.state.lock().await;
        if state.role != ReplicationRole::Leader
            || state.current_term != term
            || leader_node_id != self.config.node_id
        {
            return;
        }
        let current_next = state
            .followers
            .get(&follower_node_id)
            .map(|progress| progress.next_sequence)
            .unwrap_or(1);
        let progress = state
            .followers
            .entry(follower_node_id)
            .or_insert(FollowerProgress {
                match_index: 0,
                next_sequence: 1,
                last_ack_at: Instant::now(),
                last_ack_term: term,
            });
        progress.last_ack_at = Instant::now();
        progress.last_ack_term = term;
        if accepted {
            progress.match_index = progress.match_index.max(match_sequence);
            progress.next_sequence = progress.match_index.saturating_add(1);
        } else {
            let fallback_next = match_sequence.saturating_add(1);
            progress.next_sequence = current_next
                .saturating_sub(1)
                .max(1)
                .min(fallback_next.max(1));
        }
        recompute_commit_sequence(&self.config, &mut state);
    }

    pub async fn local_advertise_addr(&self) -> Option<String> {
        let state = self.state.lock().await;
        self.config.advertise_addr.clone().or_else(|| {
            state
                .members
                .get(&self.config.node_id)
                .map(|m| m.advertise_addr.clone())
        })
    }

    pub async fn add_member(&self, member: ClusterMember) -> Result<()> {
        let mut state = self.state.lock().await;
        state.members.insert(member.node_id.clone(), member);
        persist_state(&self.config, &state)?;
        Ok(())
    }

    pub async fn remove_member(&self, node_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        state.members.remove(node_id);
        state.followers.remove(node_id);
        persist_state(&self.config, &state)?;
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
                applied_sequence: progress.match_index,
                lag_entries: state
                    .local_last_applied_sequence
                    .saturating_sub(progress.match_index),
                lag_ms: now.duration_since(progress.last_ack_at).as_millis() as u64,
                stale: now.duration_since(progress.last_ack_at) > self.config.stale_after,
            })
            .collect::<Vec<_>>();
        let members = state.members.values().cloned().collect::<Vec<_>>();
        let quorum = quorum_size_from_members(&state.members);
        ReplicationStatusSnapshot {
            node_id: self.config.node_id.clone(),
            group_id: self.config.group_id.clone(),
            role: state.role.as_str().to_string(),
            advertise_addr: self.config.advertise_addr.clone().or_else(|| {
                state
                    .members
                    .get(&self.config.node_id)
                    .map(|m| m.advertise_addr.clone())
            }),
            leader_node_id: state.leader_node_id.clone(),
            leader_advertise_addr: state.leader_advertise_addr.clone(),
            upstream: self
                .config
                .upstream
                .clone()
                .or_else(|| state.leader_advertise_addr.clone()),
            write_ack_mode: self.config.write_ack_mode.as_str().to_string(),
            paused: state.paused,
            health: state.health.clone(),
            reason: state.reason.clone(),
            local_last_applied_sequence: state.local_last_applied_sequence,
            commit_sequence: state.commit_sequence,
            retention_floor_sequence: followers
                .iter()
                .map(|follower| follower.applied_sequence.saturating_add(1))
                .min(),
            follower_phase: state.follower_phase.map(|phase| phase.as_str().to_string()),
            follower_lag_entries: state.follower_lag_entries,
            follower_lag_ms: state.follower_lag_ms,
            known_followers: followers.len(),
            followers,
            current_term: state.current_term,
            voted_for: state.voted_for.clone(),
            quorum_size: quorum,
            members,
        }
    }
}

fn default_true() -> bool {
    true
}

fn leader_lease_duration(config: &ReplicationConfig) -> Duration {
    config
        .stale_after
        .max(config.election_timeout_max.saturating_mul(4))
}

fn recently_heard_from_known_leader(config: &ReplicationConfig, state: &ReplicationState) -> bool {
    state.leader_node_id.is_some()
        && Instant::now().duration_since(state.last_heartbeat_at) < leader_lease_duration(config)
}

fn recompute_commit_sequence(config: &ReplicationConfig, state: &mut ReplicationState) {
    let candidate = match config.write_ack_mode {
        WriteAckMode::Local => state.local_last_applied_sequence,
        WriteAckMode::Replica => {
            let quorum = quorum_size_from_members(&state.members);
            nth_highest_voter_match(config, state, quorum)
        }
        WriteAckMode::All => min_voter_match(config, state),
    };

    state.commit_sequence = state
        .commit_sequence
        .max(candidate.min(state.local_last_applied_sequence));
}

fn voter_match_sequences(config: &ReplicationConfig, state: &ReplicationState) -> Vec<u64> {
    state
        .members
        .values()
        .filter(|member| member.voter)
        .map(|member| {
            if member.node_id == config.node_id {
                state.local_last_applied_sequence
            } else {
                state
                    .followers
                    .get(&member.node_id)
                    .map(|progress| progress.match_index)
                    .unwrap_or(0)
            }
        })
        .collect()
}

fn nth_highest_voter_match(
    config: &ReplicationConfig,
    state: &ReplicationState,
    quorum: usize,
) -> u64 {
    let mut matches = voter_match_sequences(config, state);
    if matches.is_empty() {
        return 0;
    }
    matches.sort_unstable();
    let index = matches.len().saturating_sub(quorum);
    matches[index]
}

fn min_voter_match(config: &ReplicationConfig, state: &ReplicationState) -> u64 {
    voter_match_sequences(config, state)
        .into_iter()
        .min()
        .unwrap_or(0)
}

fn voter_count(members: &BTreeMap<String, ClusterMember>) -> usize {
    members
        .values()
        .filter(|member| member.voter)
        .count()
        .max(1)
}

fn quorum_size(member_count: usize) -> usize {
    (member_count / 2) + 1
}

fn quorum_size_from_members(members: &BTreeMap<String, ClusterMember>) -> usize {
    quorum_size(voter_count(members))
}

fn random_election_timeout(config: &ReplicationConfig) -> Duration {
    if config.election_timeout_max <= config.election_timeout_min {
        return config.election_timeout_min;
    }
    let min = config.election_timeout_min.as_millis() as u64;
    let max = config.election_timeout_max.as_millis() as u64;
    let jitter = rand::rng().random_range(min..=max);
    Duration::from_millis(jitter)
}

fn load_persisted_state(path: &PathBuf) -> Result<Option<PersistedReplicationState>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|err| ServerError::InvalidArguments(err.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn persist_state(config: &ReplicationConfig, state: &ReplicationState) -> Result<()> {
    let persisted = PersistedReplicationState {
        current_term: state.current_term,
        voted_for: state.voted_for.clone(),
        members: state.members.values().cloned().collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted)
        .map_err(|err| ServerError::InvalidArguments(err.to_string()))?;
    if let Some(parent) = config.state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.state_tmp_path, bytes)?;
    fs::rename(&config.state_tmp_path, &config.state_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_state_path(label: &str) -> (PathBuf, PathBuf) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("vaylix-repl-test-{label}-{unique}"));
        (
            root.join("cluster-state.json"),
            root.join("cluster-state.json.tmp"),
        )
    }

    fn test_runtime(node_id: &str) -> ReplicationRuntime {
        let (state_path, state_tmp_path) = temp_state_path(node_id);
        ReplicationRuntime::new(ReplicationConfig {
            node_id: node_id.to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: Some(format!("{node_id}.local:9173")),
            role: ReplicationRole::Follower,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::Replica,
            ack_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(50),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(3),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            state_path,
            state_tmp_path,
            initial_members: vec![
                ClusterMember {
                    node_id: "node-1".to_string(),
                    advertise_addr: "node-1.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-2".to_string(),
                    advertise_addr: "node-2.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-3".to_string(),
                    advertise_addr: "node-3.local:9173".to_string(),
                    voter: true,
                },
            ],
        })
        .unwrap()
    }

    #[tokio::test]
    async fn candidate_allows_prevote_tie_break_for_higher_node_id() {
        let runtime = test_runtime("node-2");
        let (term, _, _, _) = runtime.begin_election().await.unwrap();

        let response = runtime
            .handle_vote_request(VoteRequest {
                term,
                candidate_node_id: "node-3".to_string(),
                candidate_addr: "node-3.local:9173".to_string(),
                last_log_index: 0,
                last_log_term: None,
                prevote: true,
            })
            .await
            .unwrap();

        assert!(response.vote_granted);
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, "candidate");
        assert_eq!(snapshot.voted_for.as_deref(), Some("node-2"));
    }

    #[tokio::test]
    async fn candidate_does_not_step_down_for_lower_node_id() {
        let runtime = test_runtime("node-2");
        let (term, _, _, _) = runtime.begin_election().await.unwrap();

        let response = runtime
            .handle_vote_request(VoteRequest {
                term,
                candidate_node_id: "node-1".to_string(),
                candidate_addr: "node-1.local:9173".to_string(),
                last_log_index: 0,
                last_log_term: None,
                prevote: false,
            })
            .await
            .unwrap();

        assert!(!response.vote_granted);
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, "candidate");
        assert_eq!(snapshot.voted_for.as_deref(), Some("node-2"));
    }

    #[tokio::test]
    async fn follower_rejects_prevote_when_recent_leader_contact_exists() {
        let runtime = test_runtime("node-2");
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Follower;
            state.current_term = 5;
            state.leader_node_id = Some("node-1".to_string());
            state.last_heartbeat_at = Instant::now();
        }

        let response = runtime
            .handle_vote_request(VoteRequest {
                term: 6,
                candidate_node_id: "node-3".to_string(),
                candidate_addr: "node-3.local:9173".to_string(),
                last_log_index: 0,
                last_log_term: None,
                prevote: true,
            })
            .await
            .unwrap();

        assert!(!response.vote_granted);
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, "follower");
        assert_eq!(snapshot.current_term, 5);
        assert_eq!(snapshot.leader_node_id.as_deref(), Some("node-1"));
    }

    #[tokio::test]
    async fn majority_commit_advances_only_after_quorum_ack() {
        let runtime = test_runtime("node-1");
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Leader;
            state.leader_node_id = Some("node-1".to_string());
        }

        runtime.set_local_last_applied_state(5, None, None).await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.commit_sequence, 0);

        runtime
            .register_follower_ack("node-2".to_string(), 5, 0, "node-1")
            .await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.commit_sequence, 5);
    }

    #[tokio::test]
    async fn all_commit_waits_for_every_voter() {
        let (state_path, state_tmp_path) = temp_state_path("all-mode");
        let runtime = ReplicationRuntime::new(ReplicationConfig {
            node_id: "node-1".to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: Some("node-1.local:9173".to_string()),
            role: ReplicationRole::Leader,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::All,
            ack_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(50),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(3),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            state_path,
            state_tmp_path,
            initial_members: vec![
                ClusterMember {
                    node_id: "node-1".to_string(),
                    advertise_addr: "node-1.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-2".to_string(),
                    advertise_addr: "node-2.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-3".to_string(),
                    advertise_addr: "node-3.local:9173".to_string(),
                    voter: true,
                },
            ],
        })
        .unwrap();
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Leader;
            state.leader_node_id = Some("node-1".to_string());
        }

        runtime.set_local_last_applied_state(7, None, None).await;
        runtime
            .register_follower_ack("node-2".to_string(), 7, 0, "node-1")
            .await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.commit_sequence, 0);

        runtime
            .register_follower_ack("node-3".to_string(), 7, 0, "node-1")
            .await;
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.commit_sequence, 7);
    }

    #[tokio::test]
    async fn heartbeat_payload_reports_committed_index() {
        let (state_path, state_tmp_path) = temp_state_path("heartbeat-commit");
        let runtime = ReplicationRuntime::new(ReplicationConfig {
            node_id: "node-1".to_string(),
            group_id: "group".to_string(),
            advertise_addr: Some("127.0.0.1:9173".to_string()),
            role: ReplicationRole::Leader,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::Replica,
            ack_timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(50),
            fetch_batch_size: 64,
            stale_after: Duration::from_millis(50),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(50),
            election_timeout_max: Duration::from_millis(50),
            state_path,
            state_tmp_path,
            initial_members: vec![
                ClusterMember {
                    node_id: "node-1".to_string(),
                    advertise_addr: "127.0.0.1:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-2".to_string(),
                    advertise_addr: "127.0.0.1:9174".to_string(),
                    voter: true,
                },
            ],
        })
        .unwrap();
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Leader;
            state.leader_node_id = Some("node-1".to_string());
        }

        runtime.set_local_last_applied_state(5, None, None).await;
        runtime
            .register_follower_ack("node-2".to_string(), 3, 0, "node-1")
            .await;

        let (_, _, _, commit_sequence, _, _) = runtime
            .leader_heartbeat_payload()
            .await
            .expect("leader runtime should expose heartbeat payload");
        assert_eq!(commit_sequence, 3);
    }

    #[tokio::test]
    async fn ignores_stale_or_misdirected_follower_acks() {
        let runtime = test_runtime("node-1");
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Leader;
            state.leader_node_id = Some("node-1".to_string());
        }

        runtime.set_local_last_applied_state(5, None, None).await;
        runtime
            .register_follower_ack("node-2".to_string(), 5, 99, "node-1")
            .await;
        assert_eq!(runtime.snapshot().await.commit_sequence, 0);

        runtime
            .register_follower_ack("node-2".to_string(), 5, 0, "node-x")
            .await;
        assert_eq!(runtime.snapshot().await.commit_sequence, 0);

        runtime
            .register_follower_ack("node-2".to_string(), 5, 0, "node-1")
            .await;
        assert_eq!(runtime.snapshot().await.commit_sequence, 5);
    }

    #[tokio::test]
    async fn leadership_term_change_clears_follower_progress_and_preserves_commit() {
        let runtime = test_runtime("node-1");
        {
            let mut state = runtime.state.lock().await;
            state.role = ReplicationRole::Leader;
            state.leader_node_id = Some("node-1".to_string());
        }

        runtime.set_local_last_applied_state(5, None, None).await;
        runtime
            .register_follower_ack("node-2".to_string(), 5, 0, "node-1")
            .await;
        assert_eq!(runtime.snapshot().await.commit_sequence, 5);

        runtime.observe_remote_term(1).await.unwrap();
        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, "follower");
        assert_eq!(snapshot.commit_sequence, 5);
        assert_eq!(snapshot.known_followers, 0);
    }

    #[tokio::test]
    async fn persisted_cluster_follower_suppresses_initial_election() {
        let (state_path, state_tmp_path) = temp_state_path("suppressed-election");
        let persisted = PersistedReplicationState {
            current_term: 4,
            voted_for: None,
            members: vec![
                ClusterMember {
                    node_id: "node-1".to_string(),
                    advertise_addr: "node-1.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-2".to_string(),
                    advertise_addr: "node-2.local:9173".to_string(),
                    voter: true,
                },
                ClusterMember {
                    node_id: "node-3".to_string(),
                    advertise_addr: "node-3.local:9173".to_string(),
                    voter: true,
                },
            ],
        };
        let bytes = serde_json::to_vec_pretty(&persisted).unwrap();
        fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        fs::write(&state_path, bytes).unwrap();

        let runtime = ReplicationRuntime::new(ReplicationConfig {
            node_id: "node-1".to_string(),
            group_id: "test-group".to_string(),
            advertise_addr: Some("node-1.local:9173".to_string()),
            role: ReplicationRole::Leader,
            upstream: None,
            upstream_username: None,
            upstream_password: None,
            write_ack_mode: WriteAckMode::Replica,
            ack_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(50),
            fetch_batch_size: 32,
            stale_after: Duration::from_secs(3),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            state_path,
            state_tmp_path,
            initial_members: Vec::new(),
        })
        .unwrap();

        let snapshot = runtime.snapshot().await;
        assert_eq!(snapshot.role, "follower");
        assert!(!runtime.election_due().await);
    }

    #[tokio::test]
    async fn catching_up_follower_does_not_start_election() {
        let runtime = test_runtime("node-1");
        runtime
            .update_follower_phase(FollowerPhase::CatchingUp, Some(12), Some(50))
            .await;
        {
            let mut state = runtime.state.lock().await;
            state.next_election_at = Instant::now() - Duration::from_millis(1);
            state.election_suppressed_until = None;
        }

        assert!(!runtime.election_due().await);
    }
}
