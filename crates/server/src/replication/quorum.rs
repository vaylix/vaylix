use std::collections::BTreeMap;

use super::{ClusterMember, ReplicationConfig, ReplicationState, WriteAckMode};

pub(super) fn recompute_commit_sequence(config: &ReplicationConfig, state: &mut ReplicationState) {
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

pub(super) fn voter_count(members: &BTreeMap<String, ClusterMember>) -> usize {
    members
        .values()
        .filter(|member| member.voter)
        .count()
        .max(1)
}

pub(super) fn quorum_size(member_count: usize) -> usize {
    (member_count / 2) + 1
}

pub(super) fn quorum_size_from_members(members: &BTreeMap<String, ClusterMember>) -> usize {
    quorum_size(voter_count(members))
}
