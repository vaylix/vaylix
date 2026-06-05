#![no_main]

use libfuzzer_sys::fuzz_target;
use server::auth::Permission;
use server::replication::{
    AppendEntriesRequest, AppendEntriesResponse, HeartbeatRequest, HeartbeatResponse,
    SnapshotInstallRequest, SnapshotInstallResponse, VoteRequest, VoteResponse,
};

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<VoteRequest>(data);
    let _ = serde_json::from_slice::<VoteResponse>(data);
    let _ = serde_json::from_slice::<HeartbeatRequest>(data);
    let _ = serde_json::from_slice::<HeartbeatResponse>(data);
    let _ = serde_json::from_slice::<AppendEntriesRequest>(data);
    let _ = serde_json::from_slice::<AppendEntriesResponse>(data);
    let _ = serde_json::from_slice::<SnapshotInstallRequest>(data);
    let _ = serde_json::from_slice::<SnapshotInstallResponse>(data);

    let text = String::from_utf8_lossy(data);
    let _ = Permission::parse(&text);
});
