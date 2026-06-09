use std::fs;
use std::path::Path;

#[test]
fn replication_and_election_timing_stay_on_monotonic_clock() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let checked_files = [
        "src/replication/mod.rs",
        "src/replication/timing.rs",
        "src/server/mod.rs",
        "src/server/ha_write_coordinator.rs",
        "src/server/replication_client.rs",
    ];

    for relative in checked_files {
        let path = manifest_dir.join(relative);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);

        assert!(
            !production_source.contains("SystemTime::now"),
            "{} uses wall-clock time in replication/election production code",
            path.display()
        );
        assert!(
            !production_source.contains("UNIX_EPOCH"),
            "{} uses unix wall-clock time in replication/election production code",
            path.display()
        );
    }
}
