use std::time::{SystemTime, UNIX_EPOCH};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use engine::{Engine, Expiration, Paths, SetCondition, SetOptions, StorageEngine};

fn temp_paths(label: &str) -> Paths {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("vaylix-bench-{label}-{unique}"));
    Paths::from_data_dir(root).expect("create benchmark data dsir")
}

fn fresh_engine(label: &str) -> Engine {
    Engine::from_paths(temp_paths(label)).expect("create engine")
}

fn bytes(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

fn seeded_engine(label: &str, entries: usize) -> Engine {
    let mut engine = fresh_engine(label);
    for idx in 0..entries {
        engine
            .set(
                format!("key-{idx:04}"),
                format!("value-{idx:04}").into_bytes(),
            )
            .expect("seed key");
    }
    engine
}

fn seed_numeric(engine: &mut Engine, key: &str, value: &str) {
    engine
        .set(key.to_string(), bytes(value))
        .expect("seed numeric value");
}

fn core_command_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/commands");

    group.bench_function("get", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("get");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("set value");
                engine
            },
            |mut engine| {
                let value = engine.get("bench-key").expect("get value");
                assert_eq!(value.as_deref(), Some(b"bench-value".as_slice()));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("set", |b| {
        b.iter_batched(
            || fresh_engine("set"),
            |mut engine| {
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("set value");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("setnx", |b| {
        b.iter_batched(
            || fresh_engine("setnx"),
            |mut engine| {
                let applied = engine
                    .set_nx("bench-key".to_string(), bytes("bench-value"))
                    .expect("setnx");
                assert!(applied);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("set_xx", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("set-xx");
                engine
                    .set("bench-key".to_string(), bytes("original"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                let outcome = engine
                    .set_with_options(
                        "bench-key".to_string(),
                        bytes("updated"),
                        SetOptions {
                            condition: Some(SetCondition::Xx),
                            ..SetOptions::default()
                        },
                    )
                    .expect("set xx");
                assert!(outcome.applied);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("getdel", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("getdel");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                let value = engine.get_del("bench-key").expect("getdel");
                assert!(value.is_some());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("getex", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("getex");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                let value = engine
                    .get_ex("bench-key", Some(Expiration::Seconds(60)), false)
                    .expect("getex");
                assert!(value.is_some());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mget", |b| {
        b.iter_batched(
            || seeded_engine("mget", 64),
            |mut engine| {
                let keys = (0..32usize)
                    .map(|idx| format!("key-{idx:04}"))
                    .collect::<Vec<_>>();
                let values = engine.mget(&keys).expect("mget");
                assert_eq!(values.len(), keys.len());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mset", |b| {
        b.iter_batched(
            || fresh_engine("mset"),
            |mut engine| {
                let entries = (0..64usize)
                    .map(|idx| {
                        (
                            format!("key-{idx:04}"),
                            format!("value-{idx:04}").into_bytes(),
                        )
                    })
                    .collect::<Vec<_>>();
                engine.mset(&entries).expect("mset");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("delete", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("delete");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                assert!(engine.delete("bench-key").expect("delete"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("delete_many", |b| {
        b.iter_batched(
            || seeded_engine("delete-many", 64),
            |mut engine| {
                let keys = (0..32usize)
                    .map(|idx| format!("key-{idx:04}"))
                    .collect::<Vec<_>>();
                let removed = engine.delete_many(&keys).expect("delete many");
                assert_eq!(removed, keys.len());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("exists", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("exists");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                assert!(engine.exists("bench-key").expect("exists"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("incr", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("incr");
                seed_numeric(&mut engine, "counter", "41");
                engine
            },
            |mut engine| {
                assert_eq!(engine.incr("counter").expect("incr"), 42);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("decr", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("decr");
                seed_numeric(&mut engine, "counter", "41");
                engine
            },
            |mut engine| {
                assert_eq!(engine.decr("counter").expect("decr"), 40);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("expire", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("expire");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                assert!(engine.expire("bench-key", 60).expect("expire"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("ttl", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("ttl");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine.expire("bench-key", 60).expect("expire");
                engine
            },
            |mut engine| {
                let ttl = engine.ttl("bench-key").expect("ttl");
                assert!(ttl > 0);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("persist", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("persist");
                engine
                    .set("bench-key".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine.expire("bench-key", 60).expect("expire");
                engine
            },
            |mut engine| {
                assert!(engine.persist("bench-key").expect("persist"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("rename", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("rename");
                engine
                    .set("source".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                assert!(engine.rename("source", "dest".to_string()).expect("rename"));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("renamenx", |b| {
        b.iter_batched(
            || {
                let mut engine = fresh_engine("renamenx");
                engine
                    .set("source".to_string(), bytes("bench-value"))
                    .expect("seed");
                engine
            },
            |mut engine| {
                assert!(
                    engine
                        .rename_nx("source", "dest".to_string())
                        .expect("renamenx")
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("scan", |b| {
        b.iter_batched(
            || seeded_engine("scan", 1024),
            |mut engine| {
                let page = engine.scan(0, Some("key-*"), Some(128)).expect("scan");
                assert!(!page.keys.is_empty());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("dbsize", |b| {
        b.iter_batched(
            || seeded_engine("dbsize", 1024),
            |mut engine| {
                let size = engine.db_size().expect("dbsize");
                assert_eq!(size, 1024);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("count", |b| {
        b.iter_batched(
            || seeded_engine("count", 1024),
            |mut engine| {
                let count = engine.count().expect("count");
                assert_eq!(count, 1024);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("list", |b| {
        b.iter_batched(
            || seeded_engine("list", 512),
            |mut engine| {
                let entries = engine.list().expect("list");
                assert_eq!(entries.len(), 512);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("info", |b| {
        b.iter_batched(
            || seeded_engine("info", 256),
            |mut engine| {
                let info = engine.info().expect("info");
                assert!(!info.is_empty());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("clear", |b| {
        b.iter_batched(
            || seeded_engine("clear", 512),
            |mut engine| {
                engine.clear().expect("clear");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("snapshot", |b| {
        b.iter_batched(
            || seeded_engine("snapshot", 512),
            |mut engine| {
                engine.snapshot().expect("snapshot");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("logical_backup", |b| {
        b.iter_batched(
            || seeded_engine("logical-backup", 512),
            |mut engine| {
                let dump = engine.logical_backup().expect("backup");
                assert!(!dump.is_empty());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("validate_logical_backup", |b| {
        b.iter_batched(
            || {
                let mut engine = seeded_engine("validate-backup", 256);
                let dump = engine.logical_backup().expect("backup");
                (engine, dump)
            },
            |(mut engine, dump)| {
                let count = engine
                    .validate_logical_backup(&dump)
                    .expect("validate backup");
                assert!(count > 0);
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("restore_logical_backup", |b| {
        b.iter_batched(
            || {
                let mut source = seeded_engine("restore-source", 256);
                let dump = source.logical_backup().expect("backup");
                let target = fresh_engine("restore-target");
                (target, dump)
            },
            |(mut engine, dump)| {
                let restored = engine.restore_logical_backup(&dump).expect("restore");
                assert!(restored > 0);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn batch_shape_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/batch_shapes");
    for entry_count in [64usize, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::from_parameter(entry_count),
            &entry_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let engine = fresh_engine("mset-scan-shape");
                        let entries = (0..count)
                            .map(|idx| (format!("key-{idx:04}"), "value".repeat(8).into_bytes()))
                            .collect::<Vec<_>>();
                        (engine, entries)
                    },
                    |(mut engine, entries)| {
                        engine.mset(&entries).expect("mset");
                        let page = engine.scan(0, Some("key-*"), Some(128)).expect("scan");
                        assert!(!page.keys.is_empty());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(engine_benches, core_command_benches, batch_shape_benches);
criterion_main!(engine_benches);
