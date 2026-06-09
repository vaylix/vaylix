use command::{
    Command, Expiration as CommandExpiration, SetCondition as CommandSetCondition,
    SetOptions as CommandSetOptions,
};
use engine::{Engine, EngineOptions, Paths, StorageEngine, TransactionResult};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed, TestCaseError, TestRunner};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

const MODEL_NOW_MS: u64 = 1_000;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
enum Step {
    Single(Op),
    Batch(Vec<Op>),
    Transaction(Vec<Op>),
}

#[derive(Clone, Debug)]
enum Op {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Vec<u8>,
        options: CommandSetOptions,
    },
    SetNx {
        key: String,
        value: Vec<u8>,
    },
    GetDel {
        key: String,
    },
    GetEx {
        key: String,
        expiration: Option<CommandExpiration>,
        persist: bool,
    },
    MGet {
        keys: Vec<String>,
    },
    MSet {
        entries: Vec<(String, Vec<u8>)>,
    },
    Delete {
        keys: Vec<String>,
    },
    Exists {
        key: String,
    },
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    Persist {
        key: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    RenameNx {
        source: String,
        destination: String,
    },
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<u16>,
    },
    DbSize,
    Count,
    List,
    Clear,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelEntry {
    value: Vec<u8>,
    expires_at_ms: Option<u64>,
    version: u64,
}

#[derive(Clone, Debug, Default)]
struct Model {
    entries: BTreeMap<String, ModelEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ModelError {
    InvalidInteger,
    NumericOverflow,
}

#[test]
fn engine_matches_reference_model_for_seeded_command_sequences() {
    let seed = test_seed();
    eprintln!("VAYLIX_TEST_SEED={seed}");

    let config = Config {
        cases: test_cases(),
        failure_persistence: None,
        max_shrink_iters: 4096,
        rng_seed: RngSeed::Fixed(seed),
        ..Config::default()
    };
    let mut runner = TestRunner::new(config);
    let strategy = prop::collection::vec(step_strategy(), 1..80);

    let result = runner.run(&strategy, |steps| run_sequence(seed, &steps));
    if let Err(err) = result {
        panic!("engine/model semantic divergence with VAYLIX_TEST_SEED={seed}: {err}");
    }
}

#[test]
fn command_batch_sequences_and_wal_recovery_match_mutations_only() {
    let seed = test_seed();
    let root = temp_dir(seed ^ 0x5e1f);
    let paths = Paths::from_data_dir(&root).unwrap();
    let mut engine = Engine::from_paths_with_options(paths.clone(), EngineOptions::default())
        .expect("engine should open");

    let results = engine
        .execute_command_batch(&[
            Command::Get {
                key: "durable".to_string(),
            },
            Command::Set {
                key: "durable".to_string(),
                value: b"value".to_vec(),
                options: CommandSetOptions::default(),
            },
            Command::MGet {
                keys: vec!["durable".to_string(), "missing".to_string()],
            },
            Command::Exists {
                key: "durable".to_string(),
            },
        ])
        .expect("batch should succeed");

    assert_eq!(
        results
            .iter()
            .map(|result| result.last_applied_sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 1, 1]
    );
    assert_eq!(engine.last_applied_sequence(), 1);
    assert_eq!(engine.get("durable").unwrap(), Some(b"value".to_vec()));

    drop(engine);

    let mut reopened = Engine::from_paths_with_options(paths, EngineOptions::default())
        .expect("engine should reopen from WAL");
    assert_eq!(reopened.last_applied_sequence(), 1);
    assert_eq!(reopened.get("durable").unwrap(), Some(b"value".to_vec()));

    std::fs::remove_dir_all(&root).ok();
}

fn run_sequence(seed: u64, steps: &[Step]) -> Result<(), TestCaseError> {
    let root = temp_dir(seed);
    let paths = Paths::from_data_dir(&root)
        .map_err(|err| TestCaseError::fail(format!("setup failed: {err}")))?;
    let mut engine = Engine::from_paths_with_options(paths, EngineOptions::default())
        .map_err(|err| TestCaseError::fail(format!("engine open failed: {err}")))?;
    let mut model = Model::default();

    let outcome = run_sequence_inner(seed, steps, &mut engine, &mut model);
    std::fs::remove_dir_all(&root).ok();
    outcome
}

fn run_sequence_inner(
    seed: u64,
    steps: &[Step],
    engine: &mut Engine,
    model: &mut Model,
) -> Result<(), TestCaseError> {
    for (index, step) in steps.iter().enumerate() {
        let ops = step_ops(step);
        let expected = model.apply_many(&ops);
        let actual = apply_engine_step(engine, step);

        match (expected, actual) {
            (Ok(expected), Ok(actual)) => {
                if !results_equivalent(&ops, &expected, &actual) {
                    return Err(failure(
                        seed,
                        steps,
                        index,
                        step,
                        format!("result mismatch: expected {expected:?}, actual {actual:?}"),
                    ));
                }
            }
            (Err(expected), Err(actual_code)) => {
                if expected.engine_code() != actual_code {
                    return Err(failure(
                        seed,
                        steps,
                        index,
                        step,
                        format!(
                            "error mismatch: expected {}, actual {actual_code}",
                            expected.engine_code()
                        ),
                    ));
                }
            }
            (Ok(expected), Err(actual_code)) => {
                return Err(failure(
                    seed,
                    steps,
                    index,
                    step,
                    format!(
                        "engine returned unexpected error {actual_code}; model result {expected:?}"
                    ),
                ));
            }
            (Err(expected), Ok(actual)) => {
                return Err(failure(
                    seed,
                    steps,
                    index,
                    step,
                    format!(
                        "engine unexpectedly succeeded with {actual:?}; model error {expected:?}"
                    ),
                ));
            }
        }

        let expected_entries = model.visible_entries();
        let actual_entries = engine
            .list()
            .map_err(|err| TestCaseError::fail(format!("engine list failed: {err}")))?;
        if expected_entries != actual_entries {
            return Err(failure(
                seed,
                steps,
                index,
                step,
                format!("state mismatch: expected {expected_entries:?}, actual {actual_entries:?}"),
            ));
        }
    }
    Ok(())
}

fn apply_engine_step(engine: &mut Engine, step: &Step) -> Result<Vec<TransactionResult>, String> {
    let commands = step_ops(step)
        .iter()
        .map(Op::to_command)
        .collect::<Vec<_>>();

    match step {
        Step::Transaction(_) => engine
            .execute_transaction(&commands)
            .map_err(|err| err.code().to_string()),
        Step::Single(_) | Step::Batch(_) => engine
            .execute_command_batch(&commands)
            .map(|results| results.into_iter().map(|result| result.result).collect())
            .map_err(|err| err.code().to_string()),
    }
}

fn results_equivalent(
    ops: &[Op],
    expected: &[TransactionResult],
    actual: &[TransactionResult],
) -> bool {
    if expected.len() != actual.len() || ops.len() != expected.len() {
        return false;
    }

    ops.iter()
        .zip(expected.iter().zip(actual))
        .all(|(op, (expected, actual))| match (op, expected, actual) {
            (
                Op::Ttl { .. },
                TransactionResult::Integer(expected),
                TransactionResult::Integer(actual),
            ) if *expected > 0 && *actual > 0 => true,
            _ => expected == actual,
        })
}

fn failure(seed: u64, steps: &[Step], index: usize, step: &Step, detail: String) -> TestCaseError {
    TestCaseError::fail(format!(
        "{detail}; seed={seed}; step_index={index}; step={step:?}; reproducer={steps:#?}"
    ))
}

fn step_ops(step: &Step) -> Vec<Op> {
    match step {
        Step::Single(op) => vec![op.clone()],
        Step::Batch(ops) | Step::Transaction(ops) => ops.clone(),
    }
}

impl Model {
    fn apply_many(&mut self, ops: &[Op]) -> Result<Vec<TransactionResult>, ModelError> {
        self.purge_expired();
        let snapshot = self.clone();
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            match self.apply_op(op) {
                Ok(result) => results.push(result),
                Err(err) => {
                    *self = snapshot;
                    return Err(err);
                }
            }
        }
        Ok(results)
    }

    fn apply_op(&mut self, op: &Op) -> Result<TransactionResult, ModelError> {
        self.purge_expired();
        match op {
            Op::Get { key } => Ok(self
                .entries
                .get(key)
                .map(|entry| TransactionResult::Value(entry.value.clone()))
                .unwrap_or(TransactionResult::NotFound)),
            Op::Set {
                key,
                value,
                options,
            } => self.apply_set(key, value, options),
            Op::SetNx { key, value } => {
                if self.entries.contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                self.entries.insert(
                    key.clone(),
                    ModelEntry {
                        value: value.clone(),
                        expires_at_ms: None,
                        version: 1,
                    },
                );
                Ok(TransactionResult::Boolean(true))
            }
            Op::GetDel { key } => Ok(self
                .entries
                .remove(key)
                .map(|entry| TransactionResult::Value(entry.value))
                .unwrap_or(TransactionResult::NotFound)),
            Op::GetEx {
                key,
                expiration,
                persist,
            } => {
                let Some(entry) = self.entries.get(key).cloned() else {
                    return Ok(TransactionResult::NotFound);
                };
                if let Some(expiration) = expiration {
                    match resolve_expiration(*expiration) {
                        Some(expires_at_ms) => {
                            if let Some(entry) = self.entries.get_mut(key) {
                                entry.expires_at_ms = Some(expires_at_ms);
                            }
                        }
                        None => {
                            self.entries.remove(key);
                        }
                    }
                } else if *persist && let Some(entry) = self.entries.get_mut(key) {
                    entry.expires_at_ms = None;
                }
                Ok(TransactionResult::Value(entry.value))
            }
            Op::MGet { keys } => Ok(TransactionResult::Strings(
                keys.iter()
                    .map(|key| self.entries.get(key).map(|entry| entry.value.clone()))
                    .collect(),
            )),
            Op::MSet { entries } => {
                for (key, value) in entries {
                    let version = self.next_version(key);
                    self.entries.insert(
                        key.clone(),
                        ModelEntry {
                            value: value.clone(),
                            expires_at_ms: None,
                            version,
                        },
                    );
                }
                Ok(TransactionResult::Ok)
            }
            Op::Delete { keys } => {
                let mut removed = 0;
                for key in keys {
                    if self.entries.remove(key).is_some() {
                        removed += 1;
                    }
                }
                Ok(TransactionResult::Count(removed))
            }
            Op::Exists { key } => Ok(TransactionResult::Boolean(self.entries.contains_key(key))),
            Op::Incr { key } => self.apply_integer_delta(key, 1),
            Op::Decr { key } => self.apply_integer_delta(key, -1),
            Op::Expire { key, seconds } => {
                if !self.entries.contains_key(key) {
                    return Ok(TransactionResult::Boolean(false));
                }
                if *seconds == 0 {
                    self.entries.remove(key);
                } else if let Some(entry) = self.entries.get_mut(key) {
                    entry.expires_at_ms = Some(MODEL_NOW_MS.saturating_add(seconds * 1_000));
                }
                Ok(TransactionResult::Boolean(true))
            }
            Op::Ttl { key } => Ok(TransactionResult::Integer(self.ttl(key))),
            Op::Persist { key } => {
                let removed = self
                    .entries
                    .get_mut(key)
                    .and_then(|entry| entry.expires_at_ms.take())
                    .is_some();
                Ok(TransactionResult::Boolean(removed))
            }
            Op::Rename {
                source,
                destination,
            } => self.apply_rename(source, destination),
            Op::RenameNx {
                source,
                destination,
            } => {
                if self.entries.contains_key(destination) {
                    return Ok(TransactionResult::Boolean(false));
                }
                self.apply_rename(source, destination)
            }
            Op::Scan {
                cursor,
                pattern,
                count,
            } => Ok(TransactionResult::Scan(self.scan(
                *cursor,
                pattern.as_deref(),
                *count,
            ))),
            Op::DbSize | Op::Count => Ok(TransactionResult::Count(self.entries.len() as u64)),
            Op::List => Ok(TransactionResult::Entries(self.visible_entries())),
            Op::Clear => {
                self.entries.clear();
                Ok(TransactionResult::Ok)
            }
        }
    }

    fn apply_set(
        &mut self,
        key: &str,
        value: &[u8],
        options: &CommandSetOptions,
    ) -> Result<TransactionResult, ModelError> {
        let previous_entry = self.entries.get(key).cloned();
        let previous = previous_entry.as_ref().map(|entry| entry.value.clone());
        let previous_expiration = previous_entry
            .as_ref()
            .and_then(|entry| entry.expires_at_ms);
        let previous_version = previous_entry.as_ref().map(|entry| entry.version);
        let allowed = match options.condition {
            Some(CommandSetCondition::Nx) => previous.is_none(),
            Some(CommandSetCondition::Xx) => previous.is_some(),
            None => true,
        } && options
            .if_version
            .map(|expected| previous_version == Some(expected))
            .unwrap_or(true);

        if !allowed {
            return Ok(if options.return_previous {
                previous
                    .map(TransactionResult::Value)
                    .unwrap_or(TransactionResult::NotFound)
            } else if options.condition.is_some() || options.if_version.is_some() {
                TransactionResult::Boolean(false)
            } else {
                TransactionResult::Ok
            });
        }

        let version = previous_version.unwrap_or(0).saturating_add(1).max(1);
        self.entries.insert(
            key.to_string(),
            ModelEntry {
                value: value.to_vec(),
                expires_at_ms: None,
                version,
            },
        );

        if let Some(expiration) = options.expiration {
            match resolve_expiration(expiration) {
                Some(expires_at_ms) => {
                    if let Some(entry) = self.entries.get_mut(key) {
                        entry.expires_at_ms = Some(expires_at_ms);
                    }
                }
                None => {
                    self.entries.remove(key);
                }
            }
        } else if options.keep_ttl
            && let Some(expires_at_ms) = previous_expiration
            && let Some(entry) = self.entries.get_mut(key)
        {
            entry.expires_at_ms = Some(expires_at_ms);
        }

        Ok(if options.return_previous {
            previous
                .map(TransactionResult::Value)
                .unwrap_or(TransactionResult::NotFound)
        } else if options.condition.is_some() || options.if_version.is_some() {
            TransactionResult::Boolean(true)
        } else {
            TransactionResult::Ok
        })
    }

    fn apply_integer_delta(
        &mut self,
        key: &str,
        delta: i64,
    ) -> Result<TransactionResult, ModelError> {
        let current = self
            .entries
            .get(key)
            .map(|entry| entry.value.clone())
            .unwrap_or_else(|| b"0".to_vec());
        let text = std::str::from_utf8(&current).map_err(|_| ModelError::InvalidInteger)?;
        let parsed = text
            .parse::<i64>()
            .map_err(|_| ModelError::InvalidInteger)?;
        let next = parsed
            .checked_add(delta)
            .ok_or(ModelError::NumericOverflow)?;
        let version = self.next_version(key);
        self.entries.insert(
            key.to_string(),
            ModelEntry {
                value: next.to_string().into_bytes(),
                expires_at_ms: None,
                version,
            },
        );
        Ok(TransactionResult::Integer(next))
    }

    fn apply_rename(
        &mut self,
        source: &str,
        destination: &str,
    ) -> Result<TransactionResult, ModelError> {
        let Some(entry) = self.entries.remove(source) else {
            return Ok(TransactionResult::Boolean(false));
        };
        let version = self.next_version(destination);
        self.entries.insert(
            destination.to_string(),
            ModelEntry {
                value: entry.value,
                expires_at_ms: entry.expires_at_ms,
                version,
            },
        );
        Ok(TransactionResult::Boolean(true))
    }

    fn scan(&self, cursor: u64, pattern: Option<&str>, count: Option<u16>) -> engine::ScanPage {
        let mut keys = self.entries.keys().cloned().collect::<Vec<_>>();
        if let Some(pattern) = pattern {
            keys.retain(|key| wildcard_matches(pattern, key));
        }

        if keys.is_empty() {
            return engine::ScanPage {
                next_cursor: 0,
                keys: Vec::new(),
            };
        }

        let start = usize::try_from(cursor).unwrap_or(usize::MAX);
        if start >= keys.len() {
            return engine::ScanPage {
                next_cursor: 0,
                keys: Vec::new(),
            };
        }

        let limit = usize::from(count.unwrap_or(10)).max(1);
        let end = (start + limit).min(keys.len());
        let next_cursor = if end >= keys.len() { 0 } else { end as u64 };
        engine::ScanPage {
            next_cursor,
            keys: keys[start..end].to_vec(),
        }
    }

    fn ttl(&self, key: &str) -> i64 {
        let Some(entry) = self.entries.get(key) else {
            return -2;
        };
        match entry.expires_at_ms {
            Some(expires_at_ms) if expires_at_ms <= MODEL_NOW_MS => -2,
            Some(expires_at_ms) => {
                expires_at_ms.saturating_sub(MODEL_NOW_MS).div_ceil(1_000) as i64
            }
            None => -1,
        }
    }

    fn visible_entries(&self) -> Vec<(String, Vec<u8>)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    fn next_version(&self, key: &str) -> u64 {
        self.entries
            .get(key)
            .map(|entry| entry.version.saturating_add(1))
            .unwrap_or(1)
    }

    fn purge_expired(&mut self) {
        self.entries.retain(|_, entry| {
            entry
                .expires_at_ms
                .is_none_or(|expires_at_ms| expires_at_ms > MODEL_NOW_MS)
        });
    }
}

impl ModelError {
    fn engine_code(&self) -> &'static str {
        match self {
            Self::InvalidInteger => "ENG-015",
            Self::NumericOverflow => "ENG-016",
        }
    }
}

impl Op {
    fn to_command(&self) -> Command {
        match self {
            Op::Get { key } => Command::Get { key: key.clone() },
            Op::Set {
                key,
                value,
                options,
            } => Command::Set {
                key: key.clone(),
                value: value.clone(),
                options: options.clone(),
            },
            Op::SetNx { key, value } => Command::SetNx {
                key: key.clone(),
                value: value.clone(),
            },
            Op::GetDel { key } => Command::GetDel { key: key.clone() },
            Op::GetEx {
                key,
                expiration,
                persist,
            } => Command::GetEx {
                key: key.clone(),
                expiration: *expiration,
                persist: *persist,
            },
            Op::MGet { keys } => Command::MGet { keys: keys.clone() },
            Op::MSet { entries } => Command::MSet {
                entries: entries.clone(),
            },
            Op::Delete { keys } => Command::Delete { keys: keys.clone() },
            Op::Exists { key } => Command::Exists { key: key.clone() },
            Op::Incr { key } => Command::Incr { key: key.clone() },
            Op::Decr { key } => Command::Decr { key: key.clone() },
            Op::Expire { key, seconds } => Command::Expire {
                key: key.clone(),
                seconds: *seconds,
            },
            Op::Ttl { key } => Command::Ttl { key: key.clone() },
            Op::Persist { key } => Command::Persist { key: key.clone() },
            Op::Rename {
                source,
                destination,
            } => Command::Rename {
                source: source.clone(),
                destination: destination.clone(),
            },
            Op::RenameNx {
                source,
                destination,
            } => Command::RenameNx {
                source: source.clone(),
                destination: destination.clone(),
            },
            Op::Scan {
                cursor,
                pattern,
                count,
            } => Command::Scan {
                cursor: *cursor,
                pattern: pattern.clone(),
                count: *count,
            },
            Op::DbSize => Command::DbSize,
            Op::Count => Command::Count,
            Op::List => Command::List,
            Op::Clear => Command::Clear,
        }
    }
}

fn step_strategy() -> BoxedStrategy<Step> {
    prop_oneof![
        op_strategy().prop_map(Step::Single),
        prop::collection::vec(op_strategy(), 1..5).prop_map(Step::Batch),
        prop::collection::vec(op_strategy(), 1..5).prop_map(Step::Transaction),
    ]
    .boxed()
}

fn op_strategy() -> BoxedStrategy<Op> {
    prop_oneof![
        key_strategy().prop_map(|key| Op::Get { key }),
        (key_strategy(), value_strategy(), set_options_strategy()).prop_map(
            |(key, value, options)| Op::Set {
                key,
                value,
                options
            }
        ),
        (key_strategy(), value_strategy()).prop_map(|(key, value)| Op::SetNx { key, value }),
        key_strategy().prop_map(|key| Op::GetDel { key }),
        (
            key_strategy(),
            prop_oneof![
                Just((None, false)),
                Just((Some(CommandExpiration::Ex(60)), false)),
                Just((Some(CommandExpiration::Px(60_000)), false)),
                Just((None, true)),
            ],
        )
            .prop_map(|(key, (expiration, persist))| Op::GetEx {
                key,
                expiration,
                persist
            }),
        prop::collection::vec(key_strategy(), 1..5).prop_map(|keys| Op::MGet { keys }),
        prop::collection::vec((key_strategy(), value_strategy()), 1..5)
            .prop_map(|entries| Op::MSet { entries }),
        prop::collection::vec(key_strategy(), 1..5).prop_map(|keys| Op::Delete { keys }),
        key_strategy().prop_map(|key| Op::Exists { key }),
        key_strategy().prop_map(|key| Op::Incr { key }),
        key_strategy().prop_map(|key| Op::Decr { key }),
        (key_strategy(), prop_oneof![Just(0_u64), Just(60_u64)])
            .prop_map(|(key, seconds)| Op::Expire { key, seconds }),
        key_strategy().prop_map(|key| Op::Ttl { key }),
        key_strategy().prop_map(|key| Op::Persist { key }),
        (key_strategy(), key_strategy()).prop_map(|(source, destination)| Op::Rename {
            source,
            destination,
        }),
        (key_strategy(), key_strategy()).prop_map(|(source, destination)| Op::RenameNx {
            source,
            destination,
        }),
        (
            0_u64..8,
            prop_oneof![
                Just(None),
                Just(Some("*".to_string())),
                Just(Some("a*".to_string())),
                Just(Some("?:*".to_string())),
            ],
            prop_oneof![Just(None), Just(Some(1_u16)), Just(Some(3_u16))],
        )
            .prop_map(|(cursor, pattern, count)| Op::Scan {
                cursor,
                pattern,
                count
            }),
        Just(Op::DbSize),
        Just(Op::Count),
        Just(Op::List),
        Just(Op::Clear),
    ]
    .boxed()
}

fn set_options_strategy() -> BoxedStrategy<CommandSetOptions> {
    let write_guard = prop_oneof![
        Just((None, None)),
        Just((Some(CommandSetCondition::Nx), None)),
        Just((Some(CommandSetCondition::Xx), None)),
        prop_oneof![Just(1_u64), Just(2_u64), Just(99_u64)]
            .prop_map(|version| (None, Some(version))),
    ];
    let ttl = prop_oneof![
        Just((None, false)),
        Just((Some(CommandExpiration::Ex(60)), false)),
        Just((Some(CommandExpiration::Px(60_000)), false)),
        Just((None, true)),
    ];

    (write_guard, ttl, any::<bool>())
        .prop_map(
            |((condition, if_version), (expiration, keep_ttl), return_previous)| {
                CommandSetOptions {
                    condition,
                    if_version,
                    expiration,
                    keep_ttl,
                    return_previous,
                }
            },
        )
        .boxed()
}

fn key_strategy() -> BoxedStrategy<String> {
    prop_oneof![
        Just("alpha".to_string()),
        Just("beta".to_string()),
        Just("counter".to_string()),
        Just("rate:login".to_string()),
        Just("session:1".to_string()),
        Just("session:2".to_string()),
    ]
    .boxed()
}

fn value_strategy() -> BoxedStrategy<Vec<u8>> {
    prop_oneof![
        Just(Vec::new()),
        Just(b"0".to_vec()),
        Just(b"1".to_vec()),
        Just(b"-1".to_vec()),
        Just(b"42".to_vec()),
        Just(b"abc".to_vec()),
        Just(vec![0, b'a', b'b']),
        Just(vec![0xff, 0x00, b'z']),
    ]
    .boxed()
}

fn resolve_expiration(expiration: CommandExpiration) -> Option<u64> {
    match expiration {
        CommandExpiration::Ex(0) | CommandExpiration::Px(0) => None,
        CommandExpiration::Ex(seconds) => {
            Some(MODEL_NOW_MS.saturating_add(seconds.saturating_mul(1_000)))
        }
        CommandExpiration::Px(milliseconds) => Some(MODEL_NOW_MS.saturating_add(milliseconds)),
    }
}

fn wildcard_matches(pattern: &str, text: &str) -> bool {
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let text_chars = text.chars().collect::<Vec<_>>();
    let mut dp = vec![vec![false; text_chars.len() + 1]; pattern_chars.len() + 1];
    dp[0][0] = true;

    for row in 1..=pattern_chars.len() {
        if pattern_chars[row - 1] == '*' {
            dp[row][0] = dp[row - 1][0];
        }
    }

    for row in 1..=pattern_chars.len() {
        for col in 1..=text_chars.len() {
            dp[row][col] = match pattern_chars[row - 1] {
                '*' => dp[row - 1][col] || dp[row][col - 1],
                '?' => dp[row - 1][col - 1],
                value => dp[row - 1][col - 1] && value == text_chars[col - 1],
            };
        }
    }

    dp[pattern_chars.len()][text_chars.len()]
}

fn test_seed() -> u64 {
    std::env::var("VAYLIX_TEST_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0x9e37_79b9_7f4a_7c15)
}

fn test_cases() -> u32 {
    std::env::var("VAYLIX_MODEL_CASES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(128)
}

fn temp_dir(seed: u64) -> std::path::PathBuf {
    let unique = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("vaylix-model-{seed}-{unique}"));
    std::fs::remove_dir_all(&root).ok();
    root
}
