use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};

use super::{EngineHandle, ServerRuntimeConfig, log_event, record_runtime_event};
use crate::metrics::Metrics;

pub(super) fn spawn_snapshotter(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match engine.snapshot().await {
                        Ok(()) => {
                            metrics.snapshots_completed.fetch_add(1, Ordering::Relaxed);
                            log_event("INFO", "server.snapshotter", "periodic snapshot complete");
                        }
                        Err(err) => log_event("ERROR", "server.snapshotter", &format!("[{}] {}: {err}", err.code(), err.name())),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_expiration_sweeper(
    engine: EngineHandle,
    metrics: Arc<Metrics>,
    every: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(every);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match engine.sweep_expired().await {
                        Ok(removed) => {
                            metrics.expiration_sweeps.fetch_add(1, Ordering::Relaxed);
                            metrics.expired_keys_removed.fetch_add(removed as u64, Ordering::Relaxed);
                        }
                        Err(err) => log_event("ERROR", "server.sweeper", &format!("[{}] {}: {err}", err.code(), err.name())),
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_tls_reloader(
    runtime: ServerRuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    #[cfg(unix)]
    return Some(tokio::spawn(async move {
        let Some(tls_state) = runtime.tls_state.clone() else {
            return;
        };
        let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };
        loop {
            tokio::select! {
                _ = signal.recv() => {
                    match tls_state.reload().await {
                        Ok(()) => {
                            log_event("INFO", "server.tls", "reloaded TLS certificates after SIGHUP");
                            record_runtime_event(
                                &runtime.audit_logger,
                                "tls_reload",
                                [("result".to_string(), "ok".to_string())].into_iter().collect(),
                            );
                        }
                        Err(err) => {
                            log_event("ERROR", "server.tls", &format!("[{}] {}: {err}", err.code(), err.name()));
                            record_runtime_event(
                                &runtime.audit_logger,
                                "tls_reload",
                                [
                                    ("result".to_string(), "error".to_string()),
                                    ("error_code".to_string(), err.code().to_string()),
                                ]
                                .into_iter()
                                .collect(),
                            );
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }));

    #[cfg(not(unix))]
    {
        let _ = runtime;
        let _ = shutdown;
        None
    }
}
