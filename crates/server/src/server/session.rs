use std::net::SocketAddr;
use std::time::Instant;

use command::Command;
use transport::Status;
use uuid::Uuid;

use super::ServerGuards;
use crate::auth::Identity;

#[derive(Clone)]
pub(super) struct RateLimiter {
    capacity: f64,
    tokens: f64,
    refill_per_second: f64,
    last: Instant,
}

impl RateLimiter {
    pub(super) fn new(requests_per_second: u32, burst: u32) -> Self {
        Self {
            capacity: burst as f64,
            tokens: burst as f64,
            refill_per_second: requests_per_second as f64,
            last: Instant::now(),
        }
    }

    pub(super) fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_second).min(self.capacity);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

pub(super) struct SessionState {
    pub(super) identity: Option<Identity>,
    pub(super) transaction_queue: Vec<Command>,
    pub(super) rate_limiter: RateLimiter,
    pub(super) transaction_started_at_ms: Option<u64>,
}

impl SessionState {
    pub(super) fn new(guards: &ServerGuards) -> Self {
        Self {
            identity: None,
            transaction_queue: Vec::new(),
            rate_limiter: RateLimiter::new(guards.requests_per_second, guards.request_burst),
            transaction_started_at_ms: None,
        }
    }

    pub(super) fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }

    pub(super) fn in_transaction(&self) -> bool {
        !self.transaction_queue.is_empty()
    }
}

pub(super) struct AuditContext<'a> {
    pub(super) connection_id: u64,
    pub(super) peer_addr: Option<SocketAddr>,
    pub(super) session: &'a SessionState,
    pub(super) request_id: Uuid,
    pub(super) opcode: &'a str,
    pub(super) status: Status,
    pub(super) error_code: Option<String>,
    pub(super) latency_ms: u128,
}
