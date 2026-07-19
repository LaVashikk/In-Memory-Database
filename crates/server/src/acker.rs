use raw_shared_types::{Batch, OP_PUT, Request};

use crate::args::Mode;

/// Durability guarantee levels
/// Order is strictly significant: each subsequent level implies the previous ones have been met
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AckPoint {
    /// Changes are applied to the in-memory state
    /// (Responses are immediate, but data will be lost upon any crash)
    Applied = 1,
    /// Data is handed off to the OS kernel
    /// (Survives process crashes, but not OS crashes or power failures)
    Written,
    /// Data is successfully flushed to persistent storage
    /// (Survives OS kernel panics and absolute power loss)
    Durable,
}

impl From<Mode> for AckPoint {
    fn from(value: Mode) -> Self {
        match value {
            Mode::NoFsync => AckPoint::Applied,
            Mode::Async => AckPoint::Written,
            Mode::Sync => AckPoint::Durable,
        }
    }
}

/// Manages the lifecycle of client responses by tracking durability requirements
#[derive(Debug, Clone, Copy)]
pub struct Acker {
    level: AckPoint
}

impl Acker {
    pub fn new(mode: Mode) -> Self {
        Self {
            level: mode.into()
        }
    }

    /// Determines the required durability level for a specific request
    #[inline]
    fn required(&self, req: &Request) -> AckPoint {
        // Read-only requests (GETs) are satisfied immediately from memory;
        // Mutations depend on the globally configured durability mode
        if req.is_read_only() { AckPoint::Applied } else { self.level }
    }

    /// Evaluates all requests against the newly `reached` guarantee level.
    /// If a request's durability requirement is met (<= reached),
    /// its response is dispatched to the client and consumed from the batch
    #[inline]
    pub fn advance(&self, batch: &mut Batch, reached: AckPoint) {
        for req in batch.items.iter_mut() {
            if self.required(req) <= reached {
                if let Some(r) = req.resp.take() {
                    let _ = req.reply.blocking_send(r);
                }
            }
        }
    }

    /// Checks if all requests in the batch have been successfully dispatched
    #[inline]
    pub fn is_settled(&self, batch: &Batch) -> bool { // todo: o(n)
        batch.items.iter().all(|r| r.resp.is_none())
    }
}
