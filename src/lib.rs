//! Rust port of ElasticMQ's queue engine and SQS service.
//!
//! The engine takes explicit millisecond timestamps so delivery behavior can be
//! tested without sleeping. The server supplies wall-clock time and serializes
//! manager access, corresponding to upstream's actor mailbox ordering.
pub mod engine;
pub mod model;

pub use engine::QueueManager;
pub use model::*;
