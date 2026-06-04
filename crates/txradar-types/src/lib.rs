//! Shared domain model for TxRadar.
//!
//! Everything network- and transport-agnostic lives here: the transaction
//! lifecycle state machine, failure taxonomy, the structured log record schema,
//! and the strongly-typed configuration loaded from `config/<profile>.toml`.
//!
//! No I/O, no network code, no Solana SDK — just types every other crate shares.

pub mod config;
pub mod failure;
pub mod lifecycle;
pub mod record;

pub use config::{Config, Network};
pub use failure::FailureClass;
pub use lifecycle::{CommitmentStage, LifecycleEvent, StageTimings};
pub use record::BundleRecord;
