//! Yellowstone/Geyser gRPC streaming layer (Phase 1).
//!
//! Owns the live view of the network: slot updates, leader schedule, and
//! transaction/account notifications used to confirm landing *from the stream*
//! (RPC polling is only a backup). Responsibilities:
//!
//! * Build a `SubscribeRequest` for slots + targeted transactions.
//! * Maintain the stream with ping/pong keepalive (~30s).
//! * Reconnect with exponential backoff and `from_slot` replay + dedupe.
//! * Apply backpressure via a bounded channel between the gRPC reader and
//!   downstream consumers.
//!
//! Phase 0 defines the event model and the consumer-facing handle; the gRPC
//! wiring lands in Phase 1.

use tokio::sync::mpsc;

/// Normalized events emitted by the stream layer onto the internal bus.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A slot reached a commitment level (processed/confirmed/finalized).
    SlotStatus { slot: u64, status: SlotStatus },
    /// A transaction we care about was observed on-chain.
    Transaction { signature: String, slot: u64, failed: bool },
    /// The currently scheduled / upcoming leader for a slot.
    Leader { slot: u64, leader: String },
    /// Stream connectivity changed (surfaced to the TUI).
    Connection(ConnectionState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    Processed,
    Confirmed,
    Finalized,
    FirstShredReceived,
    Completed,
    Dead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
}

/// Consumer handle: downstream tasks receive [`StreamEvent`]s here.
pub struct StreamHandle {
    pub events: mpsc::Receiver<StreamEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("stream not yet implemented (Phase 1)")]
    NotImplemented,
}
