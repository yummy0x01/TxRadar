//! Yellowstone/Geyser gRPC streaming layer (Phase 1).
//!
//! Owns the live view of the network: slot commitment updates and (optionally)
//! transaction notifications used to confirm bundle landing *from the stream*
//! rather than by polling RPC. Responsibilities:
//!
//! * Build a [`SubscribeRequest`] for slots + optionally targeted transactions.
//! * Maintain the stream with ping/pong keepalive so proxies/load balancers
//!   don't drop an idle connection.
//! * Reconnect with exponential backoff and `from_slot` replay so a blip
//!   doesn't lose us a slot.
//! * Apply backpressure via a bounded channel between the gRPC reader task and
//!   downstream consumers — a slow consumer slows the reader instead of
//!   blowing up memory.
//!
//! The public surface is intentionally small: callers get a [`StreamHandle`]
//! (a receiver of normalized [`StreamEvent`]s) from [`spawn`], and the gRPC
//! details stay private to this crate.

mod client;

pub use client::{spawn, StreamConfig};

use tokio::sync::mpsc;

/// Normalized events emitted by the stream layer onto the internal bus.
///
/// These are transport-agnostic on purpose: nothing downstream should need to
/// know we're on Yellowstone vs. some other Geyser source.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A slot reached a commitment level (processed/confirmed/finalized) or an
    /// intra-slot status. This is how we confirm landing from the stream.
    SlotStatus { slot: u64, parent: Option<u64>, status: SlotStatus },
    /// A transaction we subscribed to was observed on-chain.
    Transaction { signature: String, slot: u64, failed: bool },
    /// The currently scheduled / upcoming leader for a slot. (Populated in a
    /// later phase from the RPC leader schedule; the gRPC stream itself does
    /// not carry leader identity.)
    Leader { slot: u64, leader: String },
    /// Stream connectivity changed (surfaced to the TUI / agent).
    Connection(ConnectionState),
}

/// Commitment / intra-slot status of a slot.
///
/// The first three mirror Solana commitment levels and are what Yellowstone
/// 3.1.1 reports via `SubscribeUpdateSlot.status` (a `CommitmentLevel`). The
/// last three are kept for forward-compat with versions that surface
/// intra-slot statuses.
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
///
/// Dropping the handle drops the receiver, which causes the background reader
/// task's `send` to fail and the stream task to shut down cleanly.
pub struct StreamHandle {
    pub events: mpsc::Receiver<StreamEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("gRPC connect/build failed: {0}")]
    Connect(String),
    #[error("subscribe failed: {0}")]
    Subscribe(String),
    #[error("stream error: {0}")]
    Stream(String),
    #[error("missing Yellowstone x-token (set TXRADAR_YELLOWSTONE_X_TOKEN)")]
    MissingToken,
}
