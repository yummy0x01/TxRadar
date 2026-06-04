//! The transaction lifecycle state machine.
//!
//! A submitted transaction/bundle progresses through commitment stages. The
//! bounty requires capturing timestamps, slot numbers, and the latency deltas
//! between stages — so the timing model is first-class here.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Commitment stages a transaction moves through, in order.
///
/// Mirrors Solana commitment levels plus the local "submitted" moment that
/// precedes any on-chain visibility. `Failed` is terminal and carries no
/// ordering relative to the success stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentStage {
    /// We sent the bundle to the Jito block engine (local clock).
    Submitted,
    /// Seen by the cluster at `processed` commitment (newest, reversible).
    Processed,
    /// Voted by a supermajority (>2/3 stake) — `confirmed`.
    Confirmed,
    /// Maximum lockout — `finalized`.
    Finalized,
    /// Terminal failure (see [`crate::FailureClass`]).
    Failed,
}

impl CommitmentStage {
    /// Ordinal for the success path; `Failed` returns `None`.
    pub fn order(self) -> Option<u8> {
        match self {
            CommitmentStage::Submitted => Some(0),
            CommitmentStage::Processed => Some(1),
            CommitmentStage::Confirmed => Some(2),
            CommitmentStage::Finalized => Some(3),
            CommitmentStage::Failed => None,
        }
    }
}

/// A single transition observed for a transaction, with the evidence
/// (timestamp + slot) the bounty asks us to capture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub stage: CommitmentStage,
    /// Wall-clock time we observed the transition.
    pub at: DateTime<Utc>,
    /// Slot the transaction landed in (None before it lands, or on failure).
    pub slot: Option<u64>,
    /// Free-form note (e.g. which source confirmed it: stream vs. status poll).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// Per-stage timestamps and the derived latency deltas between them.
///
/// The README must reason about the `processed -> confirmed` delta as a
/// network-health signal, so these deltas are computed and stored explicitly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageTimings {
    pub submitted_at: Option<DateTime<Utc>>,
    pub processed_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub finalized_at: Option<DateTime<Utc>>,

    /// submitted -> processed (ingestion + inclusion latency), milliseconds.
    pub submit_to_processed_ms: Option<i64>,
    /// processed -> confirmed (vote propagation; network-health proxy), ms.
    pub processed_to_confirmed_ms: Option<i64>,
    /// confirmed -> finalized (lockout maturation), milliseconds.
    pub confirmed_to_finalized_ms: Option<i64>,
}

impl StageTimings {
    /// Record a stage observation and recompute any deltas that are now known.
    pub fn observe(&mut self, stage: CommitmentStage, at: DateTime<Utc>) {
        match stage {
            CommitmentStage::Submitted => self.submitted_at = Some(at),
            CommitmentStage::Processed => self.processed_at = Some(at),
            CommitmentStage::Confirmed => self.confirmed_at = Some(at),
            CommitmentStage::Finalized => self.finalized_at = Some(at),
            CommitmentStage::Failed => {}
        }
        self.recompute();
    }

    fn recompute(&mut self) {
        self.submit_to_processed_ms = delta_ms(self.submitted_at, self.processed_at);
        self.processed_to_confirmed_ms = delta_ms(self.processed_at, self.confirmed_at);
        self.confirmed_to_finalized_ms = delta_ms(self.confirmed_at, self.finalized_at);
    }
}

fn delta_ms(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Option<i64> {
    match (from, to) {
        (Some(a), Some(b)) => Some((b - a).num_milliseconds()),
        _ => None,
    }
}
