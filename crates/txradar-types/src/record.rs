//! The structured lifecycle log record — the graded deliverable.
//!
//! Each bundle submission produces one [`BundleRecord`], serialized as a line of
//! JSONL. The bounty requires every entry to contain: slot numbers, commitment
//! progression, timestamps, tip amounts, and failure classification (if any).
//! Judges cross-reference the slot/signature against Solana explorers.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::failure::FailureClass;
use crate::lifecycle::{LifecycleEvent, StageTimings};

/// One bundle submission's full lifecycle, start to terminal state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRecord {
    /// Local correlation id (monotonic per run).
    pub attempt_id: u64,
    /// Network this ran on ("testnet" / "mainnet") — for explorer cross-ref.
    pub network: String,

    /// Jito bundle id (SHA-256 of tx signatures), once submitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
    /// Primary transaction signature, for explorer verification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,

    /// Recent blockhash used, and the last block height it was valid for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blockhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_valid_block_height: Option<u64>,

    /// Slot the bundle landed in (the number judges check on the explorer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub landed_slot: Option<u64>,
    /// Leader the submission targeted, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_leader: Option<String>,

    /// Tip paid, in lamports, and a short trace of why the agent chose it.
    pub tip_lamports: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tip_rationale: Option<String>,

    /// Ordered list of observed commitment transitions.
    pub events: Vec<LifecycleEvent>,
    /// Derived per-stage timestamps and latency deltas.
    pub timings: StageTimings,

    /// Terminal failure classification, if this attempt failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureClass>,
    /// If this attempt was an agent-driven retry, the attempt_id it followed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_of: Option<u64>,
    /// Whether a fault was deliberately injected (e.g. forced stale blockhash).
    #[serde(default)]
    pub fault_injected: bool,

    /// When this record was first opened.
    pub created_at: DateTime<Utc>,
}

impl BundleRecord {
    pub fn new(attempt_id: u64, network: impl Into<String>, created_at: DateTime<Utc>) -> Self {
        Self {
            attempt_id,
            network: network.into(),
            bundle_id: None,
            signature: None,
            blockhash: None,
            last_valid_block_height: None,
            landed_slot: None,
            target_leader: None,
            tip_lamports: 0,
            tip_rationale: None,
            events: Vec::new(),
            timings: StageTimings::default(),
            failure: None,
            retry_of: None,
            fault_injected: false,
            created_at,
        }
    }
}
