//! Failure taxonomy.
//!
//! The bounty requires detecting and classifying these specific failure modes.
//! Keeping them in one enum lets the core stack, the AI agent, and the log all
//! speak the same language about *why* something failed.

use serde::{Deserialize, Serialize};

/// A classified transaction/bundle failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// Blockhash no longer valid: current block height passed
    /// `lastValidBlockHeight`. Recoverable by refreshing the blockhash.
    ExpiredBlockhash,
    /// Priority fee / Jito tip too low to land under current competition.
    FeeTooLow,
    /// Transaction exceeded its compute budget.
    ComputeExceeded,
    /// Bundle-level failure (atomicity violated, leader skipped, dropped, etc.).
    BundleFailure,
    /// Anything we could not map to the above.
    Unknown,
}

impl FailureClass {
    /// Whether a retry could plausibly succeed after the agent adjusts inputs.
    pub fn is_recoverable(self) -> bool {
        matches!(
            self,
            FailureClass::ExpiredBlockhash | FailureClass::FeeTooLow | FailureClass::BundleFailure
        )
    }

    /// Human-readable label for logs/TUI.
    pub fn label(self) -> &'static str {
        match self {
            FailureClass::ExpiredBlockhash => "expired_blockhash",
            FailureClass::FeeTooLow => "fee_too_low",
            FailureClass::ComputeExceeded => "compute_exceeded",
            FailureClass::BundleFailure => "bundle_failure",
            FailureClass::Unknown => "unknown",
        }
    }
}
