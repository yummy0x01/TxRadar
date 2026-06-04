//! Blockhash lifecycle management (Phase 2).
//!
//! Fetches a recent blockhash at a *non-finalized* commitment (see README Q2),
//! remembers its `lastValidBlockHeight`, and exposes expiry detection so the
//! agent can decide to refresh. The fault-injection harness can force a stale
//! blockhash here to exercise the autonomous-retry path.

/// A blockhash plus the block height beyond which it is no longer valid.
#[derive(Debug, Clone)]
pub struct TrackedBlockhash {
    pub blockhash: String,
    pub last_valid_block_height: u64,
    /// Set true by the fault-injection harness to simulate expiry.
    pub forced_stale: bool,
}

impl TrackedBlockhash {
    /// Expired once the current block height has passed its validity window,
    /// or if a fault was injected.
    pub fn is_expired(&self, current_block_height: u64) -> bool {
        self.forced_stale || current_block_height > self.last_valid_block_height
    }
}
