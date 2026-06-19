//! Blockhash lifecycle management (Phase 2).
//!
//! Fetches a recent blockhash at a *non-finalized* commitment (see README Q2),
//! remembers its `lastValidBlockHeight`, and exposes expiry detection so the
//! agent can decide to refresh. The fault-injection harness can force a stale
//! blockhash here to exercise the autonomous-retry path.

use solana_sdk::hash::Hash;
use std::str::FromStr;

use crate::rpc::{RpcClient, RpcError};

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

    /// Parse the base58 blockhash into the `solana_sdk` `Hash` needed to sign a
    /// transaction.
    pub fn as_hash(&self) -> Result<Hash, BlockhashError> {
        Hash::from_str(&self.blockhash).map_err(|e| BlockhashError::Parse(e.to_string()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlockhashError {
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcError),
    #[error("failed to parse blockhash: {0}")]
    Parse(String),
}

/// Owns the current blockhash and the commitment we fetch it at. The commitment
/// must NOT be `finalized` for time-sensitive sends: a finalized hash is already
/// ~31+ slots old and burns part of its ~150-block validity window.
pub struct BlockhashManager {
    rpc: RpcClient,
    commitment: String,
    current: Option<TrackedBlockhash>,
}

impl BlockhashManager {
    pub fn new(rpc: RpcClient, commitment: impl Into<String>) -> Self {
        Self { rpc, commitment: commitment.into(), current: None }
    }

    /// Fetch a fresh blockhash from RPC and store it as current.
    pub async fn refresh(&mut self) -> Result<&TrackedBlockhash, BlockhashError> {
        let latest = self.rpc.get_latest_blockhash(&self.commitment).await?;
        self.current = Some(TrackedBlockhash {
            blockhash: latest.blockhash,
            last_valid_block_height: latest.last_valid_block_height,
            forced_stale: false,
        });
        Ok(self.current.as_ref().expect("just set"))
    }

    /// The current blockhash, if one has been fetched.
    pub fn current(&self) -> Option<&TrackedBlockhash> {
        self.current.as_ref()
    }

    /// Inject a blockhash directly, bypassing RPC. Used by the simulated chain
    /// mode (Phase 6) so a real keypair can sign an offline transaction; the
    /// `blockhash` must be a valid base58 32-byte hash.
    pub fn set_simulated(&mut self, blockhash: String, last_valid_block_height: u64) {
        self.current = Some(TrackedBlockhash {
            blockhash,
            last_valid_block_height,
            forced_stale: false,
        });
    }

    /// Ask RPC for the current block height (used for expiry checks).
    pub async fn current_block_height(&self) -> Result<u64, BlockhashError> {
        Ok(self.rpc.get_block_height(&self.commitment).await?)
    }

    /// Fault injection: mark the current blockhash stale so the next expiry
    /// check trips, exercising the autonomous refresh-and-retry path.
    pub fn force_stale(&mut self) {
        if let Some(current) = self.current.as_mut() {
            current.forced_stale = true;
        }
    }
}
