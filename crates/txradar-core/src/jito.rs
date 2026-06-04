//! Jito block-engine JSON-RPC client (Phase 2).
//!
//! Thin async wrapper over the block-engine endpoints:
//! `sendBundle`, `getBundleStatuses`, `getInflightBundleStatuses`,
//! `getTipAccounts`. Respects the ~1 req/s/IP/region rate limit. The base URL
//! comes from config, so testnet <-> mainnet is a profile flip.

/// In-flight bundle status as reported by `getInflightBundleStatuses`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightStatus {
    Pending,
    Landed,
    Failed,
    Invalid,
}
