//! Tip oracle (Phase 4) — turns live data into tip *signals*, not decisions.
//!
//! Pulls Jito's tip-floor percentiles (25/50/75/95/99th + EMA) from
//! `bundles.jito.wtf`, blends them with current slot/congestion conditions, and
//! exposes a recommended range. The AI agent consumes this to make the final
//! call — there are **no hardcoded tip values** anywhere in the stack.

use serde::Deserialize;

/// Snapshot of the Jito tip floor, in lamports per percentile.
#[derive(Debug, Clone, Deserialize)]
pub struct TipFloor {
    #[serde(rename = "landed_tips_25th_percentile")]
    pub p25: f64,
    #[serde(rename = "landed_tips_50th_percentile")]
    pub p50: f64,
    #[serde(rename = "landed_tips_75th_percentile")]
    pub p75: f64,
    #[serde(rename = "landed_tips_95th_percentile")]
    pub p95: f64,
    #[serde(rename = "landed_tips_99th_percentile")]
    pub p99: f64,
    #[serde(rename = "ema_landed_tips_50th_percentile")]
    pub ema_p50: f64,
}

/// What the oracle hands the agent: live percentiles plus a derived context the
/// agent reasons over (it does NOT pre-decide the tip).
#[derive(Debug, Clone)]
pub struct TipContext {
    pub floor: TipFloor,
    /// Recent skipped-slot rate [0.0, 1.0] — a congestion / competition proxy.
    pub recent_skip_rate: f32,
}

#[derive(Debug, thiserror::Error)]
pub enum TipError {
    #[error("fetching tip floor: {0}")]
    Http(#[from] reqwest::Error),
}
