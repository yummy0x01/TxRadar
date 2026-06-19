//! Tip oracle (Phase 4) — turns live data into tip *signals*, not decisions.
//!
//! Pulls Jito's tip-floor percentiles (25/50/75/95/99th + EMA) from
//! `bundles.jito.wtf`, blends them with current slot/congestion conditions, and
//! exposes a recommended *band*. The AI agent (Phase 5) consumes this band to
//! make the final call — there are **no hardcoded tip values** anywhere in the
//! stack. Every number traces back to either the live Jito floor or the
//! operator-set safety bounds in `[tips]` of the profile.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Lamports per SOL — Jito's REST floor reports percentiles in SOL (floats),
/// the rest of the stack works in integer lamports.
pub const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

/// Snapshot of the Jito tip floor, in **SOL** per percentile (as the REST API
/// returns them). Convert to lamports via [`TipFloor::lamports`].
#[derive(Debug, Clone, Copy, Deserialize)]
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

impl TipFloor {
    /// One percentile field, converted SOL -> lamports (saturating at 0).
    fn to_lamports(sol: f64) -> u64 {
        if sol.is_finite() && sol > 0.0 {
            (sol * LAMPORTS_PER_SOL).round() as u64
        } else {
            0
        }
    }

    /// The whole floor in integer lamports.
    pub fn lamports(&self) -> FloorLamports {
        FloorLamports {
            p25: Self::to_lamports(self.p25),
            p50: Self::to_lamports(self.p50),
            p75: Self::to_lamports(self.p75),
            p95: Self::to_lamports(self.p95),
            p99: Self::to_lamports(self.p99),
            ema_p50: Self::to_lamports(self.ema_p50),
        }
    }
}

/// The Jito floor in integer lamports — what the oracle reasons over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FloorLamports {
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p95: u64,
    pub p99: u64,
    pub ema_p50: u64,
}

/// Operator safety bounds, mirrored from `[tips]` in the profile. The oracle's
/// output is always clamped into `[min, max]` — the live floor can never push a
/// tip past the operator's spend ceiling.
#[derive(Debug, Clone, Copy)]
pub struct TipBounds {
    pub min_lamports: u64,
    pub max_lamports: u64,
    /// EMA smoothing factor in (0, 1]; higher = more responsive to fresh data.
    pub ema_alpha: f64,
}

impl TipBounds {
    fn clamp(&self, lamports: u64) -> u64 {
        lamports.clamp(self.min_lamports, self.max_lamports)
    }
}

impl From<&txradar_types::config::TipsConfig> for TipBounds {
    fn from(c: &txradar_types::config::TipsConfig) -> Self {
        Self { min_lamports: c.min_lamports, max_lamports: c.max_lamports, ema_alpha: c.ema_alpha }
    }
}

/// Live conditions the oracle blends with the static floor (it does NOT
/// pre-decide the tip — it shapes which percentile the band centers on).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TipContext {
    /// Recent skipped-slot rate [0.0, 1.0] — a congestion / competition proxy.
    pub recent_skip_rate: f32,
    /// Whether this is a retry after a fee-too-low failure (bias upward).
    pub escalating: bool,
}

impl Default for TipContext {
    fn default() -> Self {
        Self { recent_skip_rate: 0.0, escalating: false }
    }
}

/// A three-point tip band in lamports, plus a machine- and human-readable trace
/// of how it was derived. The agent picks within `[low, high]`; `mid` is the
/// oracle's default recommendation; `rationale` is copied into the lifecycle
/// record's `tip_rationale`.
#[derive(Debug, Clone, Serialize)]
pub struct TipBand {
    pub low: u64,
    pub mid: u64,
    pub high: u64,
    /// Which percentile drove `mid` (e.g. "p75").
    pub basis: &'static str,
    pub rationale: String,
}

#[derive(Debug, thiserror::Error)]
pub enum TipError {
    #[error("fetching tip floor: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tip floor response was empty")]
    Empty,
}

/// Stateful tip oracle: holds the HTTP client + endpoint, the operator bounds,
/// and a smoothed EMA of the p50 floor that survives across fetches.
pub struct TipOracle {
    client: reqwest::Client,
    tip_floor_url: String,
    bounds: TipBounds,
    /// Smoothed p50 in lamports; seeded from the API's EMA field on first fetch.
    ema_p50: Option<f64>,
}

impl TipOracle {
    pub fn new(tip_floor_url: impl Into<String>, bounds: TipBounds) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        Self { client, tip_floor_url: tip_floor_url.into(), bounds, ema_p50: None }
    }

    /// The current smoothed p50 (lamports), clamped to bounds. `None` until the
    /// first successful refresh.
    pub fn smoothed_p50(&self) -> Option<u64> {
        self.ema_p50.map(|v| self.bounds.clamp(v.round() as u64))
    }

    /// Fetch the live Jito floor and fold its p50 into the running EMA. Returns
    /// the freshly observed floor (callers use the return value directly).
    pub async fn refresh(&mut self) -> Result<FloorLamports, TipError> {
        let floors: Vec<TipFloor> =
            self.client.get(&self.tip_floor_url).send().await?.error_for_status()?.json().await?;
        let floor = floors.first().ok_or(TipError::Empty)?.lamports();
        self.fold_ema(&floor);
        Ok(floor)
    }

    /// Update the EMA from a freshly observed floor. Seeds from the API's own
    /// EMA field on the very first observation so we don't start cold at 0.
    fn fold_ema(&mut self, floor: &FloorLamports) {
        let alpha = self.bounds.ema_alpha.clamp(f64::MIN_POSITIVE, 1.0);
        let sample = floor.p50 as f64;
        self.ema_p50 = Some(match self.ema_p50 {
            None => {
                // Prefer the API's published EMA as a warm seed; fall back to p50.
                if floor.ema_p50 > 0 {
                    floor.ema_p50 as f64
                } else {
                    sample
                }
            }
            Some(prev) => alpha * sample + (1.0 - alpha) * prev,
        });
    }

}

/// Pure recommendation logic, factored out so it can be unit-tested without any
/// network or mutable oracle state.
///
/// Congestion drives which percentile anchors `mid`:
/// - calm (skip < 20%): p50 — pay the median, don't overbid.
/// - busy (20–50%): p75.
/// - hot (> 50%): p95 — you're competing for scarce block space.
///
/// An `escalating` retry bumps one tier up. The smoothed EMA (when available)
/// raises `mid` if the median is trending up, damping single-sample noise. The
/// band is `[p25-ish low, anchor mid, p95/p99 high]`, all clamped to bounds.
pub fn recommend_from(
    floor: &FloorLamports,
    ema_p50: Option<f64>,
    bounds: &TipBounds,
    ctx: &TipContext,
) -> TipBand {
    let skip = ctx.recent_skip_rate.clamp(0.0, 1.0);

    // Pick the congestion tier, then escalate one tier on a fee-driven retry.
    let mut tier = if skip > 0.5 {
        2 // hot
    } else if skip >= 0.2 {
        1 // busy
    } else {
        0 // calm
    };
    if ctx.escalating {
        tier = (tier + 1).min(3);
    }

    let (anchor, basis, high) = match tier {
        0 => (floor.p50, "p50", floor.p75),
        1 => (floor.p75, "p75", floor.p95),
        2 => (floor.p95, "p95", floor.p99),
        _ => (floor.p99, "p99", floor.p99),
    };

    // Blend the anchor with the smoothed median when the trend runs hotter than
    // this single sample — protects against a momentary dip underbidding us.
    let mid_raw = match ema_p50 {
        Some(ema) if ema > anchor as f64 => ((anchor as f64 + ema) / 2.0).round() as u64,
        _ => anchor,
    };

    let low = bounds.clamp(floor.p25.max(bounds.min_lamports));
    let mid = bounds.clamp(mid_raw);
    let high = bounds.clamp(high.max(mid));

    let rationale = format!(
        "skip_rate={:.0}% -> {} anchor {} lamports{}; band [{}, {}] clamped to [{}, {}]",
        skip * 100.0,
        basis,
        anchor,
        if ctx.escalating { " (escalated retry)" } else { "" },
        low,
        high,
        bounds.min_lamports,
        bounds.max_lamports,
    );

    TipBand { low, mid, high, basis, rationale }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floor() -> FloorLamports {
        // Realistic-ish mainnet floor, in lamports.
        FloorLamports { p25: 1_000, p50: 5_000, p75: 10_000, p95: 30_000, p99: 80_000, ema_p50: 6_000 }
    }

    fn bounds() -> TipBounds {
        TipBounds { min_lamports: 1_000, max_lamports: 50_000, ema_alpha: 0.3 }
    }

    #[test]
    fn sol_floats_convert_to_lamports() {
        let raw = TipFloor {
            p25: 0.000_001,
            p50: 0.000_005,
            p75: 0.000_010,
            p95: 0.000_030,
            p99: 0.000_080,
            ema_p50: 0.000_006,
        };
        let l = raw.lamports();
        assert_eq!(l.p25, 1_000);
        assert_eq!(l.p50, 5_000);
        assert_eq!(l.p99, 80_000);
        assert_eq!(l.ema_p50, 6_000);
    }

    #[test]
    fn negative_or_nan_floor_saturates_to_zero() {
        assert_eq!(TipFloor::to_lamports(-1.0), 0);
        assert_eq!(TipFloor::to_lamports(f64::NAN), 0);
        assert_eq!(TipFloor::to_lamports(f64::INFINITY), 0);
    }

    #[test]
    fn calm_market_anchors_on_p50() {
        let ctx = TipContext { recent_skip_rate: 0.05, escalating: false };
        let band = recommend_from(&floor(), None, &bounds(), &ctx);
        assert_eq!(band.basis, "p50");
        assert_eq!(band.mid, 5_000);
        assert_eq!(band.high, 10_000); // p75
    }

    #[test]
    fn busy_market_anchors_on_p75() {
        let ctx = TipContext { recent_skip_rate: 0.35, escalating: false };
        let band = recommend_from(&floor(), None, &bounds(), &ctx);
        assert_eq!(band.basis, "p75");
        assert_eq!(band.mid, 10_000);
    }

    #[test]
    fn hot_market_anchors_on_p95() {
        let ctx = TipContext { recent_skip_rate: 0.80, escalating: false };
        let band = recommend_from(&floor(), None, &bounds(), &ctx);
        assert_eq!(band.basis, "p95");
        assert_eq!(band.mid, 30_000);
    }

    #[test]
    fn escalating_retry_bumps_one_tier() {
        let calm = TipContext { recent_skip_rate: 0.05, escalating: true };
        let band = recommend_from(&floor(), None, &bounds(), &calm);
        assert_eq!(band.basis, "p75"); // calm(p50) escalated -> p75
    }

    #[test]
    fn output_is_clamped_to_max_bound() {
        // Hot + escalating would anchor on p99 (80k), above the 50k ceiling.
        let ctx = TipContext { recent_skip_rate: 0.9, escalating: true };
        let band = recommend_from(&floor(), None, &bounds(), &ctx);
        assert_eq!(band.mid, 50_000); // clamped down to max_lamports
        assert!(band.high <= 50_000);
    }

    #[test]
    fn output_is_clamped_to_min_bound() {
        let tiny = FloorLamports { p25: 10, p50: 20, p75: 30, p95: 40, p99: 50, ema_p50: 20 };
        let ctx = TipContext { recent_skip_rate: 0.0, escalating: false };
        let band = recommend_from(&tiny, None, &bounds(), &ctx);
        assert_eq!(band.low, 1_000); // floor.p25(10) raised to min_lamports
        assert_eq!(band.mid, 1_000); // p50(20) raised to min_lamports
    }

    #[test]
    fn rising_ema_lifts_mid_above_single_sample() {
        // EMA (12k) runs hotter than the calm p50 anchor (5k) -> mid blends up.
        let ctx = TipContext { recent_skip_rate: 0.0, escalating: false };
        let band = recommend_from(&floor(), Some(12_000.0), &bounds(), &ctx);
        assert_eq!(band.mid, 8_500); // (5000 + 12000) / 2
    }

    #[test]
    fn ema_seeds_from_api_then_smooths() {
        let mut oracle = TipOracle::new("http://unused", bounds());
        // First observation seeds from the published EMA field (6000), not p50.
        oracle.fold_ema(&floor());
        assert_eq!(oracle.ema_p50, Some(6_000.0));
        // Second fold: alpha=0.3 -> 0.3*5000 + 0.7*6000 = 5700.
        oracle.fold_ema(&floor());
        assert_eq!(oracle.ema_p50, Some(5_700.0));
    }
}
