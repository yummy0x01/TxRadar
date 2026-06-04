//! AI agent layer (Phase 5) — the operational decision-maker.
//!
//! Cleanly separated from the core stack (an explicit judging criterion): the
//! agent decides *policy*, the core executes it. The agent owns four real
//! decisions, all with **visible reasoning** (every decision records its inputs
//! and rationale, which the TUI and log surface):
//!
//! 1. Tip intelligence — how much to tip, balancing cost vs. landing probability.
//! 2. Submission timing — submit now, or hold when conditions are unfavorable.
//! 3. Failure reasoning — given a failure, decide what to change.
//! 4. Autonomous retry (with fault injection) — detect blockhash expiry, reason
//!    about cause, refresh, recalculate the tip, and resubmit — not hardcoded.
//!
//! The LLM is hidden behind the [`Decider`] trait so it stays swappable
//! (Anthropic Claude is the default impl) and out of the latency-critical path:
//! it sets policy at decision points; the core applies it deterministically.

use async_trait::async_trait;

use txradar_types::FailureClass;
use txradar_tips::TipContext;

/// Inputs the agent reasons over at a decision point.
#[derive(Debug, Clone)]
pub struct DecisionContext {
    pub attempt_id: u64,
    pub current_slot: u64,
    /// Slots until our target leader's window opens (negative = passed).
    pub slots_to_leader: i64,
    pub tips: TipContext,
    /// Present when reasoning about a failed attempt.
    pub last_failure: Option<FailureClass>,
    pub retries_so_far: u32,
}

/// The agent's decision, with the rationale captured for transparency.
#[derive(Debug, Clone)]
pub struct Decision {
    pub action: Action,
    pub tip_lamports: u64,
    /// Natural-language reasoning, surfaced in the TUI and stored in the log.
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Submit the bundle now with the chosen tip.
    Submit,
    /// Hold — conditions unfavorable; re-evaluate next tick.
    Hold,
    /// Refresh the blockhash, recalculate tip, and resubmit.
    RefreshAndResubmit,
    /// Give up on this transaction.
    Abort,
}

/// Swappable reasoning backend. Anthropic Claude is the default; this boundary
/// keeps the AI layer isolated from the core stack and easy to mock in tests.
#[async_trait]
pub trait Decider: Send + Sync {
    async fn decide(&self, ctx: &DecisionContext) -> Result<Decision, AgentError>;
}

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("agent backend not yet implemented (Phase 5)")]
    NotImplemented,
    #[error("llm request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not parse model response: {0}")]
    Parse(String),
}

pub mod anthropic;
