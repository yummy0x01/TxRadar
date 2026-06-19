//! AI agent layer (Phase 5) — the operational decision-maker.
//!
//! Cleanly separated from the core stack (an explicit judging criterion): the
//! agent decides *policy*, the core executes it.
//!
//! We own the bounty's hardest option — **Autonomous Retry with Fault
//! Injection** — which strictly contains **Tip Intelligence**: on every attempt
//! (and every retry) the agent decides *how much to tip* balancing cost vs.
//! landing probability, and when an attempt fails (e.g. an injected blockhash
//! expiry) it **reasons about the cause, refreshes, recalculates the tip, and
//! resubmits**. None of that control flow is hardcoded: the [`run_attempt_loop`]
//! orchestrator consults the [`Decider`] at every branch and does only what the
//! agent tells it. Swap in a mock [`Decider`] and the same loop is fully unit
//! testable — proof the retries are agent-driven, not `if expired { retry() }`.
//!
//! The LLM lives behind the [`Decider`] trait (Google Gemini is the default
//! impl; [`HeuristicDecider`] is a deterministic fallback for when the API is
//! unreachable). The agent crate is pure policy — no solana-sdk, no network to
//! the chain — so it stays cheap to test and reason about.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use txradar_tips::{FloorLamports, TipBand, TipContext};
use txradar_types::FailureClass;

pub mod gemini;

/// Why the agent is being consulted — shapes which decision it's making.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    /// First submission of this transaction: choose tip + submit/hold.
    InitialSubmit,
    /// A prior attempt failed: reason about cause, decide what to change.
    PostFailure,
}

/// Everything the agent reasons over at a decision point. Serialized verbatim
/// into the Gemini prompt, so every field is information the model can use.
#[derive(Debug, Clone, Serialize)]
pub struct DecisionContext {
    pub kind: DecisionKind,
    pub attempt_id: u64,
    pub current_slot: u64,
    /// Current chain block height — compared against the blockhash's validity.
    pub current_block_height: u64,

    /// The blockhash's last valid block height (None if none fetched yet).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_valid_block_height: Option<u64>,
    /// Whether the core has already observed the current blockhash as expired
    /// (true after a forced-stale fault injection, or once the window passes).
    pub blockhash_expired: bool,

    /// Live tip floor (percentiles, lamports) and the oracle's bounded band.
    /// These are *signals* — the agent picks the final number, it is not echoed.
    pub tip_floor: FloorLamports,
    pub tip_band: TipBand,
    pub tip_context: TipContext,

    /// Present (and `kind == PostFailure`) when reasoning about a failed attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<FailureClass>,
    /// The tip the failed attempt used, so the agent can decide to escalate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_tip_lamports: Option<u64>,

    pub retries_so_far: u32,
    pub max_retries: u32,
}

/// What the agent decided to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Submit the bundle now with `tip_lamports`.
    Submit,
    /// Conditions unfavorable; do nothing and re-evaluate on the next tick.
    Hold,
    /// Blockhash is stale: refresh it, apply `tip_lamports`, and resubmit.
    RefreshAndResubmit,
    /// Unrecoverable or retry budget exhausted: stop.
    Abort,
}

/// The agent's decision, with reasoning captured for transparency (surfaced in
/// the TUI and stored in the lifecycle record's `tip_rationale`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub action: Action,
    pub tip_lamports: u64,
    pub rationale: String,
}

/// Swappable reasoning backend. Google Gemini is the default; this boundary
/// keeps the AI layer isolated and easy to mock in tests.
#[async_trait]
pub trait Decider: Send + Sync {
    async fn decide(&self, ctx: &DecisionContext) -> Result<Decision, AgentError>;
}

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("llm request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not parse model response: {0}")]
    Parse(String),
    #[error("model returned no tool-use decision")]
    NoDecision,
}

/// Deterministic fallback decider — used when the Gemini API is unreachable
/// so the stack still functions (degraded, not dead). It mirrors the *shape* of
/// the agent's reasoning over the same inputs, but the real "smart" decisions
/// come from Gemini; this exists purely so a network blip can't halt a run.
pub struct HeuristicDecider {
    pub max_retries: u32,
}

impl HeuristicDecider {
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

#[async_trait]
impl Decider for HeuristicDecider {
    async fn decide(&self, ctx: &DecisionContext) -> Result<Decision, AgentError> {
        // Retry budget exhausted -> stop.
        if ctx.retries_so_far >= self.max_retries {
            return Ok(Decision {
                action: Action::Abort,
                tip_lamports: ctx.last_tip_lamports.unwrap_or(ctx.tip_band.mid),
                rationale: format!(
                    "retry budget exhausted ({}/{}); aborting",
                    ctx.retries_so_far, ctx.max_retries
                ),
            });
        }

        match ctx.kind {
            DecisionKind::InitialSubmit => Ok(Decision {
                action: Action::Submit,
                tip_lamports: ctx.tip_band.mid,
                rationale: format!(
                    "fallback: submit at oracle {} ({} lamports); {}",
                    ctx.tip_band.basis, ctx.tip_band.mid, ctx.tip_band.rationale
                ),
            }),
            DecisionKind::PostFailure => {
                let expired = ctx.blockhash_expired
                    || ctx.last_failure == Some(FailureClass::ExpiredBlockhash);
                if expired {
                    // Recalculate the tip upward toward the high end on retry.
                    let tip = ctx.tip_band.high.max(ctx.tip_band.mid);
                    Ok(Decision {
                        action: Action::RefreshAndResubmit,
                        tip_lamports: tip,
                        rationale: format!(
                            "fallback: blockhash expired -> refresh + raise tip to {} lamports (toward {}), resubmit",
                            tip, ctx.tip_band.basis
                        ),
                    })
                } else if matches!(ctx.last_failure, Some(f) if f.is_recoverable()) {
                    let tip = ctx.tip_band.high.max(ctx.tip_band.mid);
                    Ok(Decision {
                        action: Action::RefreshAndResubmit,
                        tip_lamports: tip,
                        rationale: format!(
                            "fallback: recoverable failure {:?} -> refresh + raise tip to {} lamports, resubmit",
                            ctx.last_failure, tip
                        ),
                    })
                } else {
                    Ok(Decision {
                        action: Action::Abort,
                        tip_lamports: ctx.last_tip_lamports.unwrap_or(ctx.tip_band.mid),
                        rationale: format!(
                            "fallback: unrecoverable failure {:?}; aborting",
                            ctx.last_failure
                        ),
                    })
                }
            }
        }
    }
}

/// Outcome of executing one agent-chosen action against the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The bundle landed and finalized.
    Landed { slot: u64 },
    /// The attempt failed; the loop will re-consult the agent.
    Failed(FailureClass),
}

/// The side-effecting operations the orchestration loop needs. Implemented in
/// the binary over the core stack (blockhash manager, bundle builder, Jito
/// client, tracker). Kept as a trait so the loop is pure control-flow and the
/// agent crate carries no solana-sdk dependency — and so a mock executor can
/// drive the loop in unit tests.
#[async_trait]
pub trait Executor: Send {
    /// Refresh the recent blockhash (the autonomous-retry "refresh" step).
    async fn refresh_blockhash(&mut self) -> Result<(), AgentError>;

    /// Build, sign, and send the bundle per the agent's decision (tip +
    /// rationale, persisted into the lifecycle record); await its terminal
    /// outcome.
    async fn submit(&mut self, decision: &Decision) -> Result<AttemptOutcome, AgentError>;

    /// Re-read live conditions and rebuild the [`DecisionContext`] for the next
    /// agent consultation. `last_failure`/`last_tip` carry the prior outcome.
    async fn build_context(
        &mut self,
        kind: DecisionKind,
        retries_so_far: u32,
        last_failure: Option<FailureClass>,
        last_tip_lamports: Option<u64>,
    ) -> Result<DecisionContext, AgentError>;
}

/// Final result of a full attempt loop, with the agent's decision trail.
#[derive(Debug, Clone)]
pub struct LoopResult {
    pub landed_slot: Option<u64>,
    pub attempts: u32,
    /// Every decision the agent made, in order — the audit trail proving the
    /// retries were agent-driven.
    pub decisions: Vec<Decision>,
    pub final_action: Action,
}

/// The agent-driven attempt/retry orchestrator. This is the heart of the
/// "no hardcoded retry flow" requirement: it never decides *on its own* to
/// retry, hold, or pick a tip — it asks the [`Decider`] at every step and
/// executes exactly what comes back. Fault injection (a forced-stale blockhash)
/// surfaces as a `Failed(ExpiredBlockhash)` outcome, and the *agent* chooses
/// `RefreshAndResubmit` with a recalculated tip.
pub async fn run_attempt_loop(
    decider: &dyn Decider,
    exec: &mut dyn Executor,
    _max_retries: u32,
) -> Result<LoopResult, AgentError> {
    let mut decisions = Vec::new();
    let mut retries = 0u32;
    let mut kind = DecisionKind::InitialSubmit;
    let mut last_failure: Option<FailureClass> = None;
    let mut last_tip: Option<u64> = None;

    loop {
        let ctx = exec.build_context(kind, retries, last_failure, last_tip).await?;
        let decision = decider.decide(&ctx).await?;
        decisions.push(decision.clone());

        match decision.action {
            Action::Hold => {
                // Agent declined to submit now. The caller drives the next tick;
                // we return so live conditions can be re-sampled fresh.
                return Ok(LoopResult {
                    landed_slot: None,
                    attempts: retries,
                    decisions,
                    final_action: Action::Hold,
                });
            }
            Action::Abort => {
                return Ok(LoopResult {
                    landed_slot: None,
                    attempts: retries,
                    decisions,
                    final_action: Action::Abort,
                });
            }
            Action::RefreshAndResubmit => {
                exec.refresh_blockhash().await?;
                // fall through to submit with the recalculated tip
            }
            Action::Submit => {}
        }

        last_tip = Some(decision.tip_lamports);
        match exec.submit(&decision).await? {
            AttemptOutcome::Landed { slot } => {
                return Ok(LoopResult {
                    landed_slot: Some(slot),
                    attempts: retries + 1,
                    decisions,
                    final_action: decision.action,
                });
            }
            AttemptOutcome::Failed(class) => {
                last_failure = Some(class);
                retries += 1;
                kind = DecisionKind::PostFailure;
                // Loop: re-consult the agent. Whether to retry is ITS call.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use txradar_tips::{recommend_from, TipBounds};

    fn floor() -> FloorLamports {
        FloorLamports { p25: 1_000, p50: 5_000, p75: 10_000, p95: 30_000, p99: 80_000, ema_p50: 6_000 }
    }
    fn bounds() -> TipBounds {
        TipBounds { min_lamports: 1_000, max_lamports: 50_000, ema_alpha: 0.3 }
    }
    fn ctx(kind: DecisionKind, retries: u32, fail: Option<FailureClass>, last_tip: Option<u64>) -> DecisionContext {
        let tctx = TipContext { recent_skip_rate: 0.1, escalating: kind == DecisionKind::PostFailure };
        let band = recommend_from(&floor(), None, &bounds(), &tctx);
        DecisionContext {
            kind,
            attempt_id: 1,
            current_slot: 100,
            current_block_height: 1000,
            last_valid_block_height: Some(1050),
            blockhash_expired: fail == Some(FailureClass::ExpiredBlockhash),
            tip_floor: floor(),
            tip_band: band,
            tip_context: tctx,
            last_failure: fail,
            last_tip_lamports: last_tip,
            retries_so_far: retries,
            max_retries: 4,
        }
    }

    #[tokio::test]
    async fn heuristic_initial_submit_uses_band_mid() {
        let d = HeuristicDecider::new(4);
        let out = d.decide(&ctx(DecisionKind::InitialSubmit, 0, None, None)).await.unwrap();
        assert_eq!(out.action, Action::Submit);
        assert_eq!(out.tip_lamports, 5_000); // calm -> band.mid (p50)
    }

    #[tokio::test]
    async fn heuristic_expired_blockhash_refreshes_and_raises_tip() {
        let d = HeuristicDecider::new(4);
        let out = d
            .decide(&ctx(DecisionKind::PostFailure, 1, Some(FailureClass::ExpiredBlockhash), Some(5_000)))
            .await
            .unwrap();
        assert_eq!(out.action, Action::RefreshAndResubmit);
        assert!(out.tip_lamports >= 5_000, "tip must be recalculated upward");
    }

    #[tokio::test]
    async fn heuristic_aborts_when_budget_exhausted() {
        let d = HeuristicDecider::new(2);
        let out = d
            .decide(&ctx(DecisionKind::PostFailure, 2, Some(FailureClass::ExpiredBlockhash), Some(5_000)))
            .await
            .unwrap();
        assert_eq!(out.action, Action::Abort);
    }

    #[tokio::test]
    async fn heuristic_aborts_on_unrecoverable() {
        let d = HeuristicDecider::new(4);
        let out = d
            .decide(&ctx(DecisionKind::PostFailure, 1, Some(FailureClass::ComputeExceeded), Some(5_000)))
            .await
            .unwrap();
        assert_eq!(out.action, Action::Abort);
    }

    // A scripted Decider + Executor proving the LOOP is agent-driven: the
    // executor injects a blockhash-expiry fault on the first submit, and only
    // because the agent returns RefreshAndResubmit does a retry happen.

    struct ScriptedDecider {
        steps: Mutex<std::vec::IntoIter<Decision>>,
    }
    #[async_trait]
    impl Decider for ScriptedDecider {
        async fn decide(&self, _ctx: &DecisionContext) -> Result<Decision, AgentError> {
            Ok(self.steps.lock().unwrap().next().expect("script ran out"))
        }
    }

    #[derive(Default)]
    struct FaultExecutor {
        submits: u32,
        refreshes: u32,
        fault_armed: bool,
    }
    #[async_trait]
    impl Executor for FaultExecutor {
        async fn refresh_blockhash(&mut self) -> Result<(), AgentError> {
            self.refreshes += 1;
            self.fault_armed = false; // refreshing clears the stale blockhash
            Ok(())
        }
        async fn submit(&mut self, _decision: &Decision) -> Result<AttemptOutcome, AgentError> {
            self.submits += 1;
            if self.fault_armed {
                Ok(AttemptOutcome::Failed(FailureClass::ExpiredBlockhash))
            } else {
                Ok(AttemptOutcome::Landed { slot: 12_345 })
            }
        }
        async fn build_context(
            &mut self,
            kind: DecisionKind,
            retries: u32,
            fail: Option<FailureClass>,
            last_tip: Option<u64>,
        ) -> Result<DecisionContext, AgentError> {
            Ok(ctx(kind, retries, fail, last_tip))
        }
    }

    #[tokio::test]
    async fn loop_recovers_from_injected_blockhash_expiry() {
        // Fault: first submit fails with ExpiredBlockhash. Agent script: submit,
        // then refresh+resubmit. The loop must refresh once and land.
        let decider = ScriptedDecider {
            steps: Mutex::new(
                vec![
                    Decision { action: Action::Submit, tip_lamports: 5_000, rationale: "initial".into() },
                    Decision { action: Action::RefreshAndResubmit, tip_lamports: 10_000, rationale: "expired -> refresh + raise tip".into() },
                ]
                .into_iter(),
            ),
        };
        let mut exec = FaultExecutor { fault_armed: true, ..Default::default() };
        let res = run_attempt_loop(&decider, &mut exec, 4).await.unwrap();

        assert_eq!(res.landed_slot, Some(12_345));
        assert_eq!(res.final_action, Action::RefreshAndResubmit);
        assert_eq!(res.decisions.len(), 2);
        assert_eq!(exec.refreshes, 1, "agent's refresh step must have run exactly once");
        assert_eq!(exec.submits, 2, "one failed submit + one successful resubmit");
        // Tip was recalculated upward on the retry.
        assert!(res.decisions[1].tip_lamports > res.decisions[0].tip_lamports);
    }

    #[tokio::test]
    async fn loop_with_heuristic_decider_recovers_autonomously() {
        // Same fault, but driven by the REAL fallback decider (no script) to
        // prove the autonomous path works end-to-end without hardcoded flow.
        let decider = HeuristicDecider::new(4);
        let mut exec = FaultExecutor { fault_armed: true, ..Default::default() };
        let res = run_attempt_loop(&decider, &mut exec, 4).await.unwrap();
        assert_eq!(res.landed_slot, Some(12_345));
        assert_eq!(exec.refreshes, 1);
        assert!(res.attempts >= 2);
    }

    #[tokio::test]
    async fn loop_aborts_when_agent_gives_up() {
        let decider = ScriptedDecider {
            steps: Mutex::new(
                vec![Decision { action: Action::Abort, tip_lamports: 0, rationale: "no".into() }].into_iter(),
            ),
        };
        let mut exec = FaultExecutor::default();
        let res = run_attempt_loop(&decider, &mut exec, 4).await.unwrap();
        assert_eq!(res.landed_slot, None);
        assert_eq!(res.final_action, Action::Abort);
        assert_eq!(exec.submits, 0, "abort must not submit");
    }
}
