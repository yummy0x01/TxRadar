//! Radar TUI (Phase 7) — TxRadar's "mission control" dashboard.
//!
//! Renders, in real time:
//! * Slot stream + upcoming Jito leader windows.
//! * Live tip-floor gauge (percentiles + EMA).
//! * Per-transaction lifecycle table with a latency waterfall across
//!   Submitted -> Processed -> Confirmed -> Finalized.
//! * The AI agent's reasoning feed (so "reasoning is visible").
//! * Failure log with classifications.
//!
//! Built on `ratatui` + `crossterm`. Phase 0 defines the view-model the render
//! loop consumes; widgets land in Phase 7.

/// Snapshot of everything the dashboard draws on a given frame. The orchestrator
/// updates this from stream/agent/tracker events; the render loop reads it.
#[derive(Debug, Default)]
pub struct DashboardState {
    pub current_slot: u64,
    pub connection: String,
    pub next_leader_slot: Option<u64>,
    pub tip_p50: Option<f64>,
    pub recent_reasoning: Vec<String>,
}
