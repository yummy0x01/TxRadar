//! Stream ingest (Phase 8) — the bridge from the live Yellowstone stream into
//! the lifecycle tracker and a shared network view.
//!
//! In the production `run` path the executor no longer polls RPC/Jito to learn
//! that a bundle landed. Instead this background task consumes [`StreamEvent`]s
//! and:
//!   * feeds slot-commitment + transaction updates into a shared
//!     [`LifecycleTracker`], so landing is confirmed **from the stream**;
//!   * maintains a shared [`NetworkState`] (current slot, live skip rate,
//!     connectivity) that the agent reasons over for tip sizing.
//!
//! The tracker and network state are shared with the executor via `Arc<Mutex>`;
//! locks are held only for the brief, synchronous update and never across an
//! `.await`, so the render loop and agent loop can't be blocked.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::Utc;

use txradar_core::tracker::{LifecycleTracker, SlotCommitment};
use txradar_stream::{ConnectionState, SlotStatus, StreamEvent, StreamHandle};

/// Shared, continuously-updated view of the network, read by the executor when
/// it builds a [`txradar_agent::DecisionContext`].
#[derive(Debug, Clone)]
pub struct NetworkState {
    /// Highest slot observed on the stream (processed commitment).
    pub current_slot: u64,
    /// Fraction of recent slots that were skipped (never confirmed) — the live
    /// congestion / competition proxy the tip oracle consumes. `[0.0, 1.0]`.
    pub skip_rate: f32,
    /// Latest stream connectivity state (surfaced to logs / the TUI).
    pub connection: ConnectionState,
    /// Whether we've seen at least one slot yet (so callers can tell "slot 0"
    /// apart from "no data yet").
    pub seen_any_slot: bool,
}

impl Default for NetworkState {
    fn default() -> Self {
        Self {
            current_slot: 0,
            skip_rate: 0.0,
            connection: ConnectionState::Connecting,
            seen_any_slot: false,
        }
    }
}

/// Shared handle to the network view.
pub type SharedNetwork = Arc<Mutex<NetworkState>>;
/// Shared handle to the lifecycle tracker (fed by ingest, read by the executor).
pub type SharedTracker = Arc<Mutex<LifecycleTracker>>;

/// Sliding window over recently *confirmed* slot numbers, used to estimate the
/// skip rate. If every slot in the observed span confirmed, the rate is 0; if
/// half the slots in the span never confirmed, it's ~0.5.
struct SkipWindow {
    /// Confirmed slot numbers, strictly increasing, capped at `capacity`.
    slots: VecDeque<u64>,
    capacity: usize,
}

impl SkipWindow {
    fn new(capacity: usize) -> Self {
        Self { slots: VecDeque::with_capacity(capacity), capacity }
    }

    /// Record a newly confirmed slot and return the current skip-rate estimate.
    fn observe(&mut self, slot: u64) -> f32 {
        // Ignore out-of-order / duplicate slots (stream replay can re-deliver).
        if self.slots.back().map(|&b| slot <= b).unwrap_or(false) {
            return self.rate();
        }
        self.slots.push_back(slot);
        while self.slots.len() > self.capacity {
            self.slots.pop_front();
        }
        self.rate()
    }

    /// `1 - confirmed/span` over the window. Needs at least a few samples to be
    /// meaningful; returns 0 until then.
    fn rate(&self) -> f32 {
        let (Some(&first), Some(&last)) = (self.slots.front(), self.slots.back()) else {
            return 0.0;
        };
        let span = last.saturating_sub(first) + 1;
        if span < 8 {
            return 0.0;
        }
        let confirmed = self.slots.len() as u64;
        let skipped = span.saturating_sub(confirmed);
        (skipped as f32 / span as f32).clamp(0.0, 1.0)
    }
}

/// Spawn the ingest task. It runs until the stream channel closes (the
/// [`StreamHandle`] is dropped or the stream task exits).
pub fn spawn(mut handle: StreamHandle, tracker: SharedTracker, net: SharedNetwork) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut skip = SkipWindow::new(150);
        while let Some(event) = handle.events.recv().await {
            match event {
                StreamEvent::SlotStatus { slot, status, .. } => {
                    on_slot(&tracker, &net, &mut skip, slot, status);
                }
                StreamEvent::Transaction { signature, slot, failed } => {
                    // A watched tx (our fee-payer's) landed on-chain.
                    if let Ok(mut t) = tracker.lock() {
                        t.on_transaction(&signature, slot, failed, Utc::now());
                    }
                }
                StreamEvent::Connection(state) => {
                    if let Ok(mut n) = net.lock() {
                        n.connection = state;
                    }
                }
                StreamEvent::Leader { .. } => { /* leader identity handled via Jito schedule */ }
            }
        }
        tracing::debug!(target: "txradar::ingest", "stream closed; ingest task exiting");
    })
}

/// Handle a slot-commitment update: advance the tracker and refresh the shared
/// slot / skip-rate view.
fn on_slot(
    tracker: &SharedTracker,
    net: &SharedNetwork,
    skip: &mut SkipWindow,
    slot: u64,
    status: SlotStatus,
) {
    let now = Utc::now();
    let commitment = match status {
        SlotStatus::Processed => Some(SlotCommitment::Processed),
        SlotStatus::Confirmed => Some(SlotCommitment::Confirmed),
        SlotStatus::Finalized => Some(SlotCommitment::Finalized),
        // Intra-slot statuses don't move commitment; ignore for the tracker.
        SlotStatus::FirstShredReceived | SlotStatus::Completed | SlotStatus::Dead => None,
    };

    if let Some(level) = commitment {
        if let Ok(mut t) = tracker.lock() {
            t.on_slot_commitment(slot, level, now);
        }
    }

    // Skip rate is estimated from the confirmed-slot stream.
    let new_rate = if matches!(status, SlotStatus::Confirmed) {
        Some(skip.observe(slot))
    } else {
        None
    };

    if let Ok(mut n) = net.lock() {
        if slot > n.current_slot {
            n.current_slot = slot;
            n.seen_any_slot = true;
        }
        if let Some(rate) = new_rate {
            n.skip_rate = rate;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_window_zero_when_contiguous() {
        let mut w = SkipWindow::new(150);
        let mut rate = 0.0;
        for s in 100..120 {
            rate = w.observe(s);
        }
        assert_eq!(rate, 0.0, "contiguous confirmed slots => no skips");
    }

    #[test]
    fn skip_window_detects_gaps() {
        let mut w = SkipWindow::new(150);
        // Confirm only even slots over a span of 40 => ~half skipped.
        let mut rate = 0.0;
        for s in (100..140).step_by(2) {
            rate = w.observe(s);
        }
        assert!(rate > 0.4 && rate < 0.6, "expected ~50% skip, got {rate}");
    }

    #[test]
    fn skip_window_ignores_out_of_order() {
        let mut w = SkipWindow::new(150);
        for s in 100..120 {
            w.observe(s);
        }
        let before = w.rate();
        w.observe(105); // stale replay
        assert_eq!(w.rate(), before, "stale slot must not change the estimate");
    }
}
