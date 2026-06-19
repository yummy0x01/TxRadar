//! Lifecycle tracker + failure classifier (Phase 3).
//!
//! Consumes normalized signals (transaction-seen, slot-commitment, RPC/Jito
//! status) and advances each in-flight bundle through
//! `Submitted -> Processed -> Confirmed -> Finalized`, computing the latency
//! deltas the bounty asks for and classifying failures. Landing is confirmed
//! from the *stream* (slot commitment of the slot the tx landed in); RPC/Jito
//! polling is only a backup.
//!
//! This module is deliberately decoupled from `txradar-stream`: the orchestrator
//! translates `StreamEvent`s into the primitive calls here. That keeps the state
//! machine pure and unit-testable with no network.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use txradar_types::failure::FailureClass;
use txradar_types::lifecycle::{CommitmentStage, LifecycleEvent};
use txradar_types::record::BundleRecord;

/// A slot's commitment level, as observed on the stream. (Subset of
/// [`CommitmentStage`] that a slot update can carry.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotCommitment {
    Processed,
    Confirmed,
    Finalized,
}

impl SlotCommitment {
    fn stage(self) -> CommitmentStage {
        match self {
            SlotCommitment::Processed => CommitmentStage::Processed,
            SlotCommitment::Confirmed => CommitmentStage::Confirmed,
            SlotCommitment::Finalized => CommitmentStage::Finalized,
        }
    }
}

/// One bundle being followed to a terminal state. Keyed in the tracker by its
/// primary signature, so that isn't duplicated here.
struct Tracked {
    record: BundleRecord,
    /// Slot the tx landed in, once seen.
    landed_slot: Option<u64>,
    /// Highest stage reached so far.
    stage: CommitmentStage,
}

/// Tracks all in-flight bundles and produces completed [`BundleRecord`]s.
#[derive(Default)]
pub struct LifecycleTracker {
    /// Active bundles, keyed by primary signature.
    active: HashMap<String, Tracked>,
    /// Terminal records awaiting the log writer.
    completed: Vec<BundleRecord>,
}

impl LifecycleTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin tracking a freshly submitted bundle. The record must already carry
    /// its primary `signature` (set at build time). Records the `Submitted`
    /// transition at `now`.
    pub fn track(&mut self, mut record: BundleRecord, now: DateTime<Utc>) {
        let Some(signature) = record.signature.clone() else {
            // No signature => nothing to follow on the stream. Complete it now
            // as an immediate failure so it isn't silently dropped.
            record.failure = Some(FailureClass::Unknown);
            self.completed.push(record);
            return;
        };
        record.events.push(LifecycleEvent {
            stage: CommitmentStage::Submitted,
            at: now,
            slot: None,
            note: Some("submitted to block engine".into()),
        });
        record.timings.observe(CommitmentStage::Submitted, now);
        self.active.insert(
            signature,
            Tracked { record, landed_slot: None, stage: CommitmentStage::Submitted },
        );
    }

    /// Number of bundles currently being followed.
    pub fn active_len(&self) -> usize {
        self.active.len()
    }

    /// A watched transaction was seen on-chain (from a stream `Transaction`
    /// update). Records the landed slot and advances to `Processed`. If the tx
    /// landed but errored, classifies and completes it.
    pub fn on_transaction(&mut self, signature: &str, slot: u64, failed: bool, now: DateTime<Utc>) {
        let Some(t) = self.active.get_mut(signature) else { return };
        if t.landed_slot.is_none() {
            t.landed_slot = Some(slot);
            t.record.landed_slot = Some(slot);
        }
        if failed {
            // Landed but reverted — without execution logs we treat it as a
            // bundle-level failure; a later Jito status may refine it.
            Self::fail(&mut self.completed, &mut self.active, signature, FailureClass::BundleFailure, now);
            return;
        }
        Self::advance(t, CommitmentStage::Processed, Some(slot), now, "tx seen on stream");
    }

    /// A slot reached a commitment level. Advances every bundle whose landed
    /// slot is at or before `slot` to the matching stage. `Finalized` is
    /// retroactive (a finalized slot finalizes its ancestors), so a finalized
    /// update for a later slot still finalizes an earlier landing.
    pub fn on_slot_commitment(&mut self, slot: u64, level: SlotCommitment, now: DateTime<Utc>) {
        let target = level.stage();
        let target_order = target.order().unwrap_or(0);

        // Collect signatures to finalize/advance, then mutate, to satisfy the
        // borrow checker (advance may move entries to `completed`).
        let mut to_advance: Vec<String> = Vec::new();
        for (sig, t) in self.active.iter() {
            let Some(landed) = t.landed_slot else { continue };
            if landed > slot {
                continue;
            }
            let current = t.stage.order().unwrap_or(0);
            if target_order > current {
                to_advance.push(sig.clone());
            }
        }
        for sig in to_advance {
            if let Some(t) = self.active.get_mut(&sig) {
                Self::advance(t, target, t.landed_slot, now, "slot reached commitment");
                if t.stage == CommitmentStage::Finalized {
                    if let Some(done) = self.active.remove(&sig) {
                        self.completed.push(done.record);
                    }
                }
            }
        }
    }

    /// Expire any bundle that never landed and whose blockhash validity window
    /// has passed (`current_block_height > last_valid_block_height`). This is
    /// the signal the agent uses to trigger refresh-and-retry.
    /// Returns the signatures that just expired.
    pub fn expire_unlanded(&mut self, current_block_height: u64, now: DateTime<Utc>) -> Vec<String> {
        let expired: Vec<String> = self
            .active
            .iter()
            .filter(|(_, t)| {
                t.landed_slot.is_none()
                    && t.record
                        .last_valid_block_height
                        .map(|lvbh| current_block_height > lvbh)
                        .unwrap_or(false)
            })
            .map(|(sig, _)| sig.clone())
            .collect();
        for sig in &expired {
            Self::fail(&mut self.completed, &mut self.active, sig, FailureClass::ExpiredBlockhash, now);
        }
        expired
    }

    /// Force a bundle into a classified terminal failure (e.g. from the agent or
    /// a Jito `Failed` status).
    pub fn mark_failed(&mut self, signature: &str, class: FailureClass, now: DateTime<Utc>) {
        Self::fail(&mut self.completed, &mut self.active, signature, class, now);
    }

    /// Whether a signature is still being followed (not yet terminal).
    pub fn is_active(&self, signature: &str) -> bool {
        self.active.contains_key(signature)
    }

    /// The highest commitment stage an in-flight bundle has reached, and the slot
    /// it landed in (if seen). `None` if the signature isn't being tracked
    /// (either never registered, or already moved to `completed`).
    pub fn active_progress(&self, signature: &str) -> Option<(CommitmentStage, Option<u64>)> {
        self.active.get(signature).map(|t| (t.stage, t.landed_slot))
    }

    /// Terminal outcome for a signature that has already completed: `Ok(slot)` if
    /// it landed without a failure, `Err(class)` if it failed. `None` if it isn't
    /// in the completed set (still active, or never tracked).
    pub fn completed_outcome(&self, signature: &str) -> Option<Result<u64, FailureClass>> {
        self.completed
            .iter()
            .find(|r| r.signature.as_deref() == Some(signature))
            .map(|r| match (r.failure, r.landed_slot) {
                (Some(class), _) => Err(class),
                (None, Some(slot)) => Ok(slot),
                // Landed-but-no-slot shouldn't happen; treat as unknown failure.
                (None, None) => Err(FailureClass::Unknown),
            })
    }

    /// Move every still-active bundle into `completed` at its current stage —
    /// called at end of run so a bundle that landed but whose finalization the
    /// stream hasn't delivered yet is still logged. Landed bundles complete
    /// cleanly; never-landed ones are classified `Unknown`.
    pub fn flush_active(&mut self, now: DateTime<Utc>) {
        let sigs: Vec<String> = self.active.keys().cloned().collect();
        for sig in sigs {
            if let Some(mut t) = self.active.remove(&sig) {
                if t.landed_slot.is_none() {
                    t.record.failure = Some(FailureClass::Unknown);
                    t.record.events.push(LifecycleEvent {
                        stage: CommitmentStage::Failed,
                        at: now,
                        slot: None,
                        note: Some("run ended before landing".into()),
                    });
                }
                self.completed.push(t.record);
            }
        }
    }

    /// Drain completed (terminal) records for the log writer.
    pub fn drain_completed(&mut self) -> Vec<BundleRecord> {
        std::mem::take(&mut self.completed)
    }

    /// Peek at the most recently completed record without draining (for live
    /// dashboard updates).
    pub fn last_completed(&self) -> Option<&BundleRecord> {
        self.completed.last()
    }

    // --- internals ----------------------------------------------------------

    /// Advance a tracked bundle to `stage` if it's a forward move, recording the
    /// event and timing.
    fn advance(
        t: &mut Tracked,
        stage: CommitmentStage,
        slot: Option<u64>,
        now: DateTime<Utc>,
        note: &str,
    ) {
        let new_order = stage.order().unwrap_or(0);
        let cur_order = t.stage.order().unwrap_or(0);
        if new_order <= cur_order {
            return; // never regress
        }
        t.stage = stage;
        t.record.events.push(LifecycleEvent {
            stage,
            at: now,
            slot,
            note: Some(note.to_string()),
        });
        t.record.timings.observe(stage, now);
    }

    /// Move a bundle to a terminal failure state and into `completed`.
    fn fail(
        completed: &mut Vec<BundleRecord>,
        active: &mut HashMap<String, Tracked>,
        signature: &str,
        class: FailureClass,
        now: DateTime<Utc>,
    ) {
        if let Some(mut t) = active.remove(signature) {
            t.record.failure = Some(class);
            t.record.events.push(LifecycleEvent {
                stage: CommitmentStage::Failed,
                at: now,
                slot: t.landed_slot,
                note: Some(class.label().to_string()),
            });
            completed.push(t.record);
        }
    }
}

/// Map raw failure signals to a [`FailureClass`]. Used by the orchestrator when
/// it has an error string (from RPC `getSignatureStatuses` / Jito
/// `getBundleStatuses`) and/or knows the blockhash expired.
pub fn classify_failure(err_text: Option<&str>, landed: bool, blockhash_expired: bool) -> FailureClass {
    // Not landed + window passed = the canonical expired-blockhash case.
    if !landed && blockhash_expired {
        return FailureClass::ExpiredBlockhash;
    }
    let text = err_text.unwrap_or("").to_ascii_lowercase();
    if text.contains("blockhash") && (text.contains("notfound") || text.contains("not found")) {
        FailureClass::ExpiredBlockhash
    } else if text.contains("compute") || text.contains("exceeded budget") || text.contains("computationalbudgetexceeded") {
        FailureClass::ComputeExceeded
    } else if text.contains("fee") || text.contains("priority") {
        FailureClass::FeeTooLow
    } else if !text.is_empty() || landed {
        FailureClass::BundleFailure
    } else {
        FailureClass::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn rec(id: u64, sig: &str, lvbh: u64) -> BundleRecord {
        let mut r = BundleRecord::new(id, "testnet", Utc::now());
        r.signature = Some(sig.into());
        r.last_valid_block_height = Some(lvbh);
        r
    }

    fn t0() -> DateTime<Utc> {
        Utc::now()
    }

    #[test]
    fn happy_path_to_finalized() {
        let mut tr = LifecycleTracker::new();
        let start = t0();
        tr.track(rec(1, "SIGA", 1000), start);
        assert_eq!(tr.active_len(), 1);

        // tx lands in slot 500 -> processed
        tr.on_transaction("SIGA", 500, false, start + Duration::milliseconds(400));
        // slot 500 confirmed
        tr.on_slot_commitment(500, SlotCommitment::Confirmed, start + Duration::milliseconds(900));
        // a later slot finalizes (retroactive) -> 500 finalized
        tr.on_slot_commitment(512, SlotCommitment::Finalized, start + Duration::milliseconds(13000));

        assert_eq!(tr.active_len(), 0);
        let done = tr.drain_completed();
        assert_eq!(done.len(), 1);
        let r = &done[0];
        assert_eq!(r.landed_slot, Some(500));
        assert!(r.failure.is_none());
        // four stages recorded
        assert_eq!(r.events.len(), 4);
        // deltas computed
        assert_eq!(r.timings.submit_to_processed_ms, Some(400));
        assert_eq!(r.timings.processed_to_confirmed_ms, Some(500));
        assert_eq!(r.timings.confirmed_to_finalized_ms, Some(12100));
    }

    #[test]
    fn expired_blockhash_before_landing() {
        let mut tr = LifecycleTracker::new();
        let start = t0();
        tr.track(rec(2, "SIGB", 1000), start);
        // current height passes the validity window, still not landed
        let expired = tr.expire_unlanded(1001, start + Duration::seconds(60));
        assert_eq!(expired, vec!["SIGB".to_string()]);
        let done = tr.drain_completed();
        assert_eq!(done[0].failure, Some(FailureClass::ExpiredBlockhash));
    }

    #[test]
    fn no_regression_on_out_of_order_updates() {
        let mut tr = LifecycleTracker::new();
        let start = t0();
        tr.track(rec(3, "SIGC", 1000), start);
        tr.on_transaction("SIGC", 600, false, start);
        tr.on_slot_commitment(600, SlotCommitment::Finalized, start + Duration::seconds(13));
        // a late "confirmed" must not regress a finalized bundle
        tr.on_slot_commitment(600, SlotCommitment::Confirmed, start + Duration::seconds(14));
        let done = tr.drain_completed();
        assert_eq!(done.len(), 1);
        assert_eq!(*done[0].events.last().map(|e| &e.stage).unwrap(), CommitmentStage::Finalized);
    }

    #[test]
    fn landed_but_failed_is_bundle_failure() {
        let mut tr = LifecycleTracker::new();
        let start = t0();
        tr.track(rec(4, "SIGD", 1000), start);
        tr.on_transaction("SIGD", 700, true, start + Duration::milliseconds(300));
        let done = tr.drain_completed();
        assert_eq!(done[0].failure, Some(FailureClass::BundleFailure));
        assert_eq!(done[0].landed_slot, Some(700));
    }

    #[test]
    fn classifier_maps_known_errors() {
        assert_eq!(classify_failure(None, false, true), FailureClass::ExpiredBlockhash);
        assert_eq!(classify_failure(Some("Blockhash not found"), false, false), FailureClass::ExpiredBlockhash);
        assert_eq!(classify_failure(Some("ComputationalBudgetExceeded"), true, false), FailureClass::ComputeExceeded);
        assert_eq!(classify_failure(Some("priority fee too low"), false, false), FailureClass::FeeTooLow);
        assert_eq!(classify_failure(Some("some other error"), true, false), FailureClass::BundleFailure);
    }
}
