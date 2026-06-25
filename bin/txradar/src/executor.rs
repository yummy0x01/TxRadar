//! Live executor — binds the agent's decision loop to the real core stack.
//!
//! [`LiveExecutor`] implements [`txradar_agent::Executor`], so the *same*
//! `run_attempt_loop` that the agent unit tests drive runs against the real
//! blockhash manager, bundle builder, Jito client, tip oracle, and lifecycle
//! tracker. The agent stays the sole decision-maker; this type only *executes*
//! what it decides and reports the chain outcome back.
//!
//! Chain mode:
//! * [`ChainMode::Live`] — the production path. Times submission to the Jito
//!   leader window, broadcasts the bundle, and **confirms landing from the
//!   Yellowstone stream** (the shared, ingest-fed [`LifecycleTracker`]); Jito
//!   status polling is only a fast-fail backup.
//! * [`ChainMode::Simulated`] — signs a real bundle (real keypair, real
//!   blockhash when RPC is reachable) but does not broadcast; fabricates a
//!   plausible landing so the autonomous-retry pipeline can be demonstrated
//!   end-to-end with no funds. Every simulated landing is labelled in the
//!   record (`network = "<net>-sim"`) so it can never be mistaken for a graded
//!   mainnet landing.
//!
//! The blockhash-expiry fault injection (demo only) is unchanged: when armed,
//! the first submit observes the forced-stale blockhash as expired and returns
//! `Failed(ExpiredBlockhash)` *without broadcasting*. Recovery (refresh ->
//! recalc tip -> resubmit) is the agent's call, executed for real.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;

use solana_sdk::signature::Keypair;

use txradar_agent::{
    AgentError, AttemptOutcome, Decision, DecisionContext, DecisionKind, Executor,
};
use txradar_core::blockhash::BlockhashManager;
use txradar_core::bundle::{
    build_single_sender_tx, build_single_tx_bundle, random_sender_tip_account, random_tip_account,
    BuiltBundle, MIN_SENDER_TIP_LAMPORTS,
};
use txradar_core::helius::SenderClient;
use txradar_core::jito::{InflightStatus, JitoClient, NextLeader};
use txradar_core::tracker::{classify_failure, SlotCommitment};
use txradar_tips::{recommend_from, FloorLamports, TipBounds, TipContext, TipOracle};
use txradar_tui::{AttemptRow, DashboardState, TipBandView};
use txradar_types::failure::FailureClass;
use txradar_types::lifecycle::CommitmentStage;
use txradar_types::record::BundleRecord;

use crate::ingest::{SharedNetwork, SharedTracker};

/// Shared dashboard handle the executor updates as it runs (Phase 7).
pub type SharedDashboard = Arc<Mutex<DashboardState>>;

/// How the executor touches the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainMode {
    /// Really broadcast bundles and confirm landing from the stream.
    Live,
    /// Sign real bundles but don't broadcast; fabricate landings for the demo.
    Simulated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BroadcastMode {
    Jito,
    Sender,
    Hybrid,
}

/// Static recommendation used only when the public tip-floor endpoint is
/// unreachable, so a run still proceeds. Set to congestion-realistic mainnet
/// levels (the live floor during congestion runs p95≈100k, p99≈800k) so a
/// fallback tip is still competitive enough to land — clamped to the operator's
/// `[tips]` bounds regardless.
const FALLBACK_FLOOR: FloorLamports =
    FloorLamports { p25: 2_000, p50: 10_000, p75: 50_000, p95: 100_000, p99: 300_000, ema_p50: 12_000 };

/// Submit when a Jito leader is within this many slots; otherwise wait for the
/// window to approach.
const LEADER_NEAR_SLOTS: u64 = 2;
/// Cap on how long we'll wait for a leader window before sending anyway (Jito
/// buffers bundles for the next few leaders regardless).
const LEADER_MAX_WAIT: Duration = Duration::from_secs(8);
/// Rough slot time used to translate "slots until leader" into a sleep.
const SLOT_MS: u64 = 400;

pub struct LiveExecutor {
    mode: ChainMode,
    network: String,
    payer: Keypair,
    blockhash: BlockhashManager,
    jito: JitoClient,
    sender: Option<SenderClient>,
    broadcast: BroadcastMode,
    oracle: TipOracle,
    bounds: TipBounds,
    /// Lifecycle tracker, shared with the stream-ingest task in Live mode so
    /// landing is confirmed from the stream. In Simulated mode the executor is
    /// its only writer.
    tracker: SharedTracker,
    /// Live network view (slot, skip rate, connectivity) — populated by ingest
    /// in Live mode; static in Simulated mode.
    net: SharedNetwork,
    max_retries: u32,

    /// Whether to gate submission on the Jito leader window (Live only).
    leader_gate: bool,
    /// How long to wait for stream confirmation before treating a live bundle as
    /// expired/dropped.
    confirm_timeout: Duration,
    /// How often to re-check the tracker / Jito backup while confirming.
    poll_interval: Duration,

    /// When true, the next submit treats the blockhash as expired (the injected
    /// fault). Cleared once consumed so the retry can proceed.
    inject_expiry: bool,

    /// When true, the current transaction is "starved": the tip band is pinned
    /// to the rock-bottom floor (`starve_tip`) and the retry budget is zeroed,
    /// so the bundle is really broadcast but is non-competitive and won't win
    /// inclusion — producing a genuine, honestly-derived failure for the
    /// lifecycle log. Nothing is fabricated: real broadcast, real non-landing,
    /// real `ExpiredBlockhash` once the confirm window (== blockhash lifetime)
    /// elapses. Toggled per-transaction by the campaign loop via [`set_starve`].
    starve: bool,
    /// Tip (lamports) used while starving — the Jito floor minimum.
    starve_tip: u64,

    /// Per-attempt id, incremented on each fresh top-level attempt.
    attempt_id: u64,
    /// Monotonic fake slot/height used in simulated mode.
    sim_slot: u64,
    sim_height: u64,

    /// Optional live dashboard updated as the run progresses (Phase 7).
    dash: Option<SharedDashboard>,
}

/// Tunables the executor needs that aren't part of the core stack handles.
pub struct ExecutorParams {
    pub mode: ChainMode,
    pub max_retries: u32,
    pub inject_expiry: bool,
    pub confirm_timeout_secs: u64,
    pub poll_interval_secs: u64,
    /// Tip (lamports) used for starved campaign transactions — the Jito floor
    /// minimum, so the broadcast is real but non-competitive.
    pub starve_tip: u64,
    pub broadcast: BroadcastMode,
}

impl LiveExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        params: ExecutorParams,
        network: impl Into<String>,
        payer: Keypair,
        blockhash: BlockhashManager,
        jito: JitoClient,
        sender: Option<SenderClient>,
        oracle: TipOracle,
        bounds: TipBounds,
        tracker: SharedTracker,
        net: SharedNetwork,
    ) -> Self {
        Self {
            mode: params.mode,
            network: network.into(),
            payer,
            blockhash,
            jito,
            sender,
            broadcast: params.broadcast,
            oracle,
            bounds,
            tracker,
            net,
            max_retries: params.max_retries,
            // Leader-window gating is disabled: `getNextScheduledLeader` needs a
            // regions arg and is itself globally rate-limited on the free
            // endpoint, so every gating call competes with `sendBundle` for the
            // scarce rate budget. Jito buffers bundles for upcoming leaders
            // regardless, and landing is confirmed from the stream — so we spend
            // the budget on the one call that must succeed: the broadcast.
            leader_gate: false,
            confirm_timeout: Duration::from_secs(params.confirm_timeout_secs.max(1)),
            poll_interval: Duration::from_millis((params.poll_interval_secs.max(1)) * 1000),
            inject_expiry: params.inject_expiry,
            starve: false,
            starve_tip: params.starve_tip.max(1),
            attempt_id: 0,
            sim_slot: 300_000_000,
            sim_height: 280_000_000,
            dash: None,
        }
    }

    /// Attach a shared dashboard for live updates (Phase 7).
    pub fn with_dashboard(mut self, dash: SharedDashboard) -> Self {
        self.dash = Some(dash);
        self
    }

    /// Toggle "starve" mode for the next transaction(s). When on, the tip band
    /// the agent reasons over is pinned to the Jito floor minimum and the retry
    /// budget is zeroed — the bundle is really broadcast but is non-competitive,
    /// so it won't win inclusion and fails honestly (`ExpiredBlockhash`) once the
    /// confirm window elapses. Used by the campaign loop to produce the lifecycle
    /// log's required real failure cases without any fabrication.
    pub fn set_starve(&mut self, on: bool) {
        self.starve = on;
    }

    fn should_use_sender(&self) -> bool {
        matches!(self.mode, ChainMode::Live)
            && matches!(self.broadcast, BroadcastMode::Sender | BroadcastMode::Hybrid)
            && !self.starve
    }

    /// Run a closure against the dashboard state if one is attached. The lock is
    /// held only for the closure (no `.await` inside), so it can't deadlock the
    /// render loop.
    fn dash_update(&self, f: impl FnOnce(&mut DashboardState)) {
        if let Some(d) = &self.dash {
            if let Ok(mut s) = d.lock() {
                f(&mut s);
            }
        }
    }

    // --- shared-tracker helpers (lock briefly, never across .await) ----------

    fn track(&self, record: BundleRecord, now: chrono::DateTime<Utc>) {
        if let Ok(mut t) = self.tracker.lock() {
            t.track(record, now);
        }
    }

    fn mark_failed(&self, sig: &str, class: FailureClass, now: chrono::DateTime<Utc>) {
        if let Ok(mut t) = self.tracker.lock() {
            t.mark_failed(sig, class, now);
        }
    }

    fn completed_outcome(&self, sig: &str) -> Option<Result<u64, FailureClass>> {
        self.tracker.lock().ok().and_then(|t| t.completed_outcome(sig))
    }

    fn active_progress(&self, sig: &str) -> Option<(CommitmentStage, Option<u64>)> {
        self.tracker.lock().ok().and_then(|t| t.active_progress(sig))
    }

    /// Push the most recently completed tracker record to the dashboard as an
    /// attempt row (accurate stage + latency deltas read from the record).
    fn dash_note_last_completed(&self) {
        let Some(row) = self.tracker.lock().ok().and_then(|t| t.last_completed().map(row_from_record)) else {
            return;
        };
        self.dash_update(|s| s.upsert_attempt(row));
    }

    /// Move any still-active bundles to completed (e.g. landed-at-confirmed but
    /// not yet finalized on the stream) so they're included in the log.
    pub fn finalize_pending(&mut self) {
        if let Ok(mut t) = self.tracker.lock() {
            t.flush_active(Utc::now());
        }
    }

    /// Drain the lifecycle records produced by this run (for the log writer).
    pub fn drain_records(&mut self) -> Vec<BundleRecord> {
        self.tracker.lock().map(|mut t| t.drain_completed()).unwrap_or_default()
    }

    /// Network tag written into records — simulated runs are suffixed so they're
    /// never confused with a real graded landing.
    fn network_tag(&self) -> String {
        match self.mode {
            ChainMode::Live => self.network.clone(),
            ChainMode::Simulated => format!("{}-sim", self.network),
        }
    }

    /// Current (slot, block_height): real stream slot + real RPC height in live
    /// mode, advancing fakes in simulated mode (so timing/expiry math is
    /// exercised offline).
    async fn current_slot_and_height(&mut self) -> (u64, u64) {
        match self.mode {
            ChainMode::Live => {
                let height = self.blockhash.current_block_height().await.unwrap_or(0);
                let slot = self.net.lock().ok().map(|n| n.current_slot).unwrap_or(0);
                // Fall back to height as a slot stand-in only before the stream
                // has delivered its first slot.
                let slot = if slot == 0 { height } else { slot };
                (slot, height)
            }
            ChainMode::Simulated => {
                self.sim_slot += 2;
                self.sim_height += 2;
                (self.sim_slot, self.sim_height)
            }
        }
    }

    /// Live skip rate from the stream-fed network view (0.0 in simulated mode).
    fn current_skip_rate(&self) -> f32 {
        match self.mode {
            ChainMode::Live => self.net.lock().ok().map(|n| n.skip_rate).unwrap_or(0.0),
            ChainMode::Simulated => 0.0,
        }
    }

    /// Ensure we have a current blockhash, fetching one if needed.
    async fn ensure_blockhash(&mut self) -> Result<(String, u64), AgentError> {
        if self.blockhash.current().is_none() {
            self.do_refresh().await?;
        }
        let bh = self
            .blockhash
            .current()
            .ok_or_else(|| AgentError::Parse("no blockhash after refresh".into()))?;
        Ok((bh.blockhash.clone(), bh.last_valid_block_height))
    }

    /// Refresh the blockhash: real RPC in live mode; a synthetic-but-valid hash
    /// in simulated mode so signing still works without network.
    async fn do_refresh(&mut self) -> Result<(), AgentError> {
        match self.mode {
            ChainMode::Live => {
                self.blockhash
                    .refresh()
                    .await
                    .map_err(|e| AgentError::Parse(format!("blockhash refresh: {e}")))?;
            }
            ChainMode::Simulated => {
                self.sim_height += 1;
                self.blockhash.set_simulated(
                    sim_blockhash(self.attempt_id, self.sim_height),
                    self.sim_height + 150,
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for LiveExecutor {
    async fn refresh_blockhash(&mut self) -> Result<(), AgentError> {
        self.do_refresh().await
    }

    async fn submit(&mut self, decision: &Decision) -> Result<AttemptOutcome, AgentError> {
        let now = Utc::now();
        self.attempt_id += 1;

        // Surface the agent's reasoning on the dashboard the moment it acts.
        let rationale = decision.rationale.clone();
        self.dash_update(|s| {
            s.push_reasoning(format!(
                "#{} {:?} tip={} — {}",
                self.attempt_id, decision.action, decision.tip_lamports, rationale
            ));
        });

        let (blockhash_str, lvbh) = self.ensure_blockhash().await?;

        // Build the (real, signed) bundle/transaction with the agent-chosen tip.
        let use_sender = self.should_use_sender();
        let tip_account = if use_sender {
            random_sender_tip_account()
        } else {
            random_tip_account()
        };
        let note = format!("txradar attempt {} tip {}", self.attempt_id, decision.tip_lamports);
        let tracked_hash = self
            .blockhash
            .current()
            .ok_or_else(|| AgentError::Parse("blockhash vanished".into()))?
            .as_hash()
            .map_err(|e| AgentError::Parse(format!("blockhash parse: {e}")))?;

        let bundle: BuiltBundle = if use_sender {
            build_single_sender_tx(
                &self.payer,
                &tip_account,
                decision.tip_lamports.max(MIN_SENDER_TIP_LAMPORTS),
                &note,
                &tracked_hash,
                &blockhash_str,
            )
        } else {
            build_single_tx_bundle(
                &self.payer,
                &tip_account,
                decision.tip_lamports,
                &note,
                &tracked_hash,
                &blockhash_str,
            )
        }
        .map_err(|e| AgentError::Parse(format!("bundle build: {e}")))?;

        // Open a tracker record for this attempt.
        let mut record = BundleRecord::new(self.attempt_id, self.network_tag(), now);
        record.signature = bundle.primary_signature().map(String::from);
        record.blockhash = Some(blockhash_str.clone());
        record.last_valid_block_height = Some(lvbh);
        record.tip_lamports = bundle.tip_lamports;
        record.tip_rationale = Some(decision.rationale.clone());
        let sig = record.signature.clone().unwrap_or_default();

        // --- Fault injection: forced blockhash expiry (demo only) --------------
        // Trips BEFORE any broadcast: the blockhash is observed as expired, so
        // the attempt fails with ExpiredBlockhash and the agent must decide how
        // to recover. This is the bounty's required "simulate at least one
        // blockhash expiry failure".
        if self.inject_expiry {
            self.inject_expiry = false; // consume: the retry sees a fresh hash
            self.blockhash.force_stale();
            record.fault_injected = true;
            self.track(record, now);
            let class = classify_failure(None, false, true);
            self.mark_failed(&sig, class, Utc::now());
            self.dash_note_last_completed();
            tracing::warn!(
                target: "txradar::fault",
                attempt = self.attempt_id, %sig,
                "INJECTED blockhash-expiry fault — attempt fails as ExpiredBlockhash (no broadcast)"
            );
            return Ok(AttemptOutcome::Failed(FailureClass::ExpiredBlockhash));
        }

        match self.mode {
            ChainMode::Simulated => {
                self.track(record, now);
                self.dash_submitted_row(decision);
                let outcome = self.submit_simulated(&sig);
                self.dash_note_last_completed();
                Ok(outcome)
            }
            ChainMode::Live => self.submit_live(bundle, record, decision, now).await,
        }
    }

    async fn build_context(
        &mut self,
        kind: DecisionKind,
        retries_so_far: u32,
        last_failure: Option<FailureClass>,
        last_tip_lamports: Option<u64>,
    ) -> Result<DecisionContext, AgentError> {
        // Refresh the live tip floor (public endpoint, no funds needed). Fall
        // back to a static floor if it's unreachable so a run still proceeds.
        let floor = match self.oracle.refresh().await {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(target: "txradar::tips", error = %e, "tip floor fetch failed; using fallback floor");
                FALLBACK_FLOOR
            }
        };

        let (slot, height) = self.current_slot_and_height().await;

        let blockhash_expired = self.inject_expiry
            || self
                .blockhash
                .current()
                .map(|b| b.is_expired(height))
                .unwrap_or(false);
        let last_valid_block_height = self.blockhash.current().map(|b| b.last_valid_block_height);

        let tip_ctx = TipContext {
            // Real congestion proxy: live skipped-slot rate from the stream
            // (0.0 in simulated mode).
            recent_skip_rate: self.current_skip_rate(),
            escalating: kind == DecisionKind::PostFailure,
        };

        // Starve mode pins the band to the floor minimum and zeroes the retry
        // budget, so the bundle broadcasts for real but can't win inclusion —
        // an honest failure case. Otherwise the agent reasons over the real
        // operator bounds and full retry budget.
        let (eff_bounds, eff_max_retries) = if self.starve {
            (
                TipBounds {
                    min_lamports: self.starve_tip,
                    max_lamports: self.starve_tip,
                    ema_alpha: self.bounds.ema_alpha,
                },
                0,
            )
        } else {
            (self.bounds, self.max_retries)
        };
        let mut tip_band = recommend_from(&floor, self.oracle.smoothed_p50().map(|v| v as f64), &eff_bounds, &tip_ctx);
        if self.should_use_sender() {
            tip_band.low = tip_band.low.max(MIN_SENDER_TIP_LAMPORTS);
            tip_band.mid = tip_band.mid.max(MIN_SENDER_TIP_LAMPORTS);
            tip_band.high = tip_band.high.max(MIN_SENDER_TIP_LAMPORTS);
        }

        // Live dashboard: refresh slot + connection + the band the agent sees.
        let band_view = TipBandView {
            low: tip_band.low,
            mid: tip_band.mid,
            high: tip_band.high,
            basis: tip_band.basis.to_string(),
            skip_rate: tip_ctx.recent_skip_rate,
        };
        let conn = match self.mode {
            ChainMode::Live => "connected",
            ChainMode::Simulated => "simulated",
        };
        self.dash_update(|s| {
            s.current_slot = slot;
            s.connection = conn.into();
            s.tip = Some(band_view);
        });

        Ok(DecisionContext {
            kind,
            attempt_id: self.attempt_id + 1,
            current_slot: slot,
            current_block_height: height,
            last_valid_block_height,
            blockhash_expired,
            tip_floor: floor,
            tip_band,
            tip_context: tip_ctx,
            last_failure,
            last_tip_lamports,
            retries_so_far,
            max_retries: eff_max_retries,
        })
    }
}

impl LiveExecutor {
    /// Push a "submitted" row to the dashboard for the current attempt.
    fn dash_submitted_row(&self, decision: &Decision) {
        let row = AttemptRow {
            attempt_id: self.attempt_id,
            tip_lamports: decision.tip_lamports,
            stage: "submitted".into(),
            landed_slot: None,
            submit_to_processed_ms: None,
            processed_to_confirmed_ms: None,
            confirmed_to_finalized_ms: None,
            failure: None,
            fault_injected: false,
        };
        self.dash_update(|s| s.upsert_attempt(row));
    }

    /// Production submit: time to the leader window, broadcast, then confirm
    /// landing **from the stream** (the shared tracker, fed by the ingest task).
    /// Jito inflight status is only a fast-fail backup; expiry/timeout closes the
    /// attempt so the agent can decide whether to retry.
    async fn submit_live(
        &mut self,
        bundle: BuiltBundle,
        mut record: BundleRecord,
        decision: &Decision,
        now: chrono::DateTime<Utc>,
    ) -> Result<AttemptOutcome, AgentError> {
        let sig = record.signature.clone().unwrap_or_default();

        // --- Leader-window gating ---------------------------------------------
        if self.leader_gate {
            if let Some(leader) = self.await_leader_window().await {
                record.target_leader = Some(leader.next_leader_identity);
            }
        }

        // --- Broadcast --------------------------------------------------------
        let sender_mode = self.should_use_sender();
        let bundle_id = if sender_mode {
            let Some(sender) = &self.sender else {
                let class = classify_failure(Some("Helius Sender selected but not configured"), false, false);
                self.track(record, now);
                self.mark_failed(&sig, class, Utc::now());
                self.dash_note_last_completed();
                return Ok(AttemptOutcome::Failed(class));
            };
            match sender.send_transaction(&bundle).await {
                Ok(returned_sig) => {
                    tracing::info!(
                        target: "txradar::sender",
                        %returned_sig, expected_sig = %sig,
                        "transaction submitted through Helius Sender"
                    );
                    format!("helius-sender:{returned_sig}")
                }
                Err(e) => {
                    tracing::error!(target: "txradar::sender", error = %e, "Helius Sender sendTransaction failed");
                    let class = classify_failure(Some(&e.to_string()), false, false);
                    self.track(record, now);
                    self.mark_failed(&sig, class, Utc::now());
                    self.dash_note_last_completed();
                    return Ok(AttemptOutcome::Failed(class));
                }
            }
        } else {
            match self.jito.send_bundle(&bundle).await {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(target: "txradar::jito", error = %e, "sendBundle failed");
                    let class = classify_failure(Some(&e.to_string()), false, false);
                    // Track then immediately fail so the attempt is still logged.
                    self.track(record, now);
                    self.mark_failed(&sig, class, Utc::now());
                    self.dash_note_last_completed();
                    return Ok(AttemptOutcome::Failed(class));
                }
            }
        };
        record.bundle_id = Some(bundle_id.clone());
        self.track(record, now);
        self.dash_submitted_row(decision);
        if sender_mode {
            tracing::info!(
                target: "txradar::sender",
                receipt = %bundle_id, %sig,
                "sender transaction submitted; confirming landing from stream"
            );
        } else {
            tracing::info!(
                target: "txradar::jito",
                %bundle_id, %sig,
                "bundle submitted; confirming landing from stream"
            );
        }

        // --- Confirm from the stream ------------------------------------------
        let confirmed_order = CommitmentStage::Confirmed.order().unwrap_or(2);
        let start = Instant::now();
        // The Yellowstone stream is the primary, reliable landing signal (separate
        // infra, not rate-limited). Jito's inflight status is only an occasional
        // backup fast-fail — polled sparingly so it doesn't burn the globally
        // rate-limited block-engine budget. Seed `last_jito` to now so the stream
        // gets the first window before any backup call.
        let jito_backup_interval = Duration::from_secs(20);
        let mut last_jito = Instant::now();
        loop {
            // Terminal already recorded by the tracker (finalized or failed).
            if let Some(outcome) = self.completed_outcome(&sig) {
                self.dash_note_last_completed();
                return Ok(match outcome {
                    Ok(slot) => AttemptOutcome::Landed { slot },
                    Err(class) => AttemptOutcome::Failed(class),
                });
            }
            // Early landing: the tx reached `confirmed` on the stream. We declare
            // it landed now; the ingest task finalizes the record in the
            // background and the run flushes it before logging.
            if let Some((stage, Some(slot))) = self.active_progress(&sig) {
                if stage.order().unwrap_or(0) >= confirmed_order {
                    tracing::info!(target: "txradar::stream", %sig, slot, "landing confirmed from stream");
                    return Ok(AttemptOutcome::Landed { slot });
                }
            }
            // Backup: Jito reports a hard failure faster than the stream can show
            // a non-landing. Polled at most every `jito_backup_interval`.
            if !sender_mode && last_jito.elapsed() >= jito_backup_interval {
                last_jito = Instant::now();
                if let Ok(status) = self.jito.get_inflight_status(&bundle_id).await {
                    if matches!(status, InflightStatus::Failed | InflightStatus::Invalid) {
                        let class = classify_failure(Some("jito failed/invalid"), false, false);
                        self.mark_failed(&sig, class, Utc::now());
                        self.dash_note_last_completed();
                        return Ok(AttemptOutcome::Failed(class));
                    }
                }
            }
            // Timed out without landing -> treat as expired/dropped so the agent
            // can refresh and retry.
            if start.elapsed() > self.confirm_timeout {
                tracing::warn!(target: "txradar::stream", %sig, "no landing within confirm timeout; treating as expired");
                let class = FailureClass::ExpiredBlockhash;
                self.mark_failed(&sig, class, Utc::now());
                self.dash_note_last_completed();
                return Ok(AttemptOutcome::Failed(class));
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    /// Best-effort leader-window timing. Looks up the next Jito-connected leader;
    /// if it's more than a couple slots away, sleeps until it's near (capped), so
    /// the bundle arrives inside the leader's window. Returns the schedule entry
    /// (for `target_leader`), or `None` if the lookup failed.
    async fn await_leader_window(&self) -> Option<NextLeader> {
        match self.jito.get_next_scheduled_leader().await {
            Ok(nl) => {
                let dist = nl.slots_until_leader();
                tracing::info!(
                    target: "txradar::leader",
                    leader = %short_id(&nl.next_leader_identity),
                    slots_until = dist,
                    region = ?nl.next_leader_region,
                    "next Jito leader"
                );
                self.dash_update(|s| {
                    s.push_reasoning(format!(
                        "leader {} in {} slots — timing submission",
                        short_id(&nl.next_leader_identity),
                        dist
                    ))
                });
                if dist > LEADER_NEAR_SLOTS {
                    let wait = Duration::from_millis((dist - LEADER_NEAR_SLOTS) * SLOT_MS)
                        .min(LEADER_MAX_WAIT);
                    tokio::time::sleep(wait).await;
                }
                Some(nl)
            }
            Err(e) => {
                tracing::warn!(
                    target: "txradar::leader",
                    error = %e,
                    "leader lookup failed; submitting without gating"
                );
                None
            }
        }
    }

    /// Simulated landing: advance the (shared) tracker through the full
    /// commitment progression as if the bundle landed, so a complete
    /// BundleRecord is produced offline. Clearly tagged via the `-sim` suffix.
    fn submit_simulated(&mut self, sig: &str) -> AttemptOutcome {
        self.sim_slot += 1;
        let slot = self.sim_slot;
        let t0 = Utc::now();
        if let Ok(mut t) = self.tracker.lock() {
            t.on_transaction(sig, slot, false, t0 + chrono::Duration::milliseconds(420));
            t.on_slot_commitment(slot, SlotCommitment::Confirmed, t0 + chrono::Duration::milliseconds(900));
            t.on_slot_commitment(slot, SlotCommitment::Finalized, t0 + chrono::Duration::milliseconds(13_000));
        }
        tracing::info!(
            target: "txradar::sim",
            %sig, slot,
            "SIMULATED landing (no broadcast) — full lifecycle recorded"
        );
        AttemptOutcome::Landed { slot }
    }
}

/// Build a dashboard [`AttemptRow`] from a completed lifecycle record.
fn row_from_record(rec: &BundleRecord) -> AttemptRow {
    let stage = if rec.failure.is_some() {
        "failed"
    } else if rec.timings.finalized_at.is_some() {
        "finalized"
    } else if rec.timings.confirmed_at.is_some() {
        "confirmed"
    } else if rec.timings.processed_at.is_some() {
        "processed"
    } else {
        "submitted"
    };
    AttemptRow {
        attempt_id: rec.attempt_id,
        tip_lamports: rec.tip_lamports,
        stage: stage.into(),
        landed_slot: rec.landed_slot,
        submit_to_processed_ms: rec.timings.submit_to_processed_ms,
        processed_to_confirmed_ms: rec.timings.processed_to_confirmed_ms,
        confirmed_to_finalized_ms: rec.timings.confirmed_to_finalized_ms,
        failure: rec.failure.map(|f| f.label().to_string()),
        fault_injected: rec.fault_injected,
    }
}

/// Short, log-friendly form of a base58 validator identity (`abcd…wxyz`).
fn short_id(id: &str) -> String {
    if id.len() <= 12 {
        return id.to_string();
    }
    format!("{}…{}", &id[..4], &id[id.len() - 4..])
}

/// Deterministic, parseable base58 blockhash for simulated mode (32 bytes ->
/// bs58). Not a real on-chain hash; only used so signing succeeds offline.
fn sim_blockhash(attempt: u64, height: u64) -> String {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&attempt.to_le_bytes());
    bytes[8..16].copy_from_slice(&height.to_le_bytes());
    bs58::encode(bytes).into_string()
}
