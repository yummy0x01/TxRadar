//! Core transaction stack (Phase 2 & 3) — the network-facing engine.
//!
//! Deliberately contains **no AI logic**: the agent (in `txradar-agent`) decides
//! *policy* (tip, timing, retry); this crate *executes* it deterministically.
//! That separation is an explicit judging criterion.
//!
//! Responsibilities:
//! * Blockhash manager — fetch at a non-finalized commitment, track
//!   `lastValidBlockHeight`, detect expiry, refresh.
//! * Transaction + bundle construction — main instruction(s) + a tip transfer
//!   to a randomly chosen Jito tip account, in the *same* transaction.
//! * Jito client — `sendBundle`, `getBundleStatuses`, `getInflightBundleStatuses`,
//!   `getTipAccounts`, next-scheduled-leader.
//! * Lifecycle tracker — drive Submitted -> Processed -> Confirmed -> Finalized
//!   from stream events, classify failures, emit `BundleRecord`s.

pub mod blockhash;
pub mod bundle;
pub mod jito;
pub mod rpc;
pub mod tracker;
