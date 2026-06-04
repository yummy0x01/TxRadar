//! Lifecycle tracker (Phase 3).
//!
//! Consumes stream events, advances each in-flight bundle through the
//! commitment stages, computes latency deltas, classifies failures, and emits a
//! completed [`txradar_types::BundleRecord`] to the log. Stream subscriptions
//! are the source of truth for landing; Jito status polling is a backup.
