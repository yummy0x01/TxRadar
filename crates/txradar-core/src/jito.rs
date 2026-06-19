//! Jito block-engine JSON-RPC client (Phase 2).
//!
//! Thin async wrapper over the block-engine endpoints:
//! `sendBundle`, `getBundleStatuses`, `getInflightBundleStatuses`,
//! `getTipAccounts`. Respects the ~1 req/s/IP/region rate limit. The base URL
//! comes from config, so testnet <-> mainnet is a profile flip.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::bundle::BuiltBundle;

/// In-flight bundle status as reported by `getInflightBundleStatuses`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightStatus {
    Pending,
    Landed,
    Failed,
    Invalid,
}

impl InflightStatus {
    fn parse(s: &str) -> Self {
        match s {
            "Landed" => InflightStatus::Landed,
            "Failed" => InflightStatus::Failed,
            "Invalid" => InflightStatus::Invalid,
            _ => InflightStatus::Pending,
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, InflightStatus::Landed | InflightStatus::Failed | InflightStatus::Invalid)
    }
}

/// Settled status from `getBundleStatuses` (richer than inflight).
#[derive(Debug, Clone)]
pub struct BundleStatus {
    pub bundle_id: String,
    pub slot: Option<u64>,
    pub confirmation_status: Option<String>, // processed | confirmed | finalized
    pub err: bool,
}

/// The next Jito-connected leader, from `getNextScheduledLeader`. Bundles are
/// only processed when a Jito leader is up, so we time `sendBundle` to land just
/// before `next_leader_slot`.
#[derive(Debug, Clone)]
pub struct NextLeader {
    pub current_slot: u64,
    pub next_leader_slot: u64,
    pub next_leader_identity: String,
    pub next_leader_region: Option<String>,
}

impl NextLeader {
    /// Slots until the next Jito leader is up (0 if it's the current slot or
    /// already passed — the schedule advanced between our request and now).
    pub fn slots_until_leader(&self) -> u64 {
        self.next_leader_slot.saturating_sub(self.current_slot)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JitoError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("jito rpc error: {0}")]
    Rpc(String),
    #[error("unexpected response shape: {0}")]
    Shape(String),
    #[error("rate limited (429)")]
    RateLimited,
}

/// Async Jito block-engine client. Cheap to clone; the rate-limit gate is
/// shared so concurrent callers can't exceed ~1 req/s.
#[derive(Clone)]
pub struct JitoClient {
    http: reqwest::Client,
    base_url: String,
    /// Optional auth UUID (sent as `x-jito-auth`).
    uuid: Option<String>,
    /// Shared gate: timestamp of the last request, to space requests out.
    rate_gate: Arc<Mutex<Option<Instant>>>,
    min_interval: Duration,
}

impl JitoClient {
    /// `base_url` is the block-engine API root, e.g.
    /// `https://frankfurt.mainnet.block-engine.jito.wtf/api/v1`.
    pub fn new(base_url: impl Into<String>, uuid: Option<String>, max_requests_per_sec: u32) -> Self {
        let rps = max_requests_per_sec.max(1);
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            uuid,
            rate_gate: Arc::new(Mutex::new(None)),
            min_interval: Duration::from_millis(1000 / rps as u64),
        }
    }

    /// Block until enough time has elapsed since the previous request to respect
    /// the rate limit, then record this request's start.
    async fn throttle(&self) {
        let mut gate = self.rate_gate.lock().await;
        if let Some(last) = *gate {
            let elapsed = last.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *gate = Some(Instant::now());
    }

    /// POST a JSON-RPC call to `<base_url><path>` and return the `result` value.
    ///
    /// The free Jito block-engine endpoints are *globally* rate-limited: under
    /// congestion they reply `429` or a JSON-RPC `-32097` ("globally rate
    /// limited") error even for well-formed, properly-spaced requests. We retry
    /// such responses with exponential backoff so a transient global-throttle
    /// window doesn't sink an otherwise-valid call (notably `sendBundle`).
    async fn call(&self, path: &str, method: &str, params: Value) -> Result<Value, JitoError> {
        const MAX_RATE_RETRIES: u32 = 6;
        let url = format!("{}{}", self.base_url, path);
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });

        let mut attempt = 0u32;
        loop {
            self.throttle().await;
            let mut req = self.http.post(&url).json(&body);
            if let Some(uuid) = &self.uuid {
                req = req.header("x-jito-auth", uuid);
            }
            let resp = req.send().await?;
            let status = resp.status();
            let text = resp.text().await?;
            let json: Value = serde_json::from_str(&text).unwrap_or(Value::Null);

            // Global rate limit surfaces as either a 429 status or a JSON-RPC
            // -32097 error body. Back off and retry both.
            let rpc_code = json.get("error").and_then(|e| e.get("code")).and_then(Value::as_i64);
            let rate_limited = status.as_u16() == 429 || rpc_code == Some(-32097);
            if rate_limited {
                if attempt < MAX_RATE_RETRIES {
                    // 0.6, 1.2, 2.4, 4.8, 8, 8 … seconds.
                    let backoff_ms = (600u64 << attempt.min(4)).min(8_000);
                    tracing::warn!(
                        target: "txradar::jito",
                        %method, attempt = attempt + 1, max = MAX_RATE_RETRIES, backoff_ms,
                        "Jito globally rate limited; backing off and retrying"
                    );
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                return Err(JitoError::RateLimited);
            }

            if let Some(err) = json.get("error") {
                return Err(JitoError::Rpc(err.to_string()));
            }
            if !status.is_success() {
                return Err(JitoError::Rpc(format!("HTTP {status}: {}", text.trim())));
            }
            return json
                .get("result")
                .cloned()
                .ok_or_else(|| JitoError::Shape("missing `result`".into()));
        }
    }

    /// Submit a bundle. Returns the Jito `bundle_id` (a SHA-256 of the
    /// signatures) — a receipt, NOT proof of landing.
    pub async fn send_bundle(&self, bundle: &BuiltBundle) -> Result<String, JitoError> {
        let params = json!([bundle.encoded_txs, { "encoding": "base64" }]);
        let result = self.call("/bundles", "sendBundle", params).await?;
        result
            .as_str()
            .map(String::from)
            .ok_or_else(|| JitoError::Shape("bundle_id not a string".into()))
    }

    /// 5-minute-lookback inflight status for a bundle id.
    pub async fn get_inflight_status(&self, bundle_id: &str) -> Result<InflightStatus, JitoError> {
        let result = self
            .call(
                "/bundles",
                "getInflightBundleStatuses",
                json!([[bundle_id]]),
            )
            .await?;
        let status = result
            .get("value")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .and_then(|v| v.get("status"))
            .and_then(Value::as_str)
            .ok_or_else(|| JitoError::Shape("missing inflight status".into()))?;
        Ok(InflightStatus::parse(status))
    }

    /// The next Jito-connected leader and how far away it is. Drives
    /// leader-window submission timing — we hold a bundle until a Jito leader is
    /// within reach so it isn't dropped for arriving outside a leader slot.
    pub async fn get_next_scheduled_leader(&self) -> Result<NextLeader, JitoError> {
        let result = self.call("/bundles", "getNextScheduledLeader", json!([])).await?;
        let current_slot = result
            .get("currentSlot")
            .and_then(Value::as_u64)
            .ok_or_else(|| JitoError::Shape("missing currentSlot".into()))?;
        let next_leader_slot = result
            .get("nextLeaderSlot")
            .and_then(Value::as_u64)
            .ok_or_else(|| JitoError::Shape("missing nextLeaderSlot".into()))?;
        let next_leader_identity = result
            .get("nextLeaderIdentity")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let next_leader_region = result
            .get("nextLeaderRegion")
            .and_then(Value::as_str)
            .map(String::from);
        Ok(NextLeader { current_slot, next_leader_slot, next_leader_identity, next_leader_region })
    }

    /// Settled status (slot, confirmation, error) for a bundle id.
    pub async fn get_bundle_status(&self, bundle_id: &str) -> Result<Option<BundleStatus>, JitoError> {
        let result = self
            .call("/bundles", "getBundleStatuses", json!([[bundle_id]]))
            .await?;
        let entry = result
            .get("value")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .cloned();
        let Some(entry) = entry else { return Ok(None) };
        if entry.is_null() {
            return Ok(None);
        }
        Ok(Some(BundleStatus {
            bundle_id: bundle_id.to_string(),
            slot: entry.get("slot").and_then(Value::as_u64),
            confirmation_status: entry
                .get("confirmation_status")
                .and_then(Value::as_str)
                .map(String::from),
            err: entry
                .get("err")
                .map(|e| !e.is_null() && e != &json!({"Ok": null}))
                .unwrap_or(false),
        }))
    }

}
