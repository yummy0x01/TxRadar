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
    async fn call(&self, path: &str, method: &str, params: Value) -> Result<Value, JitoError> {
        self.throttle().await;
        let url = format!("{}{}", self.base_url, path);
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let mut req = self.http.post(&url).json(&body);
        if let Some(uuid) = &self.uuid {
            req = req.header("x-jito-auth", uuid);
        }
        let resp = req.send().await?;
        if resp.status().as_u16() == 429 {
            return Err(JitoError::RateLimited);
        }
        let resp: Value = resp.error_for_status()?.json().await?;
        if let Some(err) = resp.get("error") {
            return Err(JitoError::Rpc(err.to_string()));
        }
        resp.get("result")
            .cloned()
            .ok_or_else(|| JitoError::Shape("missing `result`".into()))
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

    /// The current set of tip accounts (we normally use the hardcoded list, but
    /// this lets us cross-check).
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>, JitoError> {
        let result = self.call("/bundles", "getTipAccounts", json!([])).await?;
        let arr = result
            .as_array()
            .ok_or_else(|| JitoError::Shape("tip accounts not an array".into()))?;
        Ok(arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
    }
}
