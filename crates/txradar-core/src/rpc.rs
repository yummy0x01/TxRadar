//! Minimal Solana JSON-RPC client (Phase 2).
//!
//! Deliberately thin: we only need a handful of methods (blockhash, block
//! height, signature status) and would rather not pull the heavy
//! `solana-client` stack. Landing confirmation comes from the *stream*; RPC is a
//! backup and the source for blockhashes.

use serde_json::{json, Value};

/// A Solana JSON-RPC endpoint.
#[derive(Clone)]
pub struct RpcClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rpc returned error: {0}")]
    Rpc(String),
    #[error("unexpected rpc response shape: {0}")]
    Shape(String),
}

/// Result of `getLatestBlockhash`.
#[derive(Debug, Clone)]
pub struct LatestBlockhash {
    pub blockhash: String,
    pub last_valid_block_height: u64,
}

impl RpcClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self { http: reqwest::Client::new(), url: url.into() }
    }

    /// POST a JSON-RPC call and return the `result` value, mapping a JSON-RPC
    /// `error` object into [`RpcError::Rpc`].
    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let resp: Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(err) = resp.get("error") {
            return Err(RpcError::Rpc(err.to_string()));
        }
        resp.get("result")
            .cloned()
            .ok_or_else(|| RpcError::Shape("missing `result`".into()))
    }

    /// Fetch a recent blockhash at the given commitment (never `finalized` for
    /// time-sensitive sends — see README Q2).
    pub async fn get_latest_blockhash(&self, commitment: &str) -> Result<LatestBlockhash, RpcError> {
        let result = self
            .call("getLatestBlockhash", json!([{ "commitment": commitment }]))
            .await?;
        let value = result
            .get("value")
            .ok_or_else(|| RpcError::Shape("missing value".into()))?;
        let blockhash = value
            .get("blockhash")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Shape("missing blockhash".into()))?
            .to_string();
        let last_valid_block_height = value
            .get("lastValidBlockHeight")
            .and_then(Value::as_u64)
            .ok_or_else(|| RpcError::Shape("missing lastValidBlockHeight".into()))?;
        Ok(LatestBlockhash { blockhash, last_valid_block_height })
    }

    /// Current block height at the given commitment.
    pub async fn get_block_height(&self, commitment: &str) -> Result<u64, RpcError> {
        let result = self
            .call("getBlockHeight", json!([{ "commitment": commitment }]))
            .await?;
        result
            .as_u64()
            .ok_or_else(|| RpcError::Shape("block height not a u64".into()))
    }

    /// Current slot at the given commitment. Distinct from block height (slots
    /// advance even when a block is skipped), so the lifecycle log records the
    /// real slot a bundle landed in rather than a height stand-in.
    pub async fn get_slot(&self, commitment: &str) -> Result<u64, RpcError> {
        let result = self.call("getSlot", json!([{ "commitment": commitment }])).await?;
        result
            .as_u64()
            .ok_or_else(|| RpcError::Shape("slot not a u64".into()))
    }

    /// Lamport balance of an account at the given commitment. Used for the
    /// mainnet preflight check (fee-payer must be funded before we broadcast).
    pub async fn get_balance(&self, pubkey: &str, commitment: &str) -> Result<u64, RpcError> {
        let result = self
            .call("getBalance", json!([pubkey, { "commitment": commitment }]))
            .await?;
        result
            .get("value")
            .and_then(Value::as_u64)
            .ok_or_else(|| RpcError::Shape("balance value not a u64".into()))
    }

    /// Simulate a base64-encoded transaction without broadcasting. Returns the
    /// raw `value` object (`err`, `logs`, `unitsConsumed`, …) so callers can
    /// diagnose why a bundle would be rejected before spending anything. Uses the
    /// transaction's own blockhash (`replaceRecentBlockhash: false`) and skips
    /// signature verification so a build can be checked for *execution* validity.
    pub async fn simulate_transaction_b64(&self, encoded_tx: &str) -> Result<Value, RpcError> {
        self.call(
            "simulateTransaction",
            json!([
                encoded_tx,
                {
                    "sigVerify": false,
                    "replaceRecentBlockhash": false,
                    "commitment": "processed",
                    "encoding": "base64"
                }
            ]),
        )
        .await
    }

    /// Send a base64-encoded, signed transaction via plain RPC `sendTransaction`
    /// (NOT a Jito bundle). Returns the signature. Isolation probe: if this lands
    /// but a Jito bundle of the same tx doesn't, the fault is the bundle path.
    pub async fn send_transaction_b64(&self, encoded_tx: &str) -> Result<String, RpcError> {
        self.send_transaction_b64_opts(encoded_tx, false).await
    }

    /// As [`send_transaction_b64`] with an explicit `skip_preflight` toggle.
    /// Skipping preflight bypasses a lagging RPC's local simulation (which can
    /// spuriously report `BlockhashNotFound`) and forwards straight to the
    /// leader.
    pub async fn send_transaction_b64_opts(
        &self,
        encoded_tx: &str,
        skip_preflight: bool,
    ) -> Result<String, RpcError> {
        let result = self
            .call(
                "sendTransaction",
                json!([
                    encoded_tx,
                    { "encoding": "base64", "skipPreflight": skip_preflight, "maxRetries": 5 }
                ]),
            )
            .await?;
        result
            .as_str()
            .map(String::from)
            .ok_or_else(|| RpcError::Shape("sendTransaction result not a string".into()))
    }

    /// Whether a blockhash is still valid (known + within its window) at the
    /// given commitment. Cross-RPC check: fetch a hash from one endpoint, ask
    /// another if it's on the canonical chain.
    pub async fn is_blockhash_valid(&self, blockhash: &str, commitment: &str) -> Result<bool, RpcError> {
        let result = self
            .call("isBlockhashValid", json!([blockhash, { "commitment": commitment }]))
            .await?;
        result
            .get("value")
            .and_then(Value::as_bool)
            .ok_or_else(|| RpcError::Shape("isBlockhashValid value not a bool".into()))
    }

    /// Confirmation status string for a signature (`processed`/`confirmed`/
    /// `finalized`), or `None` if not yet seen. Isolation-probe helper.
    pub async fn get_signature_status(&self, signature: &str) -> Result<Option<String>, RpcError> {
        let result = self
            .call(
                "getSignatureStatuses",
                json!([[signature], { "searchTransactionHistory": true }]),
            )
            .await?;
        let entry = result
            .get("value")
            .and_then(Value::as_array)
            .and_then(|a| a.first())
            .cloned();
        match entry {
            Some(v) if !v.is_null() => Ok(Some(
                v.get("confirmationStatus")
                    .and_then(Value::as_str)
                    .unwrap_or("processed")
                    .to_string(),
            )),
            _ => Ok(None),
        }
    }
}
