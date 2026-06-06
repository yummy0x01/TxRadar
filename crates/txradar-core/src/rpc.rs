//! Minimal Solana JSON-RPC client (Phase 2).
//!
//! Deliberately thin: we only need a handful of methods (blockhash, block
//! height, signature status) and would rather not pull the heavy
//! `solana-client` stack. Landing confirmation comes from the *stream*; RPC is a
//! backup and the source for blockhashes.

use serde::Deserialize;
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

/// One entry from `getSignatureStatuses`.
#[derive(Debug, Clone)]
pub struct SignatureStatus {
    pub slot: u64,
    pub confirmation_status: Option<String>, // "processed" | "confirmed" | "finalized"
    pub err: bool,
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

    /// Backup confirmation path: look up the status of one or more signatures.
    pub async fn get_signature_statuses(
        &self,
        signatures: &[String],
    ) -> Result<Vec<Option<SignatureStatus>>, RpcError> {
        #[derive(Deserialize)]
        struct RawStatus {
            slot: u64,
            #[serde(rename = "confirmationStatus")]
            confirmation_status: Option<String>,
            err: Option<Value>,
        }
        let result = self
            .call(
                "getSignatureStatuses",
                json!([signatures, { "searchTransactionHistory": false }]),
            )
            .await?;
        let arr = result
            .get("value")
            .and_then(Value::as_array)
            .ok_or_else(|| RpcError::Shape("missing value array".into()))?;
        Ok(arr
            .iter()
            .map(|entry| {
                if entry.is_null() {
                    return None;
                }
                serde_json::from_value::<RawStatus>(entry.clone())
                    .ok()
                    .map(|r| SignatureStatus {
                        slot: r.slot,
                        confirmation_status: r.confirmation_status,
                        err: r.err.map(|e| !e.is_null()).unwrap_or(false),
                    })
            })
            .collect())
    }
}
