//! Helius Sender client.
//!
//! Sender is an optional fast broadcast path for the campaign when public Jito
//! block-engine access is saturated. It still signs the same memo+tip Solana
//! transaction locally; this client only forwards the base64 transaction with
//! Sender's required `skipPreflight=true` setting.

use serde_json::{json, Value};

use crate::bundle::BuiltBundle;

#[derive(Clone)]
pub struct SenderClient {
    http: reqwest::Client,
    url: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SenderError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("sender returned error: {0}")]
    Rpc(String),
    #[error("unexpected sender response shape: {0}")]
    Shape(String),
}

impl SenderClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self { http: reqwest::Client::new(), url: url.into() }
    }

    pub async fn send_transaction(&self, bundle: &BuiltBundle) -> Result<String, SenderError> {
        let encoded = bundle
            .encoded_txs
            .first()
            .ok_or_else(|| SenderError::Shape("empty transaction bundle".into()))?;
        let body = json!({
            "jsonrpc": "2.0",
            "id": "txradar",
            "method": "sendTransaction",
            "params": [
                encoded,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "maxRetries": 0
                }
            ]
        });
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
            return Err(SenderError::Rpc(err.to_string()));
        }
        resp.get("result")
            .and_then(Value::as_str)
            .map(String::from)
            .ok_or_else(|| SenderError::Shape("missing string result".into()))
    }
}
