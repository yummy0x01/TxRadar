//! Google Gemini implementation of [`Decider`] (Phase 5).
//!
//! Uses the Generative Language API's `generateContent` endpoint with
//! **controlled generation** (`responseMimeType: application/json` +
//! `responseSchema`) so the model is forced to return a well-typed
//! [`Decision`] (action + tip + rationale) — no brittle prose parsing. A
//! `system_instruction` carries the static policy; the per-call user message is
//! the JSON-serialized [`DecisionContext`]. The model id comes from
//! `[agent].model` in the profile.
//!
//! This is the *real* decision-maker: Gemini reasons over the live tip floor,
//! congestion, blockhash validity, and the prior failure, then chooses the tip
//! and the next action. The orchestration loop in `lib.rs` does only what comes
//! back here — satisfying the bounty's "no hardcoded retry flow" rule.
//!
//! Gemini's free tier (https://aistudio.google.com/app/apikey) is enough to run
//! the whole demo: the per-turn decision calls are tiny.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{AgentError, Action, Decider, Decision, DecisionContext};

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com";
const MAX_TOKENS: u32 = 1024;

/// Total `generateContent` attempts (1 initial + up to 3 retries) before a
/// transient failure is allowed to surface as an error.
const MAX_ATTEMPTS: u32 = 4;
/// First retry backoff; doubles each subsequent attempt.
const BASE_BACKOFF_MS: u64 = 500;
/// Ceiling on any single backoff sleep, so a large server-suggested delay can't
/// hang the loop indefinitely.
const MAX_BACKOFF: Duration = Duration::from_secs(20);

/// Static policy the model reasons under. Sent as the system instruction so the
/// per-call user content is just the live state.
const SYSTEM_PROMPT: &str = "\
You are the operational decision agent inside TxRadar, a Solana smart \
transaction stack that lands Jito bundles. On each turn you are given the live \
state as JSON and must decide the single next action, returning ONLY a JSON \
object with the fields `action`, `tip_lamports`, and `rationale`. You own two \
real decisions:

1. TIP INTELLIGENCE — choose `tip_lamports`, balancing cost against landing \
probability. The `tip_band` gives you a live, bounds-clamped range derived from \
the Jito tip floor: `low`/`mid`/`high` in lamports and the `basis` percentile. \
Treat it as guidance, not a default to echo. Bid nearer `low` when the market \
is calm (low `recent_skip_rate`) and cost matters; bid toward `high` when \
congestion is high or you are retrying after a competition-driven failure. \
NEVER exceed `tip_band.high` (it is already clamped to the operator's ceiling).

2. AUTONOMOUS RETRY — on a `post_failure` turn, reason about the cause:
   - `expired_blockhash` (or `blockhash_expired: true`): choose \
`refresh_and_resubmit` and recalculate `tip_lamports` (usually raise it).
   - `fee_too_low`/`bundle_failure`: recoverable — `refresh_and_resubmit` with a \
higher tip, unless `retries_so_far` >= `max_retries`, then `abort`.
   - `compute_exceeded` or unknown: not fixable by retrying here — `abort`.

Use `submit` for a normal first send, `hold` only if conditions are so \
unfavorable that waiting is clearly better. Always give a concise one-sentence \
`rationale` naming the key signals you used.";

/// Gemini-backed decision-maker. Holds the API key + model + base URL;
/// constructed by the binary from `GEMINI_API_KEY` (+ optional
/// `GEMINI_BASE_URL`) and the active profile. The base URL is configurable so
/// the same Generative-Language call can target generativelanguage.googleapis.com
/// directly or a compatible proxy.
pub struct GeminiDecider {
    api_key: String,
    model: String,
    /// Base URL without the `/v1beta/models/...` path, e.g.
    /// `https://generativelanguage.googleapis.com`.
    base_url: String,
    http: reqwest::Client,
}

impl GeminiDecider {
    /// Build a decider against the default Gemini endpoint.
    pub fn new(api_key: String, model: String) -> Self {
        Self::with_base_url(api_key, model, DEFAULT_BASE_URL.to_string())
    }

    /// Build a decider against a custom base URL (a gateway/proxy that speaks the
    /// Generative Language API). An empty/whitespace base URL falls back to the
    /// default endpoint.
    pub fn with_base_url(api_key: String, model: String, base_url: String) -> Self {
        let base_url = {
            let trimmed = base_url.trim().trim_end_matches('/');
            if trimmed.is_empty() { DEFAULT_BASE_URL.to_string() } else { trimmed.to_string() }
        };
        Self { api_key, model, base_url, http: reqwest::Client::new() }
    }

    /// Full `generateContent` endpoint for this decider's model.
    fn endpoint(&self) -> String {
        format!("{}/v1beta/models/{}:generateContent", self.base_url, self.model)
    }

    /// The response schema that forces a structured [`Decision`] out of the model
    /// (Gemini controlled generation; types are uppercase per the API).
    fn decide_schema() -> Value {
        json!({
            "type": "OBJECT",
            "properties": {
                "action": {
                    "type": "STRING",
                    "enum": ["submit", "hold", "refresh_and_resubmit", "abort"],
                    "description": "The next action to take."
                },
                "tip_lamports": {
                    "type": "INTEGER",
                    "description": "Jito tip in lamports; must lie within tip_band [low, high]."
                },
                "rationale": {
                    "type": "STRING",
                    "description": "One sentence naming the key signals behind this choice."
                }
            },
            "required": ["action", "tip_lamports", "rationale"],
            "propertyOrdering": ["action", "tip_lamports", "rationale"]
        })
    }
}

#[async_trait]
impl Decider for GeminiDecider {
    async fn decide(&self, ctx: &DecisionContext) -> Result<Decision, AgentError> {
        let ctx_json =
            serde_json::to_string_pretty(ctx).map_err(|e| AgentError::Parse(e.to_string()))?;

        let body = json!({
            "system_instruction": {
                "parts": [{ "text": SYSTEM_PROMPT }]
            },
            "contents": [{
                "role": "user",
                "parts": [{
                    "text": format!("Live decision state:\n{ctx_json}\n\nReturn the decision JSON.")
                }]
            }],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": MAX_TOKENS,
                "responseMimeType": "application/json",
                "responseSchema": Self::decide_schema(),
                // Gemini 2.5 models "think" by default, and thinking tokens are
                // drawn from maxOutputTokens — which truncates the JSON answer.
                // Our reasoning lives in the `rationale` field, so disable the
                // thinking budget and give the whole allowance to the response.
                "thinkingConfig": { "thinkingBudget": 0 }
            }
        });

        let resp_json = self.send_with_retry(&body).await?;
        parse_decision(&resp_json)
    }
}

impl GeminiDecider {
    /// POST the request body to `generateContent`, retrying transient failures
    /// (HTTP 429, any 5xx, and connection/DNS/timeout blips) with exponential
    /// backoff. When the server includes a `RetryInfo.retryDelay` (Gemini does
    /// on 429), that delay is honored if it exceeds our computed backoff.
    /// Non-transient errors — other 4xx like 400/401/403 — fail immediately,
    /// since retrying them would just waste quota. Returns the parsed response
    /// JSON for [`parse_decision`].
    async fn send_with_retry(&self, body: &Value) -> Result<Value, AgentError> {
        let mut attempt: u32 = 0;
        loop {
            let result = self
                .http
                .post(self.endpoint())
                // Key travels in a header, never the URL, so it can't leak via logs.
                .header("x-goog-api-key", &self.api_key)
                .header("content-type", "application/json")
                .json(body)
                .send()
                .await;

            // `suggested` carries a server-hinted delay (if any) into the backoff.
            let suggested = match result {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp.json().await.map_err(AgentError::from);
                    }
                    if is_retryable_status(status) && attempt + 1 < MAX_ATTEMPTS {
                        // Body has no secrets; read it to honor retryDelay + log why.
                        let body_text = resp.text().await.unwrap_or_default();
                        tracing::warn!(
                            %status,
                            attempt = attempt + 1,
                            max = MAX_ATTEMPTS,
                            "gemini transient HTTP error; backing off and retrying"
                        );
                        parse_retry_delay(&body_text)
                    } else {
                        // Non-retryable status, or retries exhausted: surface it.
                        return Err(AgentError::Http(resp.error_for_status().unwrap_err()));
                    }
                }
                Err(e) => {
                    if is_retryable_transport(&e) && attempt + 1 < MAX_ATTEMPTS {
                        tracing::warn!(
                            error = %e,
                            attempt = attempt + 1,
                            max = MAX_ATTEMPTS,
                            "gemini connection error; backing off and retrying"
                        );
                        None
                    } else {
                        return Err(AgentError::Http(e));
                    }
                }
            };

            tokio::time::sleep(backoff_delay(attempt, suggested)).await;
            attempt += 1;
        }
    }
}

/// Extract the JSON decision from a `generateContent` response and map it into a
/// [`Decision`]. Controlled generation puts the JSON object in the first
/// candidate's first text part. Factored out so it is unit-testable without a
/// network.
fn parse_decision(resp: &Value) -> Result<Decision, AgentError> {
    let text = resp
        .get("candidates")
        .and_then(Value::as_array)
        .and_then(|c| c.first())
        .and_then(|cand| cand.get("content"))
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
        .and_then(|parts| parts.iter().find_map(|p| p.get("text").and_then(Value::as_str)))
        .ok_or(AgentError::NoDecision)?;

    let input: Value =
        serde_json::from_str(text).map_err(|e| AgentError::Parse(format!("decision JSON: {e}")))?;

    let action_str = input
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| AgentError::Parse("decision missing action".into()))?;
    let action: Action = serde_json::from_value(Value::String(action_str.to_string()))
        .map_err(|e| AgentError::Parse(format!("unknown action {action_str:?}: {e}")))?;

    // Controlled generation may render the integer as a JSON number or a string.
    let tip_lamports = input
        .get("tip_lamports")
        .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .ok_or_else(|| AgentError::Parse("decision missing tip_lamports".into()))?;

    let rationale = input
        .get("rationale")
        .and_then(Value::as_str)
        .unwrap_or("(no rationale provided)")
        .to_string();

    Ok(Decision { action, tip_lamports, rationale })
}

/// Whether an HTTP status warrants a retry: rate limiting (429) or any
/// server-side 5xx (500/502/503/504 — all transient on Google's end). Other 4xx
/// (bad request, auth) are the caller's fault and won't improve on retry.
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

/// Whether a transport-level error is a transient blip worth retrying — a
/// failed connect (this also covers DNS resolution failures) or a timeout.
fn is_retryable_transport(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout()
}

/// Backoff for a 0-based `attempt`: exponential (BASE_BACKOFF_MS << attempt),
/// raised to any server-`suggested` delay, then capped at MAX_BACKOFF.
fn backoff_delay(attempt: u32, suggested: Option<Duration>) -> Duration {
    let factor = 1u64 << attempt.min(5); // cap shift so it can't overflow
    let computed = Duration::from_millis(BASE_BACKOFF_MS.saturating_mul(factor));
    suggested.map_or(computed, |s| s.max(computed)).min(MAX_BACKOFF)
}

/// Extract Gemini's suggested retry delay from an error body. The 429 response
/// carries it as a `RetryInfo` detail with `retryDelay: "54s"`. Returns `None`
/// if the body isn't the expected shape.
fn parse_retry_delay(body: &str) -> Option<Duration> {
    let v: Value = serde_json::from_str(body).ok()?;
    let details = v.get("error")?.get("details")?.as_array()?;
    details
        .iter()
        .filter(|d| {
            d.get("@type")
                .and_then(Value::as_str)
                .is_some_and(|t| t.ends_with("RetryInfo"))
        })
        .find_map(|d| d.get("retryDelay").and_then(Value::as_str))
        .and_then(parse_duration_secs)
}

/// Parse a Google protobuf duration string like `"54s"` or `"1.5s"`.
fn parse_duration_secs(s: &str) -> Option<Duration> {
    let secs: f64 = s.strip_suffix('s')?.trim().parse().ok()?;
    (secs.is_finite() && secs >= 0.0).then(|| Duration::from_secs_f64(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Gemini-shaped response whose text part is `decision_json`.
    fn response_with(decision_json: &str) -> Value {
        json!({
            "candidates": [{
                "content": {
                    "role": "model",
                    "parts": [{ "text": decision_json }]
                },
                "finishReason": "STOP"
            }]
        })
    }

    #[test]
    fn parses_controlled_generation_decision() {
        let resp = response_with(
            r#"{"action":"refresh_and_resubmit","tip_lamports":12000,"rationale":"blockhash expired; refreshing and raising tip toward p75."}"#,
        );
        let d = parse_decision(&resp).unwrap();
        assert_eq!(d.action, Action::RefreshAndResubmit);
        assert_eq!(d.tip_lamports, 12_000);
        assert!(d.rationale.contains("blockhash"));
    }

    #[test]
    fn parses_tip_rendered_as_string() {
        let resp = response_with(r#"{"action":"submit","tip_lamports":"5000","rationale":"calm market."}"#);
        let d = parse_decision(&resp).unwrap();
        assert_eq!(d.action, Action::Submit);
        assert_eq!(d.tip_lamports, 5_000);
    }

    #[test]
    fn errors_when_no_candidate_text() {
        let resp = json!({ "candidates": [] });
        assert!(matches!(parse_decision(&resp), Err(AgentError::NoDecision)));
    }

    #[test]
    fn errors_on_unknown_action() {
        let resp = response_with(r#"{"action":"explode","tip_lamports":1,"rationale":"x"}"#);
        assert!(matches!(parse_decision(&resp), Err(AgentError::Parse(_))));
    }

    #[test]
    fn retryable_status_covers_429_and_5xx_only() {
        use reqwest::StatusCode;
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE)); // 503
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR)); // 500
        // Client errors (other than 429) and successes must NOT retry.
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST)); // 400
        assert!(!is_retryable_status(StatusCode::UNAUTHORIZED)); // 401
        assert!(!is_retryable_status(StatusCode::OK));
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        // 500ms, 1s, 2s, 4s ... doubling until the shift is capped at <<5, so the
        // computed-only backoff plateaus at 500ms*32 = 16s (under MAX_BACKOFF).
        assert_eq!(backoff_delay(0, None), Duration::from_millis(500));
        assert_eq!(backoff_delay(1, None), Duration::from_millis(1000));
        assert_eq!(backoff_delay(2, None), Duration::from_millis(2000));
        assert_eq!(backoff_delay(10, None), Duration::from_secs(16));
    }

    #[test]
    fn backoff_honors_larger_server_delay_but_still_caps() {
        // A server hint larger than the computed backoff wins...
        assert_eq!(backoff_delay(0, Some(Duration::from_secs(5))), Duration::from_secs(5));
        // ...a smaller hint is ignored in favor of our backoff...
        assert_eq!(backoff_delay(3, Some(Duration::from_millis(100))), Duration::from_secs(4));
        // ...and even a huge hint is clamped to the ceiling.
        assert_eq!(backoff_delay(0, Some(Duration::from_secs(999))), MAX_BACKOFF);
    }

    #[test]
    fn parses_retry_delay_from_real_429_body() {
        // The exact shape Gemini returns on a free-tier quota 429.
        let body = r#"{
            "error": {
                "code": 429,
                "status": "RESOURCE_EXHAUSTED",
                "details": [
                    { "@type": "type.googleapis.com/google.rpc.Help", "links": [] },
                    { "@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "54s" }
                ]
            }
        }"#;
        assert_eq!(parse_retry_delay(body), Some(Duration::from_secs(54)));
    }

    #[test]
    fn retry_delay_absent_or_malformed_is_none() {
        assert_eq!(parse_retry_delay(r#"{"error":{"details":[]}}"#), None);
        assert_eq!(parse_retry_delay("not json"), None);
        assert_eq!(parse_duration_secs("1.5s"), Some(Duration::from_secs_f64(1.5)));
        assert_eq!(parse_duration_secs("54"), None); // missing unit suffix
        assert_eq!(parse_duration_secs("abcs"), None);
    }
}
