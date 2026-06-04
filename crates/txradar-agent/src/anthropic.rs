//! Anthropic Claude implementation of [`Decider`] (Phase 5).
//!
//! Uses the Messages API with tool-use / structured JSON so the model returns a
//! well-typed [`Decision`] (action + tip + rationale). Prompt caching keeps the
//! static policy/system prompt cheap across the many in-loop calls. The model
//! id comes from config (`agent.model`).

use async_trait::async_trait;

use crate::{AgentError, Decider, Decision, DecisionContext};

/// Claude-backed decision-maker. Holds the API key + model; constructed by the
/// binary from `ANTHROPIC_API_KEY` and the active profile.
pub struct AnthropicDecider {
    #[allow(dead_code)]
    api_key: String,
    #[allow(dead_code)]
    model: String,
    #[allow(dead_code)]
    http: reqwest::Client,
}

impl AnthropicDecider {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Decider for AnthropicDecider {
    async fn decide(&self, _ctx: &DecisionContext) -> Result<Decision, AgentError> {
        // Phase 5: build the Messages request (cached system prompt + tool schema
        // for the structured Decision), call the API, parse the tool result.
        Err(AgentError::NotImplemented)
    }
}
