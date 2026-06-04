//! TxRadar orchestrator binary.
//!
//! Boots the stack: loads the active profile (`TXRADAR_PROFILE` ->
//! `config/<profile>.toml`), merges secrets from the environment, initializes
//! tracing, and (in later phases) wires the stream -> tracker -> agent -> TUI
//! pipeline together. Network selection is pure config — testnet and mainnet
//! run the same code path.

use std::env;

use anyhow::{Context, Result};
use txradar_types::Config;

/// Secrets pulled from the environment, kept separate from the TOML profile.
#[derive(Debug, Clone)]
struct Secrets {
    keypair_path: Option<String>,
    yellowstone_x_token: Option<String>,
    anthropic_api_key: Option<String>,
    jito_uuid: Option<String>,
}

impl Secrets {
    fn from_env() -> Self {
        Self {
            keypair_path: env::var("TXRADAR_KEYPAIR_PATH").ok(),
            yellowstone_x_token: env::var("TXRADAR_YELLOWSTONE_X_TOKEN").ok().filter(|s| !s.is_empty()),
            anthropic_api_key: env::var("ANTHROPIC_API_KEY").ok().filter(|s| !s.is_empty()),
            jito_uuid: env::var("TXRADAR_JITO_UUID").ok().filter(|s| !s.is_empty()),
        }
    }

    /// Report which secrets are present without ever logging their values.
    fn presence(&self) -> String {
        let mark = |o: &Option<String>| if o.is_some() { "set" } else { "MISSING" };
        format!(
            "keypair={}, yellowstone_x_token={}, anthropic_api_key={}, jito_uuid={}",
            mark(&self.keypair_path),
            mark(&self.yellowstone_x_token),
            mark(&self.anthropic_api_key),
            mark(&self.jito_uuid),
        )
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,txradar=debug"));
    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(filter)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let profile = env::var("TXRADAR_PROFILE").unwrap_or_else(|_| "testnet".to_string());
    let config_path = Config::path_for_profile(&profile);

    let config = Config::from_path(&config_path)
        .with_context(|| format!("loading config profile '{profile}' from {config_path}"))?;
    let secrets = Secrets::from_env();

    tracing::info!(
        target: "txradar",
        profile = %profile,
        network = config.network.as_str(),
        rpc = %config.rpc.http_url,
        block_engine = %config.jito.block_engine_url,
        agent_model = %config.agent.model,
        "TxRadar starting"
    );
    tracing::info!(target: "txradar", secrets = %secrets.presence(), "secret presence");

    // Phase 0 ends here: config + secrets load, the workspace composes, and the
    // binary runs. Subsequent phases wire in:
    //   Phase 1  txradar-stream  -> live slot/leader/tx events
    //   Phase 2  txradar-core    -> blockhash mgr, bundle build, Jito client
    //   Phase 3  txradar-core    -> lifecycle tracker + failure classifier
    //   Phase 4  txradar-tips    -> tip oracle
    //   Phase 5  txradar-agent   -> AI decision-maker
    //   Phase 6  fault injection -> forced blockhash expiry
    //   Phase 7  txradar-tui     -> radar dashboard
    tracing::info!(target: "txradar", "scaffold boot OK — stack wiring lands in later phases");

    Ok(())
}
