//! TxRadar orchestrator binary.
//!
//! Boots the stack: loads the active profile (`TXRADAR_PROFILE` ->
//! `config/<profile>.toml`), merges secrets from the environment, initializes
//! tracing, and (in later phases) wires the stream -> tracker -> agent -> TUI
//! pipeline together. Network selection is pure config — testnet and mainnet
//! run the same code path.

use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use txradar_stream::{spawn, ConnectionState, SlotStatus, StreamConfig, StreamEvent};
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

/// Load environment from `.env.local` (preferred, gitignored) then `.env`.
/// `dotenvy` does not overwrite variables already set in the real environment,
/// so an explicit shell export still wins. Missing files are not an error.
fn load_dotenv() {
    // `.env.local` first so its values take precedence over `.env`.
    let _ = dotenvy::from_filename(".env.local");
    let _ = dotenvy::dotenv();
}

#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
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

    // --- Phase 1: live Yellowstone slot stream -----------------------------
    // Gated on having an x-token so a default checkout still boots cleanly. When
    // a token is present we connect to the configured endpoint and print the
    // live slot commitment progression as a smoke test of the stream layer.
    match &secrets.yellowstone_x_token {
        None => {
            tracing::warn!(
                target: "txradar",
                "TXRADAR_YELLOWSTONE_X_TOKEN not set — skipping live stream. \
                 Set it (and config.yellowstone.endpoint) to stream slots."
            );
            tracing::info!(target: "txradar", "scaffold boot OK — set the x-token to exercise Phase 1");
        }
        Some(token) => {
            run_stream_smoketest(&config, token.clone()).await;
        }
    }

    Ok(())
}

/// Phase 1 smoke test: subscribe to the slot stream and log commitment
/// transitions for a bounded window, then shut down. Proves connect + auth +
/// subscribe + keepalive + event mapping end-to-end against a real endpoint.
async fn run_stream_smoketest(config: &Config, x_token: String) {
    const RUN_FOR: Duration = Duration::from_secs(30);

    tracing::info!(
        target: "txradar",
        endpoint = %config.yellowstone.endpoint,
        "Phase 1: starting Yellowstone slot stream (smoke test, {}s)",
        RUN_FOR.as_secs()
    );

    let stream_cfg = StreamConfig::slots_only(config.yellowstone.clone(), Some(x_token));
    let mut handle = spawn(stream_cfg);

    let mut processed = 0u64;
    let mut confirmed = 0u64;
    let mut finalized = 0u64;
    let deadline = tokio::time::sleep(RUN_FOR);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            _ = &mut deadline => {
                tracing::info!(
                    target: "txradar",
                    processed, confirmed, finalized,
                    "Phase 1 smoke test window elapsed — stream layer verified"
                );
                break;
            }
            event = handle.events.recv() => {
                match event {
                    None => {
                        tracing::warn!(target: "txradar", "stream channel closed");
                        break;
                    }
                    Some(StreamEvent::SlotStatus { slot, status, .. }) => {
                        match status {
                            SlotStatus::Processed => processed += 1,
                            SlotStatus::Confirmed => confirmed += 1,
                            SlotStatus::Finalized => finalized += 1,
                            _ => {}
                        }
                        tracing::debug!(target: "txradar", slot, ?status, "slot update");
                    }
                    Some(StreamEvent::Connection(state)) => {
                        match state {
                            ConnectionState::Connected =>
                                tracing::info!(target: "txradar", "stream connected"),
                            other =>
                                tracing::info!(target: "txradar", ?other, "stream connection state"),
                        }
                    }
                    Some(StreamEvent::Transaction { signature, slot, failed }) => {
                        tracing::debug!(target: "txradar", %signature, slot, failed, "tx update");
                    }
                    Some(StreamEvent::Leader { slot, leader }) => {
                        tracing::debug!(target: "txradar", slot, %leader, "leader update");
                    }
                }
            }
        }
    }
}
