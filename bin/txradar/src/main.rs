//! TxRadar orchestrator binary.
//!
//! Boots the stack: loads the active profile (`TXRADAR_PROFILE` ->
//! `config/<profile>.toml`), merges secrets from the environment, initializes
//! tracing, and (in later phases) wires the stream -> tracker -> agent -> TUI
//! pipeline together. Network selection is pure config — testnet and mainnet
//! run the same code path.

use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use txradar_stream::{spawn, ConnectionState, SlotStatus, StreamConfig, StreamEvent};
use txradar_types::Config;

mod executor;
mod ingest;

/// Secrets pulled from the environment, kept separate from the TOML profile.
#[derive(Debug, Clone)]
struct Secrets {
    keypair_path: Option<String>,
    yellowstone_x_token: Option<String>,
    rpc_api_key: Option<String>,
    gemini_api_key: Option<String>,
    /// Optional override for the agent's Generative Language API endpoint (a
    /// gateway/proxy that speaks the Gemini API). Not a secret, but lives with
    /// the env config. Empty -> the default generativelanguage.googleapis.com.
    gemini_base_url: Option<String>,
    jito_uuid: Option<String>,
}

impl Secrets {
    fn from_env() -> Self {
        let var = |k: &str| env::var(k).ok().filter(|s| !s.is_empty());
        Self {
            keypair_path: env::var("TXRADAR_KEYPAIR_PATH").ok(),
            yellowstone_x_token: var("TXRADAR_YELLOWSTONE_X_TOKEN"),
            rpc_api_key: var("TXRADAR_RPC_API_KEY"),
            gemini_api_key: var("GEMINI_API_KEY"),
            gemini_base_url: var("GEMINI_BASE_URL"),
            jito_uuid: var("TXRADAR_JITO_UUID"),
        }
    }

    /// Report which secrets are present without ever logging their values.
    fn presence(&self) -> String {
        let mark = |o: &Option<String>| if o.is_some() { "set" } else { "MISSING" };
        format!(
            "keypair={}, yellowstone_x_token={}, rpc_api_key={}, gemini_api_key={}, jito_uuid={}",
            mark(&self.keypair_path),
            mark(&self.yellowstone_x_token),
            mark(&self.rpc_api_key),
            mark(&self.gemini_api_key),
            mark(&self.jito_uuid),
        )
    }
}

/// Initialize tracing. When `log_file` is `Some`, logs are written there
/// instead of stdout — used in TUI mode so log lines don't corrupt the
/// alternate-screen dashboard.
fn init_tracing(log_file: Option<&str>) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,txradar=debug"));
    let registry = tracing_subscriber::registry().with(filter);
    match log_file {
        Some(path) => {
            // create the parent dir best-effort, then append.
            if let Some(parent) = std::path::Path::new(path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match std::fs::OpenOptions::new().create(true).append(true).open(path) {
                Ok(file) => {
                    let make = move || file.try_clone().expect("clone tui log file handle");
                    registry.with(fmt::layer().with_target(true).with_ansi(false).with_writer(make)).init();
                }
                Err(_) => registry.with(fmt::layer().with_target(true)).init(),
            }
        }
        None => registry.with(fmt::layer().with_target(true)).init(),
    }
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

    // Parse the command + flags early: TUI mode reroutes logging to a file so it
    // doesn't corrupt the dashboard.
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).cloned().unwrap_or_default();
    let tui = args.iter().any(|a| a == "--tui");
    init_tracing(if tui { Some("logs/txradar-tui.log") } else { None });

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

    // Subcommand dispatch.
    //   `run`        — the production campaign: live stream, real broadcast,
    //                  stream-confirmed landing, leader-window timing.
    //   `demo-fault` — the simulated autonomous-retry demo (agent-driven
    //                  blockhash-expiry recovery; signs real txs, no broadcast).
    //   default      — the Phase 1 stream smoke test.
    if cmd == "run" {
        return run_live_campaign(&config, &secrets, tui).await;
    }
    if cmd == "simulate" {
        return run_simulate(&config, &secrets).await;
    }
    if cmd == "demo-fault" {
        return run_fault_demo(&config, &secrets, tui).await;
    }

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

/// Phase 6: the autonomous-retry-with-fault-injection demonstration.
///
/// Builds the real core stack (blockhash manager, Jito client, tip oracle,
/// lifecycle tracker), arms a blockhash-expiry fault, and runs the agent's
/// `run_attempt_loop`. The *agent* detects the injected expiry, reasons about
/// it, refreshes the blockhash, recalculates the tip, and resubmits — none of
/// that control flow is hardcoded here. The resulting `BundleRecord`s are
/// appended to the lifecycle log.
///
/// Chain mode is chosen automatically: if a keypair is configured we sign with
/// it; without a funded keypair (or to stay offline) we run in *simulated* mode,
/// which signs real transactions but doesn't broadcast and labels every record
/// with a `-sim` network suffix so it's never mistaken for a graded landing.
async fn run_fault_demo(config: &Config, secrets: &Secrets, tui: bool) -> Result<()> {
    use executor::{ChainMode, ExecutorParams, LiveExecutor};
    use ingest::NetworkState;
    use txradar_core::blockhash::BlockhashManager;
    use txradar_core::bundle::load_keypair;
    use txradar_core::jito::JitoClient;
    use txradar_core::rpc::RpcClient;
    use txradar_core::tracker::LifecycleTracker;
    use txradar_tips::{TipBounds, TipOracle};
    use solana_sdk::signature::Keypair;

    tracing::info!(target: "txradar::demo", "autonomous retry + fault injection demo (SIMULATED — no broadcast)");

    // Demo always runs SIMULATED: it signs real transactions but never
    // broadcasts, so it needs no funds and never spends SOL. Use a configured
    // keypair if present (for realistic signatures), else an ephemeral one.
    let payer = match secrets.keypair_path.as_deref().filter(|p| !p.is_empty()) {
        Some(path) => load_keypair(path).unwrap_or_else(|e| {
            tracing::warn!(target: "txradar::demo", error = %e, "keypair load failed; using ephemeral signer");
            Keypair::new()
        }),
        None => Keypair::new(),
    };

    let rpc_url = config.rpc.url_with_key(secrets.rpc_api_key.as_deref());
    let blockhash = BlockhashManager::new(RpcClient::new(rpc_url), config.rpc.blockhash_commitment.clone());
    let jito = JitoClient::new(
        config.jito.block_engine_url.clone(),
        secrets.jito_uuid.clone(),
        config.jito.max_requests_per_sec,
    );
    let bounds = TipBounds::from(&config.tips);
    let oracle = TipOracle::new(config.jito.tip_floor_rest.clone(), bounds);

    let tracker = Arc::new(Mutex::new(LifecycleTracker::new()));
    let net = Arc::new(Mutex::new(NetworkState::default()));

    let params = ExecutorParams {
        mode: ChainMode::Simulated,
        max_retries: config.agent.max_retries,
        inject_expiry: true,
        confirm_timeout_secs: config.lifecycle.confirmed_timeout_secs,
        poll_interval_secs: config.lifecycle.status_poll_interval_secs,
        // Unused in the simulated fault demo (it never starves), but required.
        starve_tip: config.tips.min_lamports,
    };
    let exec = LiveExecutor::new(
        params,
        config.network.as_str(),
        payer,
        blockhash,
        jito,
        oracle,
        bounds,
        tracker,
        net,
    );

    let decider = build_decider(config, secrets, tui);

    // Demo writes to a SEPARATE log so simulated records never pollute the
    // graded `lifecycle.jsonl`.
    let max_retries = config.agent.max_retries;
    let log_path = demo_log_path(&config.log.path);
    let network_tag = format!("{}-sim", config.network.as_str());

    if tui {
        run_demo_tui(exec, decider, max_retries, log_path, network_tag, /* settle_secs = */ 0).await
    } else {
        let outcome = execute_run(exec, decider, max_retries, log_path, /* settle_secs = */ 0).await?;
        report_outcome(&outcome);
        Ok(())
    }
}

/// The production campaign (`run`): live broadcast on the configured network,
/// landing confirmed from the Yellowstone stream, submission timed to the Jito
/// leader window. Requires a funded keypair and a Yellowstone x-token.
async fn run_live_campaign(config: &Config, secrets: &Secrets, tui: bool) -> Result<()> {
    use executor::{ChainMode, ExecutorParams, LiveExecutor};
    use ingest::NetworkState;
    use solana_sdk::signature::Signer;
    use txradar_core::blockhash::BlockhashManager;
    use txradar_core::bundle::load_keypair;
    use txradar_core::jito::JitoClient;
    use txradar_core::rpc::RpcClient;
    use txradar_core::tracker::LifecycleTracker;
    use txradar_tips::{TipBounds, TipOracle};

    tracing::info!(
        target: "txradar::run",
        network = config.network.as_str(),
        "LIVE campaign — real broadcast, stream-confirmed landing, leader-window timing"
    );

    // --- Required inputs (fail fast with actionable errors) -------------------
    let keypair_path = secrets
        .keypair_path
        .as_deref()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow!(
            "TXRADAR_KEYPAIR_PATH is not set — a live run needs a funded signer. \
             Set it in .env.local and fund the keypair's pubkey."
        ))?;
    let payer = load_keypair(keypair_path)
        .map_err(|e| anyhow!("loading keypair from {keypair_path}: {e}"))?;
    let signer_pubkey = payer.pubkey().to_string();

    let x_token = secrets
        .yellowstone_x_token
        .clone()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow!(
            "TXRADAR_YELLOWSTONE_X_TOKEN is not set — a live run confirms landing \
             from the Yellowstone stream and needs the gRPC auth token."
        ))?;

    let rpc_url = config.rpc.url_with_key(secrets.rpc_api_key.as_deref());
    let rpc = RpcClient::new(rpc_url.clone());

    // --- Preflight: the fee-payer must be funded ------------------------------
    preflight_balance(&rpc, &signer_pubkey, config).await?;

    // --- Build the core stack -------------------------------------------------
    let blockhash = BlockhashManager::new(RpcClient::new(rpc_url), config.rpc.blockhash_commitment.clone());
    let jito = JitoClient::new(
        config.jito.block_engine_url.clone(),
        secrets.jito_uuid.clone(),
        config.jito.max_requests_per_sec,
    );
    let bounds = TipBounds::from(&config.tips);
    let oracle = TipOracle::new(config.jito.tip_floor_rest.clone(), bounds);

    let tracker = Arc::new(Mutex::new(LifecycleTracker::new()));
    let net = Arc::new(Mutex::new(NetworkState::default()));

    // --- Spawn the live stream + ingest (feeds the shared tracker/net) --------
    // Watch the fee-payer's transactions so we confirm our bundle's landing from
    // the stream, and subscribe to slots for commitment progression + skip rate.
    let stream_cfg = StreamConfig::slots_and_accounts(
        config.yellowstone.clone(),
        Some(x_token),
        vec![signer_pubkey.clone()],
    );
    let handle = spawn(stream_cfg);
    let _ingest = ingest::spawn(handle, tracker.clone(), net.clone());
    tracing::info!(target: "txradar::run", signer = %signer_pubkey, "stream + ingest started; watching fee-payer txs");

    let params = ExecutorParams {
        mode: ChainMode::Live,
        max_retries: config.agent.max_retries,
        inject_expiry: false,
        confirm_timeout_secs: config.lifecycle.confirmed_timeout_secs,
        poll_interval_secs: config.lifecycle.status_poll_interval_secs,
        // Starved campaign txs tip at the operator floor minimum (the Jito
        // minimum): a real broadcast that's non-competitive and won't land.
        starve_tip: config.tips.min_lamports,
    };
    let exec = LiveExecutor::new(
        params,
        config.network.as_str(),
        payer,
        blockhash,
        jito,
        oracle,
        bounds,
        tracker,
        net,
    );

    let decider = build_decider(config, secrets, tui);

    // Allow finalization to arrive on the stream after a confirmed landing,
    // bounded so the run doesn't hang waiting for finalized commitment.
    let settle_secs = config.lifecycle.finalized_timeout_secs.min(20);
    let max_retries = config.agent.max_retries;
    let log_path = config.log.path.clone();
    let network_tag = config.network.as_str().to_string();

    if tui {
        return run_demo_tui(exec, decider, max_retries, log_path, network_tag, settle_secs).await;
    }

    // Campaign sizing (graded lifecycle log): `--count N` logical transactions
    // over the single live stream; the first `--starve K` of them run starved
    // (floor-pinned tip) to produce honest real failure cases.
    let args: Vec<String> = env::args().collect();
    let count = flag_u32(&args, "--count").unwrap_or(1).max(1);
    let starve = flag_u32(&args, "--starve").unwrap_or(0).min(count);
    run_campaign(exec, decider, max_retries, log_path, settle_secs, count, starve).await
}

/// Zero-cost diagnostic: build exactly the bundle transaction the live path
/// would broadcast and run it through `simulateTransaction` (no broadcast, no
/// SOL). Prints the simulation `err` and program logs so we can see *why* Jito
/// would mark a bundle Invalid before spending anything.
async fn run_simulate(config: &Config, secrets: &Secrets) -> Result<()> {
    use solana_sdk::signature::Signer;
    use txradar_core::blockhash::BlockhashManager;
    use txradar_core::bundle::{build_single_tx_bundle, load_keypair, random_tip_account};
    use txradar_core::rpc::RpcClient;

    let keypair_path = secrets
        .keypair_path
        .as_deref()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow!("TXRADAR_KEYPAIR_PATH is not set"))?;
    let payer = load_keypair(keypair_path).map_err(|e| anyhow!("loading keypair: {e}"))?;

    let rpc_url = config.rpc.url_with_key(secrets.rpc_api_key.as_deref());
    let rpc = RpcClient::new(rpc_url.clone());

    // Fresh blockhash via the same manager the live path uses.
    let mut blockhash =
        BlockhashManager::new(RpcClient::new(rpc_url), config.rpc.blockhash_commitment.clone());
    blockhash.refresh().await.map_err(|e| anyhow!("blockhash refresh: {e}"))?;
    let bh = blockhash.current().ok_or_else(|| anyhow!("no blockhash"))?;
    let bh_hash = bh.as_hash().map_err(|e| anyhow!("blockhash parse: {e}"))?;
    let bh_str = bh.blockhash.clone();

    // Cross-RPC sanity: is the blockhash our configured RPC handed us actually on
    // the canonical chain? Ask PUBLIC mainnet-beta. If this says false, our RPC's
    // view is forked/lagging — which makes Jito drop every bundle as Invalid.
    let public = RpcClient::new("https://api.mainnet-beta.solana.com");
    match public.is_blockhash_valid(&bh_str, "processed").await {
        Ok(true) => println!("blockhash {bh_str} — VALID on public mainnet-beta ✓"),
        Ok(false) => println!(
            "blockhash {bh_str} — NOT valid on public mainnet-beta ✗  \
             (configured RPC's chain view is forked/lagging — this is why bundles are dropped)"
        ),
        Err(e) => println!("blockhash cross-check failed: {e}"),
    }

    let tip_account = random_tip_account();
    let tip = config.tips.min_lamports.max(10_000);
    let bundle =
        build_single_tx_bundle(&payer, &tip_account, tip, "txradar simulate", &bh_hash, &bh_str)
            .map_err(|e| anyhow!("bundle build: {e}"))?;

    tracing::info!(
        target: "txradar::simulate",
        payer = %payer.pubkey(), tip, tip_account = %tip_account, blockhash = %bh_str,
        sig = ?bundle.primary_signature(),
        "simulating bundle tx (no broadcast)"
    );

    let value = rpc
        .simulate_transaction_b64(&bundle.encoded_txs[0])
        .await
        .map_err(|e| anyhow!("simulateTransaction: {e}"))?;

    println!("\n=== simulateTransaction result ===");
    println!("{value}");
    match value.get("err") {
        None => println!("\nerr: none — transaction simulates cleanly"),
        Some(e) if e.is_null() => println!("\nerr: none — transaction simulates cleanly"),
        Some(e) => println!("\nerr: {e}  <-- this is why Jito rejects the bundle"),
    }

    // Isolation probe: with `--send`, broadcast the SAME logical tx end-to-end on
    // the PUBLIC mainnet-beta chain — fetch the blockhash there, sign against it,
    // and send with skipPreflight (bypassing any lagging RPC's local simulation).
    // If this lands, our transaction is provably valid on the canonical chain and
    // the fault is entirely the configured RPC / Jito bundle infra.
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|a| a == "--send") {
        use txradar_core::bundle::build_single_tx_bundle as build_tx;
        println!("\n=== --send: end-to-end on PUBLIC mainnet-beta (fetch hash + send there) ===");
        let pub_bh = public
            .get_latest_blockhash("confirmed")
            .await
            .map_err(|e| anyhow!("public getLatestBlockhash: {e}"))?;
        let pub_hash = std::str::FromStr::from_str(&pub_bh.blockhash)
            .map_err(|_| anyhow!("public blockhash parse failed"))?;
        let pub_bundle = build_tx(
            &payer,
            &random_tip_account(),
            tip,
            "txradar canonical-chain probe",
            &pub_hash,
            &pub_bh.blockhash,
        )
        .map_err(|e| anyhow!("bundle build: {e}"))?;

        match public.send_transaction_b64_opts(&pub_bundle.encoded_txs[0], true).await {
            Ok(sig) => {
                println!("submitted via public RPC, signature: {sig}");
                println!("explorer: https://solscan.io/tx/{sig}");
                for i in 0..20 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    match public.get_signature_status(&sig).await {
                        Ok(Some(status)) => {
                            println!("  poll {i}: {status}  -> LANDED on canonical chain (our tx is valid; fault is SolInfra/Jito infra)");
                            return Ok(());
                        }
                        Ok(None) => println!("  poll {i}: not yet on-chain…"),
                        Err(e) => println!("  poll {i}: status error: {e}"),
                    }
                }
                println!("  did NOT land within ~40s — investigate tx/account, not infra");
            }
            Err(e) => println!("public sendTransaction error: {e}"),
        }
    }

    // Decisive test: with `--bundle`, submit a REAL Jito bundle but built on a
    // PUBLIC mainnet-beta blockhash (not the lagging configured RPC). This
    // isolates the two confounded variables — if a competitively-tipped bundle on
    // a canonical blockhash lands, the free Jito endpoint is fine and no paid
    // endpoint is needed; if it's still dropped, the congested free block-engine
    // is the blocker and a paid/authenticated endpoint is required.
    if args.iter().any(|a| a == "--bundle") {
        use txradar_core::bundle::build_single_tx_bundle as build_tx;
        use txradar_core::jito::JitoClient;
        println!("\n=== --bundle: REAL Jito bundle on a canonical (mainnet-beta) blockhash ===");
        println!("    submitting the SAME bundle to every free Jito region (Jito dedups; any can land)\n");
        let pub_bh = public
            .get_latest_blockhash("confirmed")
            .await
            .map_err(|e| anyhow!("public getLatestBlockhash: {e}"))?;
        let pub_hash = std::str::FromStr::from_str(&pub_bh.blockhash)
            .map_err(|_| anyhow!("public blockhash parse failed"))?;
        let bundle_tip = config.tips.max_lamports; // competitive: clears p95
        let jb = build_tx(
            &payer,
            &random_tip_account(),
            bundle_tip,
            "txradar jito-bundle probe",
            &pub_hash,
            &pub_bh.blockhash,
        )
        .map_err(|e| anyhow!("bundle build: {e}"))?;
        let sig = jb.primary_signature().unwrap_or_default().to_string();

        // All free Jito block-engine regions + the global router.
        let regions = [
            ("mainnet/global", "https://mainnet.block-engine.jito.wtf/api/v1"),
            ("amsterdam", "https://amsterdam.mainnet.block-engine.jito.wtf/api/v1"),
            ("frankfurt", "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1"),
            ("ny", "https://ny.mainnet.block-engine.jito.wtf/api/v1"),
            ("slc", "https://slc.mainnet.block-engine.jito.wtf/api/v1"),
            ("tokyo", "https://tokyo.mainnet.block-engine.jito.wtf/api/v1"),
            ("london", "https://london.mainnet.block-engine.jito.wtf/api/v1"),
        ];
        let mut clients = Vec::new();
        for (name, url) in regions {
            let jito = JitoClient::new(url, secrets.jito_uuid.clone(), config.jito.max_requests_per_sec);
            match jito.send_bundle(&jb).await {
                Ok(id) => {
                    println!("  [{name}] accepted: id={id}");
                    clients.push((name, jito, id));
                }
                Err(e) => println!("  [{name}] sendBundle error: {e}"),
            }
        }
        if clients.is_empty() {
            println!("\n  no region accepted the bundle — all free endpoints rejected/rate-limited");
            return Ok(());
        }
        println!("\n  tip={bundle_tip}  sig={sig}");
        println!("  explorer: https://solscan.io/tx/{sig}\n");
        for i in 0..30 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if let Ok(Some(status)) = public.get_signature_status(&sig).await {
                println!("  poll {i}: {status}  -> Jito bundle LANDED on a free region (NO paid endpoint needed!)");
                return Ok(());
            }
            if i % 3 == 0 {
                for (name, jito, id) in &clients {
                    if let Ok(st) = jito.get_inflight_status(id).await {
                        println!("  poll {i} [{name}]: inflight = {st:?}");
                    }
                }
            }
        }
        println!("\n  bundle did NOT land via ANY free region in ~60s — free Jito is the blocker; need a paid/authenticated endpoint");
    }
    Ok(())
}

/// Parse a `--flag N` (or `--flag=N`) unsigned integer from argv.
fn flag_u32(args: &[String], name: &str) -> Option<u32> {
    let prefix = format!("{name}=");
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if let Some(v) = a.strip_prefix(&prefix) {
            return v.parse().ok();
        }
        if a == name {
            return it.next().and_then(|v| v.parse().ok());
        }
    }
    None
}

/// Run the graded lifecycle-log campaign: `count` logical transactions over one
/// persistent live stream, appending every attempt's [`BundleRecord`] to the
/// lifecycle log. The first `starve` transactions run starved (floor-pinned tip,
/// zero retries) so they broadcast for real but don't land — the required real
/// failure cases, produced without any fabrication. A fresh blockhash is fetched
/// per transaction so a long campaign never reuses a stale one.
async fn run_campaign(
    mut exec: executor::LiveExecutor,
    decider: Box<dyn txradar_agent::Decider>,
    max_retries: u32,
    log_path: String,
    settle_secs: u64,
    count: u32,
    starve: u32,
) -> Result<()> {
    use txradar_agent::run_attempt_loop;
    use txradar_agent::Executor as _;
    use txradar_log::LifecycleLog;

    tracing::info!(
        target: "txradar::campaign",
        count, starve,
        "starting lifecycle-log campaign ({} transactions, {} starved)", count, starve
    );

    let log = LifecycleLog::open(&log_path).await.context("opening lifecycle log")?;

    let mut total_records = 0usize;
    let mut failure_records = 0usize;
    let mut landed_txs = 0usize;
    let mut landed_slots: Vec<u64> = Vec::new();

    for i in 0..count {
        let starving = i < starve;
        exec.set_starve(starving);

        // Fresh blockhash per logical tx: across a multi-minute campaign a hash
        // cached from an earlier tx would already be expired.
        if let Err(e) = exec.refresh_blockhash().await {
            tracing::warn!(target: "txradar::campaign", error = %e, "blockhash refresh failed; proceeding (agent will refresh on retry)");
        }

        tracing::info!(
            target: "txradar::campaign",
            tx = i + 1, of = count, starving,
            "── transaction {}/{}{}", i + 1, count, if starving { " (STARVED — expecting honest non-landing)" } else { "" }
        );

        let result = run_attempt_loop(decider.as_ref(), &mut exec, max_retries)
            .await
            .with_context(|| format!("campaign transaction {} failed", i + 1))?;

        // Let finalized commitment arrive on the stream after a confirmed landing.
        if settle_secs > 0 && result.landed_slot.is_some() {
            tokio::time::sleep(Duration::from_secs(settle_secs)).await;
        }
        exec.finalize_pending();

        let records = exec.drain_records();
        for rec in &records {
            log.append(rec).await.context("appending lifecycle record")?;
            total_records += 1;
            if rec.failure.is_some() {
                failure_records += 1;
            }
            if let Some(slot) = rec.landed_slot {
                landed_slots.push(slot);
            }
        }
        if result.landed_slot.is_some() {
            landed_txs += 1;
        }

        tracing::info!(
            target: "txradar::campaign",
            tx = i + 1, landed = ?result.landed_slot, attempts = result.attempts,
            records = records.len(), "transaction complete"
        );
    }

    tracing::info!(
        target: "txradar::campaign",
        transactions = count, total_records, failure_records, landed_txs,
        "CAMPAIGN COMPLETE"
    );

    println!("\n=== TxRadar campaign complete ===");
    println!("  transactions run : {count}  ({starve} starved)");
    println!("  records written  : {total_records}");
    println!("  failure records  : {failure_records}");
    println!("  landed txs       : {landed_txs}");
    println!("  landed slots     : {landed_slots:?}");
    println!("  lifecycle log    : {log_path}");
    if failure_records < 2 {
        println!("  NOTE: <2 failures so far — re-run with more `--starve` to reach the graded minimum.");
    }
    Ok(())
}

/// Build the agent decider: real Gemini if a key is available (interactively
/// prompting on a TTY), else the deterministic heuristic fallback.
fn build_decider(config: &Config, secrets: &Secrets, tui: bool) -> Box<dyn txradar_agent::Decider> {
    use txradar_agent::gemini::GeminiDecider;
    use txradar_agent::HeuristicDecider;

    let inner: Box<dyn txradar_agent::Decider> = match resolve_gemini_credentials(secrets, tui) {
        Some((key, base_url)) => {
            let endpoint = if base_url.is_empty() {
                "https://generativelanguage.googleapis.com".to_string()
            } else {
                base_url.clone()
            };
            tracing::info!(target: "txradar::agent", model = %config.agent.model, endpoint = %endpoint, "using Gemini agent");
            Box::new(GeminiDecider::with_base_url(key, config.agent.model.clone(), base_url))
        }
        None => {
            tracing::warn!(target: "txradar::agent", "no Gemini key; using heuristic fallback decider (not real LLM reasoning)");
            Box::new(HeuristicDecider::new(config.agent.max_retries))
        }
    };
    // Wrap with the budget guard so the per-context retry budget (which the
    // campaign zeroes for starved transactions) is authoritative for ANY
    // decider — the agent never retries past `ctx.max_retries`.
    Box::new(BudgetDecider { inner })
}

/// Decider middleware that enforces the per-context retry budget before
/// delegating. Without this, a decider that tracks its own budget (e.g. the
/// heuristic fallback) could ignore a context that zeroes retries — which the
/// campaign relies on to keep starved transactions to a single attempt.
struct BudgetDecider {
    inner: Box<dyn txradar_agent::Decider>,
}

#[async_trait::async_trait]
impl txradar_agent::Decider for BudgetDecider {
    async fn decide(
        &self,
        ctx: &txradar_agent::DecisionContext,
    ) -> std::result::Result<txradar_agent::Decision, txradar_agent::AgentError> {
        use txradar_agent::{Action, Decision, DecisionKind};
        if ctx.kind == DecisionKind::PostFailure && ctx.retries_so_far >= ctx.max_retries {
            return Ok(Decision {
                action: Action::Abort,
                tip_lamports: ctx.last_tip_lamports.unwrap_or(0),
                rationale: format!(
                    "retry budget exhausted ({}/{} retries) — aborting",
                    ctx.retries_so_far, ctx.max_retries
                ),
            });
        }
        self.inner.decide(ctx).await
    }
}

/// Preflight: the fee-payer must hold enough lamports to cover the worst-case
/// spend of a full attempt cycle (max tip + fee, across the retry budget).
/// Errors out before any broadcast if underfunded.
async fn preflight_balance(
    rpc: &txradar_core::rpc::RpcClient,
    pubkey: &str,
    config: &Config,
) -> Result<()> {
    const FEE_LAMPORTS_PER_TX: u64 = 5_000;
    let attempts = (config.agent.max_retries + 1) as u64;
    let needed = (config.tips.max_lamports + FEE_LAMPORTS_PER_TX) * attempts;

    let balance = rpc
        .get_balance(pubkey, &config.rpc.blockhash_commitment)
        .await
        .map_err(|e| anyhow!("preflight getBalance failed for {pubkey}: {e}"))?;

    let sol = |l: u64| l as f64 / 1_000_000_000.0;
    if balance < needed {
        return Err(anyhow!(
            "insufficient balance: {} has {:.6} SOL but a run needs ~{:.6} SOL \
             (max_tip {} + fee {} lamports × {} attempts). Fund the keypair and retry.",
            pubkey,
            sol(balance),
            sol(needed),
            config.tips.max_lamports,
            FEE_LAMPORTS_PER_TX,
            attempts
        ));
    }
    tracing::info!(
        target: "txradar::run",
        pubkey = %pubkey,
        balance_sol = sol(balance),
        needed_sol = sol(needed),
        "preflight OK — fee-payer funded"
    );
    Ok(())
}

/// Route demo records to a sibling `*-demo.jsonl` of the graded log path, so
/// simulated runs never contaminate the graded `lifecycle.jsonl`.
fn demo_log_path(graded: &str) -> String {
    match graded.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}-demo.{ext}"),
        None => format!("{graded}-demo"),
    }
}

/// Resolve the agent's `(api_key, base_url)`:
/// 1. If `GEMINI_API_KEY` is set, use it (with any `GEMINI_BASE_URL`).
/// 2. Otherwise, if we're on an interactive terminal, prompt for a key (and an
///    optional base URL for a compatible proxy). Offer to persist it to
///    `.env.local` so it's not asked again.
/// 3. Otherwise return `None` and the caller falls back to the heuristic agent.
///
/// `base_url` is returned empty to mean "use the default
/// generativelanguage.googleapis.com".
fn resolve_gemini_credentials(secrets: &Secrets, tui: bool) -> Option<(String, String)> {
    use std::io::IsTerminal;

    if let Some(key) = secrets.gemini_api_key.as_deref().filter(|k| !k.is_empty()) {
        return Some((key.to_string(), secrets.gemini_base_url.clone().unwrap_or_default()));
    }

    // Don't try to prompt when piped/non-interactive, or when the dashboard is
    // about to take over stdout (the prompt would be invisible/garbled).
    if tui || !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        return None;
    }

    prompt_gemini_credentials()
}

/// Interactively read a Gemini key (and optional base URL) from the terminal.
fn prompt_gemini_credentials() -> Option<(String, String)> {
    use std::io::Write;

    println!();
    println!("──────────────────────────────────────────────────────────────");
    println!(" GEMINI_API_KEY is not set — the live Gemini agent is off.");
    println!(" Paste a key to enable real AI reasoning, or press Enter to use");
    println!(" the offline heuristic fallback (fine for a dry run).");
    println!(" • Get a free key at: https://aistudio.google.com/app/apikey");
    println!(" • A custom proxy/gateway also needs its base URL (asked next).");
    println!("──────────────────────────────────────────────────────────────");
    print!(" API key (input is shown; leave blank to skip): ");
    let _ = std::io::stdout().flush();

    let key = read_line_trimmed()?;
    if key.is_empty() {
        println!(" → no key entered; continuing with the heuristic fallback.\n");
        return None;
    }

    // Optional: a compatible gateway/proxy needs an explicit endpoint. Blank
    // keeps the default generativelanguage.googleapis.com.
    print!(" Base URL (blank = generativelanguage.googleapis.com): ");
    let _ = std::io::stdout().flush();
    let base_url = read_line_trimmed().unwrap_or_default();

    // Offer to persist so we don't ask again.
    print!(" Save to .env.local for next time? [y/N]: ");
    let _ = std::io::stdout().flush();
    if let Some(ans) = read_line_trimmed() {
        if ans.eq_ignore_ascii_case("y") || ans.eq_ignore_ascii_case("yes") {
            match persist_env_local(&key, &base_url) {
                Ok(()) => println!(" → saved to .env.local"),
                Err(e) => eprintln!(" → could not save (.env.local): {e}"),
            }
        }
    }
    println!();
    Some((key, base_url))
}

/// Read one line from stdin, trimmed. Returns `None` on EOF/error.
fn read_line_trimmed() -> Option<String> {
    let mut buf = String::new();
    match std::io::stdin().read_line(&mut buf) {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(buf.trim().to_string()),
    }
}

/// Insert or replace `GEMINI_API_KEY` (and `GEMINI_BASE_URL` when given)
/// in `.env.local`, preserving all other lines. Creates the file if missing.
fn persist_env_local(key: &str, base_url: &str) -> std::io::Result<()> {
    let path = ".env.local";
    let existing = std::fs::read_to_string(path).unwrap_or_default();

    let mut set_key = false;
    let mut set_base = false;
    let mut out: Vec<String> = Vec::new();
    for line in existing.lines() {
        if line.starts_with("GEMINI_API_KEY=") {
            out.push(format!("GEMINI_API_KEY={key}"));
            set_key = true;
        } else if line.starts_with("GEMINI_BASE_URL=") {
            if base_url.is_empty() {
                out.push(line.to_string());
            } else {
                out.push(format!("GEMINI_BASE_URL={base_url}"));
            }
            set_base = true;
        } else {
            out.push(line.to_string());
        }
    }
    if !set_key {
        out.push(format!("GEMINI_API_KEY={key}"));
    }
    if !set_base && !base_url.is_empty() {
        out.push(format!("GEMINI_BASE_URL={base_url}"));
    }

    let mut body = out.join("\n");
    body.push('\n');
    std::fs::write(path, body)
}

/// Result of one demo run, returned from the (possibly backgrounded) loop.
struct DemoOutcome {
    landed_slot: Option<u64>,
    attempts: u32,
    final_action: txradar_agent::Action,
    decisions: Vec<txradar_agent::Decision>,
    records_written: usize,
    log_path: String,
}

/// Run the agent loop to completion, persist the lifecycle records, and return a
/// summary. Owns its inputs so it can run either inline or in a spawned task.
/// `settle_secs` lets a live run wait briefly for stream finalization after a
/// confirmed landing before flushing + logging (0 for the simulated demo).
async fn execute_run(
    mut exec: executor::LiveExecutor,
    decider: Box<dyn txradar_agent::Decider>,
    max_retries: u32,
    log_path: String,
    settle_secs: u64,
) -> Result<DemoOutcome> {
    use txradar_agent::run_attempt_loop;
    use txradar_log::LifecycleLog;

    let result = run_attempt_loop(decider.as_ref(), &mut exec, max_retries)
        .await
        .context("attempt loop failed")?;

    // Give the stream a bounded window to deliver finalized commitment, then
    // flush any bundle that landed but hasn't finalized yet so it's still logged.
    if settle_secs > 0 && result.landed_slot.is_some() {
        tokio::time::sleep(Duration::from_secs(settle_secs)).await;
    }
    exec.finalize_pending();

    let log = LifecycleLog::open(&log_path).await.context("opening lifecycle log")?;
    let records = exec.drain_records();
    let n = records.len();
    for rec in &records {
        log.append(rec).await.context("appending lifecycle record")?;
    }

    Ok(DemoOutcome {
        landed_slot: result.landed_slot,
        attempts: result.attempts,
        final_action: result.final_action,
        decisions: result.decisions,
        records_written: n,
        log_path,
    })
}

/// Log the agent's decision trail + outcome (the audit proving retries were
/// agent-driven, not hardcoded).
fn report_outcome(outcome: &DemoOutcome) {
    use txradar_agent::Action;
    tracing::info!(
        target: "txradar::demo",
        landed_slot = ?outcome.landed_slot,
        attempts = outcome.attempts,
        final_action = ?outcome.final_action,
        "attempt loop finished"
    );
    for (i, d) in outcome.decisions.iter().enumerate() {
        tracing::info!(
            target: "txradar::demo",
            step = i + 1,
            action = ?d.action,
            tip_lamports = d.tip_lamports,
            "agent decision: {}",
            d.rationale
        );
    }
    tracing::info!(
        target: "txradar::demo",
        records = outcome.records_written,
        path = %outcome.log_path,
        "lifecycle records written"
    );
    match outcome.final_action {
        Action::Abort => tracing::warn!(target: "txradar::demo", "agent aborted — see decision trail above"),
        _ if outcome.landed_slot.is_some() =>
            tracing::info!(target: "txradar::demo", "SUCCESS — bundle landed after autonomous recovery"),
        _ => tracing::info!(target: "txradar::demo", "loop ended without landing"),
    }
}

/// Phase 7: run the demo with the live radar dashboard. The agent loop runs in a
/// background task updating a shared [`DashboardState`]; the main task renders it
/// and handles `q` to quit. Falls back to the plain (logged) path if the
/// terminal can't enter raw mode (e.g. not a TTY).
async fn run_demo_tui(
    exec: executor::LiveExecutor,
    decider: Box<dyn txradar_agent::Decider>,
    max_retries: u32,
    log_path: String,
    network_tag: String,
    settle_secs: u64,
) -> Result<()> {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use crossterm::event::{self, Event, KeyCode};
    use txradar_tui::DashboardState;

    let shared: executor::SharedDashboard = Arc::new(Mutex::new(DashboardState::new(network_tag)));
    let exec = exec.with_dashboard(shared.clone());

    let mut term = match txradar_tui::enter() {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(target: "txradar::demo", error = %e, "no TTY for TUI; running plain");
            let outcome = execute_run(exec, decider, max_retries, log_path, settle_secs).await?;
            report_outcome(&outcome);
            return Ok(());
        }
    };

    // Background the agent loop; the render loop watches the shared state.
    let log_path_task = log_path.clone();
    let handle = tokio::spawn(async move { execute_run(exec, decider, max_retries, log_path_task, settle_secs).await });

    let res: Result<()> = loop {
        if let Err(e) = term.draw(|f| {
            if let Ok(state) = shared.lock() {
                txradar_tui::draw(f, &state);
            }
        }) {
            break Err(e.into());
        }

        if event::poll(Duration::from_millis(120)).unwrap_or(false) {
            if let Ok(Event::Key(k)) = event::read() {
                if matches!(k.code, KeyCode::Char('q') | KeyCode::Esc) {
                    break Ok(());
                }
            }
        }

        if handle.is_finished() {
            // Mark the footer done and draw one final frame so the result shows.
            if let Ok(mut state) = shared.lock() {
                state.status_line = "run complete — press q to exit".into();
            }
            let _ = term.draw(|f| {
                if let Ok(state) = shared.lock() {
                    txradar_tui::draw(f, &state);
                }
            });
            // Wait for the user to acknowledge.
            loop {
                match event::poll(Duration::from_millis(200)) {
                    Ok(true) => match event::read() {
                        Ok(Event::Key(k)) if matches!(k.code, KeyCode::Char('q') | KeyCode::Esc) => break,
                        Ok(_) => continue,
                        Err(_) => break, // stdin closed (e.g. EOF) — stop waiting
                    },
                    Ok(false) => continue,
                    Err(_) => break,
                }
            }
            break Ok(());
        }
    };

    let _ = txradar_tui::restore(&mut term);
    res?;

    // Join the loop task and report the outcome (now safely after restore).
    match handle.await {
        Ok(Ok(outcome)) => report_outcome(&outcome),
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(anyhow::anyhow!("agent loop task panicked: {e}")),
    }
    Ok(())
}
