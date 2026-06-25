# TxRadar Architecture

TxRadar is organized as a live control loop rather than a single broadcast
script.

## Data Flow

1. `bin/txradar` loads `config/<profile>.toml` and ignored environment secrets.
2. `txradar-stream` opens a Yellowstone/Geyser subscription for slots and the
   fee payer's transactions.
3. `ingest.rs` feeds stream events into a shared `LifecycleTracker` and a
   `NetworkState` snapshot.
4. `LiveExecutor` builds the current `DecisionContext`: slot, block height,
   blockhash expiry, tip floor, skip rate, last failure, retry count.
5. `txradar-agent` asks Gemini for a structured decision.
6. `txradar-core` signs the transaction or bundle, broadcasts it through direct
   Jito or the configured Sender fallback, and opens a lifecycle record.
7. Stream events drive `Submitted -> Processed -> Confirmed -> Finalized`.
8. `txradar-log` appends the completed JSONL record.

## Components

- `BlockhashManager`: fetches non-finalized blockhashes, tracks
  `lastValidBlockHeight`, detects expiry, supports demo fault injection.
- `bundle.rs`: builds memo + tip transactions for direct Jito bundles and
  Helius Sender fallback transactions.
- `JitoClient`: submits `sendBundle`, polls inflight and settled bundle status.
- `SenderClient`: optional fallback for saturated public Jito endpoints.
- `TipOracle`: reads live Jito tip-floor percentiles and smooths tip history.
- `LifecycleTracker`: converts stream events into structured lifecycle records.
- `GeminiDecider`: owns the visible operational reasoning.
- `Radar TUI`: renders connection state, tip bands, lifecycle rows, and agent
  reasoning.

## Failure Handling

Failures are classified into a small taxonomy:

- expired blockhash
- fee/tip too low
- compute exceeded
- bundle failure
- unknown

The agent sees the failure class before making a retry decision. The executor
does not hardcode a retry sequence; it exposes facts and executes the chosen
action. A budget guard enforces the configured maximum retries.

## Infrastructure Decisions

- Mainnet was used for the graded campaign because judges can cross-reference
  slots and signatures on public explorers.
- Blockhashes are fetched at `confirmed`, never `finalized`, to preserve the
  validity window.
- Landing proof comes from Yellowstone/Geyser stream notifications, not only
  RPC polling.
- Direct Jito `sendBundle` remains the preferred transport. During the final
  campaign, public Jito block-engine access was globally saturated, so Helius
  Sender was used as a fallback for landed transactions while the direct Jito
  path produced the intentional starved failures.

## AI Responsibility

The AI agent owns autonomous retry and tip intelligence. It observes live
network state and prior outcomes, then chooses whether to submit, hold, refresh
and resubmit, or abort. Its rationale is saved in the lifecycle record and shown
in the TUI.
