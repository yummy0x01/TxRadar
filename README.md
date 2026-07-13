# TxRadar

TxRadar is a Rust transaction-infrastructure stack for Solana. It watches the
chain as it moves, builds tipped bundle-style transactions, submits them through
the fastest available path, tracks every commitment transition from the
Yellowstone/Geyser stream, and lets an AI agent make the retry and tip decisions
instead of burying those decisions in a script.

This was built for Superteam Nigeria's Advanced Infrastructure Challenge: Build
a Smart Transaction Stack.

The project is deliberately not a "send tx and poll RPC" demo. The interesting
part is the gap between broadcast and finality: blockhash freshness, leader
availability, TPU ingestion, skipped slots, votes reaching confirmed commitment,
and the failures that happen when a transaction never becomes competitive enough
to land.

## What Runs

TxRadar has one production command:

```powershell
cargo run -p txradar -- run --count 10 --starve 2
```

That command starts a Yellowstone/Geyser stream, watches the fee payer, refreshes
blockhashes at non-finalized commitment, asks the Gemini agent for each
submission decision, broadcasts the transaction, and records the lifecycle only
after the stream observes the relevant signature and slot commitments.

For the final mainnet campaign I ran:

```powershell
TXRADAR_PROFILE=mainnet
TXRADAR_BROADCAST=hybrid
cargo run -p txradar -- run --count 10 --starve 2
```

### Transport strategy: real bundles, resilient landing

TxRadar builds and submits **real Jito bundles** — `sendBundle` calls that return
real bundle IDs. You can see them directly in the lifecycle log: the
`bundle_id` fields (e.g. `5776b584…`) and the `bundle_failure` classifications
are the receipts of genuine block-engine submissions.

During the final campaign the public Jito block-engine endpoints were globally
saturated (they returned `Invalid`/rate-limit errors across all seven regions).
Rather than fake a landing or stop there, the stack does what a production system
does under a degraded dependency: it **falls back to a staked-connection path
(Helius Sender)** while keeping the same tip economics and — critically — still
confirming every landing from the live Yellowstone/Geyser stream, not from the
sender's response. The first two campaign entries are intentionally starved
direct-Jito `sendBundle` failures at 1,000 lamports; the landings then went
through the hybrid path.

This is deliberate real-world resilience, and the logs keep the distinction
explicit: a record is tagged either with a raw Jito `bundle_id` or with
`helius-sender:<signature>`. Infrastructure tradeoffs are part of the system, not
something to hide — and a stack that keeps landing when its preferred transport
degrades is stronger than one that only works on the happy path.

## Evidence

The curated mainnet lifecycle artifact is:

- `logs/curated/lifecycle-mainnet-2026-06-20.jsonl`
- `logs/curated/lifecycle-mainnet-2026-06-20.md`

That campaign produced 14 real mainnet submission records:

- 8 landed transactions
- 6 classified failures
- 8 landed slots: `427720092`, `427720154`, `427720219`, `427720283`,
  `427720340`, `427720890`, `427720954`, `427721028`

Each landed record contains:

- signature and blockhash
- slot number
- submitted, processed, confirmed, and finalized timestamps
- latency deltas between stages
- tip amount and the agent's rationale

Each failed record contains the terminal classification, for example
`bundle_failure` or `expired_blockhash`.

## Architecture

The code is split so the AI agent can decide policy without owning the chain
plumbing.

```text
bin/txradar
  loads config/secrets, starts stream ingest, runs the agent loop

crates/txradar-stream
  Yellowstone/Geyser slot and transaction subscriptions with reconnect/backpressure

crates/txradar-core
  blockhash manager, transaction and bundle construction, Jito client,
  Helius Sender fallback, Solana RPC helpers, lifecycle tracker

crates/txradar-tips
  live Jito tip-floor oracle, EMA smoothing, congestion-aware tip bands

crates/txradar-agent
  Gemini-backed Decider trait, decision schema, retry loop, heuristic fallback

crates/txradar-tui
  Radar dashboard showing stream state, tip bands, lifecycle rows, reasoning feed

crates/txradar-log and crates/txradar-types
  append-only JSONL writer and shared domain model
```

The local architecture writeup is in `docs/ARCHITECTURE.md`. The hosted version
for the bounty can mirror that document.

## AI Agent

The agent owns the operational decision, not just the wording around it. For
each attempt it receives current slot and block height, blockhash expiry state,
live tip-floor percentiles, smoothed tip history, recent skip rate, previous
failure class, previous tip, and retry budget.

It must return structured output:

- `Submit`
- `Hold`
- `RefreshAndResubmit`
- `Abort`

The runtime consults the agent before initial submission and after every
failure. The budget guard prevents any decider from retrying past the configured
limit, but the reason to refresh, tip up, or stop comes from the decision layer.

The demo command for the required autonomous blockhash-expiry recovery is:

```powershell
cargo run -p txradar -- demo-fault
```

That demo writes to `logs/lifecycle-demo.jsonl`, separate from the graded
mainnet log, so simulated evidence cannot pollute the real campaign artifact.

## Radar TUI

The same agent loop can run with a live dashboard:

```powershell
cargo run -p txradar -- demo-fault --tui
```

The TUI shows the connection state, current slot, tip band, lifecycle waterfall,
failure markers, and the agent's reasoning feed. In non-interactive shells it
falls back to the plain logged path instead of corrupting stdout.

## How To Evaluate (for judges)

You can verify TxRadar in three tiers, from zero-cost to a full live campaign.
Tiers 1 and 2 need **no credentials, no keypair, and no SOL**.

**Prerequisites**

- Rust stable (pinned via `rust-toolchain.toml`; `rustup` will select it
  automatically).
- A C toolchain/linker (the usual Rust prerequisite: `build-essential` on Linux,
  Xcode CLT on macOS, MSVC build tools on Windows).
- **No `protoc` install required** — the Yellowstone proto build is vendored
  under `vendor/` to use a prebuilt `protoc` binary per platform.
- On Windows/MSVC, OpenSSL is pinned to a prebuilt copy; on Linux/macOS the
  system OpenSSL is used. No manual OpenSSL setup is needed for Tiers 1–2.

**Tier 1 — stack + AI, zero cost (no credentials, no SOL):**

```bash
cargo test --workspace          # deterministic unit/integration suite
cargo check --workspace         # whole workspace compiles, nothing broadcast
cargo run -p txradar -- demo-fault        # AI autonomous retry + blockhash-expiry
                                          # fault injection: signs REAL txs, but
                                          # NO broadcast and NO SOL spent
cargo run -p txradar -- demo-fault --tui  # same run in the Radar dashboard
```

`demo-fault` is the requirement-4 demonstration: the agent detects the injected
expiry, reasons about the cause, refreshes the blockhash, recalculates the tip,
and resubmits — with no hardcoded retry flow. Set `GEMINI_API_KEY` to exercise
the real Gemini agent; without it the deterministic heuristic fallback runs so a
bare checkout still works.

**Tier 2 — verify the real evidence (no build needed):**

- Open `logs/curated/lifecycle-mainnet-2026-06-20.jsonl` and its `.md` summary.
- Cross-reference any of the 8 landed slots/signatures on a Solana explorer
  (e.g. slot `427720340`, which resolves with `err: null`). These are real
  mainnet transactions produced by this stack.

**Tier 3 — reproduce a live campaign (needs your own funded keypair + Yellowstone/RPC):**

```bash
export TXRADAR_PROFILE=mainnet
export TXRADAR_BROADCAST=hybrid
cargo run -p txradar -- run --count 10 --starve 2
```

See **Setup** below for the environment variables Tier 3 requires.

## Setup

1. Install Rust stable.
2. Copy `.env.example` to `.env.local`.
3. Set `TXRADAR_PROFILE` to `mainnet` for the final campaign.
4. Set a funded `TXRADAR_KEYPAIR_PATH`.
5. Set `TXRADAR_YELLOWSTONE_X_TOKEN` for SolInfra/Triton/compatible Geyser.
6. Set `TXRADAR_RPC_API_KEY` if your RPC endpoint requires one.
7. Set `GEMINI_API_KEY` for the real agent. Without it, TxRadar uses the
   deterministic heuristic fallback, which is useful for dry runs but not ideal
   for the final AI demonstration.

Optional mainnet fallback transport:

```powershell
TXRADAR_BROADCAST=hybrid
TXRADAR_HELIUS_SENDER_URL=https://sender.helius-rpc.com/fast
```

Secrets stay in ignored environment files, never in TOML profiles.

## Local Test Checklist

Use this sequence when you want to verify the project from a fresh terminal.

```powershell
# 1. Compile and run the deterministic test suite.
cargo test --workspace

# 2. Check the whole workspace without broadcasting anything.
cargo check --workspace

# 3. Prove the agent-driven blockhash-expiry recovery path.
# This is simulated: it signs real transactions but does not broadcast or spend SOL.
cargo run -p txradar -- demo-fault

# 4. Prove the Radar dashboard path.
# Press q or Esc to exit the TUI after the run completes.
cargo run -p txradar -- demo-fault --tui

# 5. Optional live campaign, only with a funded mainnet keypair and working
# Yellowstone/RPC credentials.
$env:TXRADAR_PROFILE="mainnet"
$env:TXRADAR_BROADCAST="hybrid"
cargo run -p txradar -- run --count 10 --starve 2
```

The demo log goes to `logs/lifecycle-demo.jsonl`. The graded mainnet evidence is
the curated log under `logs/curated/`.

## Troubleshooting

Most operational issues are infrastructure or environment issues rather than
Rust build failures. The stack fails loudly where possible.

| Symptom | Likely cause | Walkthrough |
| --- | --- | --- |
| `TXRADAR_KEYPAIR_PATH is not set` | The live `run` command needs a signer. | Copy `.env.example` to `.env.local`, set `TXRADAR_KEYPAIR_PATH`, and make sure the keypair has enough SOL for fees and tips. Use `demo-fault` if you only want a no-spend test. |
| `TXRADAR_YELLOWSTONE_X_TOKEN is not set` | Live landing proof requires a Geyser stream. | Add the SolInfra/Triton/compatible Yellowstone token to `.env.local`. The live campaign should not rely on RPC polling alone. |
| `insufficient balance` during preflight | The configured fee payer cannot cover worst-case retries. | Fund the fee-payer public key or lower the campaign size/tip ceiling for a small test. Mainnet campaigns spend real SOL only when transactions land. |
| Jito bundles return `Invalid`, `Failed`, or rate-limit errors | Public block-engine endpoints can be saturated or globally rate-limited. | Use `TXRADAR_BROADCAST=hybrid` with `TXRADAR_HELIUS_SENDER_URL=https://sender.helius-rpc.com/fast`, or use a paid/reliable Jito endpoint if available. Keep the failure records if they are real submissions; they are useful evidence. |
| Gemini returns `429` or `503` | Free-tier quota or a transient model-side error. | The client retries transient failures. If quota is exhausted, wait for quota reset or use another valid Gemini key. Without a key, TxRadar falls back to the deterministic decider for dry runs, but the final AI demonstration is stronger with Gemini enabled. |
| `error sending request` or Windows `os error 10013` | Local firewall, sandbox, or network permissions blocked outbound HTTPS. | Run from a normal terminal with network access and allow Rust/Cargo through the firewall. The command itself does not print secret values. |
| Tip floor fetch fails | Jito's public tip-floor API is unavailable or blocked. | TxRadar uses a conservative fallback floor so the demo can continue. For a final run, retry when the API is reachable so tip bands reflect live market data. |
| `BlockhashNotFound` or `expired_blockhash` | The transaction used a stale blockhash or missed its validity window. | This is an expected recoverable failure. The agent should refresh the blockhash, recalculate the tip, and resubmit within the retry budget. |
| TUI appears to hang after completion | The dashboard waits for an operator acknowledgement. | Press `q` or `Esc`. In non-interactive shells, prefer `cargo run -p txradar -- demo-fault` and inspect `logs/txradar-tui.log` only if `--tui` was used. |
| Windows `Access is denied` inside `target/` | A previous Cargo/Rust process or editor scanner locked a build artifact. | Stop old `cargo`, `rustc`, or `txradar` processes and rerun. If needed, set a temporary build directory: `$env:CARGO_TARGET_DIR="target-local-check"; cargo check --workspace`. |

## Build Artifacts

`target/` and other local target directories are intentionally gitignored. Rust rebuilds the binaries from `Cargo.toml`, `Cargo.lock`,
the source crates, and the vendored Yellowstone protobuf patch. Committing
target folders would only add machine-specific build cache and make the
repository much larger.

## Required README Questions

The bounty's three required questions are answered in
`docs/README-answers.md`, using observations from the real campaign.

## Current Phase Checklist

| Phase | Status |
| --- | --- |
| 0 - scaffold, config, logging, boot | Done |
| 1 - Yellowstone streaming | Done |
| 2 - blockhash manager, bundle build, Jito client | Done |
| 3 - lifecycle tracker and failure classifier | Done |
| 4 - live tip oracle | Done |
| 5 - Gemini agent layer | Done, re-tested before submission |
| 6 - autonomous fault injection demo | Done |
| 7 - Radar TUI | Done, tested headless and interactive fallback |
| 8 - mainnet campaign and deliverables | Done, with noted Helius fallback |

## License

AGPL-3.0.
