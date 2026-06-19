# TxRadar

**Mission control for transaction landing on Solana.**

TxRadar is a smart transaction stack that observes the network in real time
(Yellowstone/Geyser gRPC), submits **Jito bundles** during the correct leader
window, tracks each transaction across every commitment stage
(Submitted → Processed → Confirmed → Finalized), classifies failures, confirms
landing **from stream subscriptions** (not RPC polling), and uses an **AI agent**
to autonomously own its operational decisions — tip sizing, submission timing,
failure reasoning, and blockhash-expiry retry — with **visible reasoning**.

Built in Rust. Submission for the Superteam Nigeria *Advanced Infrastructure
Challenge — Build a Smart Transaction Stack*.

> Architecture document (judged separately): _link TBD_

## Why it stands out

Most submissions are a CLI that fires bundles and dumps JSON. TxRadar is an
operator-grade observability + autonomy layer: a live **radar TUI** showing the
slot stream, upcoming Jito leader windows, the live tip floor, a per-transaction
**latency waterfall**, and the agent's reasoning feed as decisions happen.

## Workspace layout

```
crates/
  txradar-types    shared domain model: lifecycle state machine, failure
                   taxonomy, log record schema, typed config
  txradar-log      append-only JSONL lifecycle logger (the graded deliverable)
  txradar-stream   Yellowstone gRPC: slot/leader/tx subs, reconnect, backpressure
  txradar-core     blockhash mgr, tx+bundle construction, Jito client, tracker
  txradar-tips     tip oracle (Jito tip-floor percentiles + EMA + slot conditions)
  txradar-agent    AI layer behind a swappable `Decider` trait (Gemini default)
  txradar-tui      ratatui radar dashboard
bin/
  txradar          orchestrator wiring the pipeline together
```

The **AI layer is cleanly separated from the core stack**: the agent decides
*policy* (tip, timing, retry); the core *executes* it deterministically.

## Setup

1. Install Rust (stable) and `protoc` (needed by the Yellowstone proto build).
2. `cp .env.example .env` and fill in: keypair path, Yellowstone `x-token`,
   `GEMINI_API_KEY` (free key at https://aistudio.google.com/app/apikey).
3. Pick a network with `TXRADAR_PROFILE` (`testnet` for dev, `mainnet` for the
   final graded run). Network is **pure config** — same code, different profile.
4. `cargo run` (loads `config/<profile>.toml`).

## The three README questions

These are answered from real observations once the stack has run; see
[docs/README-answers.md](docs/README-answers.md) (filled in during the run
campaign).

1. What the `processed_at → confirmed_at` delta says about network health.
2. Why never to fetch a blockhash at `finalized` commitment for a time-sensitive
   transaction.
3. What happens to a bundle if the Jito leader skips their slot.

## License

AGPL-3.0 (the Yellowstone gRPC client is AGPL-3.0; TxRadar matches it).
