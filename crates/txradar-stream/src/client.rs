//! The live Yellowstone gRPC client: connect, subscribe, keepalive, reconnect.
//!
//! Everything here is private to the crate; consumers only touch [`spawn`] and
//! the [`StreamHandle`] it returns.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::StreamExt;
// `SinkExt` brings `.send()` into scope for the subscribe sink.
use futures::sink::SinkExt;
use tracing::{debug, info, warn};

use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

use txradar_types::config::YellowstoneConfig;

use crate::{ConnectionState, SlotStatus, StreamEvent, StreamHandle};

/// Inputs the stream task needs that aren't in the TOML profile (the x-token is
/// a secret from the environment).
#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub yellowstone: YellowstoneConfig,
    pub x_token: Option<String>,
    /// Accounts whose transactions we want streamed (e.g. our signer + the
    /// Jito tip accounts). Empty = subscribe to slots only.
    pub watch_accounts: Vec<String>,
    /// Commitment we ask the server to gate slot notifications on. We still see
    /// every commitment transition; this just bounds the firehose.
    pub commitment: CommitmentLevel,
}

impl StreamConfig {
    /// Slots-only configuration (no transaction filter) at processed commitment
    /// so we observe the full processed→confirmed→finalized progression.
    pub fn slots_only(yellowstone: YellowstoneConfig, x_token: Option<String>) -> Self {
        Self {
            yellowstone,
            x_token,
            watch_accounts: Vec::new(),
            commitment: CommitmentLevel::Processed,
        }
    }
}

/// Spawn the streaming task and hand back a consumer handle.
///
/// The task runs until the receiver is dropped or it hits an unrecoverable
/// error. Transient disconnects are handled internally by the reconnect loop.
pub fn spawn(cfg: StreamConfig) -> StreamHandle {
    let (tx, rx) = mpsc::channel(cfg.yellowstone.channel_capacity.max(1));
    tokio::spawn(run(cfg, tx));
    StreamHandle { events: rx }
}

/// Top-level supervisor: connect, stream, and on transient failure back off and
/// reconnect (optionally replaying from the last slot we saw).
async fn run(cfg: StreamConfig, tx: mpsc::Sender<StreamEvent>) {
    let reconnect = &cfg.yellowstone.reconnect;
    let mut backoff_ms = reconnect.initial_backoff_ms.max(1);
    // Last slot we observed — used as `from_slot` on reconnect to replay the gap.
    let mut last_slot: Option<u64> = None;

    loop {
        let _ = tx.send(StreamEvent::Connection(ConnectionState::Connecting)).await;

        let from_slot = if reconnect.replay_from_last_slot { last_slot } else { None };
        match connect_and_stream(&cfg, &tx, from_slot, &mut last_slot).await {
            Ok(()) => {
                // Clean end-of-stream (e.g. server closed). Treat as reconnectable
                // unless the consumer has gone away.
                if tx.is_closed() {
                    info!(target: "txradar::stream", "consumer dropped; stream task exiting");
                    return;
                }
                warn!(target: "txradar::stream", "stream ended; will reconnect");
            }
            Err(e) => {
                if tx.is_closed() {
                    return;
                }
                // Auth / permission / balance failures won't fix themselves on
                // retry — surface clearly and stop instead of spin-reconnecting.
                if is_fatal(&e) {
                    warn!(target: "txradar::stream", error = %e, "fatal stream error; not retrying");
                    let _ = tx.send(StreamEvent::Connection(ConnectionState::Disconnected)).await;
                    return;
                }
                warn!(target: "txradar::stream", error = %e, "stream error; will reconnect");
            }
        }

        if !reconnect.enabled {
            let _ = tx.send(StreamEvent::Connection(ConnectionState::Disconnected)).await;
            info!(target: "txradar::stream", "reconnect disabled; stream task exiting");
            return;
        }

        let _ = tx.send(StreamEvent::Connection(ConnectionState::Reconnecting)).await;
        debug!(target: "txradar::stream", backoff_ms, ?from_slot, "backing off before reconnect");
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        // Exponential backoff, capped.
        backoff_ms = (backoff_ms.saturating_mul(2)).min(reconnect.max_backoff_ms.max(backoff_ms));
    }
}

/// Whether an error is permanent (no point reconnecting). Covers auth, quota,
/// and balance rejections — e.g. SolInfra's "insufficient balance: minimum
/// $0.06 required to start a PAYG stream".
fn is_fatal(e: &crate::StreamError) -> bool {
    let msg = e.to_string();
    msg.contains("PermissionDenied")
        || msg.contains("Unauthenticated")
        || msg.contains("insufficient balance")
        || (msg.contains("invalid") && msg.contains("token"))
}

/// One connection lifecycle: build the client, subscribe, then pump updates
/// until the stream ends or errors. Resets the caller's backoff on a successful
/// connect by returning normally on clean end.
async fn connect_and_stream(
    cfg: &StreamConfig,
    tx: &mpsc::Sender<StreamEvent>,
    from_slot: Option<u64>,
    last_slot: &mut Option<u64>,
) -> Result<(), crate::StreamError> {
    use crate::StreamError;

    let y = &cfg.yellowstone;

    // --- Build + connect -----------------------------------------------------
    // TLS with the system root store; the endpoint must be https for this.
    let tls = ClientTlsConfig::new().with_native_roots();

    let mut client = GeyserGrpcClient::build_from_shared(y.endpoint.clone())
        .map_err(|e| StreamError::Connect(e.to_string()))?
        .x_token(cfg.x_token.clone())
        .map_err(|e| StreamError::Connect(e.to_string()))?
        .tls_config(tls)
        .map_err(|e| StreamError::Connect(e.to_string()))?
        // Backpressure / flow control: bound the gRPC window so the server
        // can't outrun our bounded channel without TCP-level slowing.
        .initial_stream_window_size(Some(y.stream_window_bytes as u32))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| StreamError::Connect(e.to_string()))?;

    info!(target: "txradar::stream", endpoint = %y.endpoint, ?from_slot, "connected to Yellowstone");

    // --- Subscribe -----------------------------------------------------------
    let request = build_request(cfg, from_slot);
    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| StreamError::Subscribe(e.to_string()))?;

    let _ = tx.send(StreamEvent::Connection(ConnectionState::Connected)).await;

    // --- Keepalive ping ------------------------------------------------------
    // The server pings ~every 15s; we also proactively ping on our own cadence
    // so load balancers expecting client traffic keep the pipe open.
    let mut ping_timer = interval(Duration::from_secs(y.ping_interval_secs.max(1)));
    ping_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping_id: i32 = 0;

    // --- Pump ----------------------------------------------------------------
    loop {
        tokio::select! {
            // Our own keepalive cadence.
            _ = ping_timer.tick() => {
                ping_id = ping_id.wrapping_add(1);
                if let Err(e) = subscribe_tx
                    .send(SubscribeRequest { ping: Some(SubscribeRequestPing { id: ping_id }), ..Default::default() })
                    .await
                {
                    return Err(StreamError::Stream(format!("keepalive send failed: {e}")));
                }
                debug!(target: "txradar::stream", ping_id, "sent keepalive ping");
            }

            // Inbound updates.
            msg = stream.next() => {
                let Some(msg) = msg else {
                    // Stream closed cleanly.
                    return Ok(());
                };
                let update = msg.map_err(|e| StreamError::Stream(e.to_string()))?;

                match update.update_oneof {
                    Some(UpdateOneof::Slot(slot)) => {
                        let status = map_commitment(slot.status);
                        *last_slot = Some(slot.slot);
                        // If the consumer is gone, stop the whole task.
                        if tx
                            .send(StreamEvent::SlotStatus { slot: slot.slot, parent: slot.parent, status })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(UpdateOneof::Transaction(txn)) => {
                        if let Some((signature, failed)) = decode_tx(&txn) {
                            if tx
                                .send(StreamEvent::Transaction { signature, slot: txn.slot, failed })
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }
                    // Server asked us to prove liveness — reply immediately.
                    Some(UpdateOneof::Ping(_)) => {
                        if let Err(e) = subscribe_tx
                            .send(SubscribeRequest { ping: Some(SubscribeRequestPing { id: ping_id }), ..Default::default() })
                            .await
                        {
                            return Err(StreamError::Stream(format!("pong send failed: {e}")));
                        }
                    }
                    Some(UpdateOneof::Pong(_)) => { /* keepalive acknowledged */ }
                    // We don't subscribe to these, but be permissive.
                    _ => {}
                }
            }
        }
    }
}

/// Build the subscription request from our config.
fn build_request(cfg: &StreamConfig, from_slot: Option<u64>) -> SubscribeRequest {
    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    slots.insert(
        "txradar".to_string(),
        SubscribeRequestFilterSlots {
            // We want the full commitment progression, so don't gate on one level.
            filter_by_commitment: Some(false),
            ..Default::default()
        },
    );

    let mut transactions: HashMap<String, SubscribeRequestFilterTransactions> = HashMap::new();
    if !cfg.watch_accounts.is_empty() {
        transactions.insert(
            "txradar".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: None, // include both succeeded and failed (we classify failures)
                signature: None,
                account_include: cfg.watch_accounts.clone(),
                account_exclude: Vec::new(),
                account_required: Vec::new(),
            },
        );
    }

    SubscribeRequest {
        slots,
        transactions,
        commitment: Some(cfg.commitment as i32),
        from_slot,
        ..Default::default()
    }
}

/// Map Yellowstone's `CommitmentLevel` (carried in `SubscribeUpdateSlot.status`
/// as an i32) to our [`SlotStatus`].
fn map_commitment(status: i32) -> SlotStatus {
    match CommitmentLevel::try_from(status) {
        Ok(CommitmentLevel::Processed) => SlotStatus::Processed,
        Ok(CommitmentLevel::Confirmed) => SlotStatus::Confirmed,
        Ok(CommitmentLevel::Finalized) => SlotStatus::Finalized,
        // 3.1.1 only carries the three commitment levels here; anything else is
        // treated as a processed-level observation.
        _ => SlotStatus::Processed,
    }
}

/// Extract `(base58 signature, failed?)` from a transaction update, if present.
fn decode_tx(
    txn: &yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
) -> Option<(String, bool)> {
    let info = txn.transaction.as_ref()?;
    let signature = bs58_encode(&info.signature);
    // `meta.err` present => the transaction failed.
    let failed = info.meta.as_ref().map(|m| m.err.is_some()).unwrap_or(false);
    Some((signature, failed))
}

/// Minimal base58 (Bitcoin alphabet) encoder for signatures, avoiding an extra
/// crate dependency in this layer.
fn bs58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    if bytes.is_empty() {
        return String::new();
    }
    // Count leading zero bytes (encode as leading '1's).
    let leading_zeros = bytes.iter().take_while(|&&b| b == 0).count();
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len() * 138 / 100 + 1);
    for &byte in bytes {
        let mut carry = byte as u32;
        for d in digits.iter_mut() {
            carry += (*d as u32) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(leading_zeros + digits.len());
    for _ in 0..leading_zeros {
        out.push('1');
    }
    for &d in digits.iter().rev() {
        out.push(ALPHABET[d as usize] as char);
    }
    out
}
