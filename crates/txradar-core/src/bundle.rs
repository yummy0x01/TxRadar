//! Transaction + Jito bundle construction (Phase 2).
//!
//! A bundle is up to 5 sequential, atomic transactions. We keep the tip
//! transfer (to a randomly selected Jito tip account) inside the *same*
//! transaction as the core logic so the tip isn't paid if the bundle fails
//! (uncled-block / skipped-slot safety), and we never route tip accounts
//! through Address Lookup Tables.

use base64::{engine::general_purpose::STANDARD, Engine};
use rand::seq::SliceRandom;
// `system_instruction` is deprecated in solana-sdk 2.2 in favor of the
// `solana-system-interface` crate, but the re-export still works and avoids an
// extra dependency for a single `transfer` call.
#[allow(deprecated)]
use solana_sdk::system_instruction;
use solana_sdk::{
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair},
    signer::Signer,
    transaction::Transaction,
};
use std::str::FromStr;

/// The 8 Jito tip accounts. We pick one at random per bundle to reduce
/// write-lock contention (per Jito guidance).
pub const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

/// SPL Memo v2 program — our "core" instruction is a memo so each bundle is
/// self-documenting and trivially verifiable on an explorer.
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

/// Minimum Jito tip per their rules (lamports).
pub const MIN_TIP_LAMPORTS: u64 = 1000;

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("loading keypair from {path}: {reason}")]
    Keypair { path: String, reason: String },
    #[error("invalid pubkey {0}")]
    Pubkey(String),
    #[error("serializing transaction: {0}")]
    Serialize(String),
    #[error("tip {got} below Jito minimum {min} lamports")]
    TipTooLow { got: u64, min: u64 },
}

/// A built, signed, encoded bundle ready for `sendBundle`, plus the metadata the
/// tracker needs to follow it.
#[derive(Debug, Clone)]
pub struct BuiltBundle {
    /// Base64-encoded, signed transactions (bundle order preserved).
    pub encoded_txs: Vec<String>,
    /// Base58 signatures, parallel to `encoded_txs`. The first is the one we
    /// watch on the stream for landing.
    pub signatures: Vec<String>,
    /// Which tip account this bundle paid.
    pub tip_account: String,
    /// Tip amount in lamports.
    pub tip_lamports: u64,
    /// Blockhash the bundle was signed against (base58).
    pub blockhash: String,
}

impl BuiltBundle {
    /// The signature we track for landing (first transaction).
    pub fn primary_signature(&self) -> Option<&str> {
        self.signatures.first().map(String::as_str)
    }
}

/// Load a signer keypair from a `solana-keygen` JSON file.
pub fn load_keypair(path: &str) -> Result<Keypair, BundleError> {
    read_keypair_file(path).map_err(|e| BundleError::Keypair {
        path: path.to_string(),
        reason: e.to_string(),
    })
}

/// Pick a random Jito tip account (reduces write-lock contention).
pub fn random_tip_account() -> Pubkey {
    let s = JITO_TIP_ACCOUNTS
        .choose(&mut rand::thread_rng())
        .copied()
        .unwrap_or(JITO_TIP_ACCOUNTS[0]);
    Pubkey::from_str(s).expect("hardcoded tip account is valid")
}

/// Build the SPL Memo instruction carrying `note`.
fn memo_instruction(note: &str) -> Result<Instruction, BundleError> {
    let program_id =
        Pubkey::from_str(MEMO_PROGRAM_ID).map_err(|_| BundleError::Pubkey(MEMO_PROGRAM_ID.into()))?;
    Ok(Instruction { program_id, accounts: vec![], data: note.as_bytes().to_vec() })
}

/// Build a single signed transaction: `[memo(note), tip transfer]`. The tip
/// lives in the same transaction as the core logic so it isn't paid on failure.
pub fn build_tip_transaction(
    payer: &Keypair,
    tip_account: &Pubkey,
    tip_lamports: u64,
    note: &str,
    blockhash: Hash,
) -> Result<Transaction, BundleError> {
    if tip_lamports < MIN_TIP_LAMPORTS {
        return Err(BundleError::TipTooLow { got: tip_lamports, min: MIN_TIP_LAMPORTS });
    }
    let instructions = vec![
        memo_instruction(note)?,
        system_instruction::transfer(&payer.pubkey(), tip_account, tip_lamports),
    ];
    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    Ok(tx)
}

/// Serialize a signed transaction to base64 (the encoding `sendBundle` expects).
pub fn encode_transaction(tx: &Transaction) -> Result<String, BundleError> {
    let bytes = bincode::serialize(tx).map_err(|e| BundleError::Serialize(e.to_string()))?;
    Ok(STANDARD.encode(bytes))
}

/// Build a single-transaction bundle (the common case): one tx carrying the
/// memo + tip. Returns everything the tracker needs.
pub fn build_single_tx_bundle(
    payer: &Keypair,
    tip_account: &Pubkey,
    tip_lamports: u64,
    note: &str,
    tracked_blockhash: &Hash,
    blockhash_str: &str,
) -> Result<BuiltBundle, BundleError> {
    let tx = build_tip_transaction(payer, tip_account, tip_lamports, note, *tracked_blockhash)?;
    let signature = tx
        .signatures
        .first()
        .map(|s| s.to_string())
        .ok_or_else(|| BundleError::Serialize("unsigned transaction".into()))?;
    let encoded = encode_transaction(&tx)?;
    Ok(BuiltBundle {
        encoded_txs: vec![encoded],
        signatures: vec![signature],
        tip_account: tip_account.to_string(),
        tip_lamports,
        blockhash: blockhash_str.to_string(),
    })
}

