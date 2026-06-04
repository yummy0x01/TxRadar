//! Strongly-typed configuration loaded from `config/<profile>.toml`.
//!
//! Network selection is *pure config*: the same code runs on testnet or mainnet
//! by swapping `TXRADAR_PROFILE`. Secrets (keypair path, gRPC x-token, API keys)
//! come from the environment and are merged in by the binary — never stored here.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Which Solana cluster we're pointed at. Drives explorer cross-referencing
/// and nothing in the transport layer — endpoints come from the profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Devnet,
    Testnet,
    Mainnet,
}

impl Network {
    pub fn as_str(self) -> &'static str {
        match self {
            Network::Devnet => "devnet",
            Network::Testnet => "testnet",
            Network::Mainnet => "mainnet",
        }
    }
}

/// Top-level config, mirroring the TOML profile structure.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub network: Network,
    pub rpc: RpcConfig,
    pub yellowstone: YellowstoneConfig,
    pub jito: JitoConfig,
    pub tips: TipsConfig,
    pub lifecycle: LifecycleConfig,
    pub agent: AgentConfig,
    pub log: LogConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    pub http_url: String,
    /// Commitment used when fetching blockhashes. Must NOT be `finalized` for
    /// time-sensitive sends (README Q2): a finalized hash is already stale.
    pub blockhash_commitment: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct YellowstoneConfig {
    pub endpoint: String,
    pub ping_interval_secs: u64,
    pub channel_capacity: usize,
    pub stream_window_bytes: usize,
    pub reconnect: ReconnectConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReconnectConfig {
    pub enabled: bool,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub replay_from_last_slot: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JitoConfig {
    pub block_engine_url: String,
    pub max_requests_per_sec: u32,
    pub tip_floor_rest: String,
    pub tip_floor_ws: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TipsConfig {
    pub min_lamports: u64,
    pub max_lamports: u64,
    pub ema_alpha: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleConfig {
    pub processed_timeout_secs: u64,
    pub confirmed_timeout_secs: u64,
    pub finalized_timeout_secs: u64,
    pub status_poll_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentConfig {
    pub provider: String,
    pub model: String,
    pub max_retries: u32,
    pub require_approval: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    pub path: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing config: {0}")]
    Parse(#[from] toml::de::Error),
}

impl Config {
    /// Load and parse a TOML profile from disk.
    pub fn from_path(path: &str) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_string(),
            source,
        })?;
        Ok(toml::from_str(&text)?)
    }

    /// Resolve the profile path for a profile name, e.g. `testnet` ->
    /// `config/testnet.toml`.
    pub fn path_for_profile(profile: &str) -> String {
        format!("config/{profile}.toml")
    }
}

// Convenience accessors that hand back `Duration`s instead of raw seconds.
impl LifecycleConfig {
    pub fn processed_timeout(&self) -> Duration {
        Duration::from_secs(self.processed_timeout_secs)
    }
    pub fn confirmed_timeout(&self) -> Duration {
        Duration::from_secs(self.confirmed_timeout_secs)
    }
    pub fn finalized_timeout(&self) -> Duration {
        Duration::from_secs(self.finalized_timeout_secs)
    }
    pub fn status_poll_interval(&self) -> Duration {
        Duration::from_secs(self.status_poll_interval_secs)
    }
}
