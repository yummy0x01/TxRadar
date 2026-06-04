//! Structured JSONL lifecycle logger — appends one [`BundleRecord`] per line.
//!
//! This is the file judges read and cross-reference against Solana explorers,
//! so writes are append-only, flushed, and never lose a completed record.

use std::path::{Path, PathBuf};

use txradar_types::BundleRecord;

/// Append-only writer for the lifecycle log.
///
/// Phase 3 wires this to the lifecycle tracker. For now it defines the surface:
/// open a path, append a record as a JSON line, flush.
pub struct LifecycleLog {
    path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("io error on lifecycle log: {0}")]
    Io(#[from] std::io::Error),
    #[error("serializing record: {0}")]
    Serialize(#[from] serde_json::Error),
}

impl LifecycleLog {
    /// Open (creating parent dirs if needed) the JSONL log at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(Self { path })
    }

    /// Append one record as a single JSON line.
    pub async fn append(&self, record: &BundleRecord) -> Result<(), LogError> {
        use tokio::io::AsyncWriteExt;
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }
}
