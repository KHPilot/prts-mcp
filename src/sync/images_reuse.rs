//! Experiment-only whole-shard reuse. No production configuration surface.
use std::path::{Component, Path};

use sha2::{Digest, Sha256};

use super::images_meta::ImagesIndex;
use super::types::SyncError;

pub async fn copy_verified_shard(
    prior: &Path,
    staging: &Path,
    index: &ImagesIndex,
    shard_key: &str,
) -> Result<bool, SyncError> {
    let mut complete = true;
    let mut checked = 0;
    for entry in index.artworks.values() {
        for (name, variant) in &entry.variants {
            if format!("{}-{name}", entry.shard) != shard_key {
                continue;
            }
            checked += 1;
            let relative = Path::new(&variant.file);
            if relative
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(SyncError::Message("unsafe reuse path".into()));
            }
            let source = prior.join(relative);
            // Canonical containment also excludes parent-directory symlinks.
            let Ok(canonical) = tokio::fs::canonicalize(&source).await else {
                complete = false;
                continue;
            };
            if !canonical.starts_with(tokio::fs::canonicalize(prior).await?) {
                return Err(SyncError::Message(
                    "reuse path escapes prior generation".into(),
                ));
            }
            let bytes = tokio::fs::read(canonical).await?;
            if bytes.len() as f64 != variant.bytes
                || format!("{:x}", Sha256::digest(&bytes)) != variant.sha256
            {
                complete = false;
                continue;
            }
            let destination = staging.join(relative);
            if let Some(parent) = destination.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            // Write exactly the bytes that were checked; no hardlink lifetime coupling.
            tokio::fs::write(destination, bytes).await?;
        }
    }
    Ok(complete && checked > 0)
}
