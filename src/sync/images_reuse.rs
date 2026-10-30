//! Experiment-only whole-shard reuse. No production configuration surface.
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};

use super::images_meta::ImagesIndex;
use super::types::SyncError;

#[derive(Default)]
pub struct ReusedShard {
    pub complete: bool,
    pub files: BTreeSet<PathBuf>,
}

pub async fn copy_verified_shard(
    prior: &Path,
    staging: &Path,
    index: &ImagesIndex,
    shard_key: &str,
) -> Result<ReusedShard, SyncError> {
    let mut complete = true;
    let mut files = BTreeSet::new();
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
            tokio::fs::write(&destination, bytes).await?;
            files.insert(tokio::fs::canonicalize(destination).await?);
        }
    }
    Ok(ReusedShard {
        complete: complete && checked > 0,
        files,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn incomplete_shard_preserves_verified_files_during_extraction() {
        let root = std::env::temp_dir().join(format!(
            "reuse-partial-{}",
            super::super::primitives::random_suffix()
        ));
        let prior = root.join("prior");
        let staging = root.join("staging");
        std::fs::create_dir_all(&prior).unwrap();
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(prior.join("a.png"), b"verified").unwrap();
        let index = super::super::images_meta::parse_index(&serde_json::json!({
            "schemaVersion":"akdp-images/v1", "baselineVersion":"P", "currentVersion":"L", "shards":{},
            "artworks": {
                "a":{"shard":"chararts","large":{"file":"a.png","w":1,"h":1,"bytes":8,"sha256":format!("{:x}",Sha256::digest(b"verified"))}},
                "b":{"shard":"chararts","large":{"file":"b.png","w":1,"h":1,"bytes":3,"sha256":format!("{:x}",Sha256::digest(b"new"))}}
            }
        })).unwrap();
        let reused = copy_verified_shard(&prior, &staging, &index, "chararts-large")
            .await
            .unwrap();
        assert!(!reused.complete);
        assert_eq!(reused.files.len(), 1);
        let archive = root.join("shard.zip");
        let mut writer = zip::ZipWriter::new(std::fs::File::create(&archive).unwrap());
        for (name, bytes) in [
            ("a.png", b"older-baseline".as_slice()),
            ("b.png", b"new".as_slice()),
        ] {
            writer
                .start_file(name, zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(bytes).unwrap();
        }
        writer.finish().unwrap();
        super::super::activation::safe_extract_zip_preserving(&archive, &staging, &reused.files)
            .unwrap();
        assert_eq!(std::fs::read(staging.join("a.png")).unwrap(), b"verified");
        assert_eq!(std::fs::read(staging.join("b.png")).unwrap(), b"new");
        assert_eq!(std::fs::read(prior.join("a.png")).unwrap(), b"verified");
        #[cfg(unix)]
        {
            let alias = root.join("alias");
            std::os::unix::fs::symlink(&staging, &alias).unwrap();
            std::fs::remove_file(staging.join("b.png")).unwrap();
            super::super::activation::safe_extract_zip_preserving(&archive, &alias, &reused.files)
                .unwrap();
            assert_eq!(std::fs::read(staging.join("a.png")).unwrap(), b"verified");
            assert_eq!(std::fs::read(staging.join("b.png")).unwrap(), b"new");
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
