//! Explicit experimental v2 channel. Never selected by production sync.
use super::{activation, discovery, images_meta, transport::Transport};
use crate::config::DataSource;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Clone, Deserialize, PartialEq)]
struct Member {
    bytes: u64,
    sha256: String,
    group: String,
}
#[derive(Clone, Deserialize, PartialEq)]
struct Part {
    file: String,
    bytes: u64,
    sha256: String,
    group: String,
    members: BTreeMap<String, Member>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    schema_version: String,
    baseline_version: String,
    current_version: String,
    previous_version: Option<String>,
    baseline_parts: Vec<Part>,
    delta_parts: Vec<Part>,
}
fn flat(s: &str) -> Result<(), String> {
    if s.is_empty() || s == "." || s == ".." || s.contains(['/', '\\']) {
        return Err("unsafe filename".into());
    }
    Ok(())
}
fn checked(path: &Path, bytes: u64, sha: &str) -> bool {
    use std::io::Read;
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let mut h = Sha256::new();
    let mut total = 0;
    let mut buf = [0u8; 65536];
    loop {
        match f.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                total += n as u64;
                h.update(&buf[..n]);
            }
            Err(_) => return false,
        }
    }
    total == bytes && format!("{:x}", h.finalize()) == sha
}
async fn fetch(t: &dyn Transport, url: &str, path: &Path) -> Result<(), String> {
    for attempt in 0..3 {
        match t.download_file(url, path, 900_000).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt == 2 => return Err(e.to_string()),
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_secs(if attempt == 0 {
                    5
                } else {
                    20
                }))
                .await
            }
        }
    }
    unreachable!()
}
async fn read_index(
    t: &dyn Transport,
    source: &DataSource,
    tag: &str,
    root: &Path,
) -> Result<(Value, Index), String> {
    flat(tag)?;
    let tmp = root.join(".index.tmp");
    fetch(t, &source.download_url(tag, "index.json"), &tmp).await?;
    let value: Value = serde_json::from_slice(&std::fs::read(&tmp).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let index: Index = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    if index.schema_version != "akdp-images/v2"
        || tag != format!("images2-{}", index.current_version)
    {
        return Err("v2 index identity mismatch".into());
    }
    flat(&index.current_version)?;
    flat(&index.baseline_version)?;
    Ok((value, index))
}
pub async fn sync(
    root: &Path,
    t: &dyn Transport,
    source: &DataSource,
    original: bool,
    reuse: bool,
) -> Result<String, String> {
    if source.is_default() {
        return Err("experiment requires custom source".into());
    }
    super::source_guard::ensure(root, source)?;
    std::fs::create_dir_all(root).map_err(|e| e.to_string())?;
    activation::with_archive_activation_lock(
        root,
        ".images.lock",
        activation::LockTiming::default(),
        || async { run(root, t, source, original, reuse).await },
    )
    .await
    .map_err(|e| e.to_string())?
}
async fn run(
    root: &Path,
    t: &dyn Transport,
    source: &DataSource,
    original: bool,
    reuse: bool,
) -> Result<String, String> {
    super::source_guard::ensure(root, source)?;
    cleanup_staging(&root.join(".releases"))?;
    let releases = discovery::list_releases(t, source.owner(), source.repo(), 10_000)
        .await
        .ok_or("release discovery failed")?;
    let latest = releases
        .iter()
        .filter(|r| r["draft"].as_bool() != Some(true) && r["prerelease"].as_bool() != Some(true))
        .filter_map(|r| r["tag_name"].as_str())
        .filter(|s| s.starts_with("images2-") && !s.starts_with("images2-baseline-"))
        .max()
        .ok_or("no v2 release")?;
    let (value, index) = read_index(t, source, latest, root).await?;
    let keys = super::images_sync::needed_shard_keys(original);
    let mut normalized = value.clone();
    normalized["schemaVersion"] = json!("akdp-images/v1");
    let target = images_meta::parse_index(&normalized).ok_or("bad artworks")?;
    let mut expected = BTreeMap::new();
    for entry in target.artworks.values() {
        for (tier, v) in &entry.variants {
            if !original && tier == "original" {
                continue;
            }
            flat(&v.file)?;
            if expected
                .insert(v.file.clone(), (v.bytes as u64, v.sha256.clone()))
                .is_some()
            {
                return Err("duplicate target filename".into());
            }
        }
    }
    if expected.is_empty() {
        return Err("empty target".into());
    }
    let mut deltas = vec![(index.current_version.clone(), index.delta_parts.clone())];
    let mut previous = index.previous_version.clone();
    let mut newer = index.current_version.clone();
    while newer != index.baseline_version {
        let version = previous.take().ok_or("broken predecessor chain")?;
        if version >= newer || version < index.baseline_version {
            return Err("invalid predecessor order".into());
        }
        let (_, prior) = read_index(t, source, &format!("images2-{version}"), root).await?;
        if prior.baseline_version != index.baseline_version
            || prior.baseline_parts != index.baseline_parts
        {
            return Err("baseline drift".into());
        }
        deltas.push((version.clone(), prior.delta_parts));
        previous = prior.previous_version;
        newer = version;
    }
    let rel = root.join(".releases");
    std::fs::create_dir_all(&rel).map_err(|e| e.to_string())?;
    let name = format!("v2-{}", super::primitives::random_suffix());
    let stage = rel.join(format!(".{name}.tmp"));
    std::fs::create_dir(&stage).map_err(|e| e.to_string())?;
    let result = async {
        let prior = images_meta::active_generation(root);
        let mut preserved = BTreeSet::new();
        if reuse {
            if let Some(prior) = prior {
                let base = std::fs::canonicalize(&prior).map_err(|e| e.to_string())?;
                for (name, (bytes, hash)) in &expected {
                    let path = prior.join(name);
                    if let Ok(real) = path.canonicalize() {
                        if real.starts_with(&base) && checked(&real, *bytes, hash) {
                            std::fs::copy(&real, stage.join(name)).map_err(|e| e.to_string())?;
                            preserved.insert(name.clone());
                        }
                    }
                }
            }
        }
        let cache = root.join(".v2-cache");
        std::fs::create_dir_all(&cache).map_err(|e| e.to_string())?;
        let mut work = vec![(
            format!("images2-baseline-{}", index.baseline_version),
            index.baseline_parts.clone(),
        )];
        for (version, parts) in deltas.into_iter().rev() {
            work.push((format!("images2-{version}"), parts));
        }
        for (tag, parts) in work {
            for part in parts {
                if !keys.contains(&part.group) {
                    continue;
                }
                flat(&part.file)?;
                if part.sha256.len() != 64 || !part.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err("bad part digest".into());
                }
                for name in part.members.keys() {
                    flat(name)?;
                }
                if part
                    .members
                    .keys()
                    .all(|n| preserved.contains(n) || !expected.contains_key(n))
                {
                    continue;
                }
                let archive = cache.join(&part.sha256);
                if !checked(&archive, part.bytes, &part.sha256) {
                    let tmp = cache.join(format!("{}.tmp", part.sha256));
                    fetch(t, &source.download_url(&tag, &part.file), &tmp).await?;
                    if !checked(&tmp, part.bytes, &part.sha256) {
                        return Err("ZIP digest mismatch".into());
                    }
                    std::fs::rename(tmp, &archive).map_err(|e| e.to_string())?;
                }
                let mut zip =
                    zip::ZipArchive::new(std::fs::File::open(&archive).map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                let mut seen = BTreeSet::new();
                for i in 0..zip.len() {
                    use std::io::Read;
                    let mut entry = zip.by_index(i).map_err(|e| e.to_string())?;
                    let name = entry.name().to_owned();
                    let member = part.members.get(&name).ok_or("unexpected ZIP member")?;
                    if !seen.insert(name.clone()) {
                        return Err("duplicate ZIP member".into());
                    }
                    let mut hash = Sha256::new();
                    let mut count = 0;
                    let mut buf = [0u8; 65536];
                    let mut out = if preserved.contains(&name) || !expected.contains_key(&name) {
                        None
                    } else {
                        Some(std::fs::File::create(stage.join(&name)).map_err(|e| e.to_string())?)
                    };
                    loop {
                        let n = entry.read(&mut buf).map_err(|e| e.to_string())?;
                        if n == 0 {
                            break;
                        }
                        hash.update(&buf[..n]);
                        count += n as u64;
                        if let Some(f) = &mut out {
                            use std::io::Write;
                            f.write_all(&buf[..n]).map_err(|e| e.to_string())?;
                        }
                    }
                    if count != member.bytes || format!("{:x}", hash.finalize()) != member.sha256 {
                        return Err("member digest mismatch".into());
                    }
                }
                if seen.len() != part.members.len() {
                    return Err("missing ZIP member".into());
                }
            }
        }
        for (name, (bytes, hash)) in &expected {
            if !checked(&stage.join(name), *bytes, hash) {
                return Err(format!("target mismatch: {name}"));
            }
        }
        std::fs::write(
            stage.join("index.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .map_err(|e| e.to_string())?;
        let meta =
            images_meta::generation_meta(&index.baseline_version, &index.current_version, &keys);
        images_meta::save_meta(&stage, &meta).map_err(|e| e.to_string())?;
        std::fs::rename(&stage, rel.join(&name)).map_err(|e| e.to_string())?;
        let mut meta = meta;
        meta["generation_root"] = json!(format!(".releases/{name}"));
        images_meta::save_meta(root, &meta).map_err(|e| e.to_string())?;
        // Keep failed-transfer cache for resume, release it only after activation.
        if let Err(error) = std::fs::remove_dir_all(&cache) {
            eprintln!("[sharding-lab] cache cleanup: {error}");
        }
        super::primitives::prune_old_trees(
            &rel,
            &std::collections::HashSet::from([rel.join(&name)]),
            images_meta::RETENTION_MS as u128,
            true,
        );
        Ok(index.current_version.clone())
    }
    .await;
    if stage.exists() {
        let _ = std::fs::remove_dir_all(stage);
    }
    result
}

fn cleanup_staging(root: &Path) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }
    for item in std::fs::read_dir(root).map_err(|e| e.to_string())? {
        let item = item.map_err(|e| e.to_string())?;
        let name = item.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(".v2-") && name.ends_with(".tmp") {
            let kind = item.file_type().map_err(|e| e.to_string())?;
            if !kind.is_dir() {
                return Err("unexpected staging entry type".into());
            }
            std::fs::remove_dir_all(item.path()).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    #[test]
    fn abandoned_staging_cleanup_is_owned_and_repeatable() {
        let root = std::env::temp_dir().join(format!(
            "sharding-cleanup-{}",
            crate::sync::primitives::random_suffix()
        ));
        for name in [".v2-abandoned.tmp", "v2-active", ".other.tmp"] {
            std::fs::create_dir_all(root.join(name)).unwrap();
        }
        super::cleanup_staging(&root).unwrap();
        super::cleanup_staging(&root).unwrap();
        assert!(!root.join(".v2-abandoned.tmp").exists());
        assert!(root.join("v2-active").exists());
        assert!(root.join(".other.tmp").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
