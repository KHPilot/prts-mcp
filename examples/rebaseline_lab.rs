//! Offline experiment adapter. Arguments: fixture.json image-dir [original].
//! Fixture: {"releases": [...], "assets": {"https://...": "relative/path"}}.
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use prts_mcp_rust::sync::images_sync::{sync_images, ImagesSyncOpts};
use prts_mcp_rust::sync::transport::{DownloadStats, Transport, TransportError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

struct Replay {
    fixture: Value,
    root: PathBuf,
    bytes: AtomicU64,
    requests: AtomicU64,
}

#[async_trait]
impl Transport for Replay {
    async fn fetch_json(&self, url: &str, _: u64) -> Result<Value, TransportError> {
        if url.starts_with("https://api.github.com/") {
            return Ok(self.fixture["releases"].clone());
        }
        Err(TransportError::AssetNotFound(url.into()))
    }
    async fn download(
        &self,
        url: &str,
        dest: &Path,
        _: u64,
    ) -> Result<DownloadStats, TransportError> {
        let relative = self.fixture["assets"][url]
            .as_str()
            .ok_or_else(|| TransportError::AssetNotFound(url.into()))?;
        let path = self
            .root
            .join(relative)
            .canonicalize()
            .map_err(|e| TransportError::Network(e.to_string()))?;
        if !path.starts_with(
            self.root
                .canonicalize()
                .map_err(|e| TransportError::Network(e.to_string()))?,
        ) {
            return Err(TransportError::Network("fixture asset escapes root".into()));
        }
        let mut input =
            std::fs::File::open(path).map_err(|e| TransportError::Network(e.to_string()))?;
        let mut output =
            std::fs::File::create(dest).map_err(|e| TransportError::Network(e.to_string()))?;
        use std::io::{Read, Write};
        let mut buffer = vec![0; 1024 * 1024];
        let mut hash = Sha256::new();
        let mut bytes = 0;
        loop {
            let size = input
                .read(&mut buffer)
                .map_err(|e| TransportError::Network(e.to_string()))?;
            if size == 0 {
                break;
            }
            output
                .write_all(&buffer[..size])
                .map_err(|e| TransportError::Network(e.to_string()))?;
            hash.update(&buffer[..size]);
            bytes += size as u64;
        }
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.requests.fetch_add(1, Ordering::Relaxed);
        Ok(DownloadStats {
            bytes,
            sha256: format!("{:x}", hash.finalize()),
        })
    }
    fn mirrors(&self) -> Vec<String> {
        vec![]
    }
    fn github_headers(&self) -> Vec<(String, String)> {
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: rebaseline_lab fixture.json image-dir [original]".into());
    }
    let fixture = Path::new(&args[1]).canonicalize()?;
    let replay = Replay {
        fixture: serde_json::from_slice(&std::fs::read(&fixture)?)?,
        root: fixture.parent().unwrap().to_path_buf(),
        bytes: AtomicU64::new(0),
        requests: AtomicU64::new(0),
    };
    let start = std::time::Instant::now();
    let result = sync_images(
        Path::new(&args[2]),
        &replay,
        ImagesSyncOpts {
            include_original: args.get(3).is_some_and(|x| x == "original"),
            force_check: true,
        },
    )
    .await;
    println!(
        "{}",
        json!({"status":result.status.as_str(),"version":result.commit_sha,"error":result.error,"download_bytes":replay.bytes.load(Ordering::Relaxed),"downloads":replay.requests.load(Ordering::Relaxed),"seconds":start.elapsed().as_secs_f64(),"reuse":cfg!(feature="image-reuse-lab")})
    );
    if !matches!(result.status.as_str(), "updated" | "up_to_date") {
        std::process::exit(1);
    }
    Ok(())
}
