//! Offline experiment adapter. Arguments: fixture.json image-dir [original].
//! Fixture: {"releases": [...], "assets": {"https://...": "relative/path"}}.
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use prts_mcp_rust::config::DataSource;
use prts_mcp_rust::sync::images_sync::{sync_images_with_source, ImagesSyncOpts};
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
        if relative.starts_with("http://127.0.0.1:") {
            let endpoint = reqwest::Url::parse(relative)
                .map_err(|e| TransportError::Network(e.to_string()))?;
            if endpoint.host_str() != Some("127.0.0.1") || !endpoint.username().is_empty() {
                return Err(TransportError::Network(
                    "replay HTTP must be loopback".into(),
                ));
            }
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(900))
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| TransportError::Network(e.to_string()))?;
            let mut response = client
                .get(relative)
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| TransportError::Network(e.to_string()))?;
            let mut output = tokio::fs::File::create(dest)
                .await
                .map_err(|e| TransportError::Network(e.to_string()))?;
            use tokio::io::AsyncWriteExt;
            let mut hash = Sha256::new();
            let mut bytes = 0;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| TransportError::Network(e.to_string()))?
            {
                output
                    .write_all(&chunk)
                    .await
                    .map_err(|e| TransportError::Network(e.to_string()))?;
                hash.update(&chunk);
                bytes += chunk.len() as u64;
            }
            output
                .flush()
                .await
                .map_err(|e| TransportError::Network(e.to_string()))?;
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
            self.requests.fetch_add(1, Ordering::Relaxed);
            return Ok(DownloadStats {
                bytes,
                sha256: format!("{:x}", hash.finalize()),
            });
        }
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
    let source = DataSource::parse(Some(
        replay.fixture["source"]
            .as_str()
            .ok_or("fixture requires source owner/repo")?,
    ))?;
    if source.is_default() {
        return Err("experiment requires custom source".into());
    }
    let result = sync_images_with_source(
        Path::new(&args[2]),
        &replay,
        ImagesSyncOpts {
            include_original: args.get(3).is_some_and(|x| x == "original"),
            force_check: true,
        },
        &source,
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
