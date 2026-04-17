use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendKind {
    Qdrant,
    Pinecone,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    #[default]
    LocalOnly,
    LocalPlusS3,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default)]
    pub force_path_style: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SinkConfig {
    #[serde(default)]
    pub mode: SinkMode,
    pub local_output_root: PathBuf,
    pub s3: Option<S3Config>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,
    pub backend: BackendKind,
    pub upstream_base_url: String,
    #[serde(default = "default_sampling_rate")]
    pub sampling_rate: f64,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    pub sink: SinkConfig,
    #[serde(default = "default_rotation_max_bytes")]
    pub rotation_max_bytes: u64,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_flush_max_events")]
    pub flush_max_events: usize,
    #[serde(default = "default_upstream_request_timeout_ms")]
    pub upstream_request_timeout_ms: u64,
    #[serde(default = "default_include_query_representation")]
    pub include_query_representation: bool,
}

fn default_listen_addr() -> SocketAddr {
    "0.0.0.0:6333".parse().unwrap()
}

fn default_sampling_rate() -> f64 {
    0.1
}

fn default_queue_capacity() -> usize {
    8192
}

fn default_rotation_max_bytes() -> u64 {
    50 * 1024 * 1024 // 50 MB
}

fn default_flush_interval_ms() -> u64 {
    1000
}

fn default_flush_max_events() -> usize {
    500
}

fn default_upstream_request_timeout_ms() -> u64 {
    10_000
}

fn default_include_query_representation() -> bool {
    true
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !(0.0..=1.0).contains(&self.sampling_rate) {
            return Err("sampling_rate must be between 0.0 and 1.0".into());
        }
        if self.upstream_base_url.is_empty() {
            return Err("upstream_base_url must not be empty".into());
        }
        if self.sink.mode == SinkMode::LocalPlusS3 && self.sink.s3.is_none() {
            return Err("sink.s3 config is required when mode is local_plus_s3".into());
        }
        Ok(())
    }
}
