use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::S3Config;

// ---------------------------------------------------------------------------
// Upload job
// ---------------------------------------------------------------------------

pub struct UploadJob {
    pub local_path: PathBuf,
    pub remote_key: String,
    pub is_meta: bool,
}

// ---------------------------------------------------------------------------
// Per-session upload state
// ---------------------------------------------------------------------------

/// Created fresh at each session start. Each UploadJob implicitly belongs to
/// the session that spawned the uploader task — no cross-session pollution.
pub struct SessionUploadState {
    /// Jobs enqueued but not yet received by the uploader.
    pub queued: AtomicU64,
    /// Jobs currently being uploaded.
    pub in_flight: AtomicU64,
    /// Successfully uploaded files.
    pub uploaded_files: AtomicU64,
    /// Files that failed to upload.
    pub upload_failures: AtomicU64,
    /// Most recent upload error message (None if no failure yet).
    pub last_error: tokio::sync::Mutex<Option<String>>,
    /// Set to false when the uploader task exits.
    pub uploader_alive: AtomicBool,
}

impl SessionUploadState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            queued: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            uploaded_files: AtomicU64::new(0),
            upload_failures: AtomicU64::new(0),
            last_error: tokio::sync::Mutex::new(None),
            uploader_alive: AtomicBool::new(true),
        })
    }

    /// Compute upload_status string from current counters.
    pub fn upload_status(&self) -> &'static str {
        let ok = self.uploaded_files.load(Ordering::Relaxed);
        let fail = self.upload_failures.load(Ordering::Relaxed);
        match (ok, fail) {
            (0, 0) => "pending",
            (_, 0) => "ok",
            (0, _) => "all_failed",
            _ => "partial_failed",
        }
    }
}

impl Default for SessionUploadState {
    fn default() -> Self {
        Self {
            queued: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            uploaded_files: AtomicU64::new(0),
            upload_failures: AtomicU64::new(0),
            last_error: tokio::sync::Mutex::new(None),
            uploader_alive: AtomicBool::new(true),
        }
    }
}

// ---------------------------------------------------------------------------
// Uploader task
// ---------------------------------------------------------------------------

/// Background task that processes upload jobs one at a time.
///
/// Exits when `rx` is drained and all senders are dropped, OR when
/// `cancel` is triggered (in which case remaining buffered jobs are
/// attempted best-effort before exiting).
pub async fn uploader_task(
    mut rx: mpsc::Receiver<UploadJob>,
    s3_config: S3Config,
    session_state: Arc<SessionUploadState>,
    cancel: CancellationToken,
) {
    let client = build_s3_client(&s3_config);
    let bucket = s3_config.bucket.clone();

    tracing::info!(bucket = %bucket, "uploader task started");

    loop {
        tokio::select! {
            biased;
            job = rx.recv() => {
                match job {
                    Some(job) => {
                        session_state.queued.fetch_sub(1, Ordering::Relaxed);
                        session_state.in_flight.fetch_add(1, Ordering::Relaxed);
                        upload_file(&client, &bucket, &job, &session_state).await;
                        session_state.in_flight.fetch_sub(1, Ordering::Relaxed);
                    }
                    None => {
                        // All senders dropped and channel drained — normal exit.
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => {
                // Process remaining buffered jobs best-effort, then stop.
                while let Ok(job) = rx.try_recv() {
                    session_state.queued.fetch_sub(1, Ordering::Relaxed);
                    session_state.in_flight.fetch_add(1, Ordering::Relaxed);
                    upload_file(&client, &bucket, &job, &session_state).await;
                    session_state.in_flight.fetch_sub(1, Ordering::Relaxed);
                }
                break;
            }
        }
    }

    session_state.uploader_alive.store(false, Ordering::Relaxed);
    tracing::info!("uploader task stopped");
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

async fn upload_file(client: &S3Client, bucket: &str, job: &UploadJob, state: &SessionUploadState) {
    let content = match tokio::fs::read(&job.local_path).await {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("read error for {}: {}", job.local_path.display(), e);
            tracing::error!("{}", msg);
            state.upload_failures.fetch_add(1, Ordering::Relaxed);
            *state.last_error.lock().await = Some(msg);
            return;
        }
    };

    let body = ByteStream::from(content);
    let result = client
        .put_object()
        .bucket(bucket)
        .key(&job.remote_key)
        .body(body)
        .send()
        .await;

    match result {
        Ok(_) => {
            state.uploaded_files.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                key = %job.remote_key,
                is_meta = job.is_meta,
                "uploaded file to S3"
            );
        }
        Err(e) => {
            let msg = format!("{}", e);
            tracing::error!(key = %job.remote_key, error = %msg, "S3 upload failed");
            state.upload_failures.fetch_add(1, Ordering::Relaxed);
            *state.last_error.lock().await = Some(msg);
        }
    }
}

fn build_s3_client(config: &S3Config) -> S3Client {
    let credentials = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "traceowl",
    );

    let mut builder = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(credentials)
        .region(Region::new(config.region.clone()))
        .force_path_style(config.force_path_style);

    if !config.endpoint.is_empty() {
        builder = builder.endpoint_url(&config.endpoint);
    }

    S3Client::from_conf(builder.build())
}

// ---------------------------------------------------------------------------
// Remote key helper (pure, testable)
// ---------------------------------------------------------------------------

/// Compute the S3 object key for a session file.
/// - `prefix`: from config (may be empty or end with "/")
/// - `session_id`: the session identifier
/// - `filename`: e.g. "0001.jsonl" or "meta.json"
pub fn remote_key(prefix: &str, session_id: &str, filename: &str) -> String {
    format!("{}events/{}/{}", prefix, session_id, filename)
}
