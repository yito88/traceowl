use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::SinkMode;
use crate::events::now_ms;
use crate::proxy::AppState;
use crate::sink::{CloseResult, SinkCommand};
use crate::uploader::{SessionUploadState, UploadJob, remote_key, uploader_task};

// ---------------------------------------------------------------------------
// Shared state types
// ---------------------------------------------------------------------------

/// Hot-path tracing gate: lock-free reads for every proxied request.
pub struct TracingGate {
    pub enabled: AtomicBool,
    /// Runtime sampling rate encoded as f64 bits in a u64 atomic.
    pub sampling_rate_bits: AtomicU64,
}

impl TracingGate {
    pub fn new(sampling_rate: f64) -> Self {
        Self {
            enabled: AtomicBool::new(false),
            sampling_rate_bits: AtomicU64::new(sampling_rate.to_bits()),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn sampling_rate(&self) -> f64 {
        f64::from_bits(self.sampling_rate_bits.load(Ordering::Relaxed))
    }
}

/// Per-session state. Only touched by control handlers; never read on the hot path.
pub struct TracingSession {
    pub session_id: Option<String>,
    pub started_at: Option<u64>,
    pub current_output_file: Option<PathBuf>,
    /// Upload queue sender — Some in `local_plus_s3` mode. Dropped on stop to
    /// signal the uploader to drain and exit.
    pub upload_tx: Option<mpsc::Sender<UploadJob>>,
    /// Per-session upload counters shared with the uploader task.
    pub upload_state: Option<Arc<SessionUploadState>>,
    /// Cancel token for the per-session uploader task.
    pub uploader_cancel: Option<CancellationToken>,
}

impl TracingSession {
    pub fn new() -> Self {
        Self {
            session_id: None,
            started_at: None,
            current_output_file: None,
            upload_tx: None,
            upload_state: None,
            uploader_cancel: None,
        }
    }
}

impl Default for TracingSession {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Session ID generation
// ---------------------------------------------------------------------------

fn generate_session_id() -> String {
    let ms = now_ms();
    let secs = ms / 1000;
    let days = secs / 86400;
    let rem = secs % 86400;
    let hours = rem / 3600;
    let minutes = (rem % 3600) / 60;
    let seconds = rem % 60;
    let (year, month, day) = days_to_date(days);

    // Use first 6 hex chars of a v4 UUID as the random suffix.
    let uid = Uuid::new_v4();
    let hex = &uid.simple().to_string()[..6];

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z_{}",
        year, month, day, hours, minutes, seconds, hex
    )
}

fn days_to_date(days_since_epoch: u64) -> (u64, u64, u64) {
    let z = days_since_epoch as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as u64;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub struct StartRequest {
    pub sampling_rate: Option<f64>,
    pub session_label: Option<String>,
}

#[derive(Serialize)]
struct StatusResponse {
    state: &'static str,
    tracing_enabled: bool,
    current_session_id: Option<String>,
    sampling_rate: f64,
    queue_depth: usize,
    dropped_events: u64,
    current_output_file: Option<String>,
    started_at: Option<u64>,
    last_flush_at: Option<u64>,
    writer_alive: bool,
    // Upload operational state (runtime metrics, not session snapshot)
    upload_queue_depth: u64,
    in_flight_uploads: u64,
    last_upload_error: Option<String>,
    uploader_alive: Option<bool>,
}

#[derive(Serialize)]
struct SessionMeta {
    session_id: String,
    started_at: u64,
    stopped_at: u64,
    local_output_prefix: String,
    remote_output_prefix: String,
    written_files: u32,
    dropped_events: u64,
    uploaded_files: u64,
    upload_failures: u64,
    upload_status: String,
    last_upload_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn status_handler(State(state): State<AppState>) -> Response {
    let tracing_enabled = state.tracing_gate.is_enabled();
    let sampling_rate = state.tracing_gate.sampling_rate();

    let (
        current_session_id,
        started_at,
        current_output_file,
        upload_queue_depth,
        in_flight_uploads,
        last_upload_error,
        uploader_alive,
    ) = match state.tracing_session.try_lock() {
        Ok(s) => {
            let (uq, inf, uerr, ualive) = if let Some(ref us) = s.upload_state {
                let last_err = us.last_error.try_lock().ok().and_then(|g| g.clone());
                (
                    us.queued.load(Ordering::Relaxed),
                    us.in_flight.load(Ordering::Relaxed),
                    last_err,
                    Some(us.uploader_alive.load(Ordering::Relaxed)),
                )
            } else {
                (0, 0, None, None)
            };
            (
                s.session_id.clone(),
                s.started_at,
                s.current_output_file
                    .as_ref()
                    .map(|p| p.display().to_string()),
                uq,
                inf,
                uerr,
                ualive,
            )
        }
        Err(_) => (None, None, None, 0, 0, None, None),
    };

    let last_flush_ms = state.last_flush_at.load(Ordering::Relaxed);
    let last_flush_at = (last_flush_ms > 0).then_some(last_flush_ms);

    Json(StatusResponse {
        state: if tracing_enabled { "running" } else { "idle" },
        tracing_enabled,
        current_session_id,
        sampling_rate,
        queue_depth: state.event_queue.queue_depth(),
        dropped_events: state.event_queue.dropped_count(),
        current_output_file,
        started_at,
        last_flush_at,
        writer_alive: state.writer_alive.load(Ordering::Relaxed),
        upload_queue_depth,
        in_flight_uploads,
        last_upload_error,
        uploader_alive,
    })
    .into_response()
}

pub async fn start_handler(
    State(state): State<AppState>,
    body: Option<Json<StartRequest>>,
) -> Response {
    let body = body.map(|b| b.0).unwrap_or_default();

    // Serialize all session lifecycle changes through this mutex.
    let mut session = state.tracing_session.lock().await;

    if state.tracing_gate.is_enabled() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "trace_already_running",
                "current_session_id": session.session_id,
            })),
        )
            .into_response();
    }

    let session_id = generate_session_id();
    let now = now_ms();
    let sampling_rate = body.sampling_rate.unwrap_or(state.config.sampling_rate);

    // Set up upload channel and state if in local_plus_s3 mode.
    let (upload_tx_opt, upload_state_opt, uploader_cancel_opt) =
        if state.config.sink.mode == SinkMode::LocalPlusS3 {
            if let Some(ref s3_cfg) = state.config.sink.s3 {
                let upload_state = SessionUploadState::new();
                let (tx, rx) = mpsc::channel::<UploadJob>(256);
                let uploader_cancel = state.cancel_token.child_token();
                let s3_cfg_clone = s3_cfg.clone();
                let state_clone = upload_state.clone();
                let cancel_clone = uploader_cancel.clone();
                tokio::spawn(async move {
                    uploader_task(rx, s3_cfg_clone, state_clone, cancel_clone).await;
                });
                (Some(tx), Some(upload_state), Some(uploader_cancel))
            } else {
                (None, None, None)
            }
        } else {
            (None, None, None)
        };

    // Ask the writer to rotate to a new session directory/file.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<PathBuf>();
    if state
        .sink_ctl
        .send(SinkCommand::Rotate {
            session_id: session_id.clone(),
            upload_tx: upload_tx_opt.clone(),
            session_state: upload_state_opt.clone(),
            reply: reply_tx,
        })
        .await
        .is_err()
    {
        tracing::error!("sink control channel closed during start");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let output_file = match reply_rx.await {
        Ok(path) => path,
        Err(_) => {
            tracing::error!("sink did not reply to rotate command");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Enable tracing after the file is ready.
    state
        .tracing_gate
        .sampling_rate_bits
        .store(sampling_rate.to_bits(), Ordering::Relaxed);
    state.tracing_gate.enabled.store(true, Ordering::Relaxed);

    session.session_id = Some(session_id.clone());
    session.started_at = Some(now);
    session.current_output_file = Some(output_file.clone());
    session.upload_tx = upload_tx_opt;
    session.upload_state = upload_state_opt;
    session.uploader_cancel = uploader_cancel_opt;

    if let Some(ref label) = body.session_label {
        tracing::info!(session_id = %session_id, label = %label, file = %output_file.display(), "tracing started");
    } else {
        tracing::info!(session_id = %session_id, file = %output_file.display(), "tracing started");
    }

    Json(serde_json::json!({
        "status": "started",
        "session_id": session_id,
        "started_at": now,
        "sampling_rate": sampling_rate,
        "output_file": output_file.display().to_string(),
    }))
    .into_response()
}

pub async fn stop_handler(State(state): State<AppState>) -> Response {
    let mut session = state.tracing_session.lock().await;

    if !state.tracing_gate.is_enabled() {
        return Json(serde_json::json!({ "status": "idle" })).into_response();
    }

    // Disable tracing first so no new events enter the queue for this session.
    state.tracing_gate.enabled.store(false, Ordering::Relaxed);

    // Ask the writer to flush and close the current file.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<CloseResult>();
    if state
        .sink_ctl
        .send(SinkCommand::Close { reply: reply_tx })
        .await
        .is_err()
    {
        tracing::error!("sink control channel closed during stop");
    }

    let close_result = match reply_rx.await {
        Ok(r) => r,
        Err(_) => {
            tracing::error!("sink did not reply to close command");
            CloseResult {
                closed_path: None,
                file_count: 0,
            }
        }
    };

    let stopped_at = now_ms();
    let stopped_session_id = session.session_id.clone().unwrap_or_default();
    let started_at = session.started_at.unwrap_or(stopped_at);
    let dropped = state.event_queue.dropped_count();

    // Collect upload state snapshot (stop-time; uploads may still be in-flight).
    let (uploaded_files, upload_failures, upload_status_str, last_upload_error, remote_prefix) =
        if let Some(ref us) = session.upload_state {
            let uf = us.uploaded_files.load(Ordering::Relaxed);
            let fail = us.upload_failures.load(Ordering::Relaxed);
            let status = us.upload_status().to_string();
            let last_err = us.last_error.lock().await.clone();
            let prefix = state
                .config
                .sink
                .s3
                .as_ref()
                .map(|s| s.prefix.as_str())
                .unwrap_or("");
            let rprefix = format!("{}events/{}/", prefix, stopped_session_id);
            (uf, fail, status, last_err, rprefix)
        } else {
            let rprefix = format!("events/{}/", stopped_session_id);
            (0, 0, "not_configured".to_string(), None, rprefix)
        };

    let local_prefix = format!("events/{}/", stopped_session_id);

    // Write meta.json locally.
    let meta = SessionMeta {
        session_id: stopped_session_id.clone(),
        started_at,
        stopped_at,
        local_output_prefix: local_prefix.clone(),
        remote_output_prefix: remote_prefix.clone(),
        written_files: close_result.file_count,
        dropped_events: dropped,
        uploaded_files,
        upload_failures,
        upload_status: upload_status_str.clone(),
        last_upload_error: last_upload_error.clone(),
    };

    let meta_dir = state
        .config
        .sink
        .local_output_root
        .join("events")
        .join(&stopped_session_id);
    let meta_path = meta_dir.join("meta.json");

    match write_meta_json(&meta_path, &meta) {
        Ok(()) => {
            tracing::info!(path = %meta_path.display(), "wrote meta.json");
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to write meta.json");
        }
    }

    // Enqueue uploads in local_plus_s3 mode: closed JSONL file first, meta.json last.
    if let Some(ref tx) = session.upload_tx {
        let s3_prefix = state
            .config
            .sink
            .s3
            .as_ref()
            .map(|s| s.prefix.as_str())
            .unwrap_or("");

        // Enqueue the just-closed JSONL file.
        if let Some(ref closed) = close_result.closed_path {
            let filename = closed
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            let key = remote_key(s3_prefix, &stopped_session_id, &filename);
            if let Some(ref us) = session.upload_state {
                us.queued.fetch_add(1, Ordering::Relaxed);
            }
            let _ = tx
                .send(UploadJob {
                    local_path: closed.clone(),
                    remote_key: key,
                    is_meta: false,
                })
                .await;
        }

        // Enqueue meta.json last (completion marker).
        let meta_key = remote_key(s3_prefix, &stopped_session_id, "meta.json");
        if let Some(ref us) = session.upload_state {
            us.queued.fetch_add(1, Ordering::Relaxed);
        }
        let _ = tx
            .send(UploadJob {
                local_path: meta_path,
                remote_key: meta_key,
                is_meta: true,
            })
            .await;
    }

    // Drop upload_tx: the channel remains open until the uploader drains it, then it exits.
    session.upload_tx = None;
    // Keep upload_state so the uploader can continue updating counters post-stop.
    // Clear session fields.
    let final_output_file = session
        .current_output_file
        .take()
        .map(|p| p.display().to_string());
    session.session_id = None;
    session.started_at = None;
    session.uploader_cancel = None;
    // NOTE: upload_state kept alive until session cleared (uploader holds its own Arc clone).
    session.upload_state = None;

    tracing::info!(session_id = %stopped_session_id, "tracing stopped");

    Json(serde_json::json!({
        "status": "stopped",
        "stopped_session_id": stopped_session_id,
        "local_output_prefix": local_prefix,
        "remote_output_prefix": remote_prefix,
        "final_output_file": final_output_file,
        "upload_status": upload_status_str,
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_meta_json(path: &PathBuf, meta: &SessionMeta) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(meta).map_err(std::io::Error::other)?;
    std::fs::write(path, json.as_bytes())
}
