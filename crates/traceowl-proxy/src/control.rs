use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::now_ms;
use crate::proxy::AppState;
use crate::sink::SinkCommand;

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
}

impl TracingSession {
    pub fn new() -> Self {
        Self {
            session_id: None,
            started_at: None,
            current_output_file: None,
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
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn status_handler(State(state): State<AppState>) -> Response {
    let tracing_enabled = state.tracing_gate.is_enabled();
    let sampling_rate = state.tracing_gate.sampling_rate();

    let (current_session_id, started_at, current_output_file) =
        match state.tracing_session.try_lock() {
            Ok(s) => (
                s.session_id.clone(),
                s.started_at,
                s.current_output_file
                    .as_ref()
                    .map(|p| p.display().to_string()),
            ),
            Err(_) => (None, None, None),
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

    // Ask the writer to rotate to a new session file.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<PathBuf>();
    if state
        .sink_ctl
        .send(SinkCommand::Rotate {
            session_id: session_id.clone(),
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

    // Flush the writer to ensure all queued events land on disk.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<()>();
    if state
        .sink_ctl
        .send(SinkCommand::Flush { reply: reply_tx })
        .await
        .is_err()
    {
        tracing::error!("sink control channel closed during stop");
    } else {
        let _ = reply_rx.await;
    }

    let stopped_session_id = session.session_id.take();
    let final_output_file = session
        .current_output_file
        .take()
        .map(|p| p.display().to_string());
    session.started_at = None;

    let dropped = state.event_queue.dropped_count();

    tracing::info!(session_id = ?stopped_session_id, "tracing stopped");

    Json(serde_json::json!({
        "status": "stopped",
        "stopped_session_id": stopped_session_id,
        "final_output_file": final_output_file,
        "dropped_events": dropped,
    }))
    .into_response()
}
