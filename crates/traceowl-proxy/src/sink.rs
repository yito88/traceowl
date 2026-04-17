use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::events::{Event, now_ms};
use crate::uploader::{SessionUploadState, UploadJob, remote_key};

// ---------------------------------------------------------------------------
// Sink command channel
// ---------------------------------------------------------------------------

/// Returned by `SinkCommand::Close`.
pub struct CloseResult {
    /// Path of the file that was just closed (None if no file was open).
    pub closed_path: Option<PathBuf>,
    /// Number of JSONL files written during this session (including this one).
    pub file_count: u32,
}

pub enum SinkCommand {
    /// Rotate to a new session directory/file. Flushes any buffered events
    /// into the old file first, then opens `events/{session_id}/0001.jsonl`.
    /// `upload_tx` and `session_state` are Some in `local_plus_s3` mode so the
    /// writer can enqueue closed files from internal (size-based) rotations.
    Rotate {
        session_id: String,
        upload_tx: Option<mpsc::Sender<UploadJob>>,
        session_state: Option<Arc<SessionUploadState>>,
        reply: oneshot::Sender<PathBuf>,
    },
    /// Flush buffered events to the current file. Replies when done.
    #[allow(dead_code)]
    Flush { reply: oneshot::Sender<()> },
    /// Flush then close the current file without opening a new one.
    /// Used by stop_handler before writing meta.json.
    Close { reply: oneshot::Sender<CloseResult> },
}

pub type SinkControlSender = mpsc::Sender<SinkCommand>;

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// `{local_output_root}/events/{session_id}/{counter:04}.jsonl`
fn session_file_path(local_output_root: &Path, session_id: &str, counter: u32) -> PathBuf {
    local_output_root
        .join("events")
        .join(session_id)
        .join(format!("{:04}.jsonl", counter))
}

fn session_dir(local_output_root: &Path, session_id: &str) -> PathBuf {
    local_output_root.join("events").join(session_id)
}

fn open_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

// ---------------------------------------------------------------------------
// Writer task
// ---------------------------------------------------------------------------

pub async fn writer_task(
    mut rx: mpsc::Receiver<Event>,
    mut ctl_rx: mpsc::Receiver<SinkCommand>,
    config: Config,
    last_flush_at: Arc<AtomicU64>,
    writer_alive: Arc<AtomicBool>,
    cancel: CancellationToken,
) {
    let mut buffer: Vec<String> = Vec::new();
    let mut current_path: Option<PathBuf> = None;
    let mut current_file: Option<File> = None;
    let mut current_size: u64 = 0;
    let mut file_counter: u32 = 0;
    let mut session_id: Option<String> = None;
    // Upload channel and state stored from the Rotate command (Some in local_plus_s3).
    let mut upload_tx: Option<mpsc::Sender<UploadJob>> = None;
    let mut session_state: Option<Arc<SessionUploadState>> = None;
    let mut flush_interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    let mut total_flushed: u64 = 0;

    tracing::info!("writer task started, waiting for tracing session");

    loop {
        tokio::select! {
            cmd = ctl_rx.recv() => {
                match cmd {
                    Some(SinkCommand::Rotate {
                        session_id: new_sid,
                        upload_tx: new_upload_tx,
                        session_state: new_session_state,
                        reply,
                    }) => {
                        // Flush remaining events into the outgoing file before switching.
                        if !buffer.is_empty()
                            && let Some(ref mut f) = current_file
                        {
                            let (_, flushed) =
                                flush_buffer(&mut buffer, f, current_size);
                            total_flushed += flushed;
                            last_flush_at.store(now_ms(), Ordering::Relaxed);
                        } else {
                            buffer.clear();
                        }

                        // Drop old upload state.
                        upload_tx = new_upload_tx;
                        session_state = new_session_state;

                        file_counter = 1;
                        session_id = Some(new_sid.clone());

                        // Create the session directory.
                        let dir = session_dir(&config.sink.local_output_root, &new_sid);
                        if let Err(e) = std::fs::create_dir_all(&dir) {
                            tracing::error!(error = %e, "failed to create session directory");
                        }

                        let path = session_file_path(
                            &config.sink.local_output_root,
                            &new_sid,
                            file_counter,
                        );
                        match open_file(&path) {
                            Ok(f) => {
                                tracing::info!(path = %path.display(), "rotated to new session file");
                                current_file = Some(f);
                                current_path = Some(path.clone());
                                current_size = 0;
                                let _ = reply.send(path);
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "failed to open session file");
                                let _ = reply.send(PathBuf::new());
                            }
                        }
                    }

                    Some(SinkCommand::Flush { reply }) => {
                        if !buffer.is_empty()
                            && let Some(ref mut f) = current_file
                        {
                            let (new_size, flushed) =
                                flush_buffer(&mut buffer, f, current_size);
                            current_size = new_size;
                            total_flushed += flushed;
                            last_flush_at.store(now_ms(), Ordering::Relaxed);
                        }
                        let _ = reply.send(());
                    }

                    Some(SinkCommand::Close { reply }) => {
                        // Flush remaining buffer, then close the file.
                        if !buffer.is_empty()
                            && let Some(ref mut f) = current_file
                        {
                            let (_, flushed) =
                                flush_buffer(&mut buffer, f, current_size);
                            total_flushed += flushed;
                            last_flush_at.store(now_ms(), Ordering::Relaxed);
                        }
                        // Drop file handle (closes it).
                        let closed_path = current_path.take();
                        current_file = None;
                        current_size = 0;
                        let count = file_counter;
                        file_counter = 0;
                        session_id = None;
                        upload_tx = None;
                        session_state = None;
                        let _ = reply.send(CloseResult {
                            closed_path,
                            file_count: count,
                        });
                    }

                    None => break,
                }
            }

            event = rx.recv() => {
                match event {
                    Some(ev) => {
                        if current_file.is_none() {
                            tracing::warn!("event received with no active session file; dropping");
                            continue;
                        }
                        match serde_json::to_string(&ev) {
                            Ok(line) => buffer.push(line),
                            Err(e) => tracing::error!(error = %e, "failed to serialize event"),
                        }
                        if buffer.len() >= config.flush_max_events
                            && let Some(ref mut f) = current_file
                        {
                            let (new_size, flushed) =
                                flush_buffer(&mut buffer, f, current_size);
                            current_size = new_size;
                            total_flushed += flushed;
                            last_flush_at.store(now_ms(), Ordering::Relaxed);
                            maybe_rotate(
                                &config,
                                &session_id,
                                &mut file_counter,
                                &mut current_path,
                                &mut current_file,
                                &mut current_size,
                                &upload_tx,
                                &session_state,
                            );
                        }
                    }
                    None => break,
                }
            }

            _ = flush_interval.tick() => {
                if !buffer.is_empty()
                    && let Some(ref mut f) = current_file
                {
                    let (new_size, flushed) = flush_buffer(&mut buffer, f, current_size);
                    current_size = new_size;
                    total_flushed += flushed;
                    last_flush_at.store(now_ms(), Ordering::Relaxed);
                    maybe_rotate(
                        &config,
                        &session_id,
                        &mut file_counter,
                        &mut current_path,
                        &mut current_file,
                        &mut current_size,
                        &upload_tx,
                        &session_state,
                    );
                }
            }

            _ = cancel.cancelled() => {
                tracing::info!("writer task shutting down, draining queue");
                while let Ok(ev) = rx.try_recv() {
                    match serde_json::to_string(&ev) {
                        Ok(line) => buffer.push(line),
                        Err(e) => {
                            tracing::error!(error = %e, "failed to serialize event during drain");
                        }
                    }
                }
                if !buffer.is_empty()
                    && let Some(ref mut f) = current_file
                {
                    let (_, flushed) = flush_buffer(&mut buffer, f, current_size);
                    total_flushed += flushed;
                    last_flush_at.store(now_ms(), Ordering::Relaxed);
                }
                break;
            }
        }
    }

    writer_alive.store(false, Ordering::Relaxed);
    tracing::info!(total_flushed, "writer task stopped");
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn maybe_rotate(
    config: &Config,
    session_id: &Option<String>,
    file_counter: &mut u32,
    current_path: &mut Option<PathBuf>,
    current_file: &mut Option<File>,
    current_size: &mut u64,
    upload_tx: &Option<mpsc::Sender<UploadJob>>,
    session_state: &Option<Arc<SessionUploadState>>,
) {
    if *current_size < config.rotation_max_bytes {
        return;
    }
    let Some(sid) = session_id else { return };

    // Capture the just-closed file path for upload enqueueing.
    let closed_path = current_path.clone();

    *file_counter += 1;
    let path = session_file_path(&config.sink.local_output_root, sid, *file_counter);
    match open_file(&path) {
        Ok(f) => {
            tracing::info!(path = %path.display(), "rotated event file within session");
            *current_file = Some(f);
            *current_path = Some(path.clone());
            *current_size = 0;
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to open rotated event file");
            return;
        }
    }

    // Enqueue the closed file for upload if in local_plus_s3 mode.
    if let (Some(tx), Some(state), Some(closed)) = (
        upload_tx.as_ref(),
        session_state.as_ref(),
        closed_path.as_ref(),
    ) {
        let filename = closed
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let prefix = config
            .sink
            .s3
            .as_ref()
            .map(|s| s.prefix.as_str())
            .unwrap_or("");
        let key = remote_key(prefix, sid, &filename);
        state.queued.fetch_add(1, Ordering::Relaxed);
        let job = UploadJob {
            local_path: closed.clone(),
            remote_key: key,
            is_meta: false,
        };
        // best-effort: don't block the writer
        if tx.try_send(job).is_err() {
            state.queued.fetch_sub(1, Ordering::Relaxed);
            tracing::warn!("upload queue full; dropping upload job for rotated file");
        }
    }
}

fn flush_buffer(buffer: &mut Vec<String>, file: &mut File, current_size: u64) -> (u64, u64) {
    let mut bytes_written: u64 = 0;
    let count = buffer.len() as u64;
    for line in buffer.drain(..) {
        let line_bytes = line.as_bytes();
        if let Err(e) = file.write_all(line_bytes) {
            tracing::error!(error = %e, "failed to write event to file");
            continue;
        }
        if let Err(e) = file.write_all(b"\n") {
            tracing::error!(error = %e, "failed to write newline");
            continue;
        }
        bytes_written += line_bytes.len() as u64 + 1;
    }
    if let Err(e) = file.flush() {
        tracing::error!(error = %e, "failed to flush event file");
    }
    tracing::debug!(events = count, bytes = bytes_written, "flushed events");
    (current_size + bytes_written, count)
}
