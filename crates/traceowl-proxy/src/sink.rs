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

pub enum SinkCommand {
    /// Rotate to a new session file. Flushes any buffered events first, then
    /// opens `events-{session_id}-0001.jsonl`. Replies with the new file path.
    Rotate {
        session_id: String,
        reply: oneshot::Sender<PathBuf>,
    },
    /// Flush buffered events to the current file. Replies when done.
    Flush { reply: oneshot::Sender<()> },
}

pub type SinkControlSender = mpsc::Sender<SinkCommand>;

fn session_file_path(output_dir: &Path, session_id: &str, counter: u32) -> PathBuf {
    output_dir.join(format!("events-{}-{:04}.jsonl", session_id, counter))
}

fn open_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

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
    let mut flush_interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    let mut total_flushed: u64 = 0;

    tracing::info!("writer task started, waiting for tracing session");

    loop {
        tokio::select! {
            cmd = ctl_rx.recv() => {
                match cmd {
                    Some(SinkCommand::Rotate { session_id: new_sid, reply }) => {
                        // Flush remaining events into the outgoing file before switching.
                        if !buffer.is_empty() {
                            if let Some(ref mut f) = current_file {
                                let (new_size, flushed) =
                                    flush_buffer(&mut buffer, f, current_size);
                                current_size = new_size;
                                total_flushed += flushed;
                                last_flush_at.store(now_ms(), Ordering::Relaxed);
                            } else {
                                buffer.clear();
                            }
                        }
                        file_counter = 1;
                        session_id = Some(new_sid.clone());
                        let path =
                            session_file_path(&config.output_dir, &new_sid, file_counter);
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

fn maybe_rotate(
    config: &Config,
    session_id: &Option<String>,
    file_counter: &mut u32,
    current_path: &mut Option<PathBuf>,
    current_file: &mut Option<File>,
    current_size: &mut u64,
) {
    if *current_size < config.rotation_max_bytes {
        return;
    }
    let Some(sid) = session_id else { return };
    *file_counter += 1;
    let path = session_file_path(&config.output_dir, sid, *file_counter);
    match open_file(&path) {
        Ok(f) => {
            tracing::info!(path = %path.display(), "rotated event file within session");
            *current_file = Some(f);
            *current_path = Some(path);
            *current_size = 0;
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to open rotated event file");
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
