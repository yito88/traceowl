use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::events::Event;

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_file_path(output_dir: &Path) -> PathBuf {
    let now = chrono_like_now();
    let counter = FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    output_dir.join(format!("events-{}-{:04}.jsonl", now, counter))
}

fn chrono_like_now() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = dur.as_secs();
    // Simple ISO8601-ish timestamp without pulling in chrono
    let days = secs / 86400;
    let rem = secs % 86400;
    let hours = rem / 3600;
    let minutes = (rem % 3600) / 60;
    let seconds = rem % 60;

    // Calculate date from days since epoch (1970-01-01)
    let (year, month, day) = days_to_date(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_date(days_since_epoch: u64) -> (u64, u64, u64) {
    // Civil date from days since 1970-01-01 (algorithm from Howard Hinnant)
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

fn open_file(path: &PathBuf) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

pub async fn writer_task(mut rx: mpsc::Receiver<Event>, config: Config, cancel: CancellationToken) {
    let mut buffer: Vec<String> = Vec::new();
    let mut current_path = new_file_path(&config.output_dir);
    let mut current_file = match open_file(&current_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, "failed to open initial event file");
            return;
        }
    };
    let mut current_size: u64 = 0;
    let mut flush_interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    let mut total_flushed: u64 = 0;

    tracing::info!(path = %current_path.display(), "writer task started");

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(ev) => {
                        match serde_json::to_string(&ev) {
                            Ok(line) => buffer.push(line),
                            Err(e) => tracing::error!(error = %e, "failed to serialize event"),
                        }
                        if buffer.len() >= config.flush_max_events {
                            let (new_size, flushed) = flush_buffer(&mut buffer, &mut current_file, current_size);
                            current_size = new_size;
                            total_flushed += flushed;
                            if current_size >= config.rotation_max_bytes {
                                current_path = new_file_path(&config.output_dir);
                                match open_file(&current_path) {
                                    Ok(f) => {
                                        tracing::info!(path = %current_path.display(), "rotated event file");
                                        current_file = f;
                                        current_size = 0;
                                    }
                                    Err(e) => {
                                        tracing::error!(error = %e, "failed to open rotated event file");
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        // Channel closed
                        break;
                    }
                }
            }
            _ = flush_interval.tick() => {
                if !buffer.is_empty() {
                    let (new_size, flushed) = flush_buffer(&mut buffer, &mut current_file, current_size);
                    current_size = new_size;
                    total_flushed += flushed;
                    if current_size >= config.rotation_max_bytes {
                        current_path = new_file_path(&config.output_dir);
                        match open_file(&current_path) {
                            Ok(f) => {
                                tracing::info!(path = %current_path.display(), "rotated event file");
                                current_file = f;
                                current_size = 0;
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "failed to open rotated event file");
                            }
                        }
                    }
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("writer task shutting down, draining queue");
                // Drain remaining events
                while let Ok(ev) = rx.try_recv() {
                    match serde_json::to_string(&ev) {
                        Ok(line) => buffer.push(line),
                        Err(e) => tracing::error!(error = %e, "failed to serialize event during drain"),
                    }
                }
                if !buffer.is_empty() {
                    let (_, flushed) = flush_buffer(&mut buffer, &mut current_file, current_size);
                    total_flushed += flushed;
                }
                break;
            }
        }
    }

    tracing::info!(total_flushed, "writer task stopped");
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
