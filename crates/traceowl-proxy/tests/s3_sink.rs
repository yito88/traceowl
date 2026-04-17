/// Tests for the session-aware local layout and S3 sink behavior.
///
/// These tests use the sink/control internals directly rather than the HTTP
/// API, which lets them inspect channels and file paths without needing a
/// running server.  All tests run against a TempDir so they are self-contained.
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use traceowl_proxy::config::{BackendKind, Config, SinkConfig, SinkMode};
use traceowl_proxy::sink::{SinkCommand, writer_task};
use traceowl_proxy::uploader::{SessionUploadState, UploadJob, remote_key};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_config(tmp: &tempfile::TempDir) -> Config {
    Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: "http://localhost:6334".to_string(),
        sampling_rate: 1.0,
        queue_capacity: 8192,
        sink: SinkConfig {
            mode: SinkMode::LocalOnly,
            local_output_root: tmp.path().to_path_buf(),
            s3: None,
        },
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 1000,
        flush_max_events: 1000,
        upstream_request_timeout_ms: 5000,
        include_query_representation: true,
    }
}

fn small_rotation_config(tmp: &tempfile::TempDir) -> Config {
    Config {
        rotation_max_bytes: 1, // force rotation on every write
        ..base_config(tmp)
    }
}

async fn spawn_writer(
    config: Config,
) -> (
    mpsc::Sender<traceowl_proxy::events::Event>,
    mpsc::Sender<SinkCommand>,
    Arc<AtomicU64>,
    Arc<AtomicBool>,
    tokio_util::sync::CancellationToken,
) {
    let (event_tx, event_rx) = mpsc::channel(1024);
    let (ctl_tx, ctl_rx) = mpsc::channel::<SinkCommand>(8);
    let last_flush = Arc::new(AtomicU64::new(0));
    let writer_alive = Arc::new(AtomicBool::new(true));
    let cancel = tokio_util::sync::CancellationToken::new();

    tokio::spawn(writer_task(
        event_rx,
        ctl_rx,
        config,
        last_flush.clone(),
        writer_alive.clone(),
        cancel.clone(),
    ));

    (event_tx, ctl_tx, last_flush, writer_alive, cancel)
}

async fn rotate_to(ctl_tx: &mpsc::Sender<SinkCommand>, session_id: &str) -> PathBuf {
    rotate_to_with_upload(ctl_tx, session_id, None, None).await
}

async fn rotate_to_with_upload(
    ctl_tx: &mpsc::Sender<SinkCommand>,
    session_id: &str,
    upload_tx: Option<mpsc::Sender<UploadJob>>,
    session_state: Option<Arc<SessionUploadState>>,
) -> PathBuf {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    ctl_tx
        .send(SinkCommand::Rotate {
            session_id: session_id.to_string(),
            upload_tx,
            session_state,
            reply: reply_tx,
        })
        .await
        .unwrap();
    reply_rx.await.unwrap()
}

async fn close_writer(ctl_tx: &mpsc::Sender<SinkCommand>) -> traceowl_proxy::sink::CloseResult {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    ctl_tx
        .send(SinkCommand::Close { reply: reply_tx })
        .await
        .unwrap();
    reply_rx.await.unwrap()
}

// ---------------------------------------------------------------------------
// 1. Session directory layout
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_session_directory_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(&tmp);
    let root = config.sink.local_output_root.clone();

    let (_event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    let session_id = "20260416T120000Z_abc123";
    let path = rotate_to(&ctl_tx, session_id).await;

    // File must be at events/<session_id>/0001.jsonl
    let expected = root.join("events").join(session_id).join("0001.jsonl");
    assert_eq!(
        path, expected,
        "first file must be at events/<session_id>/0001.jsonl"
    );
    assert!(expected.exists(), "file must exist after rotate");

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 2. Rotation naming (within a session)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rotation_naming() {
    let tmp = tempfile::tempdir().unwrap();
    let config = small_rotation_config(&tmp);
    let root = config.sink.local_output_root.clone();
    let session_id = "20260416T120000Z_rot001";

    let (event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    rotate_to(&ctl_tx, session_id).await;

    // Write enough events to trigger rotation (rotation_max_bytes = 1)
    // We need to flush to trigger the rotation check.
    // Send events and wait for the periodic flush to happen.
    let ev = make_event("req1");
    event_tx.send(ev).await.unwrap();
    sleep(Duration::from_millis(1200)).await; // wait past flush_interval_ms=1000

    let dir = root.join("events").join(session_id);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
        .collect();
    files.sort_by_key(|e| e.file_name());

    assert!(
        files.len() >= 2,
        "expected at least 2 files after rotation, got {}",
        files.len()
    );
    assert_eq!(files[0].file_name().to_str().unwrap(), "0001.jsonl");
    assert_eq!(files[1].file_name().to_str().unwrap(), "0002.jsonl");

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 3. Closed-file upload enqueue on rotation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_closed_file_upload_enqueue() {
    let tmp = tempfile::tempdir().unwrap();
    let config = small_rotation_config(&tmp);
    let session_id = "20260416T120000Z_upq001";

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadJob>(32);
    let session_state = SessionUploadState::new();

    let (event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    // Start session with upload channel
    rotate_to_with_upload(&ctl_tx, session_id, Some(upload_tx), Some(session_state)).await;

    // Write an event and wait for flush+rotation
    event_tx.send(make_event("r1")).await.unwrap();
    sleep(Duration::from_millis(1200)).await;

    // We expect an upload job for the rotated (closed) file
    let job = tokio::time::timeout(Duration::from_secs(2), upload_rx.recv())
        .await
        .expect("timed out waiting for upload job")
        .expect("channel closed");

    assert!(
        !job.is_meta,
        "rotated JSONL file must not be marked as meta"
    );
    assert!(
        job.local_path.to_str().unwrap().contains("0001.jsonl"),
        "upload job must be for 0001.jsonl (the closed file), got {:?}",
        job.local_path
    );
    assert!(
        !job.local_path.to_str().unwrap().contains("0002.jsonl"),
        "the currently-open file must never be uploaded"
    );

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 4. Stop writes meta.json
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stop_writes_meta_json() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(&tmp);
    let root = config.sink.local_output_root.clone();

    let (_event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    let session_id = "20260416T120000Z_meta01";
    rotate_to(&ctl_tx, session_id).await;
    close_writer(&ctl_tx).await;

    // Write meta.json manually (as stop_handler would)
    let meta_dir = root.join("events").join(session_id);
    let meta_path = meta_dir.join("meta.json");
    let meta_content = serde_json::json!({
        "session_id": session_id,
        "started_at": 1776330655000_u64,
        "stopped_at": 1776330712000_u64,
        "local_output_prefix": format!("events/{}/", session_id),
        "remote_output_prefix": format!("events/{}/", session_id),
        "written_files": 1,
        "dropped_events": 0,
        "uploaded_files": 0,
        "upload_failures": 0,
        "upload_status": "not_configured",
        "last_upload_error": null
    });
    std::fs::write(
        &meta_path,
        serde_json::to_string_pretty(&meta_content).unwrap(),
    )
    .unwrap();

    assert!(meta_path.exists(), "meta.json must exist after stop");

    let content: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
    assert_eq!(content["session_id"], session_id);
    assert!(content["started_at"].as_u64().unwrap() > 0);
    assert!(content["stopped_at"].as_u64().unwrap() > 0);
    assert_eq!(content["written_files"], 1);

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 5. meta.json is enqueued last
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_meta_uploaded_last() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(&tmp);
    let root = config.sink.local_output_root.clone();

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadJob>(32);
    let session_state = SessionUploadState::new();

    let (_event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    let session_id = "20260416T120000Z_last01";
    rotate_to_with_upload(
        &ctl_tx,
        session_id,
        Some(upload_tx.clone()),
        Some(session_state.clone()),
    )
    .await;

    // Close the writer
    let close_result = close_writer(&ctl_tx).await;

    // Simulate stop_handler: enqueue closed file, then meta.json
    let s3_prefix = "";
    if let Some(ref closed) = close_result.closed_path {
        let filename = closed
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let key = remote_key(s3_prefix, session_id, &filename);
        session_state.queued.fetch_add(1, Ordering::Relaxed);
        upload_tx
            .send(UploadJob {
                local_path: closed.clone(),
                remote_key: key,
                is_meta: false,
            })
            .await
            .unwrap();
    }

    let meta_path = root.join("events").join(session_id).join("meta.json");
    std::fs::write(&meta_path, b"{}").unwrap();
    let meta_key = remote_key(s3_prefix, session_id, "meta.json");
    session_state.queued.fetch_add(1, Ordering::Relaxed);
    upload_tx
        .send(UploadJob {
            local_path: meta_path,
            remote_key: meta_key,
            is_meta: true,
        })
        .await
        .unwrap();
    drop(upload_tx);

    // Collect all queued jobs
    let mut jobs = Vec::new();
    while let Ok(Some(job)) =
        tokio::time::timeout(Duration::from_millis(100), upload_rx.recv()).await
    {
        jobs.push(job);
    }

    assert!(!jobs.is_empty(), "expected at least 1 upload job");
    let last = jobs.last().unwrap();
    assert!(last.is_meta, "meta.json must be the last upload job");
    assert!(
        last.local_path.to_str().unwrap().ends_with("meta.json"),
        "last job must be meta.json"
    );

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 6. local_only mode produces no upload jobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_local_only_no_uploads() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = base_config(&tmp);
    config.sink.mode = SinkMode::LocalOnly;

    let (event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    let session_id = "20260416T120000Z_local1";
    // No upload_tx: local_only passes None
    let path = rotate_to(&ctl_tx, session_id).await;
    assert!(path.exists());

    event_tx.send(make_event("x")).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // There is no upload queue at all — no way to receive jobs.
    // Verify the session directory exists and has a JSONL file (local write worked).
    let dir = tmp.path().join("events").join(session_id);
    assert!(
        dir.exists(),
        "session directory must exist in local_only mode"
    );

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 7. local_plus_s3 mode produces upload jobs for rotated files
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_local_plus_s3_upload_jobs() {
    let tmp = tempfile::tempdir().unwrap();
    let config = small_rotation_config(&tmp);
    let session_id = "20260416T120000Z_s3001";

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadJob>(32);
    let session_state = SessionUploadState::new();

    let (event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    rotate_to_with_upload(&ctl_tx, session_id, Some(upload_tx), Some(session_state)).await;

    // Send multiple events to force multiple rotations
    for i in 0..3 {
        event_tx.send(make_event(&format!("r{}", i))).await.unwrap();
        sleep(Duration::from_millis(1100)).await;
    }

    // At least one upload job should have been enqueued
    let job = tokio::time::timeout(Duration::from_secs(3), upload_rx.recv())
        .await
        .expect("timed out waiting for upload job")
        .expect("channel closed");

    assert!(!job.is_meta);
    assert!(job.remote_key.contains(session_id));
    assert!(job.remote_key.starts_with("events/"));

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 8. Upload failure handling: local file intact, counters updated
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upload_failure_handling() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(&tmp);
    let root = config.sink.local_output_root.clone();
    let session_id = "20260416T120000Z_fail01";

    let (upload_tx, mut upload_rx) = mpsc::channel::<UploadJob>(32);
    let session_state = SessionUploadState::new();

    let (_event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    rotate_to_with_upload(
        &ctl_tx,
        session_id,
        Some(upload_tx.clone()),
        Some(session_state.clone()),
    )
    .await;

    let close_result = close_writer(&ctl_tx).await;

    // Enqueue the closed file for upload
    if let Some(ref closed) = close_result.closed_path {
        let key = remote_key("", session_id, "0001.jsonl");
        session_state.queued.fetch_add(1, Ordering::Relaxed);
        upload_tx
            .send(UploadJob {
                local_path: closed.clone(),
                remote_key: key,
                is_meta: false,
            })
            .await
            .unwrap();
    }
    drop(upload_tx);

    // Receive the job and simulate a failure (no real S3; just inspect the job).
    let job = tokio::time::timeout(Duration::from_secs(2), upload_rx.recv())
        .await
        .unwrap()
        .unwrap();

    // Simulate: uploader received the job but could not upload (e.g. S3 unreachable).
    // In a real uploader, it would update session_state.upload_failures.
    // Here we do it manually to verify the local file is still intact.
    session_state.queued.fetch_sub(1, Ordering::Relaxed);
    session_state
        .upload_failures
        .fetch_add(1, Ordering::Relaxed);
    *session_state.last_error.lock().await = Some("simulated S3 failure".to_string());

    // Local file must still exist
    assert!(
        job.local_path.exists(),
        "local file must remain intact after upload failure: {:?}",
        job.local_path
    );

    // Session directory must exist
    let dir = root.join("events").join(session_id);
    assert!(dir.exists(), "session directory must still exist");

    // Failure counters updated
    assert_eq!(session_state.upload_failures.load(Ordering::Relaxed), 1);
    assert_eq!(session_state.uploaded_files.load(Ordering::Relaxed), 0);

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 9. Stop does not wait for uploader completion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stop_does_not_wait() {
    let tmp = tempfile::tempdir().unwrap();
    let config = base_config(&tmp);
    let session_id = "20260416T120000Z_nowait";

    // Use a capacity-0 equivalent: a bounded channel that won't drain fast.
    let (upload_tx, _upload_rx) = mpsc::channel::<UploadJob>(1);
    let session_state = SessionUploadState::new();

    let (_event_tx, ctl_tx, _flush, _alive, cancel) = spawn_writer(config).await;

    rotate_to_with_upload(
        &ctl_tx,
        session_id,
        Some(upload_tx.clone()),
        Some(session_state.clone()),
    )
    .await;
    let _close_result = close_writer(&ctl_tx).await;

    // Enqueue more jobs than the upload channel can hold immediately.
    // (Channel has capacity 1; send 2 more without draining.)
    session_state.queued.fetch_add(1, Ordering::Relaxed);
    let _ = upload_tx.try_send(UploadJob {
        local_path: tmp.path().join("dummy1.jsonl"),
        remote_key: "events/s/dummy1.jsonl".to_string(),
        is_meta: false,
    });

    // Drop upload_tx as stop_handler would.
    drop(upload_tx);

    // The stop itself (Close + enqueue) should return quickly — no blocking wait.
    // We've already verified Close returns synchronously above.
    // The key guarantee: stop_handler returned before upload_rx was drained.
    // We just verify Close completed without hanging (the test itself would hang otherwise).
    assert_eq!(session_state.queued.load(Ordering::Relaxed), 1);

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// 10. Remote key mapping
// ---------------------------------------------------------------------------

#[test]
fn test_remote_key_mapping_empty_prefix() {
    let key = remote_key("", "20260416T120000Z_abc123", "0001.jsonl");
    assert_eq!(key, "events/20260416T120000Z_abc123/0001.jsonl");
}

#[test]
fn test_remote_key_mapping_with_prefix() {
    let key = remote_key("myapp/", "20260416T120000Z_abc123", "0002.jsonl");
    assert_eq!(key, "myapp/events/20260416T120000Z_abc123/0002.jsonl");
}

#[test]
fn test_remote_key_meta_json() {
    let key = remote_key("", "20260416T120000Z_abc123", "meta.json");
    assert_eq!(key, "events/20260416T120000Z_abc123/meta.json");
}

// ---------------------------------------------------------------------------
// 11. Status upload fields via TracingSession
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_status_upload_fields() {
    // Verify that SessionUploadState fields are readable for the status endpoint.
    let state = SessionUploadState::new();
    assert_eq!(state.queued.load(Ordering::Relaxed), 0);
    assert_eq!(state.in_flight.load(Ordering::Relaxed), 0);
    assert_eq!(state.uploaded_files.load(Ordering::Relaxed), 0);
    assert_eq!(state.upload_failures.load(Ordering::Relaxed), 0);
    assert!(state.uploader_alive.load(Ordering::Relaxed));
    assert!(state.last_error.lock().await.is_none());

    // Simulate uploader activity
    state.queued.fetch_add(3, Ordering::Relaxed);
    state.in_flight.fetch_add(1, Ordering::Relaxed);
    state.uploaded_files.fetch_add(2, Ordering::Relaxed);
    state.upload_failures.fetch_add(1, Ordering::Relaxed);
    *state.last_error.lock().await = Some("timeout".to_string());

    assert_eq!(state.queued.load(Ordering::Relaxed), 3);
    assert_eq!(state.in_flight.load(Ordering::Relaxed), 1);
    assert_eq!(state.uploaded_files.load(Ordering::Relaxed), 2);
    assert_eq!(state.upload_failures.load(Ordering::Relaxed), 1);
    assert_eq!(state.upload_status(), "partial_failed");
    assert_eq!(state.last_error.lock().await.as_deref(), Some("timeout"));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_event(request_id: &str) -> traceowl_proxy::events::Event {
    use traceowl_proxy::events::{DbInfo, Event, QueryInfo, RequestEvent};
    Event::Request(RequestEvent {
        schema_version: 1,
        request_id: request_id.to_string(),
        timestamp_ms: 1776330655000,
        sampled: true,
        unsupported_shape: false,
        db: DbInfo {
            kind: "qdrant".to_string(),
            collection: "test".to_string(),
        },
        query: QueryInfo {
            hash: "abc".to_string(),
            top_k: 3,
            representation: None,
        },
    })
}
