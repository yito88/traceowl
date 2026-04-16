use axum::Router;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::{get, post};
use bytes::Bytes;
use reqwest::Client;
use serde_json::Value;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::sleep;
use traceowl_proxy::backend;
use traceowl_proxy::config::{BackendKind, Config};
use traceowl_proxy::control::{TracingGate, TracingSession};
use traceowl_proxy::proxy::AppState;
use traceowl_proxy::queue::EventQueue;
use traceowl_proxy::sink::{self, SinkCommand};

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

/// Start a mock upstream that returns a fixed JSON response.
async fn start_mock_upstream(response_body: Value, response_status: StatusCode) -> SocketAddr {
    let app = Router::new().route(
        "/collections/{collection_name}/points/query",
        post(move |_body: Bytes| {
            let resp_body = response_body.clone();
            async move {
                let mut response = Response::builder().status(response_status);
                response = response.header("content-type", "application/json");
                response
                    .body(Body::from(serde_json::to_vec(&resp_body).unwrap()))
                    .unwrap()
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Start the proxy WITHOUT auto-starting a tracing session.
/// Tracing gate starts disabled (the production default).
async fn start_proxy(
    upstream_addr: SocketAddr,
    output_dir: PathBuf,
) -> (SocketAddr, tokio_util::sync::CancellationToken) {
    let config = Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: format!("http://{}", upstream_addr),
        sampling_rate: 1.0,
        queue_capacity: 8192,
        output_dir,
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 50,
        flush_max_events: 1000,
        upstream_request_timeout_ms: 5000,
        include_query_representation: true,
    };

    std::fs::create_dir_all(&config.output_dir).unwrap();

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let (event_queue, rx) = EventQueue::new(config.queue_capacity);
    let event_queue = Arc::new(event_queue);

    let tracing_gate = Arc::new(TracingGate::new(config.sampling_rate));
    let tracing_session = Arc::new(tokio::sync::Mutex::new(TracingSession::new()));
    let (sink_ctl_tx, sink_ctl_rx) = mpsc::channel::<SinkCommand>(8);
    let last_flush_at = Arc::new(AtomicU64::new(0));
    let writer_alive = Arc::new(AtomicBool::new(true));

    let writer_cancel = cancel_token.clone();
    tokio::spawn(sink::writer_task(
        rx,
        sink_ctl_rx,
        config.clone(),
        last_flush_at.clone(),
        writer_alive.clone(),
        writer_cancel,
    ));

    let client = Client::builder()
        .timeout(Duration::from_millis(config.upstream_request_timeout_ms))
        .build()
        .unwrap();

    let state = AppState {
        client,
        config: Arc::new(config.clone()),
        event_queue,
        backend: Arc::new(backend::build_handler(&config.backend)),
        tracing_gate,
        tracing_session,
        sink_ctl: sink_ctl_tx,
        last_flush_at,
        writer_alive,
    };

    let app = Router::new()
        .route(
            "/control/status",
            get(traceowl_proxy::control::status_handler),
        )
        .route(
            "/control/tracing/start",
            post(traceowl_proxy::control::start_handler),
        )
        .route(
            "/control/tracing/stop",
            post(traceowl_proxy::control::stop_handler),
        )
        .fallback(traceowl_proxy::proxy::forward_handler)
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let serve_cancel = cancel_token.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .unwrap();
    });

    (addr, cancel_token)
}

/// Read all JSONL events from the output directory.
fn read_events(output_dir: &PathBuf) -> Vec<Value> {
    let mut events = Vec::new();
    if let Ok(entries) = std::fs::read_dir(output_dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "jsonl") {
                let content = std::fs::read_to_string(entry.path()).unwrap();
                for line in content.lines() {
                    if !line.trim().is_empty() {
                        events.push(serde_json::from_str(line).unwrap());
                    }
                }
            }
        }
    }
    events
}

fn qdrant_success_response() -> Value {
    serde_json::json!({
        "result": [
            {"id": "doc1", "score": 0.95},
            {"id": "doc2", "score": 0.85}
        ]
    })
}

fn dense_query_body() -> Value {
    serde_json::json!({
        "query": [0.1, 0.2, 0.3, 0.4],
        "limit": 2
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_startup_is_idle() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();
    let resp = client
        .get(format!("http://{}/control/status", proxy_addr))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();

    assert_eq!(body["state"], "idle");
    assert_eq!(body["tracing_enabled"], false);
    assert!(body["current_session_id"].is_null());
    assert!(body["writer_alive"].as_bool().unwrap());

    cancel.cancel();
}

#[tokio::test]
async fn test_start_creates_session() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();
    let resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "started");
    let session_id = body["session_id"].as_str().unwrap();
    assert!(!session_id.is_empty());
    assert!(body["started_at"].as_u64().unwrap() > 0);
    assert_eq!(body["sampling_rate"], 1.0);

    // Status should now show running
    let status_resp = client
        .get(format!("http://{}/control/status", proxy_addr))
        .send()
        .await
        .unwrap();
    let status: Value = status_resp.json().await.unwrap();
    assert_eq!(status["state"], "running");
    assert_eq!(status["tracing_enabled"], true);
    assert_eq!(status["current_session_id"], session_id);

    cancel.cancel();
}

#[tokio::test]
async fn test_start_conflict() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();

    // First start
    let resp1 = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 200);
    let body1: Value = resp1.json().await.unwrap();
    let first_session_id = body1["session_id"].as_str().unwrap().to_string();

    // Second start should conflict
    let resp2 = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 409);
    let body2: Value = resp2.json().await.unwrap();
    assert_eq!(body2["error"], "trace_already_running");
    assert_eq!(body2["current_session_id"], first_session_id);

    cancel.cancel();
}

#[tokio::test]
async fn test_stop_success() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();

    // Start tracing
    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), 200);
    let start_body: Value = start_resp.json().await.unwrap();
    let session_id = start_body["session_id"].as_str().unwrap().to_string();

    // Stop tracing
    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), 200);
    let stop_body: Value = stop_resp.json().await.unwrap();
    assert_eq!(stop_body["status"], "stopped");
    assert_eq!(stop_body["stopped_session_id"], session_id);

    // Status should now be idle
    let status_resp = client
        .get(format!("http://{}/control/status", proxy_addr))
        .send()
        .await
        .unwrap();
    let status: Value = status_resp.json().await.unwrap();
    assert_eq!(status["state"], "idle");
    assert_eq!(status["tracing_enabled"], false);
    assert!(status["current_session_id"].is_null());

    cancel.cancel();
}

#[tokio::test]
async fn test_stop_idle_noop() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();

    // Stop when idle — should return 200 with status=idle
    let resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "idle");

    cancel.cancel();
}

#[tokio::test]
async fn test_tracing_off_no_events() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    // Do NOT start tracing — gate is disabled by default.
    let client = Client::new();
    let resp = client
        .post(format!(
            "http://{}/collections/test_col/points/query",
            proxy_addr
        ))
        .json(&dense_query_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

    let events = read_events(&tmp.path().to_path_buf());
    assert_eq!(events.len(), 0, "no events when tracing is disabled");
}

#[tokio::test]
async fn test_tracing_on_emits_events() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();

    // Start tracing at rate=1.0
    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), 200);

    // Send a proxy request
    let proxy_resp = client
        .post(format!(
            "http://{}/collections/my_col/points/query",
            proxy_addr
        ))
        .json(&dense_query_body())
        .send()
        .await
        .unwrap();
    assert_eq!(proxy_resp.status(), 200);

    // Stop tracing (also flushes)
    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), 200);

    // Small wait for the OS to complete the flush write
    sleep(Duration::from_millis(50)).await;
    cancel.cancel();

    let events = read_events(&tmp.path().to_path_buf());
    assert_eq!(events.len(), 2, "expected request + response events");

    let req_event = events
        .iter()
        .find(|e| e["event_type"] == "request")
        .expect("missing request event");
    let resp_event = events
        .iter()
        .find(|e| e["event_type"] == "response")
        .expect("missing response event");

    assert_eq!(req_event["request_id"], resp_event["request_id"]);
    assert_eq!(req_event["db"]["kind"], "qdrant");
    assert_eq!(req_event["db"]["collection"], "my_col");
    assert_eq!(resp_event["status"]["ok"], true);
    assert_eq!(resp_event["status"]["http_status"], 200);
}

#[tokio::test]
async fn test_session_rotation() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(upstream_addr, tmp.path().to_path_buf()).await;

    let client = Client::new();

    // First session
    let resp1 = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 200);
    let body1: Value = resp1.json().await.unwrap();
    let file1 = body1["output_file"].as_str().unwrap().to_string();
    let session1 = body1["session_id"].as_str().unwrap().to_string();

    // Stop first session
    client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();

    // Small gap to ensure distinct timestamps in session IDs
    sleep(Duration::from_millis(10)).await;

    // Second session
    let resp2 = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    let body2: Value = resp2.json().await.unwrap();
    let file2 = body2["output_file"].as_str().unwrap().to_string();
    let session2 = body2["session_id"].as_str().unwrap().to_string();

    assert_ne!(file1, file2, "sessions must produce distinct output files");
    assert_ne!(session1, session2, "sessions must have distinct IDs");

    cancel.cancel();
}

#[tokio::test]
async fn test_flush_on_stop() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK).await;
    let tmp = tempfile::tempdir().unwrap();
    // Use a long flush interval so events won't auto-flush during the test
    let config = traceowl_proxy::config::Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: format!("http://{}", upstream_addr),
        sampling_rate: 1.0,
        queue_capacity: 8192,
        output_dir: tmp.path().to_path_buf(),
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 60_000, // Very long — won't auto-flush
        flush_max_events: 1000,
        upstream_request_timeout_ms: 5000,
        include_query_representation: true,
    };

    std::fs::create_dir_all(&config.output_dir).unwrap();

    let cancel_token = tokio_util::sync::CancellationToken::new();
    let (event_queue, rx) = EventQueue::new(config.queue_capacity);
    let event_queue = Arc::new(event_queue);

    let tracing_gate = Arc::new(TracingGate::new(config.sampling_rate));
    let tracing_session = Arc::new(tokio::sync::Mutex::new(TracingSession::new()));
    let (sink_ctl_tx, sink_ctl_rx) = mpsc::channel::<SinkCommand>(8);
    let last_flush_at = Arc::new(AtomicU64::new(0));
    let writer_alive = Arc::new(AtomicBool::new(true));

    let writer_cancel = cancel_token.clone();
    tokio::spawn(sink::writer_task(
        rx,
        sink_ctl_rx,
        config.clone(),
        last_flush_at.clone(),
        writer_alive.clone(),
        writer_cancel,
    ));

    let http_client = Client::builder()
        .timeout(Duration::from_millis(config.upstream_request_timeout_ms))
        .build()
        .unwrap();

    let state = AppState {
        client: http_client,
        config: Arc::new(config.clone()),
        event_queue,
        backend: Arc::new(backend::build_handler(&config.backend)),
        tracing_gate,
        tracing_session,
        sink_ctl: sink_ctl_tx,
        last_flush_at,
        writer_alive,
    };

    let app = Router::new()
        .route(
            "/control/status",
            get(traceowl_proxy::control::status_handler),
        )
        .route(
            "/control/tracing/start",
            post(traceowl_proxy::control::start_handler),
        )
        .route(
            "/control/tracing/stop",
            post(traceowl_proxy::control::stop_handler),
        )
        .fallback(traceowl_proxy::proxy::forward_handler)
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let serve_cancel = cancel_token.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .unwrap();
    });

    let client = Client::new();

    // Start tracing
    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), 200);

    // Send a proxy request — events go into buffer, won't auto-flush for 60s
    client
        .post(format!(
            "http://{}/collections/flush_col/points/query",
            proxy_addr
        ))
        .json(&dense_query_body())
        .send()
        .await
        .unwrap();

    // No auto-flush: file should be empty (or not yet written)
    sleep(Duration::from_millis(100)).await;
    let events_before = read_events(&tmp.path().to_path_buf());
    assert_eq!(
        events_before.len(),
        0,
        "events should still be buffered before stop"
    );

    // Stop — this triggers an explicit flush
    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), 200);

    // After stop, events must be on disk
    sleep(Duration::from_millis(50)).await;
    let events_after = read_events(&tmp.path().to_path_buf());
    assert_eq!(
        events_after.len(),
        2,
        "events must be flushed to disk on stop"
    );

    cancel_token.cancel();
}
