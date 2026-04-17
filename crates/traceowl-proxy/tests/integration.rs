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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::sleep;
use traceowl_proxy::backend;
use traceowl_proxy::config::{BackendKind, Config, SinkConfig, SinkMode};
use traceowl_proxy::control::{TracingGate, TracingSession};
use traceowl_proxy::proxy::AppState;
use traceowl_proxy::queue::EventQueue;
use traceowl_proxy::sink::{self, SinkCommand};

/// Start a mock upstream Qdrant server that returns a fixed response.
async fn start_mock_upstream(
    response_body: Value,
    response_status: StatusCode,
    delay: Option<Duration>,
) -> SocketAddr {
    let app = Router::new().route(
        "/collections/{collection_name}/points/query",
        post(move |_body: Bytes| {
            let resp_body = response_body.clone();
            let delay = delay;
            async move {
                if let Some(d) = delay {
                    sleep(d).await;
                }
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

/// Start the proxy pointing at the given upstream, return (proxy_addr, cancel_token).
/// Automatically starts a tracing session so events are emitted.
async fn start_proxy(
    upstream_addr: SocketAddr,
    output_dir: PathBuf,
    sampling_rate: f64,
    queue_capacity: usize,
    timeout_ms: u64,
) -> (SocketAddr, tokio_util::sync::CancellationToken) {
    let config = Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: format!("http://{}", upstream_addr),
        sampling_rate,
        queue_capacity,
        sink: SinkConfig {
            mode: SinkMode::LocalOnly,
            local_output_root: output_dir.clone(),
            s3: None,
        },
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 100,
        flush_max_events: 1000,
        upstream_request_timeout_ms: timeout_ms,
        include_query_representation: true,
    };

    std::fs::create_dir_all(&config.sink.local_output_root).unwrap();

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
        tracing_gate: tracing_gate.clone(),
        tracing_session,
        sink_ctl: sink_ctl_tx.clone(),
        last_flush_at,
        writer_alive,
        cancel_token: cancel_token.clone(),
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

    // Auto-start a tracing session so existing tests continue to emit events.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    sink_ctl_tx
        .send(SinkCommand::Rotate {
            session_id: "test-session".to_string(),
            upload_tx: None,
            session_state: None,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let _ = reply_rx.await.unwrap();
    tracing_gate.enabled.store(true, Ordering::Relaxed);

    (addr, cancel_token)
}

/// Read all JSONL events from the output directory (recursively).
fn read_events(output_dir: &PathBuf) -> Vec<Value> {
    let mut events = Vec::new();
    read_events_recursive(output_dir, &mut events);
    events
}

fn read_events_recursive(dir: &PathBuf, events: &mut Vec<Value>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                read_events_recursive(&path, events);
            } else if path.extension().is_some_and(|ext| ext == "jsonl") {
                let content = std::fs::read_to_string(&path).unwrap();
                for line in content.lines() {
                    if !line.trim().is_empty() {
                        events.push(serde_json::from_str(line).unwrap());
                    }
                }
            }
        }
    }
}

fn qdrant_success_response() -> Value {
    serde_json::json!({
        "result": [
            {"id": "doc1", "score": 0.95},
            {"id": "doc2", "score": 0.85},
            {"id": "doc3", "score": 0.75}
        ]
    })
}

fn dense_query_body() -> Value {
    serde_json::json!({
        "query": [0.1, 0.2, 0.3, 0.4],
        "limit": 3
    })
}

#[tokio::test]
async fn test_forwarding_fidelity() {
    let expected_response = qdrant_success_response();
    let upstream_addr = start_mock_upstream(expected_response.clone(), StatusCode::OK, None).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        0.0, // No sampling — just test forwarding
        8192,
        10000,
    )
    .await;

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
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body, expected_response);

    cancel.cancel();
}

#[tokio::test]
async fn test_event_emission() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK, None).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        1.0, // Sample everything
        8192,
        10000,
    )
    .await;

    let client = Client::new();
    let resp = client
        .post(format!(
            "http://{}/collections/my_collection/points/query",
            proxy_addr
        ))
        .json(&dense_query_body())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Wait for flush
    sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

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

    // Same request_id
    assert_eq!(req_event["request_id"], resp_event["request_id"]);

    // Request event fields
    assert_eq!(req_event["schema_version"], 1);
    assert_eq!(req_event["sampled"], true);
    assert_eq!(req_event["unsupported_shape"], false);
    assert_eq!(req_event["db"]["kind"], "qdrant");
    assert_eq!(req_event["db"]["collection"], "my_collection");
    assert_eq!(req_event["query"]["top_k"], 3);
    assert!(!req_event["query"]["hash"].as_str().unwrap().is_empty());

    // Response event fields
    assert_eq!(resp_event["status"]["ok"], true);
    assert_eq!(resp_event["status"]["http_status"], 200);
    assert!(resp_event["status"]["error_kind"].is_null());
    assert!(resp_event["timing"]["latency_ms"].as_u64().unwrap() < 5000);
    assert_eq!(resp_event["result"]["hits"].as_array().unwrap().len(), 3);
    assert_eq!(resp_event["result"]["hits"][0]["doc_id"], "doc1");
    assert_eq!(resp_event["result"]["hits"][0]["rank"], 1);
}

#[tokio::test]
async fn test_sampling_unsampled_no_events() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK, None).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        0.0, // Never sample
        8192,
        10000,
    )
    .await;

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

    sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

    let events = read_events(&tmp.path().to_path_buf());
    assert_eq!(
        events.len(),
        0,
        "unsampled request should produce no events"
    );
}

#[tokio::test]
async fn test_sampling_error_force_samples() {
    let error_response = serde_json::json!({"status": {"error": "internal error"}});
    let upstream_addr =
        start_mock_upstream(error_response, StatusCode::INTERNAL_SERVER_ERROR, None).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        0.0, // Never sample — but errors should force it
        8192,
        10000,
    )
    .await;

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
    assert_eq!(resp.status(), 500);

    sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

    let events = read_events(&tmp.path().to_path_buf());
    assert!(
        events.len() >= 2,
        "errors should force-sample: got {} events",
        events.len()
    );

    let resp_event = events
        .iter()
        .find(|e| e["event_type"] == "response")
        .expect("missing response event");
    assert_eq!(resp_event["status"]["ok"], false);
    assert_eq!(resp_event["status"]["http_status"], 500);
    assert_eq!(resp_event["status"]["error_kind"], "upstream_5xx");
}

#[tokio::test]
async fn test_overflow_drops_events_not_requests() {
    let upstream_addr = start_mock_upstream(qdrant_success_response(), StatusCode::OK, None).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        1.0, // Sample everything
        1,   // Tiny queue — will overflow
        10000,
    )
    .await;

    let client = Client::new();
    let mut handles = Vec::new();

    // Send many concurrent requests
    for _ in 0..20 {
        let c = client.clone();
        let addr = proxy_addr;
        handles.push(tokio::spawn(async move {
            c.post(format!("http://{}/collections/test_col/points/query", addr))
                .json(&dense_query_body())
                .send()
                .await
        }));
    }

    // All requests should succeed
    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(
            resp.status(),
            200,
            "request must succeed even with overflow"
        );
    }

    cancel.cancel();
    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_timeout_produces_error_event() {
    // Mock that delays 5 seconds
    let upstream_addr = start_mock_upstream(
        qdrant_success_response(),
        StatusCode::OK,
        Some(Duration::from_secs(5)),
    )
    .await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        upstream_addr,
        tmp.path().to_path_buf(),
        1.0, // Sample everything
        8192,
        100, // 100ms timeout — will expire
    )
    .await;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let resp = client
        .post(format!(
            "http://{}/collections/test_col/points/query",
            proxy_addr
        ))
        .json(&dense_query_body())
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 504);

    sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

    let events = read_events(&tmp.path().to_path_buf());
    let resp_event = events
        .iter()
        .find(|e| e["event_type"] == "response")
        .expect("missing response event for timeout");
    assert_eq!(resp_event["status"]["ok"], false);
    assert_eq!(resp_event["status"]["error_kind"], "upstream_timeout");
}
