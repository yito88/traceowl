use axum::Router;
use axum::routing::{get, post};
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
use traceowl_proxy::config::{BackendKind, Config};
use traceowl_proxy::control::{TracingGate, TracingSession};
use traceowl_proxy::proxy::AppState;
use traceowl_proxy::queue::EventQueue;
use traceowl_proxy::sink::{self, SinkCommand};

fn qdrant_url() -> String {
    std::env::var("QDRANT_URL").unwrap_or_else(|_| "http://localhost:6333".to_string())
}

async fn wait_for_qdrant(client: &Client, base_url: &str) {
    for _ in 0..30 {
        if client
            .get(format!("{}/readyz", base_url))
            .send()
            .await
            .is_ok()
        {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("qdrant did not become ready in time");
}

async fn create_collection(client: &Client, base_url: &str, name: &str) {
    let resp = client
        .put(format!("{}/collections/{}", base_url, name))
        .json(&serde_json::json!({
            "vectors": {
                "size": 4,
                "distance": "Cosine"
            }
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "failed to create collection: {}",
        resp.text().await.unwrap()
    );
}

async fn upsert_points(client: &Client, base_url: &str, collection: &str) {
    let resp = client
        .put(format!(
            "{}/collections/{}/points?wait=true",
            base_url, collection
        ))
        .json(&serde_json::json!({
            "points": [
                {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "payload": {"name": "alpha"}},
                {"id": 2, "vector": [0.5, 0.6, 0.7, 0.8], "payload": {"name": "beta"}},
                {"id": 3, "vector": [0.9, 0.1, 0.2, 0.3], "payload": {"name": "gamma"}}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "failed to upsert points: {}",
        resp.text().await.unwrap()
    );
}

async fn delete_collection(client: &Client, base_url: &str, name: &str) {
    let _ = client
        .delete(format!("{}/collections/{}", base_url, name))
        .send()
        .await;
}

async fn start_proxy(
    upstream_url: &str,
    output_dir: PathBuf,
    timeout_ms: u64,
) -> (SocketAddr, tokio_util::sync::CancellationToken) {
    let config = Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: upstream_url.to_string(),
        sampling_rate: 1.0,
        queue_capacity: 8192,
        output_dir,
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 100,
        flush_max_events: 1000,
        upstream_request_timeout_ms: timeout_ms,
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
        tracing_gate: tracing_gate.clone(),
        tracing_session,
        sink_ctl: sink_ctl_tx.clone(),
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

    // Auto-start a tracing session so tests emit events.
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    sink_ctl_tx
        .send(SinkCommand::Rotate {
            session_id: "test-session".to_string(),
            reply: reply_tx,
        })
        .await
        .unwrap();
    let _ = reply_rx.await.unwrap();
    tracing_gate.enabled.store(true, Ordering::Relaxed);

    (addr, cancel_token)
}

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

#[tokio::test]
#[ignore]
async fn test_successful_query() {
    let base_url = qdrant_url();
    let client = Client::new();
    let collection = "test_success";

    wait_for_qdrant(&client, &base_url).await;
    delete_collection(&client, &base_url, collection).await;
    create_collection(&client, &base_url, collection).await;
    upsert_points(&client, &base_url, collection).await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(&base_url, tmp.path().to_path_buf(), 10000).await;

    // Query through proxy
    let proxy_resp = client
        .post(format!(
            "http://{}/collections/{}/points/query",
            proxy_addr, collection
        ))
        .json(&serde_json::json!({
            "query": [0.1, 0.2, 0.3, 0.4],
            "limit": 3
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(proxy_resp.status(), 200);
    let proxy_body: Value = proxy_resp.json().await.unwrap();

    // Verify we got results from Qdrant (response shape: result.points)
    let points = proxy_body["result"]["points"]
        .as_array()
        .or_else(|| proxy_body["result"].as_array())
        .expect("expected result.points in response");
    assert!(!points.is_empty(), "expected at least one result");

    // Wait for events to flush
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

    assert_eq!(req_event["request_id"], resp_event["request_id"]);
    assert_eq!(req_event["schema_version"], 1);
    assert_eq!(req_event["sampled"], true);
    assert_eq!(req_event["unsupported_shape"], false);
    assert_eq!(req_event["db"]["kind"], "qdrant");
    assert_eq!(req_event["db"]["collection"], collection);
    assert_eq!(req_event["query"]["top_k"], 3);
    assert!(!req_event["query"]["hash"].as_str().unwrap().is_empty());

    assert_eq!(resp_event["status"]["ok"], true);
    assert_eq!(resp_event["status"]["http_status"], 200);
    assert!(resp_event["status"]["error_kind"].is_null());
    assert!(resp_event["timing"]["latency_ms"].as_u64().unwrap() < 5000);

    let hits = resp_event["result"]["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    for (i, hit) in hits.iter().enumerate() {
        assert_eq!(hit["rank"], (i + 1) as u64);
        assert!(hit["score"].as_f64().unwrap() > 0.0);
        assert!(!hit["doc_id"].as_str().unwrap().is_empty());
    }

    delete_collection(&client, &base_url, collection).await;
}

#[tokio::test]
#[ignore]
async fn test_query_nonexistent_collection() {
    let base_url = qdrant_url();
    let client = Client::new();

    wait_for_qdrant(&client, &base_url).await;
    delete_collection(&client, &base_url, "does_not_exist").await;

    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(&base_url, tmp.path().to_path_buf(), 10000).await;

    let proxy_resp = client
        .post(format!(
            "http://{}/collections/does_not_exist/points/query",
            proxy_addr
        ))
        .json(&serde_json::json!({
            "query": [0.1, 0.2, 0.3, 0.4],
            "limit": 3
        }))
        .send()
        .await
        .unwrap();

    let status = proxy_resp.status().as_u16();
    assert!(status >= 400, "expected error status, got {}", status);

    sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    sleep(Duration::from_millis(200)).await;

    let events = read_events(&tmp.path().to_path_buf());
    assert_eq!(events.len(), 2, "expected request + response events");

    let resp_event = events
        .iter()
        .find(|e| e["event_type"] == "response")
        .expect("missing response event");

    assert_eq!(resp_event["status"]["ok"], false);
    assert_eq!(resp_event["status"]["http_status"], status);
}

#[tokio::test]
#[ignore]
async fn test_upstream_unreachable() {
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) =
        start_proxy("http://127.0.0.1:19999", tmp.path().to_path_buf(), 2000).await;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let proxy_resp = client
        .post(format!(
            "http://{}/collections/any_col/points/query",
            proxy_addr
        ))
        .json(&serde_json::json!({
            "query": [0.1, 0.2, 0.3, 0.4],
            "limit": 3
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(proxy_resp.status(), 502);

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

    assert_eq!(req_event["request_id"], resp_event["request_id"]);
    assert_eq!(resp_event["status"]["ok"], false);
    assert_eq!(resp_event["status"]["http_status"], 502);
    assert_eq!(resp_event["status"]["error_kind"], "upstream_connect_error");
}
