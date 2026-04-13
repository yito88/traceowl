use axum::Router;
use reqwest::Client;
use serde_json::Value;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;
use traceowl_proxy::backend;
use traceowl_proxy::config::Config;
use traceowl_proxy::proxy::AppState;
use traceowl_proxy::queue::EventQueue;
use traceowl_proxy::sink;

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
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: upstream_url.to_string(),
        sampling_rate: 1.0, // sample everything
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

    let writer_cancel = cancel_token.clone();
    tokio::spawn(sink::writer_task(rx, config.clone(), writer_cancel));

    let client = Client::builder()
        .timeout(Duration::from_millis(config.upstream_request_timeout_ms))
        .build()
        .unwrap();

    let backends: Vec<Box<dyn backend::BackendHandler>> =
        vec![Box::new(backend::qdrant::QdrantHandler)];

    let state = AppState {
        client,
        config: Arc::new(config),
        event_queue,
        backends: Arc::new(backends),
    };

    let app = Router::new()
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

    // Same request_id
    assert_eq!(req_event["request_id"], resp_event["request_id"]);

    // Request event
    assert_eq!(req_event["schema_version"], 1);
    assert_eq!(req_event["sampled"], true);
    assert_eq!(req_event["unsupported_shape"], false);
    assert_eq!(req_event["db"]["kind"], "qdrant");
    assert_eq!(req_event["db"]["collection"], collection);
    assert_eq!(req_event["query"]["top_k"], 3);
    assert!(!req_event["query"]["hash"].as_str().unwrap().is_empty());

    // Response event
    assert_eq!(resp_event["status"]["ok"], true);
    assert_eq!(resp_event["status"]["http_status"], 200);
    assert!(resp_event["status"]["error_kind"].is_null());
    assert!(resp_event["timing"]["latency_ms"].as_u64().unwrap() < 5000);

    let hits = resp_event["result"]["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    // Ranks should be sequential starting from 1
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
    // Make sure it doesn't exist
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

    // Qdrant returns 404 for nonexistent collection
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
    // Point proxy at a port where nothing is listening
    let tmp = tempfile::tempdir().unwrap();
    let (proxy_addr, cancel) = start_proxy(
        "http://127.0.0.1:19999", // nothing here
        tmp.path().to_path_buf(),
        2000,
    )
    .await;

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
