/// MinIO integration tests for the S3-compatible uploader.
///
/// These tests require a running MinIO instance. Set the following env vars:
///   MINIO_URL      (default: http://localhost:9000)
///   MINIO_ACCESS_KEY (default: minioadmin)
///   MINIO_SECRET_KEY (default: minioadmin)
///   MINIO_BUCKET   (default: traceowl-test)
///
/// All tests are marked `#[ignore]` and only run when `--include-ignored` is
/// passed (e.g. via the minio-integration GitHub Actions workflow).
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
use traceowl_proxy::config::{BackendKind, Config, S3Config, SinkConfig, SinkMode};
use traceowl_proxy::control::{TracingGate, TracingSession};
use traceowl_proxy::proxy::AppState;
use traceowl_proxy::queue::EventQueue;
use traceowl_proxy::sink::{self, SinkCommand};

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

fn minio_url() -> String {
    std::env::var("MINIO_URL").unwrap_or_else(|_| "http://localhost:9000".to_string())
}

fn minio_access_key() -> String {
    std::env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())
}

fn minio_secret_key() -> String {
    std::env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string())
}

fn minio_bucket() -> String {
    std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "traceowl-test".to_string())
}

fn s3_config() -> S3Config {
    S3Config {
        bucket: minio_bucket(),
        prefix: String::new(),
        endpoint: minio_url(),
        region: "us-east-1".to_string(),
        access_key: minio_access_key(),
        secret_key: minio_secret_key(),
        force_path_style: true,
    }
}

fn build_s3_client(cfg: &S3Config) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let creds = Credentials::new(&cfg.access_key, &cfg.secret_key, None, None, "test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .credentials_provider(creds)
        .region(Region::new(cfg.region.clone()))
        .endpoint_url(&cfg.endpoint)
        .force_path_style(cfg.force_path_style)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

// ---------------------------------------------------------------------------
// Proxy + upstream helpers
// ---------------------------------------------------------------------------

async fn start_mock_upstream() -> SocketAddr {
    let response = serde_json::json!({
        "result": [{"id": "doc1", "score": 0.9}]
    });
    let app = Router::new().route(
        "/collections/{collection_name}/points/query",
        post(move |_body: Bytes| {
            let resp = response.clone();
            async move {
                Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&resp).unwrap()))
                    .unwrap()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn start_proxy(
    upstream_addr: SocketAddr,
    output_dir: PathBuf,
    s3: Option<S3Config>,
) -> (SocketAddr, tokio_util::sync::CancellationToken) {
    let mode = if s3.is_some() {
        SinkMode::LocalPlusS3
    } else {
        SinkMode::LocalOnly
    };
    let config = Config {
        backend: BackendKind::Qdrant,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_base_url: format!("http://{}", upstream_addr),
        sampling_rate: 1.0,
        queue_capacity: 8192,
        sink: SinkConfig {
            mode,
            local_output_root: output_dir.clone(),
            s3,
        },
        rotation_max_bytes: 50 * 1024 * 1024,
        flush_interval_ms: 100,
        flush_max_events: 1000,
        upstream_request_timeout_ms: 5000,
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

    tokio::spawn(sink::writer_task(
        rx,
        sink_ctl_rx,
        config.clone(),
        last_flush_at.clone(),
        writer_alive.clone(),
        cancel_token.clone(),
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

    (addr, cancel_token)
}

// ---------------------------------------------------------------------------
// MinIO readiness check and bucket setup
// ---------------------------------------------------------------------------

async fn wait_for_minio(client: &aws_sdk_s3::Client, bucket: &str) {
    for _ in 0..30 {
        if client.list_buckets().send().await.is_ok() {
            // Ensure the test bucket exists.
            let _ = client.create_bucket().bucket(bucket).send().await;
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("MinIO did not become ready in time");
}

async fn list_objects(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) -> Vec<String> {
    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap();
    resp.contents()
        .iter()
        .filter_map(|obj| obj.key().map(|k| k.to_string()))
        .collect()
}

async fn get_object_string(client: &aws_sdk_s3::Client, bucket: &str, key: &str) -> String {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let bytes = resp.body.collect().await.unwrap().into_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn delete_prefix(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) {
    let keys = list_objects(client, bucket, prefix).await;
    for key in keys {
        let _ = client.delete_object().bucket(bucket).key(&key).send().await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// After start → queries → stop, all session JSONL files and meta.json must
/// appear in MinIO under events/<session_id>/.
#[tokio::test]
#[ignore]
async fn test_upload_session_files_to_minio() {
    let cfg = s3_config();
    let s3 = build_s3_client(&cfg);
    wait_for_minio(&s3, &cfg.bucket).await;

    let tmp = tempfile::tempdir().unwrap();
    let upstream = start_mock_upstream().await;
    let (proxy_addr, cancel) =
        start_proxy(upstream, tmp.path().to_path_buf(), Some(cfg.clone())).await;

    let client = Client::new();

    // Start tracing
    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), StatusCode::OK);
    let start_body: Value = start_resp.json().await.unwrap();
    let session_id = start_body["session_id"].as_str().unwrap().to_string();

    // Clean up any previous test artifacts
    let prefix = format!("events/{}/", session_id);
    delete_prefix(&s3, &cfg.bucket, &prefix).await;

    // Send a proxy request
    let resp = client
        .post(format!(
            "http://{}/collections/test_col/points/query",
            proxy_addr
        ))
        .json(&serde_json::json!({ "query": [0.1, 0.2, 0.3, 0.4], "limit": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Stop tracing (closes file, writes + enqueues meta.json)
    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);
    let stop_body: Value = stop_resp.json().await.unwrap();
    assert_eq!(stop_body["status"], "stopped");
    assert_eq!(stop_body["stopped_session_id"], session_id);

    // Wait for background uploads to complete
    sleep(Duration::from_secs(5)).await;
    cancel.cancel();

    // Verify files in MinIO
    let objects = list_objects(&s3, &cfg.bucket, &prefix).await;
    assert!(
        !objects.is_empty(),
        "expected uploaded files in MinIO under {prefix}, found none"
    );

    let has_jsonl = objects.iter().any(|k| k.ends_with(".jsonl"));
    assert!(
        has_jsonl,
        "expected at least one .jsonl file in MinIO: {:?}",
        objects
    );

    let meta_key = format!("events/{}/meta.json", session_id);
    assert!(
        objects.contains(&meta_key),
        "expected meta.json in MinIO: {:?}",
        objects
    );
}

/// meta.json must be valid JSON and contain the expected fields.
#[tokio::test]
#[ignore]
async fn test_meta_json_content_in_minio() {
    let cfg = s3_config();
    let s3 = build_s3_client(&cfg);
    wait_for_minio(&s3, &cfg.bucket).await;

    let tmp = tempfile::tempdir().unwrap();
    let upstream = start_mock_upstream().await;
    let (proxy_addr, cancel) =
        start_proxy(upstream, tmp.path().to_path_buf(), Some(cfg.clone())).await;

    let client = Client::new();

    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    let start_body: Value = start_resp.json().await.unwrap();
    let session_id = start_body["session_id"].as_str().unwrap().to_string();

    let prefix = format!("events/{}/", session_id);
    delete_prefix(&s3, &cfg.bucket, &prefix).await;

    // Send two requests
    for _ in 0..2 {
        client
            .post(format!(
                "http://{}/collections/meta_col/points/query",
                proxy_addr
            ))
            .json(&serde_json::json!({ "query": [0.1, 0.2, 0.3, 0.4], "limit": 1 }))
            .send()
            .await
            .unwrap();
    }

    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);

    sleep(Duration::from_secs(5)).await;
    cancel.cancel();

    let meta_key = format!("events/{}/meta.json", session_id);
    let objects = list_objects(&s3, &cfg.bucket, &prefix).await;
    assert!(
        objects.contains(&meta_key),
        "meta.json not found in MinIO: {:?}",
        objects
    );

    let content = get_object_string(&s3, &cfg.bucket, &meta_key).await;
    let meta: Value = serde_json::from_str(&content).expect("meta.json must be valid JSON");

    assert_eq!(meta["session_id"], session_id, "session_id mismatch");
    assert!(meta["started_at"].as_u64().unwrap() > 0);
    assert!(meta["stopped_at"].as_u64().unwrap() > 0);
    assert!(meta["written_files"].as_u64().unwrap() >= 1);
    assert!(
        meta["local_output_prefix"]
            .as_str()
            .unwrap()
            .contains(&session_id)
    );
    assert!(
        meta["remote_output_prefix"]
            .as_str()
            .unwrap()
            .contains(&session_id)
    );
}

/// meta.json must be the last object uploaded — all JSONL files that were
/// written must be present in MinIO before meta.json is considered complete.
#[tokio::test]
#[ignore]
async fn test_meta_json_uploaded_after_jsonl_files() {
    let cfg = s3_config();
    let s3 = build_s3_client(&cfg);
    wait_for_minio(&s3, &cfg.bucket).await;

    let tmp = tempfile::tempdir().unwrap();
    let upstream = start_mock_upstream().await;
    let (proxy_addr, cancel) =
        start_proxy(upstream, tmp.path().to_path_buf(), Some(cfg.clone())).await;

    let client = Client::new();

    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    let start_body: Value = start_resp.json().await.unwrap();
    let session_id = start_body["session_id"].as_str().unwrap().to_string();

    let prefix = format!("events/{}/", session_id);
    delete_prefix(&s3, &cfg.bucket, &prefix).await;

    client
        .post(format!(
            "http://{}/collections/order_col/points/query",
            proxy_addr
        ))
        .json(&serde_json::json!({ "query": [0.1, 0.2, 0.3, 0.4], "limit": 1 }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();

    sleep(Duration::from_secs(5)).await;
    cancel.cancel();

    let objects = list_objects(&s3, &cfg.bucket, &prefix).await;
    let meta_key = format!("events/{}/meta.json", session_id);

    // All JSONL files must be present
    let jsonl_files: Vec<_> = objects.iter().filter(|k| k.ends_with(".jsonl")).collect();
    assert!(!jsonl_files.is_empty(), "expected at least one JSONL file");

    // meta.json must be present (signals session is complete for analysis)
    assert!(
        objects.contains(&meta_key),
        "meta.json must be present when session is complete"
    );
}

/// When MinIO is unreachable (wrong endpoint), local files must remain intact
/// and the proxy must not crash or affect request forwarding.
#[tokio::test]
#[ignore]
async fn test_upload_failure_local_files_preserved() {
    let cfg = S3Config {
        bucket: "traceowl-test".to_string(),
        prefix: String::new(),
        endpoint: "http://127.0.0.1:19998".to_string(), // nothing listening here
        region: "us-east-1".to_string(),
        access_key: "key".to_string(),
        secret_key: "secret".to_string(),
        force_path_style: true,
    };

    let tmp = tempfile::tempdir().unwrap();
    let upstream = start_mock_upstream().await;
    let (proxy_addr, cancel) = start_proxy(upstream, tmp.path().to_path_buf(), Some(cfg)).await;

    let client = Client::new();

    let start_resp = client
        .post(format!("http://{}/control/tracing/start", proxy_addr))
        .json(&serde_json::json!({ "sampling_rate": 1.0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), StatusCode::OK);
    let start_body: Value = start_resp.json().await.unwrap();
    let session_id = start_body["session_id"].as_str().unwrap().to_string();

    // Request forwarding must succeed even though S3 is unreachable
    let resp = client
        .post(format!(
            "http://{}/collections/fail_col/points/query",
            proxy_addr
        ))
        .json(&serde_json::json!({ "query": [0.1, 0.2, 0.3, 0.4], "limit": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "request forwarding must succeed regardless of S3 state"
    );

    let stop_resp = client
        .post(format!("http://{}/control/tracing/stop", proxy_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), StatusCode::OK);

    // Wait for upload attempts to fail
    sleep(Duration::from_secs(3)).await;
    cancel.cancel();

    // Local files must still be present
    let session_dir = tmp.path().join("events").join(&session_id);
    assert!(session_dir.exists(), "session directory must exist locally");

    let local_files: Vec<_> = std::fs::read_dir(&session_dir).unwrap().flatten().collect();
    assert!(
        !local_files.is_empty(),
        "local files must be preserved after upload failure"
    );

    let meta_path = session_dir.join("meta.json");
    assert!(
        meta_path.exists(),
        "meta.json must be written locally even if upload fails"
    );
}
