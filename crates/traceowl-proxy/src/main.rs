mod backend;
mod config;
mod control;
mod events;
mod health;
mod proxy;
mod queue;
mod sampling;
mod shutdown;
mod sink;

use axum::Router;
use axum::routing::{get, post};
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::control::{TracingGate, TracingSession};
use crate::proxy::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "traceowl_proxy=info".parse().unwrap()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    let config = Config::load(&config_path)?;
    tracing::info!(listen = %config.listen_addr, upstream = %config.upstream_base_url, backend = ?config.backend, "starting traceowl-proxy");

    std::fs::create_dir_all(&config.output_dir)?;

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (event_queue, rx) = queue::EventQueue::new(config.queue_capacity);
    let event_queue = Arc::new(event_queue);

    let (sink_ctl_tx, sink_ctl_rx) = mpsc::channel::<sink::SinkCommand>(8);
    let last_flush_at = Arc::new(AtomicU64::new(0));
    let writer_alive = Arc::new(AtomicBool::new(true));

    let writer_handle = tokio::spawn(sink::writer_task(
        rx,
        sink_ctl_rx,
        config.clone(),
        last_flush_at.clone(),
        writer_alive.clone(),
        cancel_token.clone(),
    ));

    tokio::spawn(health::health_loop(
        event_queue.clone(),
        cancel_token.clone(),
    ));

    let client = Client::builder()
        .timeout(Duration::from_millis(config.upstream_request_timeout_ms))
        .build()?;

    tracing::info!(backend = ?config.backend, "backend selected");

    let state = AppState {
        client,
        backend: Arc::new(backend::build_handler(&config.backend)),
        config: Arc::new(config.clone()),
        event_queue,
        tracing_gate: Arc::new(TracingGate::new(config.sampling_rate)),
        tracing_session: Arc::new(tokio::sync::Mutex::new(TracingSession::new())),
        sink_ctl: sink_ctl_tx,
        last_flush_at,
        writer_alive,
    };

    let app = Router::new()
        .route("/control/status", get(control::status_handler))
        .route("/control/tracing/start", post(control::start_handler))
        .route("/control/tracing/stop", post(control::stop_handler))
        .fallback(proxy::forward_handler)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal(cancel_token.clone()))
        .await?;

    tracing::info!("server stopped, waiting for writer to drain");
    let _ = writer_handle.await;
    tracing::info!("shutdown complete");

    Ok(())
}
