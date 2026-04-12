mod backend;
mod config;
mod error;
mod events;
mod health;
mod proxy;
mod queue;
mod sampling;
mod shutdown;
mod sink;

use axum::Router;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
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
    tracing::info!(listen = %config.listen_addr, upstream = %config.upstream_base_url, "starting traceowl-proxy");

    std::fs::create_dir_all(&config.output_dir)?;

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (event_queue, rx) = queue::EventQueue::new(config.queue_capacity);
    let event_queue = Arc::new(event_queue);

    let writer_handle = tokio::spawn(sink::writer_task(rx, config.clone(), cancel_token.clone()));

    tokio::spawn(health::health_loop(
        event_queue.clone(),
        cancel_token.clone(),
    ));

    let client = Client::builder()
        .timeout(Duration::from_millis(config.upstream_request_timeout_ms))
        .build()?;

    let backends: Vec<Box<dyn backend::BackendHandler>> =
        vec![Box::new(backend::qdrant::QdrantHandler)];

    let state = AppState {
        client,
        config: Arc::new(config.clone()),
        event_queue,
        backends: Arc::new(backends),
    };

    let app = Router::new()
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
