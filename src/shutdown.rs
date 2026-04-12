use tokio_util::sync::CancellationToken;

pub async fn shutdown_signal(cancel_token: CancellationToken) {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c => {}
        _ = sigterm.recv() => {}
    }

    #[cfg(not(unix))]
    ctrl_c.await.ok();

    tracing::info!("shutdown signal received");
    cancel_token.cancel();
}
