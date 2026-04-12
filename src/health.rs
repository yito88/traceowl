use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::queue::EventQueue;

pub async fn health_loop(queue: Arc<EventQueue>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                tracing::info!(
                    queue_depth = queue.queue_depth(),
                    sent_events = queue.sent_count(),
                    dropped_events = queue.dropped_count(),
                    "health"
                );
            }
            _ = cancel.cancelled() => break,
        }
    }
}
