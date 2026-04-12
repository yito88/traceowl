use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

use crate::events::Event;

pub struct EventQueue {
    tx: mpsc::Sender<Event>,
    dropped: AtomicU64,
    sent: AtomicU64,
}

impl EventQueue {
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<Event>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                dropped: AtomicU64::new(0),
                sent: AtomicU64::new(0),
            },
            rx,
        )
    }

    pub fn send(&self, event: Event) {
        match self.tx.try_send(event) {
            Ok(()) => {
                self.sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!("event queue full, dropping event");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("event queue closed");
            }
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn sent_count(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn queue_depth(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}
