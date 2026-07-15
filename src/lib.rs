//! Restart-safe shared-memory IPC channels: iceoryx2 transport + wincode
//! serialization. Either side of a channel can restart freely — services are
//! `open_or_create`, publishing with no subscriber is a no-op, and stale
//! resources of dead nodes are swept on startup.
//!
//! The receiver busy-spins on its polling thread for minimum latency; it must
//! be pinned to a dedicated core (required at spawn).

mod node;
mod receiver;
mod sender;

pub use receiver::IpcReceiver;
pub use sender::IpcSender;

#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// Max serialized message size; also the fixed shm slice capacity.
    pub max_message_size: usize,
    /// Per-subscriber queue depth (oldest overwritten when full).
    pub buffer_size: usize,
    pub max_publishers: usize,
    pub max_subscribers: usize,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_message_size: 2048 * 10,
            buffer_size: 1024,
            max_publishers: 2,
            max_subscribers: 2,
        }
    }
}
