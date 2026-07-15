//! Restart-safe shared-memory IPC channels: iceoryx2 transport + wincode
//! serialization. Either side of a channel can restart freely — services are
//! `open_or_create`, publishing with no subscriber is a no-op, and stale
//! resources of dead nodes are swept on startup.

mod node;
mod receiver;
mod sender;

pub use receiver::IpcReceiver;
pub use sender::IpcSender;

use std::time::Duration;

/// How the receiver thread waits for new samples.
#[derive(Debug, Clone, Copy)]
pub enum PollMode {
    /// Spin on `receive()` — lowest latency, burns the core. Pin it.
    BusySpin,
    /// Spin for `spin`, then block on the event listener until notified.
    SpinThenWait { spin: Duration },
    /// Block on the event listener, waking every `cycle` to check cancellation.
    Event { cycle: Duration },
}

#[derive(Debug, Clone)]
pub struct IpcConfig {
    /// Max serialized message size; also the fixed shm slice capacity.
    pub max_message_size: usize,
    /// Per-subscriber queue depth (oldest overwritten when full).
    pub buffer_size: usize,
    pub max_publishers: usize,
    pub max_subscribers: usize,
    /// Fire the event notifier after each send. Disable when receivers busy-spin.
    pub notify_on_send: bool,
    pub poll_mode: PollMode,
    /// Pin the receiver polling thread to this core.
    pub core_affinity: Option<usize>,
}

impl Default for IpcConfig {
    fn default() -> Self {
        Self {
            max_message_size: 2048,
            buffer_size: 1024,
            max_publishers: 2,
            max_subscribers: 2,
            notify_on_send: true,
            poll_mode: PollMode::SpinThenWait {
                spin: Duration::from_micros(100),
            },
            core_affinity: None,
        }
    }
}

pub(crate) fn event_service_name(channel: &str) -> String {
    format!("{channel}/evt")
}
