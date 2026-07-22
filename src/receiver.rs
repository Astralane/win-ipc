use crate::IpcConfig;
use crate::node::{create_node, create_port_with_retry};
use eyre::WrapErr as _;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use metrics::{Counter, counter};
use std::thread::JoinHandle;
use tokio_util::sync::CancellationToken;
use wincode::SchemaRead;
use wincode::config::DefaultConfig;

pub struct IpcReceiver;

/// Per-channel counter handles, resolved once at spawn rather than re-looked-up
/// on every message (`received` fires in the busy-poll hot path).
struct ReceiverMetrics {
    received: Counter,
    deserialize_failures: Counter,
    channel_full: Counter,
}

impl ReceiverMetrics {
    fn new(channel: &str) -> Self {
        Self {
            received: counter!("ipc_received", "channel" => channel.to_string()),
            deserialize_failures: counter!("ipc_deserialize_failures", "channel" => channel.to_string()),
            channel_full: counter!("ipc_receiver_channel_full", "channel" => channel.to_string()),
        }
    }
}

impl IpcReceiver {
    /// Zero-copy: `view` gets the raw serialized bytes directly in shared
    /// memory — no deserialize, no copy. Borrow only within the callback (the
    /// shm slot is released when it returns); use a borrowed wincode
    /// `SchemaRead` type inside for a zero-copy typed view.
    /// The thread busy-spins pinned to `core` (a dedicated core is required);
    /// pinning or service setup failure fails the spawn.
    pub fn spawn_with_view_handler<F>(
        channel: &str,
        cfg: &IpcConfig,
        core: usize,
        cancel: CancellationToken,
        view: F,
    ) -> eyre::Result<JoinHandle<()>>
    where
        F: FnMut(&[u8]) + Send + 'static,
    {
        let channel = channel.to_string();
        let cfg = cfg.clone();
        let metrics = ReceiverMetrics::new(&channel);
        let (setup_tx, setup_rx) = std::sync::mpsc::sync_channel::<eyre::Result<()>>(1);
        let hdl = std::thread::Builder::new()
            .name(format!("win-ipc-{channel}"))
            .spawn(move || {
                let setup = || -> eyre::Result<_> {
                    affinity::set_thread_affinity([core]).map_err(|e| {
                        eyre::eyre!("failed to pin receiver '{channel}' to core {core}: {e:?}")
                    })?;
                    subscribe(&channel, &cfg)
                };
                // node kept alive on this stack for the subscriber's lifetime
                let (_node, subscriber) = match setup() {
                    Ok(v) => {
                        let _ = setup_tx.send(Ok(()));
                        v
                    }
                    Err(e) => {
                        let _ = setup_tx.send(Err(e));
                        return;
                    }
                };
                let mut view = view;
                while !cancel.is_cancelled() {
                    if !drain(&channel, &subscriber, &mut view, &metrics.received) {
                        std::hint::spin_loop();
                    }
                }
            })
            .wrap_err("failed to spawn receiver thread")?;
        setup_rx
            .recv()
            .wrap_err("receiver thread died during setup")??;
        Ok(hdl)
    }

    /// Deserializing convenience over [`Self::spawn_with_view_handler`]:
    /// `handler` gets an owned `T` (one deserialize per message).
    pub fn spawn_with_handler<T, F>(
        channel: &str,
        cfg: &IpcConfig,
        core: usize,
        cancel: CancellationToken,
        mut handler: F,
    ) -> eyre::Result<JoinHandle<()>>
    where
        T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T>,
        F: FnMut(T) + Send + 'static,
    {
        let label = channel.to_string();
        let deserialize_failures = ReceiverMetrics::new(channel).deserialize_failures;
        Self::spawn_with_view_handler(channel, cfg, core, cancel, move |bytes: &[u8]| {
            match wincode::deserialize::<T>(bytes) {
                Ok(msg) => handler(msg),
                Err(e) => {
                    deserialize_failures.increment(1);
                    tracing::warn!("win-ipc {label}: deserialize failed: {e:?}");
                }
            }
        })
    }

    /// Bridge for tokio consumers; drop-on-full like the UDS paths it replaces.
    pub fn spawn<T>(
        channel: &str,
        cfg: &IpcConfig,
        core: usize,
        cancel: CancellationToken,
    ) -> eyre::Result<(tokio::sync::mpsc::Receiver<T>, JoinHandle<()>)>
    where
        T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T> + Send + 'static,
    {
        use tokio::sync::mpsc::error::TrySendError;
        let (tx, rx) = tokio::sync::mpsc::channel(cfg.buffer_size);
        let channel_full = ReceiverMetrics::new(channel).channel_full;
        // A dropped `rx` closes the channel: cancel so the busy-poll thread
        // stops burning its core deserializing messages nobody will read.
        let handler_cancel = cancel.clone();
        let hdl = Self::spawn_with_handler(channel, cfg, core, cancel, move |msg: T| {
            match tx.try_send(msg) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => channel_full.increment(1),
                Err(TrySendError::Closed(_)) => handler_cancel.cancel(),
            }
        })?;
        Ok((rx, hdl))
    }
}

type NodeAndSubscriber = (
    iceoryx2::node::Node<ipc_threadsafe::Service>,
    Subscriber<ipc_threadsafe::Service, [u8], ()>,
);

fn subscribe(channel: &str, cfg: &IpcConfig) -> eyre::Result<NodeAndSubscriber> {
    let node = create_node().wrap_err_with(|| format!("receiver for channel '{channel}'"))?;
    let service = node
        .service_builder(
            &channel
                .try_into()
                .map_err(|e| eyre::eyre!("invalid channel name '{channel}': {e:?}"))?,
        )
        .publish_subscribe::<[u8]>()
        .subscriber_max_buffer_size(cfg.buffer_size)
        .enable_safe_overflow(true)
        .history_size(0)
        .max_publishers(cfg.max_publishers)
        .max_subscribers(cfg.max_subscribers)
        .open_or_create()
        .map_err(|e| {
            eyre::eyre!(
                "failed to open pub/sub service '{channel}': {e:?} \
                 (peers must use identical IpcConfig QoS settings)"
            )
        })?;
    let subscriber = create_port_with_retry("subscriber", channel, || {
        service.subscriber_builder().create()
    })?;
    Ok((node, subscriber))
}

fn drain<F: FnMut(&[u8])>(
    channel: &str,
    subscriber: &Subscriber<ipc_threadsafe::Service, [u8], ()>,
    view: &mut F,
    received: &Counter,
) -> bool {
    let mut got = false;
    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                got = true;
                received.increment(1);
                view(sample.payload());
            }
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("win-ipc {channel}: receive failed: {e:?}");
                break;
            }
        }
    }
    got
}
