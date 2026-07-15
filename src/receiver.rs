use crate::IpcConfig;
use crate::node::{create_node, create_port_with_retry};
use eyre::WrapErr as _;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use metrics::counter;
use std::thread::JoinHandle;
use tokio_util::sync::CancellationToken;
use wincode::SchemaRead;
use wincode::config::DefaultConfig;

pub struct IpcReceiver;

impl IpcReceiver {
    /// Lowest latency: `handler` runs on the polling thread, zero extra hops.
    /// The thread busy-spins pinned to `core` (a dedicated core is required);
    /// pinning or service setup failure fails the spawn.
    pub fn spawn_with_handler<T, F>(
        channel: &str,
        cfg: &IpcConfig,
        core: usize,
        cancel: CancellationToken,
        handler: F,
    ) -> eyre::Result<JoinHandle<()>>
    where
        T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T>,
        F: FnMut(T) + Send + 'static,
    {
        let channel = channel.to_string();
        let cfg = cfg.clone();
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
                let mut handler = handler;
                while !cancel.is_cancelled() {
                    if !drain(&channel, &subscriber, &mut handler) {
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
        let (tx, rx) = tokio::sync::mpsc::channel(cfg.buffer_size);
        let full_label = channel.to_string();
        let hdl = Self::spawn_with_handler(channel, cfg, core, cancel, move |msg: T| {
            if tx.try_send(msg).is_err() {
                counter!("ipc_receiver_channel_full", "channel" => full_label.clone())
                    .increment(1);
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

fn drain<T, F>(
    channel: &str,
    subscriber: &Subscriber<ipc_threadsafe::Service, [u8], ()>,
    handler: &mut F,
) -> bool
where
    T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T>,
    F: FnMut(T),
{
    let mut got = false;
    loop {
        match subscriber.receive() {
            Ok(Some(sample)) => {
                got = true;
                match wincode::deserialize::<T>(sample.payload()) {
                    Ok(msg) => {
                        counter!("ipc_received", "channel" => channel.to_string()).increment(1);
                        handler(msg);
                    }
                    Err(e) => {
                        counter!("ipc_deserialize_failures", "channel" => channel.to_string())
                            .increment(1);
                        tracing::warn!("win-ipc {channel}: deserialize failed: {e:?}");
                    }
                }
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
