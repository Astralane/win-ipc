use crate::node::{create_node, create_port_with_retry};
use crate::{IpcConfig, PollMode, event_service_name};
use eyre::WrapErr as _;
use iceoryx2::port::listener::Listener;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use metrics::counter;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use wincode::SchemaRead;
use wincode::config::DefaultConfig;

const EVENT_WAIT_CYCLE: Duration = Duration::from_millis(100);

pub struct IpcReceiver;

impl IpcReceiver {
    /// Lowest latency: `handler` runs on the polling thread, zero extra hops.
    pub fn spawn_with_handler<T, F>(
        channel: &str,
        cfg: &IpcConfig,
        cancel: CancellationToken,
        handler: F,
    ) -> eyre::Result<JoinHandle<()>>
    where
        T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T>,
        F: FnMut(T) + Send + 'static,
    {
        let channel = channel.to_string();
        let cfg = cfg.clone();
        std::thread::Builder::new()
            .name(format!("win-ipc-{channel}"))
            .spawn(move || {
                if let Some(core) = cfg.core_affinity {
                    if let Err(e) = affinity::set_thread_affinity([core]) {
                        tracing::warn!("win-ipc: failed to pin core {core}: {e:?}");
                    }
                }
                if let Err(e) = run_receive_loop(&channel, &cfg, cancel, handler) {
                    tracing::error!("win-ipc receiver {channel} exited: {e:?}");
                }
            })
            .wrap_err("failed to spawn receiver thread")
    }

    /// Bridge for tokio consumers; drop-on-full like the UDS paths it replaces.
    pub fn spawn<T>(
        channel: &str,
        cfg: &IpcConfig,
        cancel: CancellationToken,
    ) -> eyre::Result<(tokio::sync::mpsc::Receiver<T>, JoinHandle<()>)>
    where
        T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(cfg.buffer_size);
        let full_label = channel.to_string();
        let hdl = Self::spawn_with_handler(channel, cfg, cancel, move |msg: T| {
            if tx.try_send(msg).is_err() {
                counter!("ipc_receiver_channel_full", "channel" => full_label.clone())
                    .increment(1);
            }
        })?;
        Ok((rx, hdl))
    }
}

fn run_receive_loop<T, F>(
    channel: &str,
    cfg: &IpcConfig,
    cancel: CancellationToken,
    mut handler: F,
) -> eyre::Result<()>
where
    T: for<'de> SchemaRead<'de, DefaultConfig, Dst = T>,
    F: FnMut(T),
{
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
    let subscriber: Subscriber<ipc_threadsafe::Service, [u8], ()> =
        create_port_with_retry("subscriber", channel, || {
            service.subscriber_builder().create()
        })?;

    let listener: Option<Listener<ipc_threadsafe::Service>> = match cfg.poll_mode {
        PollMode::BusySpin => None,
        _ => {
            let event = node
                .service_builder(
                    &event_service_name(channel)
                        .as_str()
                        .try_into()
                        .map_err(|e| eyre::eyre!("invalid event service name: {e:?}"))?,
                )
                .event()
                .open_or_create()
                .map_err(|e| eyre::eyre!("failed to open event service '{channel}/evt': {e:?}"))?;
            Some(create_port_with_retry("listener", channel, || {
                event.listener_builder().create()
            })?)
        }
    };

    while !cancel.is_cancelled() {
        let drained = drain(channel, &subscriber, &mut handler);
        if drained {
            continue;
        }
        match cfg.poll_mode {
            PollMode::BusySpin => std::hint::spin_loop(),
            PollMode::SpinThenWait { spin } => {
                let deadline = Instant::now() + spin;
                let mut got = false;
                while Instant::now() < deadline {
                    if drain(channel, &subscriber, &mut handler) {
                        got = true;
                        break;
                    }
                    std::hint::spin_loop();
                }
                if !got {
                    if let Some(l) = &listener {
                        let _ = l.timed_wait_all(|_| {}, EVENT_WAIT_CYCLE);
                    }
                }
            }
            PollMode::Event { cycle } => {
                if let Some(l) = &listener {
                    let _ = l.timed_wait_all(|_| {}, cycle);
                }
            }
        }
    }
    Ok(())
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
