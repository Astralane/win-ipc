use crate::node::{create_node, create_port_with_retry};
use crate::{IpcConfig, event_service_name};
use eyre::WrapErr as _;
use iceoryx2::node::Node;
use iceoryx2::port::notifier::Notifier;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::prelude::*;
use metrics::counter;
use std::marker::PhantomData;
use wincode::SchemaWrite;
use wincode::config::DefaultConfig;

/// Publishes wincode-serialized `T`s into shared memory. Single copy: the
/// message is serialized directly into the loaned shm sample on the caller
/// thread (`ipc_threadsafe` ports synchronize internally). Never blocks — a
/// slow or absent subscriber overwrites oldest.
pub struct IpcSender<T> {
    _node: Node<ipc_threadsafe::Service>,
    publisher: Publisher<ipc_threadsafe::Service, [u8], ()>,
    notifier: Option<Notifier<ipc_threadsafe::Service>>,
    channel: String,
    max_message_size: usize,
    _marker: PhantomData<fn(&T)>,
}

impl<T: SchemaWrite<DefaultConfig, Src = T>> IpcSender<T> {
    pub fn new(channel: &str, cfg: &IpcConfig) -> eyre::Result<Self> {
        let node = create_node().wrap_err_with(|| format!("sender for channel '{channel}'"))?;
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
        let publisher = create_port_with_retry("publisher", channel, || {
            service
                .publisher_builder()
                .initial_max_slice_len(cfg.max_message_size)
                .allocation_strategy(AllocationStrategy::Static)
                .create()
        })?;
        let notifier = if cfg.notify_on_send {
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
            Some(create_port_with_retry("notifier", channel, || {
                event.notifier_builder().create()
            })?)
        } else {
            None
        };
        Ok(Self {
            _node: node,
            publisher,
            notifier,
            channel: channel.to_string(),
            max_message_size: cfg.max_message_size,
            _marker: PhantomData,
        })
    }

    pub fn try_send(&self, msg: &T) -> eyre::Result<()> {
        let size = wincode::serialized_size(msg)
            .map_err(|e| eyre::eyre!("cannot size message for '{}': {e:?}", self.channel))?
            as usize;
        if size > self.max_message_size {
            counter!("ipc_publish_failures", "channel" => self.channel.clone()).increment(1);
            eyre::bail!(
                "message of {size}B exceeds max_message_size {}B on channel '{}'",
                self.max_message_size,
                self.channel
            );
        }
        let mut sample = self.publisher.loan_slice_uninit(size).map_err(|e| {
            eyre::eyre!(
                "failed to loan {size}B sample on '{}': {e:?} \
                 (all loaned samples in use? subscriber holding too many borrows?)",
                self.channel
            )
        })?;
        wincode::serialize_into(sample.payload_mut(), msg)
            .map_err(|e| eyre::eyre!("serialize failed on '{}': {e:?}", self.channel))?;
        // payload fully written by serialize_into (size == serialized_size)
        unsafe { sample.assume_init() }
            .send()
            .map_err(|e| eyre::eyre!("shm send failed on '{}': {e:?}", self.channel))?;
        if let Some(notifier) = &self.notifier {
            let _ = notifier.notify();
        }
        counter!("ipc_sent", "channel" => self.channel.clone()).increment(1);
        Ok(())
    }
}
