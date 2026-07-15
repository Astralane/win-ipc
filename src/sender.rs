use crate::node::{create_node, create_port_with_retry};
use crate::{IpcConfig, event_service_name};
use anyhow::Context as _;
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
    pub fn new(channel: &str, cfg: &IpcConfig) -> anyhow::Result<Self> {
        let node = create_node()?;
        let service = node
            .service_builder(&channel.try_into()?)
            .publish_subscribe::<[u8]>()
            .subscriber_max_buffer_size(cfg.buffer_size)
            .enable_safe_overflow(true)
            .history_size(0)
            .max_publishers(cfg.max_publishers)
            .max_subscribers(cfg.max_subscribers)
            .open_or_create()
            .context("opening pub/sub service")?;
        let publisher = create_port_with_retry(|| {
            service
                .publisher_builder()
                .initial_max_slice_len(cfg.max_message_size)
                .allocation_strategy(AllocationStrategy::Static)
                .create()
        })?;
        let notifier = if cfg.notify_on_send {
            let event = node
                .service_builder(&event_service_name(channel).as_str().try_into()?)
                .event()
                .open_or_create()
                .context("opening event service")?;
            Some(create_port_with_retry(|| {
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

    pub fn try_send(&self, msg: &T) -> anyhow::Result<()> {
        let size = wincode::serialized_size(msg)
            .map_err(|e| anyhow::anyhow!("serialized_size: {e:?}"))? as usize;
        if size > self.max_message_size {
            counter!("ipc_publish_failures", "channel" => self.channel.clone()).increment(1);
            anyhow::bail!("message size {size} exceeds max {}", self.max_message_size);
        }
        let mut sample = self
            .publisher
            .loan_slice_uninit(size)
            .map_err(|e| anyhow::anyhow!("loan: {e:?}"))?;
        wincode::serialize_into(sample.payload_mut(), msg)
            .map_err(|e| anyhow::anyhow!("serialize: {e:?}"))?;
        // payload fully written by serialize_into (size == serialized_size)
        unsafe { sample.assume_init() }
            .send()
            .map_err(|e| anyhow::anyhow!("send: {e:?}"))?;
        if let Some(notifier) = &self.notifier {
            let _ = notifier.notify();
        }
        counter!("ipc_sent", "channel" => self.channel.clone()).increment(1);
        Ok(())
    }
}
