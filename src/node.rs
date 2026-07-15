use eyre::WrapErr as _;
use iceoryx2::config::Config;
use iceoryx2::node::{Node, NodeState};
use iceoryx2::prelude::*;

/// Create a node, sweeping stale resources of dead nodes first so a
/// crash-restart can't be blocked by leaked port slots.
pub(crate) fn create_node() -> eyre::Result<Node<ipc_threadsafe::Service>> {
    sweep_dead_nodes();
    NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .map_err(|e| eyre::eyre!("{e:?}"))
        .wrap_err("failed to create iceoryx2 node (is /dev/shm and /tmp/iceoryx2 writable?)")
}

pub(crate) fn sweep_dead_nodes() {
    let _ = Node::<ipc_threadsafe::Service>::list(Config::global_config(), |node_state| {
        if let NodeState::Dead(view) = node_state {
            if let Err(e) = view.try_remove_stale_resources() {
                tracing::warn!("win-ipc: failed to remove stale resources: {e:?}");
            }
        }
        CallbackProgression::Continue
    });
}

/// Retry `create` once after a dead-node sweep; a crashed peer's port slot may
/// still be occupied until the sweep runs.
pub(crate) fn create_port_with_retry<T, E: core::fmt::Debug>(
    what: &str,
    channel: &str,
    mut create: impl FnMut() -> Result<T, E>,
) -> eyre::Result<T> {
    match create() {
        Ok(v) => Ok(v),
        Err(first) => {
            sweep_dead_nodes();
            create().map_err(|e| {
                eyre::eyre!(
                    "failed to create {what} on channel '{channel}': {first:?} \
                     (retried after dead-node sweep: {e:?}). If this is \
                     ExceedsMaxSupported*, raise max_publishers/max_subscribers in \
                     IpcConfig or check for orphaned peers"
                )
            })
        }
    }
}
