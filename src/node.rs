use anyhow::Context as _;
use iceoryx2::config::Config;
use iceoryx2::node::{Node, NodeState};
use iceoryx2::prelude::*;

/// Create a node, sweeping stale resources of dead nodes first so a
/// crash-restart can't be blocked by leaked port slots.
pub(crate) fn create_node() -> anyhow::Result<Node<ipc_threadsafe::Service>> {
    sweep_dead_nodes();
    NodeBuilder::new()
        .create::<ipc_threadsafe::Service>()
        .context("creating iceoryx2 node")
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
pub(crate) fn create_port_with_retry<T, E: std::fmt::Debug>(
    mut create: impl FnMut() -> Result<T, E>,
) -> anyhow::Result<T> {
    match create() {
        Ok(v) => Ok(v),
        Err(first) => {
            sweep_dead_nodes();
            create().map_err(|e| anyhow::anyhow!("port creation failed: {first:?}, then {e:?}"))
        }
    }
}
