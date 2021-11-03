use std::collections::HashMap;

use crate::events::EventNotifier;
use crate::node::{self, subtree, RunningNode};
use crate::Context;

use super::cleanup::*;
use super::spec::*;

/// Handles the state of the child nodes of a supervision tree.
///
/// The restart logic from the supervision tree is handled in this private type,
/// as this type is responsible of node state and ownership.
///
pub(crate) struct RunningNodes {
    // vector to keep track of the initial node order
    nodes_ix: Vec<String>,
    // hashmap with nodes mapped by names, used for easy lookup
    // of nodes when dealing with errors
    running_nodes: HashMap<String, RunningNode>,
    // cleanup function for the existing running nodes
    cleanup: CleanupFn,
}

impl RunningNodes {
    pub(crate) fn new(input_nodes: Vec<RunningNode>, cleanup: CleanupFn) -> Self {
        let mut nodes_ix = Vec::new();
        let mut running_nodes = HashMap::new();

        for node in input_nodes {
            nodes_ix.push(node.get_name().to_owned());
            running_nodes.insert(node.get_name().to_owned(), node);
        }

        Self {
            nodes_ix,
            running_nodes,
            cleanup,
        }
    }

    pub(crate) async fn handle_node_error(
        &mut self,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
        node_name: &str,
    ) -> Result<(), subtree::TerminationMessage> {
        // Fetch the collection of running nodes and take ownership of the
        // failed node to restart.
        if let Some(running_node) = self.running_nodes.remove(node_name) {
            // Execute the restart logic depending on the RestartStrategy of
            // this node.
            let restart_strategy = running_node.get_restart_strategy().clone();
            match restart_strategy {
                node::Restart::OneForOne => {
                    // Terminate node to get back the original spec. This spec
                    // allows us to create a new running node (effective
                    // restart).
                    let (_, mut node_spec) = running_node.terminate(ev_notifier.clone()).await;

                    // Create a new context with the appropiate parent_name
                    let ctx = Context::new();
                    let ctx = ctx.with_parent_name(parent_name);

                    // Loop on the worker creation until surpassing error
                    // tolerance or the node starts without an error.
                    loop {
                        let result = node_spec
                            .start(
                                ctx.clone(),
                                ev_notifier.clone(),
                                parent_name,
                                parent_chan.clone(),
                            )
                            .await;

                        match result {
                            Err((_start_err, failed_node_spec)) => {
                                // Recover the node_spec to do another iteration
                                // in the loop.
                                node_spec = failed_node_spec;
                                continue;
                            }
                            Ok(running_node) => {
                                // Re-assign ownership of the running node back
                                // to the RunningNodes record.
                                let _ = self
                                    .running_nodes
                                    .insert(node_name.to_owned(), running_node);
                                return Ok(());
                            }
                        }
                    }
                }
                restart => todo!("pending restart strategy implementation: {:?}", restart),
            }
        }

        // In the situation the node is not found in the running_nodes, ignore.
        todo!("handle node is not present in running_nodes")
    }

    /// Iterates over all running nodes in reversed order (relative to the start
    /// order). This function is used when terminating or restarting a
    /// supervision tree.
    ///
    /// Note, a child node having a termination error won't abort the
    /// termination procedure of the remaining running nodes.
    pub(crate) async fn terminate(
        self,
        ev_notifier: EventNotifier,
        parent_name: &str,
    ) -> Result<(), subtree::TerminationMessage> {
        let Self {
            nodes_ix,
            mut running_nodes,
            cleanup,
            ..
        } = self;

        // Accumulate all the termination errors from our child nodes.
        let mut node_termination_errors = Vec::new();

        // Terminate workers in reverse order.
        // TODO: take into account start order option.
        for node_name in nodes_ix.into_iter().rev() {
            if let Some(node) = running_nodes.remove(&node_name) {
                let result = node.terminate(ev_notifier.clone()).await;
                match result {
                    // Note, we do not return the node_specs as we do not need them again to
                    // restart.
                    (Err(worker_err), _node_spec) => {
                        // Append error if child node failed to terminate.
                        node_termination_errors.push(worker_err);
                    }
                    _ => (),
                }
            }
        }

        // Next, cleanup resources allocated by the supervisor
        let cleanup_err = cleanup.call().err();

        // Report an error if the child nodes failed to terminate or there was a
        // cleanup error
        if node_termination_errors.len() > 0 || cleanup_err.is_some() {
            let termination_err = subtree::TerminationMessage::termination_failed(
                parent_name,
                node_termination_errors,
                cleanup_err,
            );
            Err(termination_err)
        } else {
            Ok(())
        }
    }
}

/// Represents the root of a tree of tasks. A Supervisor may have leaf or
/// sub-tree child nodes, where each of the nodes in the tree represent a task
/// that gets automatic restart abilities as soon as the parent supervisor
/// detects an error has occured. A Supervisor will always be generated from a
/// `capataz::Spec`
///
/// Since: 0.0.0
pub struct Root {
    spec: Spec,
    running_subtree: subtree::RunningSubtree,
    ev_notifier: EventNotifier,
}

impl Root {
    pub(crate) fn new(
        spec: Spec,
        running_subtree: subtree::RunningSubtree,
        ev_notifier: EventNotifier,
    ) -> Self {
        Self {
            spec,
            running_subtree,
            ev_notifier,
        }
    }

    /// Executes the termination logic of the supervision tree. This function
    /// will halt execution on the current thread until all nodes on the tree
    /// are terminated in the desired order.
    ///
    /// Since: 0.0.0
    pub async fn terminate(self) -> (Result<(), subtree::TerminationMessage>, Spec) {
        let spec = self.spec;
        // Every subtree restart re-creates all the child nodes, let us ignore
        // the previously created subtree spec.
        let (result, _subtree_spec) = self.running_subtree.terminate(self.ev_notifier).await;
        (result, spec)
    }
}
