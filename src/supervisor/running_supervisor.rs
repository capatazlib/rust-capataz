use either::Either;
use std::collections::HashMap;

use crate::events::EventNotifier;
use crate::node::{self, subtree, RunningNode, Strategy};
use crate::supervisor::opts::StartOrder;
use crate::task::Restart;
use crate::Context;

use super::cleanup::*;
use super::spec::*;

/// Handles the state of the child nodes of a supervision tree.
///
/// The restart logic from the supervision tree is handled in this private type,
/// as this type is responsible of node state and ownership.
///
pub(crate) struct RunningNodes {
    // indicates how nodes should get started/terminated
    start_order: StartOrder,
    // restart indicates what is the restart strategy for the nodes
    strategy: Strategy,
    // vector to keep track of the initial node order
    nodes_ix: Vec<node::RuntimeName>,
    // hashmap with nodes mapped by names, used for easy lookup
    // of nodes when dealing with errors
    running_nodes: HashMap<node::RuntimeName, RunningNode>,
    // cleanup function for the existing running nodes
    cleanup: CleanupFn,
}

impl RunningNodes {
    pub(crate) fn new(
        start_order: StartOrder,
        strategy: Strategy,
        input_nodes: Vec<RunningNode>,
        cleanup: CleanupFn,
    ) -> Self {
        let mut nodes_ix = Vec::new();
        let mut running_nodes = HashMap::new();

        for node in input_nodes {
            nodes_ix.push(node.get_name().to_owned());
            running_nodes.insert(node.get_name().to_owned(), node);
        }

        Self {
            start_order,
            strategy,
            nodes_ix,
            running_nodes,
            cleanup,
        }
    }

    async fn one_for_all_restart(
        &mut self,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
        node_name: &str,
        prev_running_node: RunningNode,
    ) -> Result<(), subtree::TerminationMessage> {
        // Terminate node to get back the original spec. This spec allows us to
        // create a new running node (effective restart).
        let (_, node_spec) = prev_running_node.terminate(ev_notifier.clone()).await;

        // Build a new hashmap of Node specs to recreate them
        let mut node_specs = HashMap::new();
        node_specs.insert(node_name, node_spec);

        // Accumulate all the termination errors from our child nodes.
        let mut node_termination_errors = Vec::new();

        // Get the iterator with desired order
        let node_names = match self.start_order {
            StartOrder::LeftToRight => Either::Left(self.nodes_ix.iter().rev()),
            StartOrder::RightToLeft => Either::Right(self.nodes_ix.iter()),
        };

        // Modify the internal state of the parent_chan to avoid receiving
        // notifications on child node termination
        parent_chan.skip_notifications();

        // Iterate over all running nodes
        for node_name in node_names {
            // Take ownership of the running node to terminate and (later)
            // replace it with a new node.
            if let Some(node) = self.running_nodes.remove(node_name) {
                let result = node.terminate(ev_notifier.clone()).await;
                match result {
                    // Note, we do not return the node_specs as we do not need them again to
                    // restart.
                    (Err(worker_err), _node_spec) => {
                        // Append error if child node failed to terminate.
                        node_termination_errors.push(worker_err);
                    }
                    (_, node_spec) => {
                        // accumulate the node specs to start them later
                        node_specs.insert(node_name, node_spec);
                    }
                }
            }
        }

        // Now that all terminations have been triggered, modify again the
        // internal state of the parent_chan to resume the regular behavior
        parent_chan.resume_notifications();

        // Report an error if the child nodes failed to terminate or there was a
        // cleanup error
        if node_termination_errors.len() > 0 {
            let termination_err = subtree::TerminationMessage::termination_failed(
                parent_name,
                node_termination_errors,
                None,
            );
            // Restore listening of terminations
            Err(termination_err)
        } else {
            // Otherwise, commence the start children node procedure

            // Create a new context with the appropiate parent_name
            let ctx = Context::new();
            let ctx = ctx.with_parent_name(parent_name);

            //
            let node_names = match self.start_order {
                StartOrder::LeftToRight => Either::Left(self.nodes_ix.iter()),
                StartOrder::RightToLeft => Either::Right(self.nodes_ix.iter().rev()),
            };

            // Perform start of previusly terminated nodes
            for node_name in node_names {
                // Take ownership of the Node to start it
                let node_spec = node_specs.remove(node_name as &str).unwrap();

                // Start node to get a RunningNode
                let result = node_spec
                    .start(
                        ctx.clone(),
                        ev_notifier.clone(),
                        parent_name,
                        parent_chan.clone(),
                    )
                    .await;

                match result {
                    Err((start_err, _failed_node_spec)) => {
                        // Report an error indicating that there was a restart
                        // failure
                        return Err(subtree::TerminationMessage::restart_failed(start_err));
                    }
                    Ok(running_node) => {
                        // Re-assign ownership of the running node back
                        // to the RunningNodes record.
                        let _ = self
                            .running_nodes
                            .insert(node_name.to_owned(), running_node);
                    }
                }
            }
            // Restore listening of terminations
            Ok(())
        }
    }

    async fn one_for_one_restart(
        &mut self,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
        node_name: &str,
        prev_running_node: RunningNode,
    ) -> Result<(), subtree::TerminationMessage> {
        // Terminate node to get back the original spec. This spec
        // allows us to create a new running node (effective
        // restart).
        let (_, mut node_spec) = prev_running_node.terminate(ev_notifier.clone()).await;

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

    pub(crate) async fn handle_node_termination(
        &mut self,
        was_err: bool,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
        strategy: Strategy,
        node_name: &str,
    ) -> Result<(), subtree::TerminationMessage> {
        // Fetch the collection of running nodes and take ownership of the
        // failed node to restart.
        if let Some(running_node) = self.running_nodes.remove(node_name) {
            let should_restart = match running_node.get_restart() {
                Restart::Permanent => true,
                Restart::Transient => was_err,
                Restart::Temporary => false,
            };

            // short-circuit if we should not restart the subtree
            if !should_restart {
                return Ok(());
            }

            // Execute the restart logic depending on the RestartStrategy of
            // this node.
            match strategy {
                node::Strategy::OneForOne => {
                    self.one_for_one_restart(
                        ev_notifier,
                        parent_name,
                        parent_chan,
                        node_name,
                        running_node,
                    )
                    .await
                }
                node::Strategy::OneForAll => {
                    self.one_for_all_restart(
                        ev_notifier,
                        parent_name,
                        parent_chan,
                        node_name,
                        running_node,
                    )
                    .await
                }
                restart => todo!("pending restart strategy implementation: {:?}", restart),
            }
        } else {
            // In the situation the node is not found in the running_nodes, ignore.
            todo!("handle node is not present in running_nodes")
        }
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
            start_order,
            nodes_ix,
            mut running_nodes,
            cleanup,
            ..
        } = self;

        // Accumulate all the termination errors from our child nodes.
        let mut node_termination_errors = Vec::new();

        let nodes_it = match start_order {
            StartOrder::LeftToRight => Either::Left(nodes_ix.iter().rev()),
            StartOrder::RightToLeft => Either::Right(nodes_ix.iter()),
        };

        for node_name in nodes_it {
            if let Some(node) = running_nodes.remove(node_name) {
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
pub struct Supervisor {
    spec: Spec,
    running_subtree: subtree::RunningSubtree,
    ev_notifier: EventNotifier,
}

impl Supervisor {
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

    /// Blocks the current thread, waiting for the supervision tree to
    /// terminate.
    ///
    /// Since: 0.0.0
    pub async fn wait(self) -> (Result<(), subtree::TerminationMessage>, Spec) {
        let spec = self.spec;
        // Every subtree restart re-creates all the child nodes, let us ignore
        // the previously created subtree spec.
        let (result, _subtree_spec) = self.running_subtree.wait(self.ev_notifier).await;
        (result, spec)
    }
}
