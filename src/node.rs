use thiserror::Error;

use crate::context::Context;
use crate::events::EventNotifier;
use crate::notifier;
use crate::task;

/// Contains the types and logic to create, start and terminate leaf nodes
/// (workers) in the supervision tree.
pub(crate) mod leaf;
/// Contains the types and logic to create, start and terminate subtree nodes
/// (sub-supervisor) in the supervision tree.
pub(crate) mod root;
/// Contains the types and logic to create, start and terminate the root node of
/// the supervision tree.
pub(crate) mod subtree;

////////////////////////////////////////////////////////////////////////////////

type TerminationNotifier =
    notifier::TerminationNotifier<String, task::TerminationMessage<TerminationMessage>>;

/// Represents an error reported on leaf or subtree when trying to spawn a
/// task (green thread).
#[derive(Debug, Error)]
pub enum StartError {
    #[error("{0}")]
    Leaf(#[from] leaf::StartError),
    #[error("{0}")]
    Subtree(#[from] subtree::StartError),
}

impl StartError {
    pub fn get_runtime_name(&self) -> &str {
        todo!("get_runtime_name for start error")
    }
}

/// Represents an error reported on leaf or subtree when a spawned task (green
/// thread) is terminated.
#[derive(Debug, Error)]
pub enum TerminationMessage {
    #[error("{0}")]
    Leaf(#[from] leaf::TerminationMessage),
    #[error("{0}")]
    Subtree(#[from] subtree::TerminationMessage),
    #[error("restart error: {0}")]
    RestartError(StartError),
}

impl TerminationMessage {
    pub fn get_cause_err(self) -> anyhow::Error {
        match self {
            TerminationMessage::Leaf(termination_err) => termination_err.get_cause_err(),
            TerminationMessage::Subtree(termination_err) => anyhow::Error::new(termination_err),
            TerminationMessage::RestartError(start_err) => anyhow::Error::new(start_err),
        }
    }

    pub fn get_runtime_name(&self) -> &str {
        match &self {
            TerminationMessage::Leaf(termination_err) => termination_err.get_runtime_name(),
            TerminationMessage::Subtree(termination_err) => termination_err.get_runtime_name(),
            TerminationMessage::RestartError(start_err) => start_err.get_runtime_name(),
        }
    }

    pub(crate) async fn notify_runtime_error(&self, mut ev_notifier: EventNotifier) {
        match &self {
            TerminationMessage::Leaf(leaf::TerminationMessage::RuntimeFailed(err)) => {
                ev_notifier
                    .worker_runtime_failed(err.get_runtime_name(), err.clone())
                    .await
            }
            TerminationMessage::Subtree(subtree::TerminationMessage::ToManyRestarts(
                to_many_restarts_err,
            )) => {
                ev_notifier
                    .supervisor_restarted_to_many_times(
                        to_many_restarts_err.get_runtime_name(),
                        to_many_restarts_err.clone(),
                    )
                    .await
            }
            _ => todo!("pending implementation: {:?}", self),
        }
    }
}

/// BLAH BLAH BLAH
#[derive(Clone, Debug)]
pub enum Restart {
    /// BLAH
    OneForOne,
    /// BLAH
    OneForAll,
    /// BLAH
    RestForOne,
}

/// Node represents a tree node in a supervision tree, it could either be a
/// Subtree or a Worker
///
/// Since: 0.0.0
pub struct Node(NodeSpec);

impl Node {
    pub(crate) async fn start(
        self,
        ctx: Context,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: TerminationNotifier,
    ) -> Result<RunningNode, (StartError, Node)> {
        self.0
            .start(ctx, ev_notifier, parent_name, Some(parent_chan))
            .await
            .map_err(|(start_err, spec_node)| (start_err, Node(spec_node)))
    }
}

/// Represents the specification of a node in the supervision tree. This type
/// allows the unification of subtrees and leafs specifications.
enum NodeSpec {
    Leaf(leaf::Spec),
    Subtree(subtree::Spec),
}

impl NodeSpec {
    /// Executes the bootstrap logic for this node in the supervision tree.
    pub async fn start(
        self,
        ctx: Context,
        ev_notifier: EventNotifier,
        parent_name: &str,
        parent_chan: Option<TerminationNotifier>,
    ) -> Result<RunningNode, (StartError, NodeSpec)> {
        match self {
            NodeSpec::Leaf(leaf) => {
                // Ensure we have a parent_chan when this start method is
                // called.
                //
                // SAFETY: The public API does not allow the execution of the
                // start method in nodes, so if a termination notifier is not
                // given by the caller, we have an implementation mistake.
                let parent_chan =
                    parent_chan.expect("invalid implementation; leaf node received no parent_chan");

                // Delegate to internal leaf start method, and transform the
                // leaf start error into a node start error.
                leaf.start(ctx, ev_notifier.clone(), parent_name, parent_chan)
                    .await
                    .map(RunningNode::Leaf)
                    .map_err(|(start_err, leaf)| {
                        (StartError::Leaf(start_err), NodeSpec::Leaf(leaf))
                    })
            }
            NodeSpec::Subtree(subtree) => {
                // Delegate to internal subtree start method, and transform the
                // subtree start error into a node start error.
                subtree
                    .start(
                        ctx,
                        ev_notifier.clone(),
                        parent_name.to_owned(),
                        parent_chan,
                    )
                    .await
                    .map(RunningNode::Subtree)
                    .map_err(|(start_err, subtree)| {
                        (StartError::Subtree(start_err), NodeSpec::Subtree(subtree))
                    })
            }
        }
    }

    pub fn get_restart_strategy(&self) -> &Restart {
        match self {
            NodeSpec::Leaf(leaf_spec) => leaf_spec.get_restart_strategy(),
            NodeSpec::Subtree(subtree_spec) => subtree_spec.get_restart_strategy(),
        }
    }
}

/// Represents the runtime of a node in the supervision tree. A value of this
/// type represents a future task running on a green thread.
///
/// The Node's `start` and `terminate` (both on and `Leaf`, `Subtree`) are the
/// ones responsible of interacting with the event notification system.
///
/// This type allows the unification of runtime representation for subtrees and
/// leafs.
#[derive(Debug)]
pub(crate) enum RunningNode {
    Leaf(leaf::RunningLeaf),
    Subtree(subtree::RunningSubtree),
}

impl RunningNode {
    pub(crate) fn get_runtime_name(&self) -> &str {
        match self {
            Self::Leaf(leaf) => leaf.get_runtime_name(),
            Self::Subtree(subtree) => subtree.get_runtime_name(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Self::Leaf(leaf) => leaf.get_name(),
            Self::Subtree(subtree) => subtree.get_name(),
        }
    }
    /// Executes the termination logic of this running node (leaf or subtree).
    pub(crate) async fn terminate(
        self,
        ev_notifier: EventNotifier,
    ) -> (Result<(), TerminationMessage>, Node) {
        // Delegate the termination logic to leaf and subtree types.
        match self {
            RunningNode::Leaf(leaf) => {
                let (result, leaf_spec) = leaf.terminate(ev_notifier).await;
                let result = result.map_err(TerminationMessage::Leaf);
                (result, Node(NodeSpec::Leaf(leaf_spec)))
            }
            RunningNode::Subtree(subtree) => {
                let (result, subtree_spec) = subtree.terminate(ev_notifier).await;
                let result = result.map_err(TerminationMessage::Subtree);
                (result, Node(NodeSpec::Subtree(subtree_spec)))
            }
        }
    }

    pub(crate) fn get_restart_strategy(&self) -> &Restart {
        match self {
            RunningNode::Leaf(leaf) => leaf.get_restart_strategy(),
            RunningNode::Subtree(subtree) => subtree.get_restart_strategy(),
        }
    }
}

pub(crate) fn to_node_name(runtime_name: &str) -> String {
    match runtime_name.split("/").last() {
        Some(item) => item.to_owned(),
        None => panic!("invalid runtime_name given: {}", runtime_name),
    }
}

pub(crate) fn build_runtime_name(parent_name: &str, name: &str) -> String {
    format!("{}/{}", parent_name, name)
}
