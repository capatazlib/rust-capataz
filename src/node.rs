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

type TerminationNotifier = notifier::TerminationNotifier<task::TerminationError<TerminationError>>;

/// Represents an error reported on leaf or subtree when trying to spawn a
/// task (green thread).
#[derive(Debug, Error)]
pub enum StartError {
    #[error("{0}")]
    Leaf(#[from] leaf::StartError),
    #[error("{0}")]
    Subtree(#[from] subtree::StartError),
}

/// Represents an error reported on leaf or subtree when a spawned task (green
/// thread) is terminated.
#[derive(Debug, Error)]
pub enum TerminationError {
    #[error("{0}")]
    Leaf(#[from] leaf::TerminationError),
    #[error("{0}")]
    Subtree(#[from] subtree::TerminationError),
}

/// Node represents a tree node in a supervision tree, it could either be a
/// Subtree or a Worker
///
/// Since: 0.0.0
pub struct Node(NodeSpec);

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
        runtime_name: &str,
        parent_chan: Option<TerminationNotifier>,
    ) -> Result<RunningNode, StartError> {
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
                let running_leaf = leaf
                    .start(ctx, ev_notifier.clone(), runtime_name, parent_chan)
                    .await?;

                Ok(RunningNode::Leaf(running_leaf))
            }
            NodeSpec::Subtree(subtree) => {
                // Delegate to internal subtree start method, and transform the
                // subtree start error into a node start error.
                let running_subtree = subtree
                    .start(
                        ctx,
                        ev_notifier.clone(),
                        runtime_name.to_owned(),
                        parent_chan,
                    )
                    .await?;
                Ok(RunningNode::Subtree(running_subtree))
            }
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
    /// Executes the termination logic of this running node (leaf or subtree).
    pub(crate) async fn terminate(
        self,
        ev_notifier: EventNotifier,
    ) -> Result<(), TerminationError> {
        // Delegate the termination logic to leaf and subtree types.
        match self {
            RunningNode::Leaf(leaf) => leaf.terminate(ev_notifier).await?,
            RunningNode::Subtree(subtree) => subtree.terminate(ev_notifier).await?,
        }
        Ok(())
    }
}
