use futures::future::{BoxFuture, FutureExt};

use crate::context::Context;
use crate::events::EventNotifier;
use crate::node::{self, leaf};
use crate::notifier;
use crate::supervisor;
use crate::task;

use super::errors::*;
use super::RunningSubtree;

/// Specific instance that reports successful starts or start error in a subtree
type StartNotifier = notifier::StartNotifier<StartError>;

/// Specific instance that reports termination errors in a subtree.
type TerminationNotifier =
    notifier::TerminationNotifier<String, task::TerminationMessage<node::TerminationMessage>>;

pub(crate) struct Spec {
    spec: supervisor::Spec,
    opts: Vec<leaf::Opt>,
    strategy: node::Strategy,
}

impl Spec {
    pub(crate) fn from_running_subtree(
        spec: supervisor::Spec,
        opts: Vec<leaf::Opt>,
        strategy: node::Strategy,
    ) -> Self {
        Self {
            spec,
            opts,
            strategy,
        }
    }

    pub(crate) fn new(spec: supervisor::Spec, opts: Vec<leaf::Opt>) -> Self {
        Self {
            spec,
            opts,
            strategy: node::Strategy::OneForOne,
        }
    }

    pub(crate) fn to_node(self) -> node::Node {
        node::Node(node::NodeSpec::Subtree(self))
    }

    // Executes the `capataz::SupervisorSpec` run logic in a new spawned task.
    pub(crate) fn start(
        self,
        ctx: Context,
        mut ev_notifier: EventNotifier,
        parent_name: String,
        parent_chan: Option<TerminationNotifier>,
    ) -> BoxFuture<'static, Result<RunningSubtree, (StartError, Self)>> {
        // The function must return a boxed future rather than using async fn
        // because this is a co-recursive function; `subtree::Spec::start`
        // relies on `Node::start` which relies on `subtree::Spec::start`.
        //
        // For more details see:
        // https://rust-lang.github.io/async-book/07_workarounds/04_recursion.html
        let Self {
            spec,
            opts,
            strategy,
            ..
        } = self;

        // Clone all the required metadata to spawn a new node without
        // invalidating variables.
        let subtree_name = spec.get_name().to_owned();
        let subtree_parent_name = parent_name.clone();
        let subtree_ev_notifier = ev_notifier.clone();
        let subtree_spec = spec.clone();

        async move {
            // Build task that contains the supervision tree logic.
            let task_spec = task::TaskSpec::new_with_start(
                move |ctx: Context, start_notifier: StartNotifier| {
                    // Move ownership to the FnMut first, otherwise the compiler
                    // complains with E0507.
                    let subtree_spec = subtree_spec.clone();
                    let subtree_ev_notifier = subtree_ev_notifier.clone();
                    let subtree_parent_name = subtree_parent_name.clone();
                    async move {
                        // Finally, run the supervision tree logic.
                        subtree_spec
                            .run(
                                ctx,
                                subtree_ev_notifier,
                                start_notifier,
                                &subtree_parent_name,
                            )
                            .await
                    }
                },
            );

            // Start the supervision tree.
            let runtime_name = node::build_runtime_name(&parent_name, &subtree_name);
            let result = task_spec.start(&ctx, parent_chan).await;

            match result {
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err((task::StartError::StartRecvError(_), _)) => {
                    unreachable!("invalid implementation; supervisors always listen to channel")
                }
                // SAFETY: The only way this error occurs is if we implemented
                // the supervision logic wrong.
                Err((task::StartError::StartTimeoutError(_), _)) => {
                    unreachable!("invalid implementation; supervisors don't have a start timeout")
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err((
                    task::StartError::BusinessLogicFailed(StartError::BuildFailed(build_err)),
                    _task_spec,
                )) => {
                    ev_notifier
                        .supervisor_build_failed(&runtime_name, build_err.clone())
                        .await;
                    let spec = Spec {
                        spec,
                        opts,
                        strategy,
                    };
                    Err((StartError::BuildFailed(build_err), spec))
                }
                // A start error notification was signalled via the provided
                // `start_notifier`, return the error to our creator.
                Err((
                    task::StartError::BusinessLogicFailed(StartError::StartFailed(start_err)),
                    _task_spec,
                )) => {
                    ev_notifier
                        .supervisor_start_failed(&runtime_name, start_err.clone())
                        .await;
                    let spec = Spec {
                        spec,
                        opts,
                        strategy,
                    };
                    Err((StartError::StartFailed(start_err), spec))
                }
                // The supervisor (including it's child nodes) started without
                // any errors
                Ok(running_supervisor) => {
                    ev_notifier.supervisor_started(&runtime_name).await;
                    Ok(RunningSubtree::new(
                        runtime_name,
                        spec,
                        running_supervisor,
                        opts,
                        strategy,
                    ))
                }
            }
        }
        .boxed()
    }

    pub(crate) fn get_name(&self) -> &str {
        self.spec.get_name()
    }
}
