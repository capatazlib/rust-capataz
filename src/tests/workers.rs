use crate::context::Context;
use crate::notifier;
use crate::{Node, Worker, WorkerOpt};
use anyhow::anyhow;

pub(crate) mod worker_trigger;

type StartNotifier = notifier::StartNotifier<anyhow::Error>;

/// Creates a supervised worker that will block until a termination signal is
/// given via it's Context value (supervisor termination).
pub(crate) fn wait_done_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
    Worker::new(name, opts, |ctx: Context| async move {
        let _ = ctx.done().await;
        Ok(())
    })
}

/// Creates a supervised worker that will always fail to start.
pub(crate) fn fail_start_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
    Worker::new_with_start(
        name,
        opts,
        |_ctx: Context, start_notifier: StartNotifier| async move {
            let err = anyhow!("failing worker");
            start_notifier.failed(err);
            Ok(())
        },
    )
}

/// Creates a supervised worker that will never signal a successful start.
pub(crate) fn never_start_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
    Worker::new_with_start(
        name,
        opts,
        |ctx: Context, start_notifier: StartNotifier| async move {
            // wait for termination of the supervision tree
            let _ = ctx.done().await;
            // use the start_notifer in the body of the function to avoid
            // already dropped errors
            start_notifier.success();
            Ok(())
        },
    )
}

/// Creates a supervised worker that will never respect termination signals from
/// its supervisor.
pub(crate) fn never_terminate_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
    Worker::new(name, opts, |_ctx: Context| async move {
        futures::future::pending().await
    })
}
