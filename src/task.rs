use std::fmt;
use std::time::Duration;

use futures::future::{self, BoxFuture};
use futures::FutureExt;
use lazy_static::lazy_static;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use tokio::time;

use crate::context::Context;
use crate::notifier::{self, StartNotifier};

////////////////////////////////////////////////////////////////////////////////

// TODO: replace with OnceCell
lazy_static! {
    /// Default timeout duration for TaskSpec bootstrap procedure
    static ref TASK_START_TIMEOUT: Duration = Duration::from_secs(1);
}

/// Error that can be reported back by a RunningTask when it is starting.
#[derive(Debug, Error)]
pub enum StartError<E>
where
    E: fmt::Debug + fmt::Display,
{
    /// Returned when the task did not get started after an specified timeout
    /// duration.
    #[error("task took to long to get started")]
    StartTimeoutError(time::error::Elapsed),
    /// Returned when the RunningTask implementation did not make use of the given
    /// `StartNotifier` to signal a RunningTask started successfully or failed to
    /// start.
    #[error("task routine did not notify start success or failure")]
    StartRecvError(oneshot::error::RecvError),
    /// Returned when the RunningTask implementation invokes a `StartNotifier#failed`
    /// method.
    #[error("worker start failed: {0}")]
    BusinessLogicFailed(E),
}

/// A value that indicates how the parent supervisor will handle the stoppping
/// of a `RunningTask`.
#[derive(Debug, Clone)]
pub enum Shutdown {
    /// A `Shutdown` value that specifies the parent supervisor must wait
    /// indefinitely for the task task to stop executing.
    Indefinitely,
    /// A `Shutdown` value that indicates the time that the supervisor will wait
    /// before "force-killing" a RunningTask task.
    Timeout(Duration),
}

/// A value that indicates how the parent supervisor will handle the starting of
/// a `Task`.
#[derive(Debug, Clone)]
pub enum Startup {
    /// A `Shutdown` value that specifies the parent supervisor must wait
    /// indefinitely for the task to start executing.
    Indefinitely,
    /// A `Shutdown` value that indicates the time that the supervisor will wait
    /// before halting the bootstrap of a `Task`.
    Timeout(Duration),
}

/// Represents a task specification; it serves as a template for the
/// construction of tasks.
pub struct TaskSpec<A, SE, TE> {
    name: String,
    startup: Startup,
    shutdown: Shutdown,
    routine:
        Box<dyn (FnOnce(Context, StartNotifier<SE>) -> BoxFuture<'static, Result<A, TE>>) + Send>,
}

/// Error that can be reported back when terminating a RunningTask
#[derive(Debug, Error)]
pub enum TerminationError<E>
where
    E: fmt::Debug,
{
    /// Indicates the task did not respect the shutdown mechanism and had to
    /// be force-killed.
    #[error("task was hard killed after timeout")]
    TaskForcedKilled,
    /// Indicates the task was cancelled.
    #[error("task was aborted")]
    TaskAborted,
    /// Indicates the task had an error before it terminated.
    #[error("task runtime failed: {0:?}")]
    TaskFailed(E),
    /// Indicates the task had a panic while on runtime.
    #[error("task panicked at runtime")]
    TaskPanic,
    /// Returned when the failure has already been reported to error listener
    #[error("should never see this error message")]
    TaskFailureNotified,
}

/// Runtime representation of a TaskSpec.
#[derive(Debug)]
pub struct RunningTask<A, TE>
where
    TE: fmt::Debug,
{
    runtime_name: String,
    join_handle: JoinHandle<Result<A, TerminationError<TE>>>,
    termination_handle: future::AbortHandle,
    kill_handle: future::AbortHandle,

    // static information
    name: String,
    shutdown: Shutdown,
}

impl<A, TE> RunningTask<A, TE>
where
    TE: fmt::Debug,
{
    /// Internal implementation of the wait logic used in both the `wait` and
    /// `terminate` methods.
    async fn wait_handle(
        join_handle: JoinHandle<Result<A, TerminationError<TE>>>,
    ) -> Result<A, TerminationError<TE>> {
        // await for the RunningTask routine result; the result is going to be
        // wrapped by a Result created from the JoinHandle API.
        let join_result = join_handle.await;
        match join_result {
            // JoinHandle Error.
            Err(join_error) => {
                if join_error.is_panic() {
                    // Caused when the task panicked.
                    Err(TerminationError::TaskPanic)
                } else {
                    // The `JoinHandle` was dropped or the `JoinHandle.abort`
                    // method was called explicitly. The current implementation
                    // doesn't allow this, so this branch cannot happen.
                    unreachable!("RunningTask's JoinHandle was used in an unexpected way")
                }
            }
            // The RunningTask routine finished with an error.
            Ok(Err(err)) => Err(err),
            // The RunningTask routine finished without errors.
            Ok(Ok(result)) => Ok(result),
        }
    }

    /// Waits for the task task to finish indefinitely.
    pub(crate) async fn wait(self) -> Result<A, TerminationError<TE>> {
        Self::wait_handle(self.join_handle).await
    }

    /// Executes the abort logic of the routine and then waits for a specified
    /// duration of time before it kills the task.
    pub(crate) async fn terminate(self) -> Result<A, TerminationError<TE>> {
        let Self {
            kill_handle,
            join_handle,
            termination_handle,
            shutdown,
            ..
        } = self;

        // Signal the context.done() inside the task to return so that the task
        // starts to shutdown.
        termination_handle.abort();

        // Handle the JoinHandle API result once for Indefinitely and Timeout
        // shutdown branches.
        let wait_result = Self::wait_handle(join_handle);

        match shutdown {
            // Wait for a duration of time
            Shutdown::Timeout(wait_duration) => {
                // Create context value with the shutdown timeout to do a select
                // between the JoinHandle await and the context timeout.
                let ctx = Context::new().with_timeout(wait_duration);
                tokio::select! {
                    _ = ctx.done() => {
                        // Timeout duration has been reached, force-kill the
                        // routine.
                        kill_handle.abort();
                        Err(TerminationError::TaskForcedKilled)
                    }
                    join_result = wait_result => {
                        // Task finished before the timeout, return result
                        join_result
                    }
                }
            }
            // Client doesn't care if this task takes time to terminate.
            Shutdown::Indefinitely => wait_result.await,
        }
    }

    pub(crate) fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }
}

/// Builds a `RunningTask` runtime name.
pub(crate) fn build_runtime_name(parent_name: &str, name: &str) -> String {
    format!("{}/{}", parent_name, name)
}

type TerminationNotifier<TE> = notifier::TerminationNotifier<TerminationError<TE>>;

impl<A, SE, TE> TaskSpec<A, SE, TE>
where
    A: Send + Sync + 'static,
    SE: fmt::Debug + fmt::Display + Send + Sync + 'static,
    TE: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    /// Similar to `new`, with the addition of passing an extra argument to the
    /// start function, a `StartNotifier` value.
    ///
    /// ### The StartNotifier argument
    ///
    /// Sometimes you want to consider a routine started after certain
    /// initialization is done; like doing a read from a Database or API, or
    /// when some socket is bound, etc. The `StartNotifier` value allows the
    /// spawned task routine to signal when it has initialized.
    ///
    /// It is essential to call the API from the `StartNotifier` function in
    /// your business logic as soon as you consider the task is initialized,
    /// otherwise the parent supervisor spawning this task will fail after a
    /// timeout.
    ///
    /// ### Report a start error with the StartNotifier
    ///
    /// If for some reason, a task is not able to start correctly (e.g. DB
    /// connection fails, network is kaput, etc.), the task may call the
    /// `StartNotifier#failed` function with the impending error as a parameter.
    /// This call will cause the whole supervision system start procedure to
    /// abort and fail fast.
    ///
    pub fn new_with_start<S, F, O>(name: S, routine: F) -> Self
    where
        S: Into<String>,
        F: (FnOnce(Context, StartNotifier<SE>) -> O) + Send + 'static,
        O: future::Future<Output = Result<A, TE>> + FutureExt + Send + Sized + 'static,
    {
        let routine =
            move |ctx: Context, on_start: StartNotifier<SE>| routine(ctx, on_start).boxed();
        TaskSpec {
            name: name.into(),
            startup: Startup::Indefinitely,
            shutdown: Shutdown::Indefinitely,
            routine: Box::new(routine),
            // restart: Restart::Permanent,
        }
    }

    /// Specifies how long a client API is willing to wait for the termination
    /// of this task.
    pub fn with_shutdown(&mut self, shutdown: Shutdown) {
        self.shutdown = shutdown;
    }

    /// Specifies how long a client API is willing to wait for the start of this
    /// task.
    pub fn with_startup(&mut self, startup: Startup) {
        self.startup = startup;
    }

    /// Spawns a new task that executes this `TaskSpec`'s routine `Future`.
    ///
    /// ### Blocking on start function
    ///
    /// This function will block and wait until a notification from the start
    /// function (executing on a newly spawned routine) is received; this
    /// notification indicates if the start was successful or not.
    ///
    /// This blocking is necessary to ensure that supervised trees get spawned
    /// in the specified/expected order.
    ///
    pub(crate) async fn start(
        self,
        parent_ctx: &Context,
        parent_name: &str,
        parent_chan: Option<TerminationNotifier<TE>>,
    ) -> Result<RunningTask<A, TE>, StartError<SE>> {
        let Self {
            name,
            routine,
            startup,
            shutdown,
            ..
        } = self;

        let runtime_name = build_runtime_name(parent_name, &name);

        // Create the notification channel that allow users of this API to
        // signal capataz that the task started.
        let (start_sx, start_rx) = oneshot::channel::<Result<(), SE>>();
        let start_notifier = StartNotifier::from_oneshot(start_sx);

        // Allow the capataz API to signal a cancellation of the task via our
        // context API.
        let (ctx, termination_handle) = Context::with_cancel(parent_ctx);
        let task_routine = routine(ctx, start_notifier);
        let (task_routine, kill_handle) = futures::future::abortable(task_routine);

        // Create an intermediary future that flattens the result tree.
        let task_routine = async move {
            use TerminationError::*;
            // start_notifier is invoked internally in the task_routine.
            let task_result = task_routine.await;
            match task_result {
                Err(_) => {
                    if let Some(ref parent_chan) = &parent_chan {
                        parent_chan.report(TaskAborted).await?;
                    }
                    Err(TaskAborted)
                }
                Ok(Err(err)) => {
                    if let Some(ref parent_chan) = &parent_chan {
                        parent_chan.report(TaskFailed(err)).await?;
                        Err(TaskFailureNotified)
                    } else {
                        Err(TaskFailed(err))
                    }
                }
                Ok(Ok(result)) => Ok(result),
            }
        };

        // Perform the future asynchronously in a new task.
        let join_handle = task::spawn(task_routine);

        use StartError::*;
        // Wait for the result of the task via the notification channel.
        let start_result = match startup {
            Startup::Indefinitely => start_rx.await,
            Startup::Timeout(start_timeout) => {
                match time::timeout(start_timeout, start_rx).await {
                    // RunningTask took to long to get started. Short-circuit
                    // with a timeout error.
                    Err(start_timeout_err) => return Err(StartTimeoutError(start_timeout_err)),
                    Ok(start_result) => start_result,
                }
            }
        };

        match start_result {
            // Oneshot API failed to receive message.
            Err(start_sender_err) => Err(StartRecvError(start_sender_err)),
            // API client signals a start error.
            Ok(Err(task_start_err)) => Err(BusinessLogicFailed(task_start_err)),
            // Everything went Ok.
            Ok(Ok(())) => Ok(RunningTask {
                runtime_name,
                join_handle,
                termination_handle,
                kill_handle,
                name,
                shutdown,
            }),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub(crate) fn build_runtime_name(&self, parent_name: &str) -> String {
        format!("{}/{}", parent_name, self.name)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use tokio::sync::mpsc;
    use tokio::time;

    use crate::context::*;
    use crate::notifier::TerminationNotifier;
    use crate::task::{self, StartError, TerminationError};

    type TaskSpec = task::TaskSpec<(), anyhow::Error, anyhow::Error>;

    #[tokio::test]
    async fn test_task_start_ok() {
        let task: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                notify.success();
                let _ = ctx.done().await;
                Ok(())
            },
        );
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();

        let result = task.start(&ctx, "caller", Some(sender)).await;
        match result {
            Ok(task) => {
                let _ = task.terminate();
            }
            Err(err) => {
                assert!(false, "expected ok, got error {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_task_start_with_notify_failed_routine() {
        let task: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |_ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                let err = anyhow!("task start failure");
                notify.failed(err);
                Ok(())
            },
        );
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();

        let result = task.start(&ctx, "caller", Some(sender)).await;
        match result {
            Ok(task) => {
                let _ = task.terminate();
                assert!(false, "expected start error; got task")
            }
            Err(StartError::BusinessLogicFailed(err)) => {
                assert_eq!("task start failure", format!("{}", err))
            }
            Err(err) => {
                assert!(false, "expected client error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_task_start_timeout_err() {
        time::pause();

        let mut task: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                let _ = ctx.done().await;
                notify.success();
                Ok(())
            },
        );
        task.with_startup(task::Startup::Timeout(Duration::from_secs(1)));
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();
        let result_fut = task.start(&ctx, "caller", Some(sender));

        time::advance(time::Duration::from_secs(5)).await;

        let result = result_fut.await;
        match result {
            Ok(task) => {
                let _ = task.terminate();
                assert!(false, "expected start error; got task")
            }
            Err(StartError::StartTimeoutError(_)) => (),
            Err(err) => {
                assert!(false, "expected timeout error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_task_start_recv_err() {
        time::pause();

        let task: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, _notify: task::StartNotifier<anyhow::Error>| async move {
                let _ = ctx.done().await;
                Ok(())
            },
        );
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();
        let result_fut = task.start(&ctx, "caller", Some(sender));

        time::advance(time::Duration::from_secs(5)).await;

        let result = result_fut.await;
        match result {
            Ok(task) => {
                let _ = task.terminate();
                assert!(false, "expected start error; got task")
            }
            Err(StartError::StartRecvError(_)) => (),
            Err(err) => {
                assert!(false, "expected start recv error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_task_termination_kill() {
        time::pause();
        let mut task_spec: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                let inner_ctx = Context::new();
                // wait forever
                notify.success();
                let _ = inner_ctx.done().await;
                let _ = ctx.done().await;
                Ok(())
            },
        );
        task_spec.with_shutdown(task::Shutdown::Timeout(time::Duration::from_secs(1)));
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();
        let task = task_spec
            .start(&ctx, "caller", Some(sender))
            .await
            .expect("task should start without errors");

        let result = task.terminate();

        time::advance(time::Duration::from_secs(5)).await;

        match result.await {
            Err(TerminationError::TaskForcedKilled) => {
                // Everything ok.
            }
            Err(err) => {
                assert!(false, "expected TaskForcedKilled, got: {}", err)
            }
            Ok(()) => {
                assert!(false, "expected error; got valid result");
            }
        }
    }

    #[tokio::test]
    async fn test_task_termination_with_no_error() {
        time::pause();
        let mut task_spec: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                notify.success();
                let _ = ctx.done().await;
                Ok(())
            },
        );
        task_spec.with_shutdown(task::Shutdown::Timeout(time::Duration::from_secs(1)));
        let (sender, _) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();
        let task = task_spec
            .start(&ctx, "caller", Some(sender))
            .await
            .expect("task should start without errors");

        let result = task.terminate().await;
        match result {
            Err(err) => {
                assert!(false, "expected Ok, got: {}", err)
            }
            Ok(()) => {
                // Everything ok.
            }
        }
    }

    #[tokio::test]
    async fn test_task_termination_with_error() {
        time::pause();
        let mut task_spec: TaskSpec = task::TaskSpec::new_with_start(
            "task",
            |ctx: Context, notify: task::StartNotifier<anyhow::Error>| async move {
                notify.success();
                let _ = ctx.done().await;
                Err(anyhow::anyhow!("some failure"))
            },
        );
        task_spec.with_shutdown(task::Shutdown::Timeout(time::Duration::from_secs(1)));
        let (sender, mut receiver) = mpsc::channel(100);
        let sender = TerminationNotifier::from_mpsc(sender);

        let ctx = Context::new();
        let task = task_spec
            .start(&ctx, "caller", Some(sender))
            .await
            .expect("task should start without errors");

        let result = task.terminate().await;
        let err = receiver
            .recv()
            .await
            .expect("receiver should get the error");
        // Assert that the error received in the channel is the same error
        // returned in the result.
        assert_eq!("task runtime failed: some failure", format!("{}", err));
        match result {
            Err(TerminationError::TaskFailureNotified) => (),
            Err(TerminationError::TaskFailed(err)) => {
                assert!(false, "expected TaskFailureNotified, got: {}", err)
            }
            Err(err) => {
                assert!(false, "expected TaskFailed error, got: {}", err)
            }
            Ok(()) => {
                assert!(false, "expected error; got valid result");
            }
        }
    }
}
