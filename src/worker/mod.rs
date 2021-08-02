use std::error;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::{self, BoxFuture};
use futures::FutureExt;
use lazy_static::lazy_static;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::{self, JoinHandle};
use tokio::time;
use uuid::Uuid;

use super::context::Context;

////////////////////////////////////////////////////////////////////////////////

/// Allows internal APIs running on a different thread to report a success
/// or failure outcome
pub struct Notifier<T, E>(Box<dyn FnOnce(Result<T, E>) + Send>);

impl<T, E> Notifier<T, E> {
    /// Create a notifier from a oneshot channel
    pub fn from_oneshot(sender: oneshot::Sender<Result<T, E>>) -> Self
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        Notifier(Box::new(move |result| {
            sender.send(result).ok();
        }))
    }

    /// Internal function that calls the lambda holding the notification logic
    fn call(self, result: Result<T, E>) {
        self.0(result)
    }

    /// Use this function to report a successful outcome
    pub fn success(self, result: T) {
        self.call(Ok(result))
    }

    /// Use this function to report a failed outcome
    pub fn failed(self, err: E) {
        self.call(Err(err))
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A more concrete `Notification` type used to report errors produced on the
/// bootstrap of a Worker.
pub struct StartNotifier(Notifier<(), anyhow::Error>);

impl StartNotifier {
    /// Create a notifier from a oneshot channel
    pub fn from_oneshot(sender: oneshot::Sender<Result<(), anyhow::Error>>) -> Self {
        StartNotifier(Notifier::from_oneshot(sender))
    }

    /// Use this function to report a successful outcome
    pub fn success(self) {
        self.0.success(())
    }

    /// Use this function to report a failed outcome
    pub fn failed<E>(self, err: E)
    where
        E: error::Error + Send + Sync + 'static,
    {
        self.0.failed(Box::new(err).into())
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO: replace with OnceCell
lazy_static! {
    /// Default timeout duration for Worker bootstrap procedure
    static ref WORKER_START_TIMEOUT: Duration = Duration::from_secs(1);
}

/// Error that can be reported back by a Worker when it is starting.
#[derive(Debug, Error)]
pub enum StartError {
    /// Returned when the worker did not get started after an specified timeout
    /// duration.
    #[error("worker took to long to get started")]
    StartTimeoutError(time::error::Elapsed),
    /// Returned when the Worker implementation did not make use of the given
    /// `StartNotifier` to signal a Worker started successfully or failed to
    /// start.
    #[error("worker routine did not notify start success or failure")]
    StartRecvError(oneshot::error::RecvError),
    /// Returned when the Worker implementation invokes a `StartNotifier#failed`
    /// method.
    #[error("worker signaled start error on client routine")]
    ClientError(anyhow::Error),
}

/// An enum type that indicates how the parent supervisor will handle the
/// stoppping of the Worker task
#[derive(Debug)]
pub enum Shutdown {
    /// A `Shutdown` value that specifies the parent supervisor must wait
    /// indefinitely for the worker task to stop executing.
    Indefinitely,
    /// A `Shutdown` value that indicates the time that the supervisor will wait
    /// before "force-killing" a Worker task.
    Timeout(Duration),
}

/// Represents a Worker specification; it serves as a template for the
/// construction of tasks. The Worker's Spec record is used in conjunction with
/// the supervisor's Spec.
pub struct Spec {
    name: String,
    start_timeout: Duration,
    shutdown: Shutdown,
    routine: Box<
        dyn (FnMut(Context, StartNotifier) -> BoxFuture<'static, Result<(), anyhow::Error>>) + Send,
    >,
}

/// Error that can be reported back when terminating a Worker
#[derive(Debug, Error)]
pub enum TerminationError {
    /// Indicates the worker did not respect the shutdown mechanism and had to
    /// be force-killed.
    #[error("worker was hard killed after timeout")]
    WorkerForcedKilled,
    /// Indicates the worker was cancelled.
    #[error("worker was aborted")]
    WorkerAborted,
    /// Indicates the worker had an error before it terminated.
    #[error("worker runtime failed")]
    WorkerFailed(anyhow::Error),
    /// Indicates the worker had a panic while on runtime.
    #[error("Worker panicked at runtime")]
    WorkerPanic,
}

/// Runtime representation of a Spec.
pub struct Worker<'a> {
    spec: &'a Spec,
    uuid: Uuid,
    created_timestamp: DateTime<Utc>,
    runtime_name: String,
    join_handle: JoinHandle<Result<(), TerminationError>>,
    termination_handle: future::AbortHandle,
    kill_handle: future::AbortHandle,
}

impl<'a> Worker<'a> {
    /// Waits for the worker task to finish indefinitely.
    pub(crate) async fn wait(self) -> Result<(), TerminationError> {
        // await for the Worker routine result; the result is going to be
        // wrapped by a Result created from the JoinHandle API.
        let join_result = self.join_handle.await;
        match join_result {
            // JoinHandle Error.
            Err(join_error) => {
                if join_error.is_panic() {
                    // Caused when the task panicked.
                    Err(TerminationError::WorkerPanic)
                } else {
                    // The `JoinHandle` was dropped or the
                    // `JoinHandle.abort` method was called explicitly. The
                    // current implementation doesn't allow this, so this
                    // branch cannot happen.
                    unreachable!("Worker's JoinHandle was used in an unexpected way")
                }
            }
            // The Worker routine finished with an error.
            Ok(Err(err)) => Err(err),
            // The Worker routine finished without errors.
            Ok(Ok(_)) => Ok(()),
        }
    }

    /// Executes the abort logic of the routine and then waits for a specified
    /// duration of time before it kills the task.
    pub(crate) async fn terminate(self) -> Result<(), TerminationError> {
        let Worker {
            spec,
            kill_handle,
            join_handle,
            termination_handle,
            ..
        } = self;

        // Signal the context.done() inside the worker routine to return so
        // that the worker starts to shutdown.
        termination_handle.abort();

        // Handle the JoinHandle API result once for Indefinitely and Timeout
        // shutdown branches. We cannot use the `Worker#wait` method (which has
        // a very similar implementation) because of partial moves.
        let wait_result = async {
            // await for the Worker routine result; the result is going to be
            // wrapped by a Result created from the JoinHandle API.
            let join_result = join_handle.await;
            match join_result {
                // JoinHandle Error.
                Err(join_error) => {
                    if join_error.is_panic() {
                        // Caused when the task panicked.
                        Err(TerminationError::WorkerPanic)
                    } else {
                        // The `JoinHandle` was dropped or the
                        // `JoinHandle.abort` method was called explicitly. The
                        // current implementation doesn't allow this, so this
                        // branch cannot happen.
                        unreachable!("Worker's JoinHandle was used in an unexpected way")
                    }
                }
                // The Worker routine finished with an error.
                Ok(Err(err)) => Err(err),
                // The Worker routine finished without errors.
                Ok(Ok(_)) => Ok(()),
            }
        };

        match spec.shutdown {
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
                        Err(TerminationError::WorkerForcedKilled)
                    }
                    join_result = wait_result => {
                        join_result
                    }
                }
            }
            // Client doesn't care if this worker takes time to terminate.
            Shutdown::Indefinitely => wait_result.await,
        }
    }
}

impl Spec {
    /// Similar to `new`, with the addition of passing an extra argument to the
    /// start function, a `StartNotifier` value.
    ///
    /// ### The StartNotifier argument
    ///
    /// Sometimes you want to consider a routine started after certain
    /// initialization is done; like doing a read from a Database or API, or
    /// when some socket is bound, etc. The `StartNotifier` value allows the
    /// spawned worker routine to signal when it has initialized.
    ///
    /// It is essential to call the API from the `StartNotifier` function in
    /// your business logic as soon as you consider the worker is initialized,
    /// otherwise the parent supervisor spawning this worker will fail after a
    /// timeout.
    ///
    /// ### Report a start error with the StartNotifier
    ///
    /// If for some reason, a worker is not able to start correctly (e.g. DB
    /// connection fails, network is kaput, etc.), the worker may call the
    /// `StartNotifier#failed` function with the impending error as a parameter.
    /// This call will cause the whole supervision system start procedure to
    /// abort and fail fast.
    ///
    pub fn new_with_start<S, F, O>(name: S, mut routine: F) -> Self
    where
        S: Into<String>,
        F: (FnMut(Context, StartNotifier) -> O) + Send + 'static,
        O: future::Future<Output = Result<(), anyhow::Error>> + FutureExt + Send + Sized + 'static,
    {
        let routine = move |ctx: Context, on_start: StartNotifier| routine(ctx, on_start).boxed();
        Spec {
            name: name.into(),
            start_timeout: *WORKER_START_TIMEOUT,
            routine: Box::new(routine),
            shutdown: Shutdown::Indefinitely,
            // restart: Restart::Permanent,
        }
    }

    /// Specifies how long a client API is willing to wait for the termination
    /// of this worker.
    pub fn with_shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Spawns a new task that executes this `Spec`'s routine `Future`.
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
        &mut self,
        parent_ctx: &Context,
        parent_name: &str,
    ) -> Result<Worker<'_>, StartError> {
        let uuid = Uuid::new_v4();
        let created_timestamp = Utc::now();
        let runtime_name = format!("{}/{}", parent_name, self.name);

        // Create the notification channel that allow users of this API to
        // signal capataz that the task started.
        let (start_sx, start_rx) = oneshot::channel::<Result<(), anyhow::Error>>();
        let start_notifier = StartNotifier::from_oneshot(start_sx);

        // Allow the capataz API to signal a cancellation of the task via our
        // context API.
        let (ctx, termination_handle) = Context::with_cancel(parent_ctx);
        let worker_routine = (*self.routine)(ctx, start_notifier);
        let (worker_routine, kill_handle) = futures::future::abortable(worker_routine);

        // Create an intermediary future that flattens the result tree.
        let worker_routine = async {
            use TerminationError::*;
            // start_notifier is invoked internally in the worker_routine.
            let worker_result = worker_routine.await;
            match worker_result {
                Err(_) => Err(WorkerAborted),
                Ok(Err(err)) => Err(WorkerFailed(err)),
                Ok(Ok(_)) => Ok(()),
            }
        };

        // Perform the future asynchronously in a new task.
        let join_handle = task::spawn(worker_routine);

        // Wait for the result of the worker via the notification channel.
        let start_result = time::timeout(self.start_timeout, start_rx).await;

        use StartError::*;
        match start_result {
            // Worker took to long to get started.
            Err(start_timeout_err) => Err(StartTimeoutError(start_timeout_err)),
            // Oneshot API failed to receive message.
            Ok(Err(start_sender_err)) => Err(StartRecvError(start_sender_err)),
            // API client signals a start error.
            Ok(Ok(Err(worker_start_err))) => Err(ClientError(worker_start_err)),
            // Everything went Ok.
            Ok(Ok(Ok(()))) => Ok(Worker {
                spec: self,
                uuid,
                created_timestamp,
                runtime_name,
                join_handle,
                termination_handle,
                kill_handle,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use thiserror::Error;

    use tokio::time;

    use crate::context::*;
    use crate::worker::{self, StartError, TerminationError};

    #[tokio::test]
    async fn test_worker_start_ok() {
        let mut worker = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, notify: worker::StartNotifier| async move {
                notify.success();
                let _ = ctx.done().await;
                Ok(())
            },
        );

        let ctx = Context::new();

        let result = worker.start(&ctx, "caller").await;
        match result {
            Ok(worker) => {
                let _ = worker.terminate();
            }
            Err(err) => {
                assert!(false, "expected ok, got error {}", err);
            }
        }
    }

    #[derive(Clone, Debug, Error)]
    pub enum TestErr {
        #[error("TestErr: {0}")]
        TestErr(String),
    }

    #[tokio::test]
    async fn test_worker_start_with_notify_failed_routine() {
        let mut worker = worker::Spec::new_with_start(
            "worker",
            |_ctx: Context, notify: worker::StartNotifier| async move {
                let err = TestErr::TestErr("worker start failure".to_owned());
                notify.failed(err.clone());
                // Usually errors will come from called functions, and the
                // implicit error transformation to anyhow::Error will take
                // the verbosity away.
                let _ = Err(err)?;
                Ok(())
            },
        );

        let ctx = Context::new();

        let result = worker.start(&ctx, "caller").await;
        match result {
            Ok(worker) => {
                let _ = worker.terminate();
                assert!(false, "expected start error; got worker")
            }
            Err(StartError::ClientError(err)) => {
                assert_eq!("TestErr: worker start failure", format!("{}", err))
            }
            Err(err) => {
                assert!(false, "expected client error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_worker_start_timeout_err() {
        time::pause();

        let mut worker = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, notify: worker::StartNotifier| async move {
                let _ = ctx.done().await;
                notify.success();
                Ok(())
            },
        );

        let ctx = Context::new();
        let result_fut = worker.start(&ctx, "caller");

        time::advance(time::Duration::from_secs(5)).await;

        let result = result_fut.await;
        match result {
            Ok(worker) => {
                let _ = worker.terminate();
                assert!(false, "expected start error; got worker")
            }
            Err(StartError::StartTimeoutError(_)) => (),
            Err(err) => {
                assert!(false, "expected timeout error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_worker_start_recv_err() {
        time::pause();

        let mut worker = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, _notify: worker::StartNotifier| async move {
                let _ = ctx.done().await;
                Ok(())
            },
        );

        let ctx = Context::new();
        let result_fut = worker.start(&ctx, "caller");

        time::advance(time::Duration::from_secs(5)).await;

        let result = result_fut.await;
        match result {
            Ok(worker) => {
                let _ = worker.terminate();
                assert!(false, "expected start error; got worker")
            }
            Err(StartError::StartRecvError(_)) => (),
            Err(err) => {
                assert!(false, "expected start recv error; got {}", err);
            }
        }
    }

    #[tokio::test]
    async fn test_worker_termination_kill() {
        time::pause();
        let mut worker_spec = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, notify: worker::StartNotifier| async move {
                let inner_ctx = Context::new();
                // wait forever
                notify.success();
                let _ = inner_ctx.done().await;
                let _ = ctx.done().await;
                Ok(())
            },
        )
        .with_shutdown(worker::Shutdown::Timeout(time::Duration::from_secs(1)));

        let ctx = Context::new();
        let worker = worker_spec
            .start(&ctx, "caller")
            .await
            .expect("worker should start without errors");

        let result = worker.terminate();

        time::advance(time::Duration::from_secs(5)).await;

        match result.await {
            Err(TerminationError::WorkerForcedKilled) => {
                // Everything ok.
            }
            Err(err) => {
                assert!(false, "expected WorkerForcedKilled, got: {}", err)
            }
            Ok(()) => {
                assert!(false, "expected error; got valid result");
            }
        }
    }

    #[tokio::test]
    async fn test_worker_termination_with_no_error() {
        time::pause();
        let mut worker_spec = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, notify: worker::StartNotifier| async move {
                notify.success();
                let _ = ctx.done().await;
                Ok(())
            },
        )
        .with_shutdown(worker::Shutdown::Timeout(time::Duration::from_secs(1)));

        let ctx = Context::new();
        let worker = worker_spec
            .start(&ctx, "caller")
            .await
            .expect("worker should start without errors");

        let result = worker.terminate().await;
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
    async fn test_worker_termination_with_error() {
        time::pause();
        let mut worker_spec = worker::Spec::new_with_start(
            "worker",
            |ctx: Context, notify: worker::StartNotifier| async move {
                notify.success();
                let _ = ctx.done().await;
                Err(anyhow::anyhow!("some failure"))
            },
        )
        .with_shutdown(worker::Shutdown::Timeout(time::Duration::from_secs(1)));

        let ctx = Context::new();
        let worker = worker_spec
            .start(&ctx, "caller")
            .await
            .expect("worker should start without errors");

        let result = worker.terminate().await;
        match result {
            Err(TerminationError::WorkerFailed(err)) => {
                assert_eq!("some failure", format!("{}", err))
            }
            Err(err) => {
                assert!(false, "expected WorkerFailed error, got: {}", err)
            }
            Ok(()) => {
                assert!(false, "expected error; got valid result");
            }
        }
    }
}
