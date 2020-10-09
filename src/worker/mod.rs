#[cfg(test)]
pub(crate) mod tests;

use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::future::{abortable, AbortHandle, Aborted, BoxFuture, Future, FutureExt};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::{self, JoinError, JoinHandle};
use tokio::time;

use crate::context::Context;

// TODO: replace with OnceCell
lazy_static! {
    static ref WORKER_START_TIMEOUT: Duration = Duration::from_secs(1);
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TerminationTimeout {
    Infinity,
    Timeout(Duration),
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Restart {
    Permanent,
    Transient,
    Temporary,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Shutdown {
    Indefinitely,
    Timeout(Duration),
}

#[derive(Error, Debug)]
pub enum TerminationError {
    #[error("worker routine termination timeout")]
    TimeoutError {
        #[from]
        source: time::Elapsed,
    },
    #[error("worker routine termination canceled or paniced")]
    JoinHandleError {
        #[from]
        source: JoinError,
    },
    #[error("worker routine terminated by hard kill")]
    KillError {
        #[from]
        source: Aborted,
    },
    #[error(transparent)]
    WorkerError {
        #[from]
        source: anyhow::Error,
    },
}

impl TerminationError {
    pub fn is_timeout_error(&self) -> bool {
        match self {
            TerminationError::TimeoutError { .. } => true,
            _ => false,
        }
    }
    fn is_join_handle_error(&self) -> bool {
        match self {
            TerminationError::JoinHandleError { .. } => true,
            _ => false,
        }
    }

    // TODO: need to find a decent way to setup an error scenario
    // where this verification method is called
    // fn is_kill_error(&self) -> bool {
    //     match self {
    //         TerminationError::KillError { .. } => true,
    //         _ => false,
    //     }
    // }

    pub fn is_worker_termination_error(&self) -> bool {
        match self {
            TerminationError::WorkerError { .. } => true,
            _ => false,
        }
    }
}

#[derive(Error, Debug)]
pub enum StartError {
    #[error("worker routine start timeout")]
    TimeoutError {
        #[from]
        source: time::Elapsed,
    },
    #[error("worker routine did not notify start")]
    NotifyStartError {
        #[from]
        source: oneshot::error::RecvError,
    },
    #[error(transparent)]
    InitError {
        #[from]
        source: anyhow::Error,
    },
}

impl StartError {
    pub fn is_timeout_error(&self) -> bool {
        match self {
            StartError::TimeoutError { .. } => true,
            _ => false,
        }
    }
    fn is_notify_start_error(&self) -> bool {
        match self {
            StartError::NotifyStartError { .. } => true,
            _ => false,
        }
    }
    pub fn is_worker_init_error(&self) -> bool {
        match self {
            StartError::InitError { .. } => true,
            _ => false,
        }
    }
}

/// StartNotifier offers a convenient way to notify a worker spawner (a
/// Supervisor in the general case) that the worker got started or that it
/// failed to start.
pub struct StartNotifier(Box<dyn FnOnce(Result<(), StartError>) + Send>);

impl StartNotifier {
    fn from_oneshot(sender: oneshot::Sender<Result<(), StartError>>) -> Self {
        StartNotifier(Box::new(move |err| {
            let _ = sender.send(err);
        }))
    }

    fn call(self, err: Result<(), StartError>) {
        self.0(err)
    }

    pub fn success(self) {
        self.call(Ok(()))
    }

    pub fn failed(self, err: anyhow::Error) {
        self.call(Err(StartError::InitError { source: err }))
    }
}

pub struct Spec {
    pub name: String,

    start_timeout: Duration,
    termination_timeout: TerminationTimeout,
    restart: Restart,
    shutdown: Shutdown,
    routine:
        Box<dyn FnMut(Context, StartNotifier) -> BoxFuture<'static, Result<(), anyhow::Error>>>,
}

/// Worker represents a routine that got started from a `worker::Spec`. It
/// contains metadata and also offers an API to perform graceful and forceful
/// termination
pub struct Worker {
    pub runtime_name: String,

    spec: Spec,
    created_at: DateTime<Utc>,
    join_handle: JoinHandle<Result<anyhow::Result<()>, Aborted>>,
    termination_handle: AbortHandle,
    kill_handle: AbortHandle,
}

impl Spec {
    /// new creates a worker routine spec. It requires two arguments, a name
    /// that is used for runtime tracing and a start function that returns a
    /// routine that is going to be executed in a Future task concurrently.
    ///
    /// ### The name argument
    ///
    /// A name argument must not be empty nor contain forward slash characters.
    /// It is used for runtime telemetry.
    ///
    /// ### The start function
    ///
    /// This function is where your business logic should be located; it will be
    /// running on a new supervised routine.
    ///
    /// The start function will receive a Context record that **must** be used
    /// inside your business logic to accept stop signals from the supervisor
    /// that manages this routine life-cycle.
    ///
    pub fn new<F, O>(name: &str, mut routine0: F) -> Self
    where
        F: FnMut(Context) -> O + 'static,
        O: Future<Output = anyhow::Result<()>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx: Context, on_start: StartNotifier| {
            on_start.success();
            routine0(ctx).boxed()
        };
        Spec {
            name: name.to_owned(),
            start_timeout: *WORKER_START_TIMEOUT,
            termination_timeout: TerminationTimeout::Infinity,
            shutdown: Shutdown::Indefinitely,
            restart: Restart::Permanent,
            routine: Box::new(routine1),
        }
    }

    /// new_with_start accomplishes the same goal as new with the addition of
    /// passing an extra argument to the start function, a StartNotifier record.
    ///
    /// ### The StartNotifier argument in the start function
    ///
    /// Sometimes you want to consider a routine started after certain
    /// initialization is done; like doing a read from a Database or API, or
    /// when some socket is bound, etc. The StartNotifier allows the spawned
    /// worker routine to signal when it has initialized.
    ///
    /// It is essential to call the API from the StartNotifier function in your
    /// business logic as soon as you consider the worker is initialized,
    /// otherwise the parent supervisor will fail fast.
    ///
    /// ### Report a start error with the StartNotifier
    ///
    /// If for some reason, a worker is not able to start correctly (e.g. DB
    /// connection fails, network is kaput, etc.), the worker may call the
    /// `StartNotifier#failed` function with the impending error as a parameter.
    /// This will cause the whole supervision system start procedure to abort
    /// and fail fast.
    ///
    pub fn new_with_start<F, O>(name: &str, mut routine0: F) -> Self
    where
        F: FnMut(Context, StartNotifier) -> O + 'static,
        O: Future<Output = anyhow::Result<()>> + FutureExt + Send + Sized + 'static,
    {
        let routine1 = move |ctx: Context, on_start: StartNotifier| routine0(ctx, on_start).boxed();
        Spec {
            name: name.to_owned(),
            start_timeout: *WORKER_START_TIMEOUT,
            termination_timeout: TerminationTimeout::Infinity,
            shutdown: Shutdown::Indefinitely,
            restart: Restart::Permanent,
            routine: Box::new(routine1),
        }
    }

    /// restart is a builder method that allows to modify the Restart settings
    /// for the spawned worker
    pub fn restart(mut self, restart: Restart) -> Self {
        self.restart = restart;
        self
    }

    /// shutdown is a builder method that allows to modify the Shutdown settings
    /// for the spawned worker
    pub fn shutdown(mut self, shutdown: Shutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// start_timeout is a builder method that modifies the timeout for starting
    /// a worker routine. Defaults to 1 second.
    pub fn start_timeout(mut self, timeout: Duration) -> Self {
        self.start_timeout = timeout;
        self
    }

    /// termination_timeout is a builder method that modifies the timeout for
    /// terminating a worker routine. Defaults to infinity.
    pub fn termination_timeout(mut self, dur: Duration) -> Self {
        self.termination_timeout = TerminationTimeout::Timeout(dur);
        self
    }

    /// start spawns a new routine that executes the start function of this
    /// Spec.
    ///
    /// ### Blocking on start function
    ///
    /// This function will block and wait until a notification from the
    /// start function (executing on a newly spawned routine) is received; this
    /// notification indicates if the start was successful or not.
    ///
    /// This blocking is necessary to ensure that supervised worker trees get
    /// spawned in the specified/expected order.
    pub async fn start(
        mut self,
        parent_ctx: &Context,
        parent_name: &str,
        // TODO: pending parent channel to notify errors that happened
    ) -> Result<Worker, (Self, StartError)> {
        let runtime_name = format!("{}/{}", parent_name, self.name);
        let created_at = Utc::now();

        // ON_START setup
        //
        // START WRAPPER (A)
        // StartNotifier wraps the original worker routine, of type Result<(), StartError>
        // with a Result<ORIGINAL, RecvError>
        let (started_tx, started_rx) = oneshot::channel::<Result<(), StartError>>();
        let start_notifier = StartNotifier::from_oneshot(started_tx);

        // CANCEL setup
        let (ctx, termination_handle) = Context::with_cancel(parent_ctx);
        let worker_routine0 = (*self.routine)(ctx, start_notifier);

        // KILL setup
        //
        // TERMINATION WRAPPER (A)
        // abortable wraps the original worker routine, of type Result<(), anyhow::Error>
        // with a Result<ORIGINAL, Aborted>
        let (worker_routine1, kill_handle) = abortable(worker_routine0);

        // TODO: OBSERVE setup -- decorate routine to wait for client routine
        // result, and if it is an error, send the error to the supervisor
        // through the given channel

        // SPAWN -- actually spawn concurrent worker future
        //
        // TERMINATION WRAPPER (B)
        // spawn wraps the original worker routine with a Result<ORIGINAL, JoinError>
        let join_handle = task::spawn(worker_routine1);

        // ON_START blocking -- block before moving to the next start
        //
        // START WRAPPER (B)
        // timeout wraps the already decorated worker routine with a Result<WRAPPER (A), Timeout::Delay>
        let result = time::timeout(
            self.start_timeout,
            started_rx, // START WRAPER (A)
        )
        .await;

        // The following match is nasty, but is inherent to the way we are decorating
        // the original routine future provided by the API client.
        match result {
            // START WRAPER (B)
            Err(start_timeout_err) => Err((self, From::from(start_timeout_err))),
            // START WRAPER (A)
            Ok(Err(start_sender_err)) => Err((self, From::from(start_sender_err))),
            // ORIGINAL Init ERROR
            Ok(Ok(Err(worker_init_err))) => Err((self, From::from(worker_init_err))),
            // No Error registered, returning Worker
            Ok(Ok(Ok(_))) => Ok(Worker {
                spec: self,
                runtime_name,
                created_at,
                join_handle,
                termination_handle,
                kill_handle,
            }),
        }
    }
}

impl Worker {
    /// terminate tries to stop gracefuly a worker routine. In the scenario that
    /// the routine doesn't stop after a timeout, it is brutally killed.
    pub async fn terminate(self) -> Result<Spec, (Spec, TerminationError)> {
        // SIGNAL WORKER TERMINATION -- this call will cause the ctx.done future on the worker's
        // select! call to signal a finish for cleanup
        self.termination_handle.abort();

        // TERMINATION TIMEOUT --- we check the worker spec for a termination
        // timeout, if there is one we do the waiting
        //
        // TERMINATION WRAPPER (C)
        // timeout wraps the already decorated worker routine with a Result<WRAPPER (A), Timeout::Delay>
        let result = match self.spec.termination_timeout {
            TerminationTimeout::Infinity => {
                // Given that the timeout wraps the result with Result<_, Timeout::Delay>,
                // we need to wrap the original value with Ok so that the `match` brackets have
                // the same type
                let result = self.join_handle.await;
                Ok(result)
            }
            TerminationTimeout::Timeout(ref termination_timeout) => {
                time::timeout(*termination_timeout, self.join_handle).await
            }
        };

        match result {
            // TERMINATION WRAPPER (C)
            Err(termination_timeout_err) => {
                self.kill_handle.abort();
                Err((self.spec, From::from(termination_timeout_err)))
            }
            // TERMINATION WRAPPER (B)
            Ok(Err(join_handle_err)) => Err((self.spec, From::from(join_handle_err))),
            // TERMINATION WRAPPER (A)
            Ok(Ok(Err(kill_err))) => Err((self.spec, From::from(kill_err))),
            // ORIGINAL ERROR (worker routine failed)
            Ok(Ok(Ok(Err(worker_err)))) => Err((self.spec, From::from(worker_err))),
            // Worker terminated without any hiccup
            Ok(Ok(Ok(Ok(_)))) => Ok(self.spec),
        }
    }
}
