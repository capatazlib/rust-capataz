use crate::prelude::*;
use crate::Context;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Allows testing code to trigger errors on supervised workers on command. This
/// type is used to test asynchronous error handling.
struct WorkerTrigger {
    // internal signaling mechanism (used by unit test)
    sender: mpsc::Sender<()>,
    // internal signaling mechanism (used by supervised worker body)
    receiver: Arc<Mutex<mpsc::Receiver<()>>>,
    // keeps track of all errors that have happened so far
    call_counter: Arc<Mutex<u32>>,
}

impl WorkerTrigger {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(10);
        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            call_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Triggers a signal to a worker to fail (internal implementation).
    pub(crate) async fn trigger(&self) {
        let _ = self.sender.send(()).await;
    }

    /// Blocks the current routine until a trigger call is made by a test code.
    pub(crate) async fn wait_trigger(&mut self) {
        let mut receiver = self.receiver.lock().await;
        let _ = receiver.recv().await;
    }

    /// Increments the stored error count
    pub(crate) async fn register_call(&mut self) {
        let mut error_count = self.call_counter.lock().await;
        *error_count += 1
    }

    /// Returns the number of errors registered by this signaler
    pub(crate) async fn get_call_count(&self) -> u32 {
        let error_count = self.call_counter.lock().await;
        *error_count
    }
}

impl Clone for WorkerTrigger {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
            call_counter: self.call_counter.clone(),
        }
    }
}

/// Allows unit testing code to trigger errors on a supervised worker.
pub(crate) struct WorkerTriggerer(WorkerTrigger);

impl WorkerTriggerer {
    /// Triggers a signal to a worker to fail (internal implementation).
    pub(crate) async fn trigger(&self) {
        self.0.trigger().await
    }
}

/// Allows the creation of various supervised workers that make use of error
/// trigger signals.
#[derive(Clone)]
pub(crate) struct WorkerTriggerListener(WorkerTrigger);

impl WorkerTriggerListener {
    /// Transforms this record to a supervised worker that fails at runtime
    /// until a specified maximum number of times.
    pub(crate) fn to_fail_runtime_worker<S>(
        self,
        name: S,
        opts: Vec<worker::Opt>,
        max_fail_count: u32,
    ) -> Node
    where
        S: Into<String>,
    {
        let data = self.0;

        // Build the worker that we are going to use for tests
        let node = worker::Spec::new(name, opts, move |ctx: Context| {
            // Clone the signaler for every time we return a new worker routine
            let mut data = data.clone();

            async move {
                // If the fail_count has reached the max_fail_count, wait for termination
                // of the given Context.
                let fail_count = data.get_call_count().await;
                if fail_count >= max_fail_count {
                    let _ = ctx.done().await;
                    return Ok(());
                }

                // Otherwise, block the thread waiting for the API client to signal
                // an error should happen.
                data.wait_trigger().await;

                // Increase the fail count after waiting for the first failure.
                data.register_call().await;

                Err(anyhow!(
                    "fail_runtime_worker ({}/{})",
                    (fail_count + 1),
                    max_fail_count
                ))
            }
        });
        node
    }

    pub(crate) fn to_success_termination_worker<S>(
        self,
        name: S,
        opts: Vec<worker::Opt>,
        max_termination_count: u32,
    ) -> Node
    where
        S: Into<String>,
    {
        let data = self.0;

        // Build the worker that we are going to use for tests
        let node = worker::Spec::new(name, opts, move |ctx: Context| {
            // Clone the signaler for every time we return a new worker routine
            let mut data = data.clone();

            async move {
                // If the fail_count has reached the max_fail_count, wait for termination
                // of the given Context.
                let termination_count = data.get_call_count().await;
                if termination_count >= max_termination_count {
                    let _ = ctx.done().await;
                    return Ok(());
                }

                // Otherwise, block the thread waiting for the API client to signal
                // a termination should happen.
                data.wait_trigger().await;
                // Increase the termination count after waiting for the first termination.
                data.register_call().await;
                Ok(())
            }
        });
        node
    }
}

/// Creates a tuple of WorkerTriggerer and WorkerTriggerListener, these types are
/// used to create workers that may fail and signal errors on command.
pub(crate) fn new() -> (WorkerTriggerer, WorkerTriggerListener) {
    let data = WorkerTrigger::new();
    (WorkerTriggerer(data.clone()), WorkerTriggerListener(data))
}
