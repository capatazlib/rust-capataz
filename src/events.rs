use std::boxed::Box;
use std::sync::{Arc, Mutex};
use std::time;

use futures::future::{BoxFuture, Future, FutureExt};
use tokio::sync::{broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::{timeout_at, Instant};

use crate::node::{leaf, subtree};
use crate::supervisor;

/// A value that represents all the different events that may happen on a
/// running supervision tree.
#[derive(Debug, Clone)]
pub enum Event {
    /// Signals a supervisor that started.
    SupervisorStarted(NodeData),
    /// Signals a supervisor failed to build.
    SupervisorBuildFailed(NodeData, Arc<subtree::BuildFailed>),
    /// Signals a supervisor fails to starts.
    SupervisorStartFailed(NodeData, Arc<subtree::StartFailed>),
    /// Signals a supervisor that got terminated.
    SupervisorTerminated(NodeData),
    /// Signals a supervisor failed to terminate.
    SupervisorTerminationFailed(NodeData, Arc<subtree::TerminationFailed>),
    /// Signals a supervisor failed with many restarts
    SupervisorRestartedToManyTimes(NodeData, Arc<supervisor::TooManyRestarts>),
    /// Signals a worker got started.
    WorkerStarted(NodeData),
    /// Signals a worker took too long to start and failed.
    WorkerStartTimedOut(NodeData, Arc<leaf::StartTimedOut>),
    /// Signals a worker failed to start.
    WorkerStartFailed(NodeData, Arc<leaf::StartFailed>),
    /// Signals a worker was terminated.
    WorkerTerminated(NodeData),
    /// Signals a worker took too long to terminate and failed.
    WorkerTerminationTimedOut(NodeData, Arc<leaf::TerminationTimedOut>),
    /// Signals a worker failed at termination.
    WorkerTerminationFailed(NodeData, Arc<leaf::TerminationFailed>),
    /// Signals a worker panicked at termination.
    WorkerTerminationPanicked(NodeData, Arc<leaf::TerminationPanicked>),
    /// Signals a worker failed at runtime.
    WorkerRuntimeFailed(NodeData, Arc<leaf::RuntimeFailed>),
    /// Signals a worker panicked at runtime.
    WorkerRuntimePanicked(NodeData, Arc<leaf::RuntimePanicked>),
}

/// Struct that holds details about the producer of the event (supervisor or
/// worker)
#[derive(Debug, Clone, PartialEq)]
pub struct NodeData {
    /// The name of the node that created this event
    pub runtime_name: String,
}

type NotifyFn = Box<dyn Fn(Event) -> BoxFuture<'static, ()> + Send + Sync>;

/// A function provided by API consumers to observe the internal state of a
/// supervision tree.
#[derive(Clone)]
pub struct EventListener(Option<Arc<NotifyFn>>);

impl EventListener {
    /// Creates an `EventListener` from a closure.
    pub fn new<F, O>(notify_fn: F) -> Self
    where
        F: (Fn(Event) -> O) + Send + Sync + 'static,
        O: Future<Output = ()> + Send + 'static,
    {
        // Transform the input function to one that returns a boxed future.
        let notify_fn = move |ev| {
            let fut = notify_fn(ev);
            fut.boxed()
        };
        Self(Some(Arc::new(Box::new(notify_fn))))
    }

    /// Creates an `EventListener` from a `tokio::sync::mpsc::Sender`.
    pub fn from_mpsc(sender: mpsc::Sender<Event>) -> Self {
        Self::new(move |ev: Event| {
            let sender = sender.clone();
            async move {
                let _ = sender.send(ev).await;
                ()
            }
        })
    }

    /// Creates an EventListener that does nothing
    pub fn empty() -> Self {
        Self(None)
    }

    #[cfg(test)]
    /// Returns an `EventListener` that sends its events to an `EventBufferCollector`.
    pub async fn new_testing_listener() -> (EventListener, EventBufferCollector) {
        let (send_ev, rx_ev) = mpsc::channel(1);
        let notifier = EventListener::from_mpsc(send_ev);
        let buffer = EventBufferCollector::from_mpsc(rx_ev).await;
        (notifier, buffer)
    }

    pub(crate) async fn call(&mut self, ev: Event) {
        if let Some(ref notify_fn) = self.0 {
            notify_fn(ev).await
        }
    }
}

/// A wrapper that offers easy to use method to send events to the inner
/// `EventListener`.
#[derive(Clone)]
pub(crate) struct EventNotifier(EventListener);

impl EventNotifier {
    pub(crate) fn new(ev_listener: EventListener) -> Self {
        EventNotifier(ev_listener)
    }
}

impl EventNotifier {
    pub(crate) fn empty() -> Self {
        EventNotifier(EventListener::empty())
    }

    pub(crate) async fn supervisor_started(&mut self, runtime_name: &str) {
        let ev = Event::SupervisorStarted(NodeData {
            runtime_name: runtime_name.to_owned(),
        });
        self.0.call(ev).await;
    }

    pub(crate) async fn supervisor_start_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<subtree::StartFailed>,
    ) {
        let ev = Event::SupervisorStartFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn supervisor_build_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<subtree::BuildFailed>,
    ) {
        let ev = Event::SupervisorBuildFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn supervisor_terminated(&mut self, runtime_name: &str) {
        let ev = Event::SupervisorTerminated(NodeData {
            runtime_name: runtime_name.to_owned(),
        });
        self.0.call(ev).await;
    }

    pub(crate) async fn supervisor_termination_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<subtree::TerminationFailed>,
    ) {
        let ev = Event::SupervisorTerminationFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn supervisor_restarted_too_many_times(
        &mut self,
        runtime_name: &str,
        err: Arc<supervisor::TooManyRestarts>,
    ) {
        let ev = Event::SupervisorRestartedToManyTimes(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_started(&mut self, runtime_name: &str) {
        let ev = Event::WorkerStarted(NodeData {
            runtime_name: runtime_name.to_owned(),
        });
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_start_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::StartFailed>,
    ) {
        let ev = Event::WorkerStartFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_start_timed_out(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::StartTimedOut>,
    ) {
        let ev = Event::WorkerStartTimedOut(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_terminated(&mut self, runtime_name: &str) {
        let ev = Event::WorkerTerminated(NodeData {
            runtime_name: runtime_name.to_owned(),
        });
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_runtime_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::RuntimeFailed>,
    ) {
        let ev = Event::WorkerRuntimeFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_runtime_panicked(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::RuntimePanicked>,
    ) {
        let ev = Event::WorkerRuntimePanicked(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_termination_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::TerminationFailed>,
    ) {
        let ev = Event::WorkerTerminationFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_termination_panicked(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::TerminationPanicked>,
    ) {
        let ev = Event::WorkerTerminationPanicked(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }

    pub(crate) async fn worker_termination_timed_out(
        &mut self,
        runtime_name: &str,
        err: Arc<leaf::TerminationTimedOut>,
    ) {
        let ev = Event::WorkerTerminationTimedOut(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        );
        self.0.call(ev).await;
    }
}

////////////////////////////////////////////////////////////////////////////////

/// EventBufferCollector is an event listener that collects all the events that
/// have been published by a supervision tree. It later can be used to assert
/// events that have happened.
pub struct EventBufferCollector {
    events: Arc<Mutex<Vec<Event>>>,
    on_push: broadcast::Receiver<()>,
    join_handle: JoinHandle<()>,
    current_index: usize,
}

impl EventBufferCollector {
    /// Creates a new EventBufferCollector from a `tokio::sync::mpsc::Reciever`.
    pub async fn from_mpsc(receiver: mpsc::Receiver<Event>) -> EventBufferCollector {
        let events = Arc::new(Mutex::new(Vec::new()));
        // We use unbounded_channel as this is intended to be used for
        // assertions on test-suites
        let (notify_push, on_push) = broadcast::channel(10);
        // why 10? is an arbitrary number, we are not expecting this to collect
        // to many messages before they get read by consumers
        let join_handle = task::spawn(run_event_collector(events.clone(), notify_push, receiver));
        EventBufferCollector {
            events,
            join_handle,
            on_push,
            current_index: 0,
        }
    }

    /// Returns the events that have happened so far in the supervision system.
    pub async fn get_events(&self) -> Vec<Event> {
        let events = self.events.lock().unwrap();
        (*events).clone()
    }

    /// Checks that the accumulated events that have happened so far match the
    /// given assertions in order.
    pub async fn assert_exact(&self, asserts: Vec<EventAssert>) {
        let events = self.get_events().await;
        assert_eq!(asserts.len(), events.len(), "{:?}", events);
        for (ev, assert) in events.into_iter().zip(asserts.into_iter()) {
            assert.check(&ev)
        }
    }

    /// wait_till iterates over the events that have happened and will happen to
    /// check the given EventAssert was matched.
    pub async fn wait_till(
        &mut self,
        assert: EventAssert,
        wait_duration: time::Duration,
    ) -> Result<(), String> {
        loop {
            let events: Vec<Event> = self.get_events().await;
            for (i, ev) in events[self.current_index..].iter().enumerate() {
                self.current_index = i;
                // if we did not get an error message, we matched, and we need
                // to stop
                if let None = assert.call(ev) {
                    return Ok(());
                }
            }

            // unlock the events
            drop(events);

            // we finished the loop, we have to wait until the next push and try
            // again. if we wait too long, fail with a timeout error
            if let Err(_) = timeout_at(Instant::now() + wait_duration, self.on_push.recv()).await {
                return Err(
                    "wait_till: Expected assertion after timeout, did not happen".to_owned(),
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// EventAssert is a well-defined function that asserts properties from an Event
/// emitted by a running supervision tree.
pub struct EventAssert(Box<dyn Fn(&Event) -> Option<String>>);

impl EventAssert {
    fn call(&self, ev: &Event) -> Option<String> {
        (*self.0)(ev)
    }
    /// Runs the assertion with the given `Event`.
    pub fn check(&self, ev: &Event) {
        let result = self.call(ev);
        if let Some(err_msg) = result {
            panic!("EventAssert failed: {}", err_msg);
        };
    }

    /// supervisor_started asserts an event that tells a supervisor with the given
    /// name started
    pub fn supervisor_started<S>(input_name0: S) -> EventAssert
    where
        S: Into<String>,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorStarted(NodeData { runtime_name }) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorStarted with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!("Expecting SupervisorStarted; got {:?} instead", ev)),
        }))
    }

    /// supervisor_terminated asserts an event that tells a supervisor with the given
    /// name was terminated
    pub fn supervisor_terminated<S>(input_name0: S) -> EventAssert
    where
        S: Into<String> + Clone,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorTerminated(NodeData { runtime_name }) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorTerminated with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting SupervisorTerminated; got {:?} instead",
                ev
            )),
        }))
    }

    /// TODO
    pub fn supervisor_termination_failed<S>(input_name0: S) -> EventAssert
    where
        S: Into<String> + Clone,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorTerminationFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorTerminationFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting SupervisorTerminationFailed; got {:?} instead",
                ev
            )),
        }))
    }

    /// TODO
    pub fn supervisor_build_failed<S>(input_name0: S) -> EventAssert
    where
        S: Into<String> + Clone,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorBuildFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorBuildFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting SupervisorTerminationFailed; got {:?} instead",
                ev
            )),
        }))
    }

    /// supervisor_terminated asserts an event that tells a supervisor with the given
    /// name was terminated
    pub fn supervisor_start_failed<S>(input_name0: S) -> EventAssert
    where
        S: Into<String> + Clone,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorStartFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorStartFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting SupervisorStartFailed; got {:?} instead",
                ev
            )),
        }))
    }

    /// TODO
    pub fn supervisor_restarted_too_many_times<S>(input_name0: S) -> EventAssert
    where
        S: Into<String> + Clone,
    {
        let input_name = input_name0.into();
        EventAssert(Box::new(move |ev| match &ev {
            Event::SupervisorRestartedToManyTimes(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting SupervisorRestartedToManyTimes with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting SupervisorRestartedToManyTimes; got {:?} instead",
                ev
            )),
        }))
    }

    /// worker_started asserts an event that tells a worker with the given name
    /// started
    pub fn worker_started(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerStarted(NodeData { runtime_name }) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerStarted with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!("Expecting WorkerStarted; got {:?} instead", ev)),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerStartFailed` with the specified
    /// worker name.
    pub fn worker_start_failed(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerStartFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerStartFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!("Expecting WorkerStarted; got {:?} instead", ev)),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerStartTimedOut` with the specified
    /// worker name.
    pub fn worker_start_timed_out(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerStartTimedOut(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerStartTimedOut with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting WorkerStartTimedOut; got {:?} instead",
                ev
            )),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerTerminated` with the specified
    /// worker name.
    pub fn worker_terminated(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerTerminated(NodeData { runtime_name }) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerTerminated with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!("Expecting WorkerTerminated; got {:?} instead", ev)),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerRuntimeFailed` with the
    /// specified worker name.
    pub fn worker_runtime_failed(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerRuntimeFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerRuntimeFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting WorkerTerminationFailed; got {:?} instead",
                ev
            )),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerTerminationFailed` with the
    /// specified worker name.
    pub fn worker_termination_failed(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerTerminationFailed(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerTerminationFailed with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting WorkerTerminationFailed; got {:?} instead",
                ev
            )),
        }))
    }

    /// Asserts that an `Event` is of value `WorkerTerminationTimedOut` with the specified
    /// worker name.
    pub fn worker_termination_timed_out(input_name0: &str) -> EventAssert {
        let input_name = input_name0.to_owned();
        EventAssert(Box::new(move |ev| match &ev {
            Event::WorkerTerminationTimedOut(NodeData { runtime_name }, _) => {
                if runtime_name != &*input_name {
                    Some(format!(
                        "Expecting WorkerTerminationTimedOut with name {}; got {:?} instead",
                        input_name, ev
                    ))
                } else {
                    None
                }
            }
            _ => Some(format!(
                "Expecting WorkerTerminationTimedOut; got {:?} instead",
                ev
            )),
        }))
    }
}

/// run_event_collector is an internal function that receives supervision events
/// from a channel and stores them on a thread-safe buffer.
async fn run_event_collector(
    events: Arc<Mutex<Vec<Event>>>,
    notify_push: broadcast::Sender<()>,
    mut receiver: mpsc::Receiver<Event>,
) {
    while let Some(ev) = receiver.recv().await {
        // IMPORTANT: DO NOT REMOVE DEBUG LINE COMMENT BELLOW
        // println!("{:?}", ev);

        let mut ev_vec = events.lock().unwrap();
        ev_vec.push(ev);

        // unlock the event buffer
        drop(ev_vec);

        // IMPORTANT: We do not ever want to .await for the send, as we do not
        // always read from the on_push channel
        let _ = notify_push.send(());
    }
}
