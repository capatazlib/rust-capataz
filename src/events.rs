use std::sync::Arc;

use futures::future::{BoxFuture, Future, FutureExt};
use tokio::sync::{mpsc, Mutex};
use tokio::task::{self, JoinHandle};

use crate::supervisor;
use crate::worker;

/// Event represents all the different events that may happen on a running
/// supervision tree.
#[derive(Debug, Clone)]
pub enum Event {
    SupervisorStarted(NodeData),
    SupervisorStartFailed(NodeData, Arc<supervisor::StartError>),
    SupervisorTerminated(NodeData),
    SupervisorTerminationFailed(NodeData, Arc<supervisor::TerminationError>),
    WorkerStarted(NodeData),
    WorkerStartFailed(NodeData, Arc<worker::StartError>),
    WorkerTerminated(NodeData),
    WorkerTerminationFailed(NodeData, Arc<worker::TerminationError>),
}

/// NodeData holds details about the producer of the event (supervisor or worker)
#[derive(Debug, Clone, PartialEq)]
pub struct NodeData {
    pub runtime_name: String,
}

/// NotifyFn is used by the supervision API to send events to an interested
/// listener.
type NotifyFn = Box<dyn Fn(Event) -> BoxFuture<'static, ()>>;

/// EventNotifier is used by the internal supervision API to send events about a
/// running supervision tree
#[derive(Clone)]
pub struct EventNotifier(Arc<NotifyFn>);

impl EventNotifier {
    pub fn new<F, O>(notify0: F) -> Self
    where
        F: Fn(Event) -> O + 'static,
        O: Future<Output = ()> + FutureExt + Send + 'static,
    {
        let notify = move |ev| {
            let fut = notify0(ev);
            fut.boxed()
        };
        EventNotifier(Arc::new(Box::new(notify)))
    }

    pub fn from_mpsc(sender: mpsc::Sender<Event>) -> Self {
        Self::new(move |ev: Event| {
            let mut sender = sender.clone();
            async move {
                let _ = sender.send(ev).await;
                ()
            }
        })
    }

    async fn notify(&self, ev: Event) {
        (self.0)(ev).await
    }

    pub async fn supervisor_started(&mut self, runtime_name: impl Into<String>) {
        self.notify(Event::SupervisorStarted(NodeData {
            runtime_name: runtime_name.into(),
        }))
        .await
    }

    pub async fn supervisor_start_failed(
        &mut self,
        runtime_name: impl Into<String>,
        err: Arc<supervisor::StartError>,
    ) {
        self.notify(Event::SupervisorStartFailed(
            NodeData {
                runtime_name: runtime_name.into(),
            },
            err,
        ))
        .await
    }

    pub async fn supervisor_terminated(&mut self, runtime_name: impl Into<String>) {
        self.notify(Event::SupervisorTerminated(NodeData {
            runtime_name: runtime_name.into(),
        }))
        .await
    }

    pub async fn supervisor_termination_failed(
        &mut self,
        runtime_name: impl Into<String>,
        err: Arc<supervisor::TerminationError>,
    ) {
        self.notify(Event::SupervisorTerminationFailed(
            NodeData {
                runtime_name: runtime_name.into(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_started(&mut self, runtime_name: impl Into<String>) {
        self.notify(Event::WorkerStarted(NodeData {
            runtime_name: runtime_name.into(),
        }))
        .await
    }

    pub async fn worker_start_failed(
        &mut self,
        runtime_name: impl Into<String>,
        err: Arc<worker::StartError>,
    ) {
        self.notify(Event::WorkerStartFailed(
            NodeData {
                runtime_name: runtime_name.into(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_terminated(&mut self, runtime_name: impl Into<String>) {
        self.notify(Event::WorkerTerminated(NodeData {
            runtime_name: runtime_name.into(),
        }))
        .await
    }

    pub async fn worker_termination_failed(
        &mut self,
        runtime_name: impl Into<String>,
        err: Arc<worker::TerminationError>,
    ) {
        self.notify(Event::WorkerTerminationFailed(
            NodeData {
                runtime_name: runtime_name.into(),
            },
            err,
        ))
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////

/// EventBufferCollector is an event listener that collects all the events that
/// have been published by a supervision tree. It later can be used to assert
/// events that have happened.
pub struct EventBufferCollector {
    events: Arc<Mutex<Vec<Event>>>,
    join_handle: JoinHandle<()>,
}

impl EventBufferCollector {
    pub async fn from_mpsc(receiver: mpsc::Receiver<Event>) -> EventBufferCollector {
        let events = Arc::new(Mutex::new(Vec::new()));
        let join_handle = task::spawn(run_event_collector(events.clone(), receiver));
        EventBufferCollector {
            events,
            join_handle,
        }
    }

    /// get_events returns the events that have happened so far
    pub async fn get_events(&self) -> Vec<Event> {
        let events = self.events.lock().await;
        (*events).clone()
    }

    /// assert_exact checks that the accumulated events that have happened so
    /// far match the given assertions in order.
    pub async fn assert_exact(&self, asserts: Vec<EventAssert>) {
        let events = self.get_events().await;
        assert_eq!(events.len(), asserts.len(), "{:?}", events);
        for (ev, assert) in events.into_iter().zip(asserts.into_iter()) {
            assert.check(&ev)
        }
    }

    // TODO: A function that will block current thread until an event assert is
    // successful
    //
    // pub async fn wait_till(&self, assert: EventAssert, timeout: time::Duration)
}

////////////////////////////////////////////////////////////////////////////////

/// EventAssert is a well-defined function that asserts properties from an Event
/// emitted by a running supervision tree.
pub struct EventAssert(Box<dyn Fn(&Event) -> Option<String>>);

impl EventAssert {
    fn call(&self, ev: &Event) -> Option<String> {
        (*self.0)(ev)
    }
    pub fn check(&self, ev: &Event) {
        let result = self.call(ev);
        if let Some(err_msg) = result {
            panic!("EventAssert failed: {}", err_msg);
        };
    }
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

/// worker_terminated asserts an event that tells a worker with the given name
/// was terminated
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

/// run_event_collector is an internal function that receives supervision events
/// from a channel and stores them on a thread-safe buffer.
async fn run_event_collector(events: Arc<Mutex<Vec<Event>>>, mut receiver: mpsc::Receiver<Event>) {
    while let Some(ev) = receiver.recv().await {
        let mut ev_vec = events.lock().await;
        ev_vec.push(ev);
    }
}

/// testing_event_notifier returns an `EventNotifier` that sends its events
/// to an EventBufferCollector.
pub async fn testing_event_notifier() -> (EventNotifier, EventBufferCollector) {
    let (send_ev, rx_ev) = mpsc::channel(1);
    let notifier = EventNotifier::from_mpsc(send_ev);
    let buffer = EventBufferCollector::from_mpsc(rx_ev).await;
    (notifier, buffer)
}
