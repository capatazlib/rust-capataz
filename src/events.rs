use std::sync::Arc;

use crate::worker;
use futures::future::{BoxFuture, Future, FutureExt};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio::task::{self, JoinHandle};

/// Event represents all the different events that may happen on a running
/// supervision tree.
#[derive(Debug, Clone)]
pub enum Event {
    SupervisorStarted(NodeData),
    SupervisorStartFailed(NodeData, Arc<anyhow::Error>),
    SupervisorTerminated(NodeData),
    SupervisorTerminationFailed(NodeData, Arc<anyhow::Error>),
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
type NotifyFn = Box<dyn Fn(Event) -> BoxFuture<'static, ()> + Send>;

/// EventNotifier is used by the internal supervision API to send events about a
/// running supervision tree
#[derive(Clone)]
pub struct EventNotifier(Arc<Mutex<NotifyFn>>);

impl EventNotifier {
    pub fn new<F, O>(notify0: F) -> Self
    where
        F: Fn(Event) -> O + Send + 'static,
        O: Future<Output = ()> + FutureExt + Send + 'static,
    {
        let notify = move |ev| {
            let fut = notify0(ev);
            fut.boxed()
        };
        EventNotifier(Arc::new(Mutex::new(Box::new(notify))))
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
        let notifier: MutexGuard<NotifyFn> = self.0.lock().await;
        notifier(ev).await
    }

    pub async fn supervisor_started(&mut self, runtime_name: &str) {
        self.notify(Event::SupervisorStarted(NodeData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn supervisor_start_failed(&mut self, runtime_name: &str, err: Arc<anyhow::Error>) {
        self.notify(Event::SupervisorStartFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn supervisor_terminated(&mut self, runtime_name: &str) {
        self.notify(Event::SupervisorTerminated(NodeData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn supervisor_termination_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<anyhow::Error>,
    ) {
        self.notify(Event::SupervisorTerminationFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_started(&mut self, runtime_name: &str) {
        self.notify(Event::WorkerStarted(NodeData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn worker_start_failed(&mut self, runtime_name: &str, err: Arc<worker::StartError>) {
        self.notify(Event::WorkerStartFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_terminated(&mut self, runtime_name: &str) {
        self.notify(Event::WorkerTerminated(NodeData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn worker_termination_failed(
        &mut self,
        runtime_name: &str,
        err: Arc<worker::TerminationError>,
    ) {
        self.notify(Event::WorkerTerminationFailed(
            NodeData {
                runtime_name: runtime_name.to_owned(),
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

    /// get_events returned the number of events that have happened so far
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
            assert.check(ev)
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
pub struct EventAssert(Box<dyn Fn(Event) -> String>);

impl EventAssert {
    fn call(&self, ev: Event) -> String {
        (*self.0)(ev)
    }
    pub fn check(&self, ev: Event) {
        let result = self.call(ev);
        if result.len() != 0 {
            assert!(false, result);
        }
    }
}

/// supervisor_started asserts an event that tells a supervisor with the given
/// name started
pub fn supervisor_started(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::SupervisorStarted(NodeData { runtime_name }) => {
            if runtime_name != input_name {
                format!(
                    "Expecting SupervisorStarted with name {}; got {:?} instead",
                    input_name, ev
                )
            } else {
                "".to_owned()
            }
        }
        _ => format!("Expecting SupervisorStarted; got {:?} instead", ev),
    }))
}

/// supervisor_terminated asserts an event that tells a supervisor with the given
/// name was terminated
pub fn supervisor_terminated(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::SupervisorTerminated(NodeData { runtime_name }) => {
            if runtime_name != input_name {
                format!(
                    "Expecting SupervisorTerminated with name {}; got {:?} instead",
                    input_name, ev
                )
            } else {
                "".to_owned()
            }
        }
        _ => format!("Expecting SupervisorTerminated; got {:?} instead", ev),
    }))
}

/// worker_started asserts an event that tells a worker with the given name
/// started
pub fn worker_started(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::WorkerStarted(NodeData { runtime_name }) => {
            println!(
                "{} {} {}",
                runtime_name == input_name,
                runtime_name,
                input_name
            );
            if runtime_name != input_name {
                format!(
                    "Expecting WorkerStarted with name {}; got {:?} instead",
                    input_name, ev
                )
            } else {
                "".to_owned()
            }
        }
        _ => format!("Expecting WorkerStarted; got {:?} instead", ev),
    }))
}

/// worker_terminated asserts an event that tells a worker with the given name
/// was terminated
pub fn worker_terminated(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::WorkerTerminated(NodeData { runtime_name }) => {
            if runtime_name != input_name {
                format!(
                    "Expecting WorkerTerminated with name {}; got {:?} instead",
                    input_name, ev
                )
            } else {
                "".to_owned()
            }
        }
        _ => format!("Expecting WorkerTerminated; got {:?} instead", ev),
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
