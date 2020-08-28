use std::sync::Arc;

use futures::future::{BoxFuture, Future, FutureExt};
use tokio::sync::{mpsc, Mutex, MutexGuard};
use tokio::task::{self, JoinHandle};

#[derive(Debug, Clone)]
pub enum Event {
    SupervisorStarted(EventData),
    SupervisorStartFailed(EventData, Arc<anyhow::Error>),
    SupervisorTerminated(EventData),
    SupervisorTerminationFailed(EventData, Arc<anyhow::Error>),
    WorkerStarted(EventData),
    WorkerStartFailed(EventData, Arc<anyhow::Error>),
    WorkerTerminated(EventData),
    WorkerTerminationFailed(EventData, Arc<anyhow::Error>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventData {
    pub runtime_name: String,
}

type NotifyFn = Box<dyn Fn(Event) -> BoxFuture<'static, ()> + Send>;

#[derive(Clone)]
pub struct EventNotifier(Arc<Mutex<NotifyFn>>);

impl EventNotifier {
    pub fn new<F, O>(notify0: F) -> EventNotifier
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

    pub fn from_mpsc(sender: mpsc::Sender<Event>) -> EventNotifier {
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
        self.notify(Event::SupervisorStarted(EventData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn supervisor_start_failed(&mut self, runtime_name: &str, err: Arc<anyhow::Error>) {
        self.notify(Event::SupervisorStartFailed(
            EventData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn supervisor_terminated(&mut self, runtime_name: &str) {
        self.notify(Event::SupervisorTerminated(EventData {
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
            EventData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_started(&mut self, runtime_name: &str) {
        self.notify(Event::WorkerStarted(EventData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn worker_start_failed(&mut self, runtime_name: &str, err: Arc<anyhow::Error>) {
        self.notify(Event::WorkerStartFailed(
            EventData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }

    pub async fn worker_terminated(&mut self, runtime_name: &str) {
        self.notify(Event::WorkerTerminated(EventData {
            runtime_name: runtime_name.to_owned(),
        }))
        .await
    }

    pub async fn worker_termination_failed(&mut self, runtime_name: &str, err: Arc<anyhow::Error>) {
        self.notify(Event::WorkerTerminationFailed(
            EventData {
                runtime_name: runtime_name.to_owned(),
            },
            err,
        ))
        .await
    }
}

pub struct EventBuffer {
    events: Arc<Mutex<Vec<Event>>>,
    join_handle: JoinHandle<()>,
}

impl EventBuffer {
    pub async fn from_mpsc(receiver: mpsc::Receiver<Event>) -> EventBuffer {
        let events = Arc::new(Mutex::new(Vec::new()));
        let join_handle = task::spawn(run_event_collector(events.clone(), receiver));
        EventBuffer {
            events,
            join_handle,
        }
    }

    pub async fn get_events(&self) -> Vec<Event> {
        let events = self.events.lock().await;
        (*events).clone()
    }

    pub async fn assert_exact(&self, asserts: Vec<EventAssert>) {
        let events = self.get_events().await;
        assert_eq!(events.len(), asserts.len(), "{:?}", events);
        for (ev, assert) in events.into_iter().zip(asserts.into_iter()) {
            assert.check(ev)
        }
    }
}

pub struct EventAssert(Box<dyn Fn(Event) -> String>);

impl EventAssert {
    pub fn check(&self, ev: Event) {
        let result = (*self.0)(ev);
        if result.len() != 0 {
            assert!(false, result);
        }
    }
}

pub fn supervisor_started(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::SupervisorStarted(EventData { runtime_name }) => {
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

pub fn supervisor_terminated(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::SupervisorTerminated(EventData { runtime_name }) => {
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

pub fn worker_started(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::WorkerStarted(EventData { runtime_name }) => {
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

pub fn worker_terminated(input_name: &'static str) -> EventAssert {
    EventAssert(Box::new(move |ev| match &ev {
        Event::WorkerTerminated(EventData { runtime_name }) => {
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

async fn run_event_collector(events: Arc<Mutex<Vec<Event>>>, mut receiver: mpsc::Receiver<Event>) {
    while let Some(ev) = receiver.recv().await {
        let mut ev_vec = events.lock().await;
        ev_vec.push(ev);
    }
}

pub async fn testing_event_notifier() -> (EventNotifier, EventBuffer) {
    let (send_ev, rx_ev) = mpsc::channel(1);
    let notifier = EventNotifier::from_mpsc(send_ev);
    let buffer = EventBuffer::from_mpsc(rx_ev).await;
    (notifier, buffer)
}
