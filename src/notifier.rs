use std::fmt;
use tokio::sync::{mpsc, oneshot};

/// Allows internal APIs running on a different thread to report a success
/// or failure outcome
pub struct StartNotifier<E>(Box<dyn FnOnce(Result<(), E>) + Send>);

impl<E> StartNotifier<E> {
    /// Create a notifier from a oneshot channel
    pub fn from_oneshot(sender: oneshot::Sender<Result<(), E>>) -> Self
    where
        E: Send + 'static,
    {
        Self(Box::new(move |result| {
            sender.send(result).ok();
        }))
    }

    /// Internal function that calls the lambda holding the notification logic
    fn call(self, result: Result<(), E>) {
        self.0(result)
    }

    /// Use this function to report a successful outcome
    pub fn success(self) {
        self.call(Ok(()))
    }

    /// Use this function to report a failed outcome
    pub fn failed(self, err: E) {
        self.call(Err(err))
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Allows internal APIs running on a different thread to report a termination failure.
pub(crate) struct TerminationNotifier<A, E> {
    skip_notifications: bool,
    notifier: mpsc::Sender<Result<A, E>>,
}

impl<A, E> Clone for TerminationNotifier<A, E> {
    fn clone(&self) -> Self {
        TerminationNotifier {
            skip_notifications: self.skip_notifications,
            notifier: self.notifier.clone(),
        }
    }
}

impl<A, E> TerminationNotifier<A, E>
where
    A: fmt::Debug,
    E: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    /// Create a notifier from a oneshot channel
    pub(crate) fn from_mpsc(notifier: mpsc::Sender<Result<A, E>>) -> Self {
        TerminationNotifier {
            skip_notifications: false,
            notifier,
        }
    }

    pub(crate) async fn report_err(&self, err: E) -> Result<(), E> {
        if self.notifier.is_closed() {
            Err(err)
        } else {
            self.notifier
                .send(Err(err))
                .await
                .expect("implementation error");
            Ok(())
        }
    }

    pub(crate) async fn report_ok(&self, res: A) -> Result<(), A> {
        if self.notifier.is_closed() || self.skip_notifications {
            Err(res)
        } else {
            self.notifier
                .send(Ok(res))
                .await
                .expect("implementation error");
            Ok(())
        }
    }

    pub(crate) async fn skip_notifications(mut self) -> Self {
        self.skip_notifications = true;
        self
    }
}

////////////////////////////////////////////////////////////////////////////////
