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
pub(crate) struct TerminationNotifier<E>(mpsc::Sender<E>);

impl<E> Clone for TerminationNotifier<E> {
    fn clone(&self) -> Self {
        TerminationNotifier(self.0.clone())
    }
}

impl<E> TerminationNotifier<E>
where
    E: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    /// Create a notifier from a oneshot channel
    pub(crate) fn from_mpsc(sender: mpsc::Sender<E>) -> Self {
        TerminationNotifier(sender)
    }

    pub(crate) async fn report(&self, err: E) -> Result<(), E> {
        if self.0.is_closed() {
            Err(err)
        } else {
            self.0.send(err).await.expect("implementation error");
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
