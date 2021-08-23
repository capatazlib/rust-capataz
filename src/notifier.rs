use std::fmt;
use tokio::sync::oneshot;

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
/// bootstrap of a `Task`.
pub struct StartNotifier<E>(Notifier<(), E>);

impl<E> StartNotifier<E>
where
    E: fmt::Display + fmt::Debug + Send + Sync + 'static,
{
    /// Create a notifier from a oneshot channel
    pub fn from_oneshot(sender: oneshot::Sender<Result<(), E>>) -> Self {
        StartNotifier(Notifier::from_oneshot(sender))
    }

    /// Use this function to report a successful outcome
    pub fn success(self) {
        self.0.success(())
    }

    /// Use this function to report a failed outcome
    pub fn failed(self, err: E) {
        self.0.failed(err)
    }
}

////////////////////////////////////////////////////////////////////////////////
