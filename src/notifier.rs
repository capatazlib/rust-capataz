use tokio::sync::oneshot;

/// StartNotifier offers a convenient way to notify a supervising task that this
/// task got started or that it failed to start.
pub struct StartNotifier<T, E>(Box<dyn FnOnce(Result<T, E>) + Send>);

impl<T, E> StartNotifier<T, E> {
    pub fn from_oneshot(sender: oneshot::Sender<Result<T, E>>) -> Self
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        StartNotifier(Box::new(move |err| {
            sender.send(err).ok();
        }))
    }

    fn call(self, err: Result<T, E>) {
        self.0(err)
    }

    pub fn success(self, result: T) {
        self.call(Ok(result))
    }

    pub fn failed(self, err: E) {
        self.call(Err(err))
    }
}
