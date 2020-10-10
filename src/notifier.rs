use tokio::sync::oneshot;

/// SpawnedNotifier offers a convenient way to notify a supervising task that this
/// task got started or that it failed to start.
pub struct SpawnedNotifier<T, E>(Box<dyn FnOnce(Result<T, E>) + Send>);

impl<T, E> SpawnedNotifier<T, E> {
    pub fn from_oneshot(sender: oneshot::Sender<Result<T, E>>) -> Self
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        SpawnedNotifier(Box::new(move |err| {
            let _ = sender.send(err);
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
