use futures::channel::oneshot;
use futures::future::{pending, select, BoxFuture, FutureExt, Shared};
use tokio::sync::mpsc;
use tokio::time::{delay_for, Duration};

pub struct CancelHandle {
    cancel_fn: Box<dyn FnOnce()>,
}

impl CancelHandle {
    pub fn cancel(self) {
        let _ = (self.cancel_fn)();
    }
}

#[derive(Clone)]
pub struct Context {
    pub done: Shared<BoxFuture<'static, ()>>,
}

impl Context {
    pub fn new() -> Context {
        Context {
            done: pending().boxed().shared(),
        }
    }

    pub fn with_cancel(ctx: &Context) -> (Context, CancelHandle) {
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let parent_done = ctx.done.clone();
        let done = select(parent_done, cancel_rx).map(|_| ()).boxed().shared();
        let cancel_fn = Box::new(move || {
            let _ = cancel_tx.send(());
        });
        (Context { done }, CancelHandle { cancel_fn })
    }

    pub fn with_timeout(ctx: &Context, timeout: Duration) -> (Context, CancelHandle) {
        let (mut cancel_tx, mut cancel_rx) = mpsc::channel::<()>(2);
        let cancel_fn = Box::new(move || {
            let _ = cancel_tx.send(());
        });

        let parent_done = ctx.done.clone();
        let done = async move {
            tokio::select! {
                _ = parent_done => (),
                _ = delay_for(timeout) => (),
                _ = cancel_rx.recv() => (),
            }
        }
        .boxed()
        .shared();

        (Context { done }, CancelHandle { cancel_fn })
    }
}
