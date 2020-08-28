use futures::future::{pending, BoxFuture, FutureExt, Shared};
use tokio::time::Duration;

pub struct CancelHandle {
    cancel_fn: Box<dyn FnOnce()>,
}

impl CancelHandle {
    pub fn cancel(self) {
        (self.cancel_fn)();
    }
}

#[derive(Clone, Debug)]
pub struct Context {
    pub done: Shared<BoxFuture<'static, ()>>,
}

impl Context {
    pub fn new() -> Self {
        Self {
            done: pending().boxed().shared(),
        }
    }

    pub fn with_cancel(&self) -> (Self, CancelHandle) {
        let (done0, canceler) = futures::future::abortable(self.done.clone());
        let done = done0.map(|_| ()).boxed().shared();
        let cancel_fn = Box::new(move || { canceler.abort() });
        (Self { done }, CancelHandle { cancel_fn })
    }

    pub fn with_timeout(&self, timeout: Duration) -> (Self, CancelHandle) {
        let done0 = tokio::time::timeout(timeout, self.done.clone());
        let (done1, canceler) = futures::future::abortable(done0);
        let done = done1.map(|_| ()).boxed().shared();
        let cancel_fn = Box::new(move || { canceler.abort() });
        (Self { done }, CancelHandle { cancel_fn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{self, Duration};
    use tokio_test::{assert_pending, assert_ready, task};

    #[tokio::test]
    async fn test_with_timeout_completion() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx, _cancel_timeout) = ctx0.with_timeout(Duration::from_millis(100));

        let mut fut = task::spawn(ctx.done);
        assert_pending!(fut.poll());

        // fast-forward
        time::advance(Duration::from_millis(101)).await;
        assert_ready!(fut.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_child_completion() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, _cancel_timeout1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, _cancel_timeout2) = ctx1.with_timeout(Duration::from_millis(50));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);

        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        // fast-forward
        time::advance(Duration::from_millis(60)).await;
        assert_pending!(fut1.poll());
        assert_ready!(fut2.poll());

        // fast-forward
        time::advance(Duration::from_millis(41)).await;
        assert_ready!(fut1.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_parent_completion() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, _cancel_timeout1) = ctx0.with_timeout(Duration::from_millis(50));
        let (ctx2, _cancel_timeout2) = ctx1.with_timeout(Duration::from_millis(100));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);

        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        // fast-forward
        time::advance(Duration::from_millis(60)).await;
        // both should have finished, parent context is smaller on its timeout
        assert_ready!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_cancelation_simple() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx, cancel_timeout) = ctx0.with_timeout(Duration::from_millis(100));

        let mut fut = task::spawn(ctx.done);
        assert_pending!(fut.poll());

        // cancel
        cancel_timeout.cancel();
        assert_ready!(fut.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_child_cancel() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, _cancel_handle1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, cancel_handle2) = ctx1.with_timeout(Duration::from_millis(100));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        cancel_handle2.cancel();
        assert_pending!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_parent_cancel() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, cancel_handle1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, _cancel_handle2) = ctx1.with_timeout(Duration::from_millis(100));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        cancel_handle1.cancel();
        assert_ready!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_simple() {
        let ctx0 = Context::new();

        let (ctx, cancel_handle) = ctx0.with_cancel();

        let mut fut = task::spawn(ctx.done);
        assert_pending!(fut.poll());

        cancel_handle.cancel();
        assert_ready!(fut.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_nested_child_cancel() {
        let ctx0 = Context::new();

        let (ctx1, _cancel_handle1) = ctx0.with_cancel();
        let (ctx2, cancel_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        cancel_handle2.cancel();

        assert_pending!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_nested_parent_cancel() {
        let ctx0 = Context::new();

        let (ctx1, cancel_handle1) = ctx0.with_cancel();
        let (ctx2, _cancel_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        cancel_handle1.cancel();

        assert_ready!(fut1.poll());
        assert_ready!(fut2.poll());
    }
}
