pub use futures::future::AbortHandle;

use futures::future::{pending, BoxFuture, FutureExt, Shared};
use tokio::time::Duration;

/// Context offers a well defined API to create futures that are intended to be
/// used for routine life-cycle only.
#[derive(Clone, Debug)]
pub struct Context {
    pub done: Shared<BoxFuture<'static, ()>>,
}

impl Context {
    /// new creates a Contex that contains a future that will always block
    /// when waiting on it
    pub fn new() -> Self {
        Self {
            done: pending().boxed().shared(),
        }
    }

    /// with_cancel returns a new context with an AbortHandle, if the
    /// `AbortHandle#abort` method gets invoked, the returned `Context.done`
    /// future will return a value.
    pub fn with_cancel(&self) -> (Self, AbortHandle) {
        let (done, aborter) = futures::future::abortable(self.done.clone());
        let done = done.map(|_| ()).boxed().shared();
        (Self { done }, aborter)
    }

    /// with_timeout returns a new context with an AbortHandle (same as
    /// with_cancel). After an specified amount of time, the returned `Context.done`
    /// future will return a value.
    pub fn with_timeout(&self, timeout: Duration) -> (Self, AbortHandle) {
        let done = tokio::time::timeout(timeout, self.done.clone());
        let (done, aborter) = futures::future::abortable(done);
        let done = done.map(|_| ()).boxed().shared();
        (Self { done }, aborter)
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

        let ctx = Context::new();

        let (ctx, _abort_timeout) = ctx.with_timeout(Duration::from_millis(100));

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

        let (ctx1, _abort_timeout1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, _abort_timeout2) = ctx1.with_timeout(Duration::from_millis(50));

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

        let (ctx1, _abort_timeout1) = ctx0.with_timeout(Duration::from_millis(50));
        let (ctx2, _abort_timeout2) = ctx1.with_timeout(Duration::from_millis(100));

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

        let ctx = Context::new();

        let (ctx, abort_timeout) = ctx.with_timeout(Duration::from_millis(100));

        let mut fut = task::spawn(ctx.done);
        assert_pending!(fut.poll());

        // cancel
        abort_timeout.abort();
        assert_ready!(fut.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_child_cancel() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, _abort_handle1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, abort_handle2) = ctx1.with_timeout(Duration::from_millis(100));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        abort_handle2.abort();
        assert_pending!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_timeout_nested_parent_cancel() {
        time::pause();

        let ctx0 = Context::new();

        let (ctx1, abort_handle1) = ctx0.with_timeout(Duration::from_millis(100));
        let (ctx2, _abort_handle2) = ctx1.with_timeout(Duration::from_millis(100));

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        abort_handle1.abort();
        assert_ready!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_simple() {
        let ctx = Context::new();

        let (ctx, abort_handle) = ctx.with_cancel();

        let mut fut = task::spawn(ctx.done);
        assert_pending!(fut.poll());

        abort_handle.abort();
        assert_ready!(fut.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_nested_child_cancel() {
        let ctx0 = Context::new();

        let (ctx1, _abort_handle1) = ctx0.with_cancel();
        let (ctx2, abort_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        abort_handle2.abort();

        assert_pending!(fut1.poll());
        assert_ready!(fut2.poll());
    }

    #[tokio::test]
    async fn test_with_cancel_nested_parent_cancel() {
        let ctx0 = Context::new();

        let (ctx1, abort_handle1) = ctx0.with_cancel();
        let (ctx2, _abort_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done);
        let mut fut2 = task::spawn(ctx2.done);
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        abort_handle1.abort();

        assert_ready!(fut1.poll());
        assert_ready!(fut2.poll());
    }
}
