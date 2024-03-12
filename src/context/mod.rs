pub use futures::future::AbortHandle;
pub use tokio::time::Duration;

use futures::future::{abortable, pending, Aborted, BoxFuture, FutureExt, Shared};
use thiserror::Error;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

/// Represents an error reported by the `Context` value.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum Error {
    /// Gets returned after the given duration from a `with_timeout` invocation
    /// has been reached.
    #[error("context timed out")]
    TimedOut(Duration),
    /// Gets returned after the `abort` method from an `AbortHandler` returned
    /// by a `with_cancel` call has been invoced.
    #[error("context was canceled")]
    Cancelled,
}

/// Offers a contract to terminate futures in a way that is explicit, reliable
/// and safe. Values of `Context` are given to every supervised process to
/// listen for termination signals.
#[derive(Clone, Debug)]
pub struct Context {
    // The `Future` that signals a process is done must be inside a `Box` so
    // that we can allow different kinds of futures (Pending, Abortable,
    // etc.) to be a available inside a `Context`; in parallel, we want to
    // allow multiple worker routines to listen to the `Context` future, for
    // that we use the `Shared` wrapper.
    done: Shared<BoxFuture<'static, Result<(), Error>>>,
    runtime_name: String,
}

impl Context {
    /// Creates a `Context` that will never expire
    pub fn new() -> Self {
        Self {
            // we use pending, a future that will never end, we then wrap it
            // on a box to allow multiple kinds of futures; finally we wrap
            // it on `Shared` value to allow mutliple reads and cheap
            // clones.
            done: pending().boxed().shared(),
            runtime_name: "".to_owned(),
        }
    }

    /// Returns the runtime_name stored in this `Context` record.
    pub fn get_runtime_name(&self) -> &str {
        &self.runtime_name
    }

    /// Clones a given `Context` and transforms it into one that contains the
    /// specified parent name.
    pub(crate) fn with_runtime_name(&self, runtime_name: &str) -> Self {
        let mut ctx = self.clone();
        ctx.runtime_name = runtime_name.to_owned();
        ctx
    }

    /// Clones a given `Context` and transforms it into one that can be
    /// cancelled when calling the returned `AbortHandle#abort` function.
    pub fn with_cancel(&self) -> (Self, AbortHandle) {
        let to_context_err = |r: Result<Result<(), Error>, Aborted>| match r {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::Cancelled),
        };

        let (done, aborter) = abortable(self.done.clone());
        let done = done.map(to_context_err).boxed().shared();
        let runtime_name = self.runtime_name.clone();
        (Self { done, runtime_name }, aborter)
    }

    /// Clones a given `Context` and transforms it into one that can time out
    /// once the given time duration has been reached.
    pub fn with_timeout(&self, d: Duration) -> Self {
        let err_d = d.clone();
        let to_context_err = move |r: Result<Result<(), Error>, Elapsed>| match r {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::TimedOut(err_d)),
        };

        let done = timeout(d, self.done.clone());
        let done = done.map(to_context_err).boxed().shared();
        let runtime_name = self.runtime_name.clone();
        Self { done, runtime_name }
    }

    /// Returns a future that is used on `select!` statements to asses if we
    /// should terminate a process.
    pub fn done(&self) -> Shared<BoxFuture<'static, Result<(), Error>>> {
        self.done.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::context::*;
    use tokio::time::{self, Duration};
    use tokio_test::{assert_pending, assert_ready, task};

    #[tokio::test]
    async fn test_context_with_cancel() {
        let ctx = Context::new();
        let (ctx, abort_handle) = ctx.with_cancel();
        let mut fut = task::spawn(ctx.done());

        // because the original done `Future` is of value `Pending`, this
        // future should never be ready unless the `abort_handle` is
        // invoked.
        assert_pending!(fut.poll());

        // the `Cancelled` value is what gets returned.
        abort_handle.abort();
        let result = assert_ready!(fut.poll());
        assert_eq!(result, Err(Error::Cancelled))
    }

    #[tokio::test]
    async fn test_nested_context_with_child_cancel() {
        let ctx = Context::new();

        let (ctx1, _abort_handle1) = ctx.with_cancel();
        // `ctx2` is a context that will be ready/done if
        // `abort_handle2.abort()` is invoked, or if
        // `_abort_handle1.abort()` is invoked.
        let (ctx2, abort_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done());
        let mut fut2 = task::spawn(ctx2.done());

        // because neither `abort_handle1` nor `abort_handle2` have been
        // invoked both futures return a pending value
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        // with `abort_handle2` invoked, only `fut2` gets ready, `fut1` is
        // unaffected.
        abort_handle2.abort();
        assert_pending!(fut1.poll());

        // the `Cancelled` value is what gets returned.
        let result = assert_ready!(fut2.poll());
        assert_eq!(result, Err(Error::Cancelled))
    }

    #[tokio::test]
    async fn test_nested_context_with_parent_cancel() {
        let ctx = Context::new();

        let (ctx1, abort_handle1) = ctx.with_cancel();
        // `ctx2` is a context that will be ready/done if
        // `abort_handle2.abort()` is invoked, or if `abort_handle1.abort()`
        // is invoked.
        let (ctx2, _abort_handle2) = ctx1.with_cancel();

        let mut fut1 = task::spawn(ctx1.done());
        let mut fut2 = task::spawn(ctx2.done());

        // because neither `abort_handle1` nor `abort_handle2` have been
        // invoked both futures return a pending value.
        assert_pending!(fut1.poll());
        assert_pending!(fut2.poll());

        // with `abort_handle1` invoked, both `fut1` and `fut2` report they
        // are ready.
        abort_handle1.abort();
        let result1 = assert_ready!(fut1.poll());
        assert_eq!(result1, Err(Error::Cancelled));
        let result2 = assert_ready!(fut2.poll());
        assert_eq!(result2, Err(Error::Cancelled));
    }

    #[tokio::test]
    async fn test_context_with_timeout() {
        // we pause the time to allow us to fast-forward our virtual clock
        // later in the test
        time::pause();

        let timeout_duration = Duration::from_millis(100);
        let ctx = Context::new();
        let ctx = ctx.with_timeout(timeout_duration.clone());

        let mut fut = task::spawn(ctx.done());
        // this future will be pending as long as we have the virtual clock
        // paused and with time 0
        assert_pending!(fut.poll());

        // let us advance the virtual time to a point before the timeout
        // limit, the future should still be pending
        time::advance(Duration::from_millis(50)).await;
        assert_pending!(fut.poll());

        // let us advance the future one millisecond after the timeout
        // limit, the future should be ready/cancelled
        time::advance(Duration::from_millis(101)).await;

        // TimedOut gets returned
        let result = assert_ready!(fut.poll());
        assert_eq!(result, Err(Error::TimedOut(timeout_duration)))
    }

    #[tokio::test]
    async fn test_nested_context_with_parent_timeout() {
        // we pause the time to allow us to fast-forward our virtual clock
        // later in the test
        time::pause();

        // parent timeout is going to be shorter than child timeout, which
        // will translate to both contexts timing out by shorter duration.
        let parent_duration = Duration::from_millis(50);
        let child_duration = Duration::from_millis(100);

        let ctx = Context::new();
        let parent = ctx.with_timeout(parent_duration.clone());
        let child = parent.with_timeout(child_duration);

        let mut fut_parent = task::spawn(parent.done());
        let mut fut_child = task::spawn(child.done());

        // let us advance the future one millisecond after the shorter
        // timeout (parent's) the future should be ready/cancelled
        time::advance(Duration::from_millis(51)).await;

        let parent_result = assert_ready!(fut_parent.poll());
        assert_eq!(parent_result, Err(Error::TimedOut(parent_duration.clone())));
        let child_result = assert_ready!(fut_child.poll());
        assert_eq!(child_result, Err(Error::TimedOut(parent_duration)));
    }

    #[tokio::test]
    async fn test_nested_context_with_child_timeout() {
        // we pause the time to allow us to fast-forward our virtual clock
        // later in the test
        time::pause();

        // child timeout is going to be shorter than parent timeout, which
        // will translate to only child context timing out.
        let parent_duration = Duration::from_millis(100);
        let child_duration = Duration::from_millis(50);

        let ctx = Context::new();
        let parent = ctx.with_timeout(parent_duration);
        let child = parent.with_timeout(child_duration.clone());

        let mut fut_parent = task::spawn(parent.done());
        let mut fut_child = task::spawn(child.done());

        // let us advance the future one millisecond after the shorter
        // timeout (child's) the future should be ready/cancelled
        time::advance(Duration::from_millis(51)).await;

        // the parent should still be pending
        assert_pending!(fut_parent.poll());
        // while the child, should be ready
        let child_result = assert_ready!(fut_child.poll());
        assert_eq!(child_result, Err(Error::TimedOut(child_duration)));
    }
}
