#![cfg(test)]

use capataz::context::*;
use tokio::time::{self, Duration};
use tokio_test::{assert_pending, assert_ready, task};

#[tokio::test]
async fn test_with_timeout_completion() {
    time::pause();

    let ctx0 = Context::new();

    let (ctx, _cancel_timeout) = Context::with_timeout(&ctx0, Duration::from_millis(100));

    let mut fut = task::spawn(ctx.done);
    assert_pending!(fut.poll());

    // fast-forward
    time::advance(Duration::from_millis(101)).await;
    assert_ready!(fut.poll());
}

#[tokio::test]
async fn test_with_timeout_nested_child_completion() {
    use tokio::time;
    use tokio_test::{assert_pending, assert_ready, task};

    time::pause();

    let ctx0 = Context::new();

    let (ctx1, _cancel_timeout1) = Context::with_timeout(&ctx0, Duration::from_millis(100));
    let (ctx2, _cancel_timeout2) = Context::with_timeout(&ctx1, Duration::from_millis(50));

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
    use tokio::time;
    use tokio_test::{assert_pending, assert_ready, task};

    time::pause();

    let ctx0 = Context::new();

    let (ctx1, _cancel_timeout1) = Context::with_timeout(&ctx0, Duration::from_millis(50));
    let (ctx2, _cancel_timeout2) = Context::with_timeout(&ctx1, Duration::from_millis(100));

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
    use tokio::time;
    use tokio_test::{assert_pending, assert_ready, task};

    time::pause();

    let ctx0 = Context::new();

    let (ctx, cancel_timeout) = Context::with_timeout(&ctx0, Duration::from_millis(100));

    let mut fut = task::spawn(ctx.done);
    assert_pending!(fut.poll());

    // cancel
    cancel_timeout.cancel();
    assert_ready!(fut.poll());
}

#[tokio::test]
async fn test_with_timeout_nested_child_cancel() {
    use tokio::time;
    use tokio_test::{assert_pending, assert_ready, task};

    time::pause();

    let ctx0 = Context::new();

    let (ctx1, _cancel_handle1) = Context::with_timeout(&ctx0, Duration::from_millis(100));
    let (ctx2, cancel_handle2) = Context::with_timeout(&ctx1, Duration::from_millis(100));

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
    use tokio::time;
    use tokio_test::{assert_pending, assert_ready, task};

    time::pause();

    let ctx0 = Context::new();

    let (ctx1, cancel_handle1) = Context::with_timeout(&ctx0, Duration::from_millis(100));
    let (ctx2, _cancel_handle2) = Context::with_timeout(&ctx1, Duration::from_millis(100));

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
    use tokio_test::{assert_pending, assert_ready, task};

    let ctx0 = Context::new();

    let (ctx, cancel_handle) = Context::with_cancel(&ctx0);

    let mut fut = task::spawn(ctx.done);
    assert_pending!(fut.poll());

    cancel_handle.cancel();
    assert_ready!(fut.poll());
}

#[tokio::test]
async fn test_with_cancel_nested_child_cancel() {
    use tokio_test::{assert_pending, assert_ready, task};

    let ctx0 = Context::new();

    let (ctx1, _cancel_handle1) = Context::with_cancel(&ctx0);
    let (ctx2, cancel_handle2) = Context::with_cancel(&ctx1);

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
    use tokio_test::{assert_pending, assert_ready, task};

    let ctx0 = Context::new();

    let (ctx1, cancel_handle1) = Context::with_cancel(&ctx0);
    let (ctx2, _cancel_handle2) = Context::with_cancel(&ctx1);

    let mut fut1 = task::spawn(ctx1.done);
    let mut fut2 = task::spawn(ctx2.done);
    assert_pending!(fut1.poll());
    assert_pending!(fut2.poll());

    cancel_handle1.cancel();

    assert_ready!(fut1.poll());
    assert_ready!(fut2.poll());
}
