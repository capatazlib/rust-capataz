use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::context::*;
use crate::worker::{self, StartNotifier};

pub fn wait_done_worker(name: &str) -> worker::Spec {
    worker::Spec::new(name, move |ctx: Context| async move {
        ctx.done.await;
        Ok(())
    })
}

pub fn start_err_worker(name: &str, err_msg: &'static str) -> worker::Spec {
    worker::Spec::new_with_start(name, move |_: Context, start: StartNotifier| async move {
        start.failed(anyhow::Error::msg(err_msg.to_owned()));
        Err(anyhow::Error::msg("should not see this"))
    })
}

pub fn start_timeout_worker(name: &str, start_timeout: Duration, delay: Duration) -> worker::Spec {
    let spec =
        worker::Spec::new_with_start(name, move |_: Context, start: StartNotifier| async move {
            // we want to advance time so that timeout on the caller side expires
            time::delay_for(delay).await;
            start.success();
            Err(anyhow::Error::msg("should not see this"))
        });
    spec.start_timeout(start_timeout)
}

fn no_start_worker(name: &str) -> worker::Spec {
    worker::Spec::new_with_start(name, move |ctx: Context, _: StartNotifier| async move {
        // we want to advance time so that timeout on the caller side expires
        ctx.done.await;
        Err(anyhow::Error::msg("should not see this"))
    })
}

fn termination_success_worker(name: &str) -> worker::Spec {
    worker::Spec::new(name, move |ctx: Context| async move {
        ctx.done.await;
        Ok(())
    })
}

pub fn termination_failed_worker(name: &str) -> worker::Spec {
    worker::Spec::new(name, move |ctx: Context| async move {
        ctx.done.await;
        Err(anyhow::Error::msg("termination_failed_worker"))
    })
}

fn termination_timeout_worker(
    name: &str,
    termination_timeout: Duration,
    delay: Duration,
) -> worker::Spec {
    let spec = worker::Spec::new(name, move |ctx: Context| async move {
        ctx.done.await;
        time::delay_for(delay).await;
        Err(anyhow::Error::msg("termination_timeout_worker"))
    });
    spec.termination_timeout(termination_timeout)
}

fn panic_worker(name: &str, panic_msg0: &str) -> worker::Spec {
    let panic_msg1 = panic_msg0.to_owned();
    worker::Spec::new(name, move |_: Context| {
        let panic_msg = panic_msg1.clone();
        async move {
            panic!(panic_msg);
        }
    })
}

#[tokio::test]
async fn test_worker_simple_start() {
    let (before_tx, mut before_rx) = mpsc::channel(1);
    let (after_tx, mut after_rx) = mpsc::channel(1);

    let routine = move |ctx: Context| {
        let mut before_tx = before_tx.clone();
        let mut after_tx = after_tx.clone();
        async move {
            // wait for worker to be done
            let _ = before_tx.send(()).await;
            ctx.done.await;
            let _ = after_tx.send(()).await;
            Ok(())
        }
    };

    let spec = worker::Spec::new("child1", routine);

    let ctx = Context::new();
    let start_result = spec.start(&ctx, "root").await.map_err(|err| err.1);
    let worker = start_result.expect("successful worker creation");

    before_rx.recv().await;
    worker
        .terminate()
        .await
        .map_err(|err| err.1)
        .expect("worker termination must succeed");
    after_rx.recv().await;
}

#[tokio::test]
async fn test_worker_start_success() {
    let (after_tx, mut after_rx) = mpsc::channel(1);

    let routine = move |ctx: Context, start: StartNotifier| {
        let mut after_tx = after_tx.clone();
        async move {
            // wait for worker to be done
            start.success();
            ctx.done.await;
            let _ = after_tx.send(()).await;
            Ok(())
        }
    };

    let spec = worker::Spec::new_with_start("child1", routine);

    let ctx = Context::new();
    let result = spec.start(&ctx, "root").await.map_err(|err| err.1);
    let worker = result.expect("expecting successful worker creation");

    let _ = worker.terminate().await;
    after_rx.recv().await;
}

#[tokio::test]
async fn test_worker_start_failure() {
    let spec = start_err_worker("child1", "boom");

    let ctx = Context::new();
    let start_err = spec
        .start(&ctx, "root")
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker start must fail");
    assert!(start_err.is_worker_init_error());
}

#[tokio::test]
async fn test_worker_start_timeout_failure() {
    time::pause();

    let spec = start_timeout_worker(
        "child1",
        Duration::from_secs(1), // timeout
        Duration::from_secs(3), // delay
    );

    let ctx = Context::new();
    let worker_fut = spec.start(&ctx, "root");

    time::advance(Duration::from_secs(4)).await;

    let err = worker_fut
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker start must fail");

    assert!(err.is_timeout_error());
}

#[tokio::test]
async fn test_worker_start_not_invoked() {
    let spec = no_start_worker("child1");

    let ctx = Context::new();
    let err = spec
        .start(&ctx, "root")
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker start must fail");
    assert!(err.is_notify_start_error());
}

#[tokio::test]
async fn test_worker_termination_with_panic() {
    let spec = panic_worker("child1", "panic boom");
    let ctx = Context::new();
    let worker = spec
        .start(&ctx, "root")
        .await
        .map_err(|err| err.1)
        .expect("worker start must succeed");
    let err = worker
        .terminate()
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker termination must fail");
    assert!(err.is_join_handle_error())
}

#[tokio::test]
async fn test_worker_termination_success() {
    let spec = termination_success_worker("child1");

    let ctx = Context::new();
    let worker = spec
        .start(&ctx, "root")
        .await
        .map_err(|err| err.1)
        .expect("worker must be spawned");

    worker
        .terminate()
        .await
        .map_err(|err| err.1)
        .expect("worker must terminate");
}

#[tokio::test]
async fn test_worker_termination_failure() {
    let spec = termination_failed_worker("child1");

    let ctx = Context::new();
    let worker = spec
        .start(&ctx, "root")
        .await
        .map_err(|err| err.1)
        .expect("worker start must succeed");

    let err = worker
        .terminate()
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker termination must fail");
    assert!(err.is_worker_termination_error());
}

#[tokio::test]
async fn test_worker_termination_timeout_failure() {
    time::pause();

    let spec = termination_timeout_worker(
        "child1",
        Duration::from_secs(1), // timeout
        Duration::from_secs(3), // delay
    );

    let ctx = Context::new();
    let worker = spec
        .start(&ctx, "root")
        .await
        .map_err(|err| err.1)
        .expect("worker start must succeed");

    let termination_fut = worker.terminate();

    time::advance(Duration::from_secs(4)).await;

    let err = termination_fut
        .await
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("worker termination must fail");
    assert!(err.is_timeout_error());
}
