#![cfg(test)]

mod common;

use tokio::sync::mpsc;

use capataz::context::*;
use capataz::worker::{self, StartNotifier};

use common::*;

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
    let start_result = spec.start(&ctx, "root").await;
    let worker = start_result.expect("successful worker creation");

    before_rx.recv().await;
    let stop_result = worker.terminate().await;
    let _ = stop_result.expect("successful worker termination");
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
    let result = spec.start(&ctx, "root").await;
    let worker = result.expect("expecting successful worker creation");

    let _ = worker.terminate().await;
    after_rx.recv().await;
}

#[tokio::test]
async fn test_worker_start_failure() {
    let spec = start_err_worker("child1", "boom");

    let ctx = Context::new();
    let result = spec.start(&ctx, "root").await;
    match result {
        Err(start_err1) => assert_eq!("boom", format!("{:?}", start_err1)),
        Ok(_) => panic!("expecting error, got result"),
    }
}
