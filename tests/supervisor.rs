#![cfg(test)]

mod common;

use capataz::context::*;
use capataz::events::{
    supervisor_started, testing_event_notifier, worker_started, worker_terminated,
};
use capataz::supervisor;

use common::*;

#[tokio::test]
async fn test_simple_start_and_stop_with_one_child() {
    let (event_notifier, event_buffer) = testing_event_notifier().await;
    let spec = supervisor::Spec::new("root", vec![wait_done_worker("one")], event_notifier);

    let ctx = Context::new();

    let start_result = spec.start(&ctx).await;
    let sup = start_result.expect("successful supervisor start");

    let terminate_result = sup.terminate().await;
    let _spec = terminate_result.expect("successful supervisor termination");

    event_buffer
        .assert_exact(vec![
            worker_started("/root/one"),
            supervisor_started("/root"),
            worker_terminated("/root/one"),
        ])
        .await;
}

#[tokio::test]
async fn test_simple_start_and_stop_with_multiple_children() {
    let (event_notifier, event_buffer) = testing_event_notifier().await;
    let spec = supervisor::Spec::new(
        "root",
        vec![
            wait_done_worker("one"),
            wait_done_worker("two"),
            wait_done_worker("three"),
        ],
        event_notifier,
    );

    let ctx = Context::new();

    let start_result = spec.start(&ctx).await;
    let sup = start_result.expect("successful supervisor start");

    let terminate_result = sup.terminate().await;
    let _spec = terminate_result.expect("successful supervisor termination");

    event_buffer
        .assert_exact(vec![
            worker_started("/root/one"),
            worker_started("/root/two"),
            worker_started("/root/three"),
            supervisor_started("/root"),
            worker_terminated("/root/three"),
            worker_terminated("/root/two"),
            worker_terminated("/root/one"),
        ])
        .await;
}
