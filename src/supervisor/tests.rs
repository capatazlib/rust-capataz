use std::sync::Arc;
use std::time::Duration;

use tokio::time;

use crate::context::*;
use crate::events::{
    supervisor_start_failed, supervisor_started, supervisor_terminated, testing_event_notifier,
    worker_start_failed, worker_started, worker_terminated, worker_termination_failed, EventAssert,
};
use crate::supervisor;
use crate::worker;
use crate::worker::tests::{
    start_err_worker, start_timeout_worker, termination_failed_worker, wait_done_worker,
};

// start_stop_supervisor is a helper function that creates a supervisor and
// then asserts it started successfuly; after that assertion, it executes
// its termination and asserts that termination was done successfully. This
// helper function expects workers that do not fail on start or termination
// (wait_done_worker).
async fn start_stop_supervisor(
    root_name: &str,
    children: Vec<worker::Spec>,
    assertions: Vec<EventAssert>,
) {
    let (event_notifier, mut event_buffer) = testing_event_notifier().await;
    let spec = supervisor::Spec::new(root_name, children, event_notifier);
    let ctx = Context::new();
    let start_result = spec.start(&ctx).await;
    let sup = start_result
        .map_err(|err| err.1)
        .expect("successful supervisor start");

    event_buffer
        .wait_till(
            supervisor_started(format!("/{}", root_name)),
            Duration::from_millis(100),
        )
        .await
        .expect("supervisor started timeout");

    let (_, terminate_result) = sup.terminate().await;
    let _ = terminate_result.expect("successful supervisor termination");

    event_buffer
        .wait_till(
            supervisor_terminated(format!("/{}", root_name)),
            Duration::from_millis(10),
        )
        .await
        .expect("supervisor termination timeout");

    event_buffer.assert_exact(assertions).await;
}

// err_start_supervisor is a helper function that creates a supervisor and
// then asserts that it fails at start. This helper function expects workers
// that fail on start (start_err_worker).
async fn err_start_supervisor(
    root_name: &str,
    children: Vec<worker::Spec>,
    assertions: Vec<EventAssert>,
) {
    let (event_notifier, mut event_buffer) = testing_event_notifier().await;
    let spec = supervisor::Spec::new(root_name, children, event_notifier);
    let ctx = Context::new();

    let start_result = spec.start(&ctx).await;
    let start_err = start_result
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("supervisor start must fail");

    assert!(start_err.failing_child_error.is_worker_init_error());

    event_buffer
        .wait_till(
            supervisor_start_failed(format!("/{}", root_name)),
            Duration::from_millis(10),
        )
        .await
        .expect("expected supervisor starte failure event timed out");

    event_buffer.assert_exact(assertions).await;
}

// start_timeout_supervisor is a helper function that creates a supervisor
// and then asserts that it fails with a timeout at start. This helper
// function expects workers that fail on the start (start_timeout_worker).
// The first parameter is a delay to forward to the virtual clock to kick
// timeouts.
async fn start_timeout_supervisor(
    start_delay: time::Duration,
    root_name: &str,
    children: Vec<worker::Spec>,
    assertions: Vec<EventAssert>,
) -> Arc<supervisor::StartError> {
    let (event_notifier, mut event_buffer) = testing_event_notifier().await;
    let spec = supervisor::Spec::new(root_name, children, event_notifier);

    let ctx = Context::new();

    time::pause();

    let start_fut = spec.start(&ctx);

    time::advance(start_delay).await;

    let start_result = start_fut.await;
    let start_err = start_result
        .map(|_| ())
        .map_err(|err| err.1)
        .expect_err("supervisor start must fail");

    assert!(start_err.failing_child_error.is_timeout_error());

    event_buffer
        .wait_till(
            supervisor_start_failed(format!("/{}", root_name)),
            Duration::from_millis(10),
        )
        .await
        .expect("expected supervisor started failure event timed out");

    event_buffer.assert_exact(assertions).await;
    return start_err;
}

#[tokio::test]
async fn test_simple_start_and_stop_with_one_child() {
    start_stop_supervisor(
        "root",
        vec![wait_done_worker("one")],
        vec![
            worker_started("/root/one"),
            supervisor_started("/root"),
            worker_terminated("/root/one"),
            supervisor_terminated("/root"),
        ],
    )
    .await
}

#[tokio::test]
async fn test_simple_start_and_stop_with_multiple_children() {
    start_stop_supervisor(
        "root",
        vec![
            wait_done_worker("one"),
            wait_done_worker("two"),
            wait_done_worker("three"),
        ],
        vec![
            worker_started("/root/one"),
            worker_started("/root/two"),
            worker_started("/root/three"),
            supervisor_started("/root"),
            worker_terminated("/root/three"),
            worker_terminated("/root/two"),
            worker_terminated("/root/one"),
            supervisor_terminated("/root"),
        ],
    )
    .await
}

#[tokio::test]
async fn test_failing_start_with_multiple_children() {
    err_start_supervisor(
        "root",
        vec![
            wait_done_worker("one"),
            start_err_worker("two", "boom"),
            wait_done_worker("three"),
        ],
        vec![
            worker_started("/root/one"),
            worker_start_failed("/root/two"),
            worker_terminated("/root/one"),
            supervisor_start_failed("/root"),
        ],
    )
    .await
}

#[tokio::test]
async fn test_timeout_start_with_multiple_children() {
    let worker_max_start_delay = Duration::from_secs(1);
    let worker_start_delay = Duration::from_secs(3);
    let _ = start_timeout_supervisor(
        worker_start_delay,
        "root",
        vec![
            wait_done_worker("one"),
            wait_done_worker("two"),
            start_timeout_worker(
                "three",
                worker_max_start_delay, // timeout
                worker_start_delay,     // delay
            ),
        ],
        vec![
            worker_started("/root/one"),
            worker_started("/root/two"),
            worker_start_failed("/root/three"),
            worker_terminated("/root/two"),
            worker_terminated("/root/one"),
            supervisor_start_failed("/root"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_start_timeout_and_termination_timeout_with_multiple_children() {
    let worker_max_start_delay = Duration::from_secs(1);
    let worker_start_delay = Duration::from_secs(3);

    let start_err = start_timeout_supervisor(
        worker_start_delay,
        "root",
        vec![
            wait_done_worker("one"),
            termination_failed_worker("two"),
            start_timeout_worker(
                "three",
                worker_max_start_delay, // declared timeout
                worker_start_delay,
            ),
        ],
        vec![
            worker_started("/root/one"),
            worker_started("/root/two"),
            worker_start_failed("/root/three"),
            worker_termination_failed("/root/two"),
            worker_terminated("/root/one"),
            supervisor_start_failed("/root"),
        ],
    )
    .await;

    // there is an entry in the termination error
    let mtermination_err = start_err.failing_sibling_termination_error.as_ref();
    match mtermination_err {
        None => panic!("termination error should be present"),
        Some(ref termination_err) => {
            assert!(
                !termination_err.0.is_empty(),
                "termination error map must not be empty"
            );
            termination_err
                .0
                .get("/root/two")
                .expect("termination error must be in '/root/two' child");
        }
    }
}
