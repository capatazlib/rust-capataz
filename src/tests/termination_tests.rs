use anyhow::anyhow;

use crate::tests::workers::*;
use crate::{with_restart, Context, EventAssert, EventListener, Restart, SupervisorSpec};

#[tokio::test]
async fn test_supervisor_termination_cleanup_error() {
    let spec = SupervisorSpec::new_with_cleanup("root", vec![], || {
        let nodes = vec![wait_done_worker("worker", vec![])];
        let cleanup = || Err(anyhow!("failing on cleanup"));
        Ok((nodes, cleanup))
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start without errors");

    let (result, _spec) = sup.terminate().await;
    let termination_err = result.expect_err("supervisor should terminate with error");

    assert_eq!(
        "supervisor failed to terminate",
        format!("{}", termination_err)
    );

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_termination_failed("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_termination_failed("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_two_level_nested_supervisor_termination_cleanup_failure() {
    let spec = SupervisorSpec::new("root", vec![], || {
        let subtree_spec = SupervisorSpec::new_with_cleanup("subtree", vec![], || {
            let nodes = vec![wait_done_worker("worker", vec![])];
            let cleanup = || Err(anyhow!("failing on cleanup"));
            Ok((nodes, cleanup))
        });
        vec![subtree_spec.subtree(vec![])]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    let (result, _spec) = sup.terminate().await;
    let _ = result.expect_err("supervisor should terminate without errors");

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_termination_failed("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/subtree/worker"),
            EventAssert::supervisor_started("/root/subtree"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/subtree/worker"),
            EventAssert::supervisor_termination_failed("/root/subtree"),
            EventAssert::supervisor_termination_failed("/root"),
        ])
        .await;
}

#[tokio::test]
async fn temporary_children_terminate_without_errors() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        let trigger_listener = trigger_listener.clone();
        let max_termination_count = 1;
        let worker = trigger_listener.to_success_termination_worker(
            "one",
            vec![with_restart(Restart::Temporary)],
            max_termination_count,
        );

        vec![worker]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    // wait for the child to terminate on it's own before calling the wait
    // endpoint; this avoids race conditions on the test
    ev_buffer
        .wait_till(
            EventAssert::supervisor_started("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    // Send a signal to worker to terminate without error.
    worker_triggerer.trigger().await;

    ev_buffer
        .wait_till(
            EventAssert::worker_terminated("/root/one"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    let (result, _spec) = sup.wait().await;
    assert!(result.is_ok());

    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/one"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/one"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}
