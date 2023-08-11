use crate::tests::workers::worker_trigger;
use crate::{with_restart, Context, EventAssert, EventListener, Restart, SupervisorSpec};

#[tokio::test]
async fn test_one_for_one_single_level_worker_restart_with_failure() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        // clone the signaler reference every time we restart the
        // supervision tree. In this test-case it should happen only once.
        let trigger_listener = trigger_listener.clone();
        let max_err_count = 1;

        let worker = trigger_listener.to_fail_runtime_worker(
            "worker",
            vec![with_restart(Restart::Transient)],
            max_err_count,
        );

        vec![worker]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    // Wait till the event buffer has collected the supervisor root started
    // event.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_started("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("supervisor should have started");

    // Send a signal to the fail_runtime_worker to return a runtime error.
    worker_triggerer.trigger().await;

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::worker_started("/root/worker"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("worker should have re-started");

    // Now, terminate the supervision tree, the test has concluded.
    let (result, _spec) = sup.terminate().await;

    // Wait for the root supervisor to report it has finished.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("supervisor should have terminated");

    result.expect("supervisor should terminate without errors");

    // Assert events happened in the correct order.
    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_runtime_failed("/root/worker"),
            EventAssert::worker_started("/root/worker"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_one_for_one_single_level_worker_does_not_restart_with_ok_termination() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        // clone the signaler reference every time we restart the
        // supervision tree. In this test-case it should happen only once.
        let trigger_listener = trigger_listener.clone();
        let max_termination_count = 1;

        let worker = trigger_listener.to_success_termination_worker(
            "worker",
            vec![with_restart(Restart::Transient)],
            max_termination_count,
        );

        vec![worker]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    // Wait till the event buffer has collected the supervisor root started
    // event.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_started("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("supervisor should have started");

    // Send a signal to the fail_runtime_worker to return a runtime error.
    worker_triggerer.trigger().await;

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::worker_terminated("/root/worker"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("worker should have re-started");

    // Now, terminate the supervision tree, the test has concluded.
    let (result, _spec) = sup.terminate().await;

    // Wait for the root supervisor to report it has finished.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("supervisor should have terminated");

    result.expect("supervisor should terminate without errors");

    // Assert events happened in the correct order.
    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}
