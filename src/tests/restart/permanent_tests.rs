use tokio::time;

use crate::tests::workers::{wait_done_worker, worker_trigger};
use crate::{Context, EventAssert, EventListener, SupervisorSpec};

#[tokio::test]
async fn test_single_level_worker_restart_with_failure() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        // clone the signaler reference every time we restart the
        // supervision tree. In this test-case it should happen only once.
        let trigger_listener = trigger_listener.clone();
        let max_err_count = 1;

        let worker = trigger_listener.to_fail_runtime_worker("worker", vec![], max_err_count);

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
            EventAssert::worker_termination_failed("/root/worker"),
            EventAssert::worker_started("/root/worker"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_single_level_worker_restart_with_ok_termination() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        // clone the signaler reference every time we restart the
        // supervision tree. In this test-case it should happen only once.
        let trigger_listener = trigger_listener.clone();
        let max_termination_count = 1;

        let worker =
            trigger_listener.to_success_termination_worker("worker", vec![], max_termination_count);

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
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::worker_started("/root/worker"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_single_level_worker_to_many_restart() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new(
        "root",
        vec![SupervisorSpec::with_restart_tolerance(
            0,
            time::Duration::from_secs(5),
        )],
        move || {
            // clone the signaler reference every time we restart the
            // supervision tree. In this test-case it should happen only once.
            let trigger_listener = trigger_listener.clone();
            let max_err_count = 1;
            let worker = trigger_listener.to_fail_runtime_worker("worker", vec![], max_err_count);
            vec![worker]
        },
    );

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

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_restarted_to_many_times("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("supervisor should have failed");

    result.expect_err("supervisor should terminate with errors");

    // Assert events happened in the correct order.
    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_termination_failed("/root/worker"),
            EventAssert::supervisor_restarted_to_many_times("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_multi_level_worker_to_many_restart_recovery() {
    let (worker_triggerer, trigger_listener) = worker_trigger::new();

    let spec = SupervisorSpec::new("root", vec![], move || {
        // Build no tolerance for child sub-tree
        let restart_tolerance =
            SupervisorSpec::with_restart_tolerance(0, time::Duration::from_secs(5));
        let subtree_listener = trigger_listener.clone();

        let subtree_spec = SupervisorSpec::new("subtree", vec![restart_tolerance], move || {
            let subtree_listener = subtree_listener.clone();
            let max_err_count = 1;
            let worker = subtree_listener.to_fail_runtime_worker("worker-2", vec![], max_err_count);
            vec![
                wait_done_worker("worker-1", vec![]),
                worker,
                wait_done_worker("worker-3", vec![]),
            ]
        });
        vec![subtree_spec.subtree(vec![])]
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
            EventAssert::worker_started("/root/subtree/worker-3"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("worker should have re-started");

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_restarted_to_many_times("/root/subtree"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("subtree should have failed");

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_started("/root/subtree"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("subtree should have re-started");

    // Now, terminate the supervision tree, the test has concluded.
    let (result, _spec) = sup.terminate().await;

    // Wait for the worker restart to propagate on the event buffer.
    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("root supervisor should have terminated");

    result.expect("root supervisor should not terminate with errors");

    // Assert events happened in the correct order.
    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/subtree/worker-1"),
            EventAssert::worker_started("/root/subtree/worker-2"),
            EventAssert::worker_started("/root/subtree/worker-3"),
            EventAssert::supervisor_started("/root/subtree"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_termination_failed("/root/subtree/worker-2"),
            EventAssert::worker_terminated("/root/subtree/worker-3"),
            EventAssert::worker_terminated("/root/subtree/worker-1"),
            EventAssert::supervisor_restarted_to_many_times("/root/subtree"),
            EventAssert::worker_started("/root/subtree/worker-1"),
            EventAssert::worker_started("/root/subtree/worker-2"),
            EventAssert::worker_started("/root/subtree/worker-3"),
            EventAssert::supervisor_started("/root/subtree"),
            EventAssert::worker_terminated("/root/subtree/worker-3"),
            EventAssert::worker_terminated("/root/subtree/worker-2"),
            EventAssert::worker_terminated("/root/subtree/worker-1"),
            EventAssert::supervisor_terminated("/root/subtree"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}
