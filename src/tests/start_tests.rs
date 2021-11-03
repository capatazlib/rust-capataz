use anyhow::anyhow;

use crate::tests::workers::*;
use crate::{Context, EventAssert, EventListener, SupervisorSpec};

#[tokio::test]
async fn test_single_worker_start_and_termination() {
    let spec = SupervisorSpec::new("root", vec![], || vec![wait_done_worker("worker", vec![])]);

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    let (result, _spec) = sup.terminate().await;

    result.expect("supervisor should terminate without errors");

    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/worker"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_multiple_single_level_workers_start_and_termination() {
    let spec = SupervisorSpec::new("root", vec![], || {
        vec![
            wait_done_worker("worker1", vec![]),
            wait_done_worker("worker2", vec![]),
            wait_done_worker("worker3", vec![]),
        ]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    let (result, _spec) = sup.terminate().await;

    result.expect("supervisor should terminate without errors");

    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker1"),
            EventAssert::worker_started("/root/worker2"),
            EventAssert::worker_started("/root/worker3"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/worker3"),
            EventAssert::worker_terminated("/root/worker2"),
            EventAssert::worker_terminated("/root/worker1"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_multiple_single_level_fail_start_worker() {
    let spec = SupervisorSpec::new("root", vec![], || {
        vec![
            wait_done_worker("worker1", vec![]),
            wait_done_worker("worker2", vec![]),
            fail_start_worker("worker3", vec![]),
            // The start procedure never arrives to this worker
            wait_done_worker("worker4", vec![]),
        ]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let result = spec.start(Context::new(), ev_listener).await;

    match result {
        Err(start_err) => assert_eq!(
            "supervisor failed to start: worker failed to start: failing worker",
            format!("{}", start_err)
        ),
        Ok(_) => assert!(false, "expecting error, got started supervisor"),
    };

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_start_failed("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    // worker never gets started/terminated because we start from left to
    // right
    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/worker1"),
            EventAssert::worker_started("/root/worker2"),
            EventAssert::worker_start_failed("/root/worker3"),
            EventAssert::worker_terminated("/root/worker2"),
            EventAssert::worker_terminated("/root/worker1"),
            EventAssert::supervisor_start_failed("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_single_level_supervisor_build_error() {
    let spec = SupervisorSpec::new_with_cleanup("root", vec![], || {
        Err(anyhow!("build failure"))?;
        let nodes = vec![];
        let cleanup = || Ok(());
        Ok((nodes, cleanup))
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let result = spec.start(Context::new(), ev_listener).await;

    match result {
        Err(start_err) => assert_eq!(
            "supervisor failed to build nodes: build failure",
            format!("{}", start_err)
        ),
        Ok(_) => assert!(false, "expecting error, got started supervisor"),
    };

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_build_failed("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    // worker never gets started/terminated because we start from left to
    // right
    ev_buffer
        .assert_exact(vec![EventAssert::supervisor_build_failed("/root")])
        .await;
}

#[tokio::test]
async fn test_two_level_start_and_termination() {
    let spec = SupervisorSpec::new("root", vec![], || {
        let subtree_spec = SupervisorSpec::new("subtree", vec![], || {
            vec![wait_done_worker("worker", vec![])]
        });
        vec![subtree_spec.subtree(vec![])]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    let (result, _spec) = sup.terminate().await;
    result.expect("supervisor should terminate without errors");

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
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
            EventAssert::supervisor_terminated("/root/subtree"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_two_level_nested_supervisor_build_error() {
    let spec = SupervisorSpec::new("root", vec![], || {
        let subtree_spec = SupervisorSpec::new_with_cleanup("subtree", vec![], || {
            // Usually, clients will never return an Err from a failure but instead would
            // use the ? operator.
            Err(anyhow!("build failure"))?;
            // Return an empty vector because otherwise, the compiler is not able to figure out the
            // result type. This makes the example more realistic because having a construction
            // that always fails is non-sensical
            let nodes = vec![];
            let cleanup = || Ok(());
            Ok((nodes, cleanup))
        });
        vec![subtree_spec.subtree(vec![])]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let result = spec.start(Context::new(), ev_listener).await;

    match result {
        Err(err) => assert_eq!(
            "supervisor failed to start: supervisor failed to build nodes: build failure",
            format!("{}", err)
        ),
        Ok(_) => assert!(false, "supervisor should have build error; it didn't"),
    }

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_start_failed("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::supervisor_build_failed("/root/subtree"),
            EventAssert::supervisor_start_failed("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_multi_level_start_and_termination() {
    let spec = SupervisorSpec::new("root", vec![], || {
        let mut nodes = Vec::new();
        for i in 1..=3 {
            let level_one_spec = SupervisorSpec::new(format!("subtree-{}", i), vec![], move || {
                let mut nodes = Vec::new();
                for j in 1..=3 {
                    let node_name = format!("worker-{}-{}", i, j);
                    let level_two_spec =
                        SupervisorSpec::new(format!("subtree-{}-{}", i, j), vec![], move || {
                            vec![wait_done_worker(&node_name, vec![])]
                        });
                    nodes.push(level_two_spec.subtree(vec![]));
                }
                nodes
            });
            nodes.push(level_one_spec.subtree(vec![]));
        }
        nodes
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;
    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    let (result, _spec) = sup.terminate().await;
    result.expect("supervisor should terminate without errors");

    // Wait till /root has terminated to get right assertion of events
    ev_buffer
        .wait_till(
            EventAssert::supervisor_terminated("/root"),
            std::time::Duration::from_millis(250),
        )
        .await
        .expect("event should happen");

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_started("/root/subtree-1/subtree-1-1/worker-1-1"),
            EventAssert::supervisor_started("/root/subtree-1/subtree-1-1"),
            EventAssert::worker_started("/root/subtree-1/subtree-1-2/worker-1-2"),
            EventAssert::supervisor_started("/root/subtree-1/subtree-1-2"),
            EventAssert::worker_started("/root/subtree-1/subtree-1-3/worker-1-3"),
            EventAssert::supervisor_started("/root/subtree-1/subtree-1-3"),
            EventAssert::supervisor_started("/root/subtree-1"),
            EventAssert::worker_started("/root/subtree-2/subtree-2-1/worker-2-1"),
            EventAssert::supervisor_started("/root/subtree-2/subtree-2-1"),
            EventAssert::worker_started("/root/subtree-2/subtree-2-2/worker-2-2"),
            EventAssert::supervisor_started("/root/subtree-2/subtree-2-2"),
            EventAssert::worker_started("/root/subtree-2/subtree-2-3/worker-2-3"),
            EventAssert::supervisor_started("/root/subtree-2/subtree-2-3"),
            EventAssert::supervisor_started("/root/subtree-2"),
            EventAssert::worker_started("/root/subtree-3/subtree-3-1/worker-3-1"),
            EventAssert::supervisor_started("/root/subtree-3/subtree-3-1"),
            EventAssert::worker_started("/root/subtree-3/subtree-3-2/worker-3-2"),
            EventAssert::supervisor_started("/root/subtree-3/subtree-3-2"),
            EventAssert::worker_started("/root/subtree-3/subtree-3-3/worker-3-3"),
            EventAssert::supervisor_started("/root/subtree-3/subtree-3-3"),
            EventAssert::supervisor_started("/root/subtree-3"),
            EventAssert::supervisor_started("/root"),
            EventAssert::worker_terminated("/root/subtree-3/subtree-3-3/worker-3-3"),
            EventAssert::supervisor_terminated("/root/subtree-3/subtree-3-3"),
            EventAssert::worker_terminated("/root/subtree-3/subtree-3-2/worker-3-2"),
            EventAssert::supervisor_terminated("/root/subtree-3/subtree-3-2"),
            EventAssert::worker_terminated("/root/subtree-3/subtree-3-1/worker-3-1"),
            EventAssert::supervisor_terminated("/root/subtree-3/subtree-3-1"),
            EventAssert::supervisor_terminated("/root/subtree-3"),
            EventAssert::worker_terminated("/root/subtree-2/subtree-2-3/worker-2-3"),
            EventAssert::supervisor_terminated("/root/subtree-2/subtree-2-3"),
            EventAssert::worker_terminated("/root/subtree-2/subtree-2-2/worker-2-2"),
            EventAssert::supervisor_terminated("/root/subtree-2/subtree-2-2"),
            EventAssert::worker_terminated("/root/subtree-2/subtree-2-1/worker-2-1"),
            EventAssert::supervisor_terminated("/root/subtree-2/subtree-2-1"),
            EventAssert::supervisor_terminated("/root/subtree-2"),
            EventAssert::worker_terminated("/root/subtree-1/subtree-1-3/worker-1-3"),
            EventAssert::supervisor_terminated("/root/subtree-1/subtree-1-3"),
            EventAssert::worker_terminated("/root/subtree-1/subtree-1-2/worker-1-2"),
            EventAssert::supervisor_terminated("/root/subtree-1/subtree-1-2"),
            EventAssert::worker_terminated("/root/subtree-1/subtree-1-1/worker-1-1"),
            EventAssert::supervisor_terminated("/root/subtree-1/subtree-1-1"),
            EventAssert::supervisor_terminated("/root/subtree-1"),
            EventAssert::supervisor_terminated("/root"),
        ])
        .await;
}
