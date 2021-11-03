use tokio::time::{self, Duration};

use crate::tests::workers::*;
use crate::{Context, EventAssert, EventListener, SupervisorSpec, Worker};

#[tokio::test]
async fn test_worker_with_start_timeout() {
    time::pause();
    let spec = SupervisorSpec::new("root", vec![], || {
        vec![never_start_worker(
            "worker",
            vec![Worker::with_start_timeout(Duration::from_secs(1))],
        )]
    });

    let (ev_listener, ev_buffer) = EventListener::new_testing_listener().await;
    let result = spec.start(Context::new(), ev_listener).await;

    time::advance(time::Duration::from_secs(2)).await;

    match result {
        Err(start_err) => assert_eq!(
            "supervisor failed to start: worker timed out on start",
            format!("{}", start_err)
        ),
        Ok(_) => assert!(false, "expecting error, got started supervisor"),
    };

    ev_buffer
        .assert_exact(vec![
            EventAssert::worker_start_timed_out("/root/worker"),
            EventAssert::supervisor_start_failed("/root"),
        ])
        .await;
}

#[tokio::test]
async fn test_worker_with_termination_timeout() {
    time::pause();
    let spec = SupervisorSpec::new("root", vec![], || {
        vec![never_terminate_worker(
            "worker",
            vec![Worker::with_termination_timeout(Duration::from_secs(1))],
        )]
    });

    let (ev_listener, mut ev_buffer) = EventListener::new_testing_listener().await;

    let sup = spec
        .start(Context::new(), ev_listener)
        .await
        .expect("supervisor should start with no error");

    // Register the termination on the futures runtime.
    let termination = sup.terminate();

    // Wait more than the max termination duration
    time::advance(time::Duration::from_secs(2)).await;

    let (result, _spec) = termination.await;
    match result {
        Err(termination_err) => assert_eq!(
            "supervisor failed to terminate",
            format!("{}", termination_err)
        ),
        Ok(_) => assert!(false, "expecting error, got terminated supervisor"),
    };

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
            EventAssert::worker_termination_timed_out("/root/worker"),
            EventAssert::supervisor_termination_failed("/root"),
        ])
        .await;
}
