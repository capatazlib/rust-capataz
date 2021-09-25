#![deny(missing_docs)]

//! The capataz crate offers a lightweight, composable supervision tree API
//! inspired by Erlang's OTP which sits on top of the tokio future library. The
//! crate provides:
//!
//! * A `Context` type to signal termination of supervised tasks

/// This module provides the `Context` type which offers a contract to terminate
/// supervised processes futures in a way that is explicit, reliable and safe.
pub mod context;
pub use context::Context;

/// Provides an internal `StartNotifier` type that helps notify task start
/// outcomes from a caller running on a different thread.
mod notifier;

/// Provides an internal `TaskSpec` and `RunningTask` types that help track the
/// outcome of a wrapped future.
mod task;

/// Provides an API to notify and collect events in the supervision tree.
mod events;

/// Contains the types and logic to create, start and terminate nodes in the
/// supervision tree.
mod node;
pub use events::Event;
pub use events::EventListener;
pub use node::leaf::Opt as WorkerOpt;
pub use node::leaf::Spec as Worker;
pub use node::leaf::StartNotifier;
pub use node::root::Opt as SupervisorOpt;
pub use node::root::Root as StartOrder;
pub use node::root::Root as Supervisor;
pub use node::root::Spec as SupervisorSpec;
pub use node::Node;
pub use std::time::Duration;

#[cfg(test)]
pub use events::{EventAssert, EventBufferCollector};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::anyhow;
    use futures::future::{BoxFuture, FutureExt};
    use tokio::sync::{mpsc, Mutex};
    use tokio::time;

    use crate::context::Context;
    use crate::notifier;
    use crate::{Duration, Node, SupervisorSpec, Worker, WorkerOpt};
    use crate::{EventAssert, EventListener};

    type StartNotifier = notifier::StartNotifier<anyhow::Error>;

    fn wait_done_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
        Worker::new(name, opts, |ctx: Context| async move {
            let _ = ctx.done().await;
            Ok(())
        })
    }

    fn fail_start_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
        Worker::new_with_start(
            name,
            opts,
            |_ctx: Context, start_notifier: StartNotifier| async move {
                let err = anyhow!("failing worker");
                start_notifier.failed(err);
                Ok(())
            },
        )
    }

    fn never_start_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
        Worker::new_with_start(
            name,
            opts,
            |ctx: Context, start_notifier: StartNotifier| async move {
                // wait for termination of the supervision tree
                let _ = ctx.done().await;
                // use the start_notifer in the body of the function to avoid
                // already dropped errors
                start_notifier.success();
                Ok(())
            },
        )
    }

    fn never_terminate_worker(name: &str, opts: Vec<WorkerOpt>) -> Node {
        Worker::new(name, opts, |_ctx: Context| async move {
            futures::future::pending().await
        })
    }

    struct Signaler {
        sender: mpsc::Sender<()>,
        receiver: Arc<Mutex<mpsc::Receiver<()>>>,
    }

    impl Signaler {
        pub(crate) fn new() -> Self {
            let (sender, receiver) = mpsc::channel(10);
            Self {
                sender,
                receiver: Arc::new(Mutex::new(receiver)),
            }
        }

        pub(crate) async fn send_signal(&self) {
            let _ = self.sender.send(()).await;
        }

        pub(crate) async fn wait_signal(&mut self) {
            let mut receiver = self.receiver.lock().await;
            let _ = receiver.recv().await;
        }
    }

    impl Clone for Signaler {
        fn clone(&self) -> Self {
            Self {
                sender: self.sender.clone(),
                receiver: self.receiver.clone(),
            }
        }
    }

    fn fail_runtime_worker0(
        name: &str,
        opts: Vec<WorkerOpt>,
        max_fail_count: i32,
    ) -> (Node, impl FnMut() -> BoxFuture<'static, ()>) {
        // Create the communication channel that will signal start/end of
        // failures inside a task
        let (sender, receiver) = mpsc::channel(10);

        // Transform the sender in a callback that the API client can use; abstracting
        // away the fact we are using mpsc::Sender
        let fail_signal_sender = sender.clone();
        let send_fail_signal = move || {
            let sender = fail_signal_sender.clone();
            async move {
                let _ = sender.send(()).await;
            }
            .boxed()
        };

        // Given that receiver needs to be called multiple times because
        // of FnMut constraints on the Worker, we wrap the mpsc::Receiver in
        // an Arc/Mutex.
        let wait_for_fail_signal = Arc::new(Mutex::new(receiver));

        // Keep track of all the failures that have happened so far
        let fail_count = Arc::new(Mutex::new(0));

        // Build the worker that we are going to use for tests
        let node = Worker::new(name, opts, move |ctx: Context| {
            // Clone the wait_for_fail_signal receiver inside the Lambda
            let wait_for_fail_signal = wait_for_fail_signal.clone();
            let fail_count = fail_count.clone();
            async move {
                // If the fail_count has reached the max_fail_count, wait for termination
                // of the given Contet.
                let mut fail_count_ref = fail_count.lock().await;

                if *fail_count_ref >= max_fail_count {
                    let _ = ctx.done().await;
                    return Ok(());
                }
                // Otherwise, block the thread waiting for the API client to signal
                // an error should happen.
                let mut wait_for_fail_signal = wait_for_fail_signal.lock().await;
                wait_for_fail_signal.recv().await;
                std::mem::drop(wait_for_fail_signal);

                // Increase the fail count after waiting for the first failure.
                *fail_count_ref += 1;

                Err(anyhow!(
                    "fail_runtime_worker ({}/{})",
                    *fail_count_ref,
                    max_fail_count
                ))
            }
        });
        (node, send_fail_signal)
    }

    fn fail_runtime_worker(
        name: &str,
        opts: Vec<WorkerOpt>,
        max_fail_count: i32,
        signaler: Signaler,
    ) -> Node {
        // Keep track of all the failures that have happened so far on a shared
        // reference.
        let fail_count = Arc::new(Mutex::new(0));

        // Build the worker that we are going to use for tests
        let node = Worker::new(name, opts, move |ctx: Context| {
            // Clone the signaler for every time we return a new worker routine
            let mut signaler = signaler.clone();
            // Clone the shared failure count reference for every worker
            // instance.
            let fail_count = fail_count.clone();

            async move {
                // If the fail_count has reached the max_fail_count, wait for termination
                // of the given Context.
                let mut fail_count_ref = fail_count.lock().await;

                if *fail_count_ref >= max_fail_count {
                    let _ = ctx.done().await;
                    return Ok(());
                }

                // Otherwise, block the thread waiting for the API client to signal
                // an error should happen.
                signaler.wait_signal().await;

                // Increase the fail count after waiting for the first failure.
                *fail_count_ref += 1;

                Err(anyhow!(
                    "fail_runtime_worker ({}/{})",
                    *fail_count_ref,
                    max_fail_count
                ))
            }
        });
        node
    }

    #[tokio::test]
    async fn test_single_worker() {
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

    #[tokio::test]
    async fn test_multiple_single_level_workers() {
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
    async fn test_multiple_single_level_failing_worker() {
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
    async fn test_single_level_build_error() {
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
    async fn test_cleanup_error() {
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
    async fn test_two_level_nested_termination_cleanup_failure() {
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
    async fn test_two_level_nested_build_error() {
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
                let level_one_spec =
                    SupervisorSpec::new(format!("subtree-{}", i), vec![], move || {
                        let mut nodes = Vec::new();
                        for j in 1..=3 {
                            let node_name = format!("worker-{}-{}", i, j);
                            let level_two_spec = SupervisorSpec::new(
                                format!("subtree-{}-{}", i, j),
                                vec![],
                                move || vec![wait_done_worker(&node_name, vec![])],
                            );
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

    #[tokio::test]
    async fn test_single_level_worker_permanent_restart() {
        let signaler = Signaler::new();
        let root_signaler = signaler.clone();

        let spec = SupervisorSpec::new("root", vec![], move || {
            // clone the signaler reference every time we restart the
            // supervision tree. In this test-case it should happen only once.
            let signaler = root_signaler.clone();
            let max_err_count = 1;

            let worker = fail_runtime_worker("worker", vec![], max_err_count, signaler);

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
        signaler.send_signal().await;

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

    // // /*
    // // #[tokio::test]
    // // async fn test_single_level_worker_transient_restart()

    // // #[tokio::test]
    // // async fn test_single_level_worker_temporary_restart()
    // // */
}
