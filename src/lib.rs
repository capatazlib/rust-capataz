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
    mod restart;
    mod start_tests;
    mod termination_tests;
    mod timeout_tests;
    mod workers;

    // // /*
    // // #[tokio::test]
    // // async fn test_single_level_worker_transient_restart()

    // // #[tokio::test]
    // // async fn test_single_level_worker_temporary_restart()
    // // */
}
