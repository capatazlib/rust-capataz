#![deny(missing_docs)]

//! The capataz crate offers a lightweight, composable supervision tree API
//! inspired by Erlang's OTP which sits on top of the tokio future library. The
//! crate provides:
//!
//! * A `Context` type to signal termination of supervised tasks

/// This module exports all the symbols needed to effectively use the Capataz
/// API without any other import.
pub mod prelude;

/// This module provides the `Context` type which offers a contract to terminate
/// supervised processes futures in a way that is explicit, reliable and safe.
pub mod context;
// pub use context::Context;

/// Provides an internal `StartNotifier` type that helps notify task start
/// outcomes from a caller running on a different thread.
mod notifier;

/// Provides an internal `TaskSpec` and `RunningTask` types that help track the
/// outcome of a wrapped future.
mod task;

/// Provides an API to notify and collect events in the supervision tree.
mod events;
// pub use events::Event;
// pub use events::EventListener;

/// Contains the types and logic to create, start and terminate nodes in the
/// supervision tree.
mod node;

/// Contains the types and logic to manage a supervisor.
pub mod supervisor;

/// Contains the types and logic to manage a worker.
pub mod worker;

// pub use task::Restart;

pub use crate::events::{Event, EventListener, NodeData};
pub use context::Context;

#[cfg(test)]
pub use events::{EventAssert, EventBufferCollector};

#[cfg(test)]
mod tests {
    mod restart;
    mod start_tests;
    mod termination_tests;
    mod timeout_tests;
    mod workers;
}
