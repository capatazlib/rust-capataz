#![deny(missing_docs)]

//! The capataz crate offers a lightweight, composable supervision tree API
//! inspired by Erlang's OTP which sits on top of the tokio future library. The
//! crate provides:
//!
//! * A `Context` type to signal termination of supervised tasks

/// This module provides the `Context` type which offers a contract to terminate
/// supervised processes futures in a way that is explicit, reliable and safe.
pub mod context;

/// Provides an internal `StartNotifier` type that helps notify task start
/// outcomes from a caller running on a different thread.
mod notifier;

/// Provides an internal `TaskSpec` and `RunningTask` types that help track the
/// outcome of a wrapped future.
mod task;
