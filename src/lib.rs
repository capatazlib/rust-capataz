#[macro_use]
extern crate lazy_static;

// I dislike Default trait, like... a lot
#[allow(clippy::new_without_default)]
mod context;
mod events;
mod worker;

pub use context::Context;
pub use events::{Event, EventNotifier};
pub use worker::{Restart, Shutdown, StartNotifier};

pub type Worker = worker::Spec;
