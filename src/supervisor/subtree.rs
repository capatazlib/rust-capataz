use futures::future::{abortable, AbortHandle, Aborted, BoxFuture, Future, FutureExt};
use std::time::Duration;

use crate::supervisor::{Context, Spec, Supervisor};
use crate::worker::{self, StartNotifier};

impl Supervisor {
    pub fn subtree(&mut self, sup_spec: Spec) -> (worker::Spec, oneshot::Receiver<Spec>) {
        let parent_name: String = self.data.name.clone();
        worker::Spec::new_with_start(
            &self.data.name,
            move |parent_ctx: Context, start_notifier: StartNotifier| async move {
                let (ctx, cancel) = parent_ctx.with_cancel();
                sup_spec
                    .run_supervisor(&ctx, &parent_name, start_notifier)
                    .await
            },
        )
        .shutdown(worker::Shutdown::Indefinitely)
        .tolerance(1, Duration::from_secs(5))
        .tag(worker::Tag::Supervisor)
    }
}
