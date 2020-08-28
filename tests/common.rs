use capataz::context::*;
use capataz::worker::{self, StartNotifier};

pub fn wait_done_worker(name: &str) -> worker::Spec {
    worker::Spec::new(name, |ctx: Context| async {
        // wait for worker to be done
        ctx.done.await;
        Ok(())
    })
}

pub fn start_err_worker(name: &str, err_msg: &'static str) -> worker::Spec {
    worker::Spec::new_with_start(name, move |_: Context, start: StartNotifier| async move {
        start.failed(anyhow::Error::msg(err_msg.to_owned()));
        Ok(())
    })
}
