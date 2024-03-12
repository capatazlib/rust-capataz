use thiserror::Error;
use tokio::time;

/// Handles the restart tolerance logic of a Supervisor.
#[derive(Debug)]
pub(crate) struct RestartManager {
    supervisor_name: String,
    max_allowed_restarts: u32,
    restart_window: time::Duration,

    window_first_error: Option<anyhow::Error>,
    last_window_timestamp: Option<time::Instant>,
    restart_count: u32,
}

impl RestartManager {
    /// Creates a new RestartManager instance
    pub(crate) fn new(
        supervisor_name: &str,
        max_allowed_restarts: u32,
        restart_window: time::Duration,
    ) -> Self {
        Self {
            supervisor_name: supervisor_name.to_owned(),
            max_allowed_restarts,
            restart_window,
            window_first_error: None,
            last_window_timestamp: None,
            restart_count: 0,
        }
    }

    /// Returns the current restart count
    #[cfg(test)]
    pub(crate) fn get_restart_count(&self) -> u32 {
        self.restart_count
    }

    /// Registers an error and validates that the error tolerance has not been
    /// surpassed.
    pub(crate) fn register_error(mut self, err: anyhow::Error) -> Result<Self, TooManyRestarts> {
        // Verify that we surpassed the restart window to reset
        // the restart_count
        let first_err_for_window = match self.last_window_timestamp {
            None => true,
            Some(then) if then.elapsed() > self.restart_window => true,
            Some(_) => false,
        };

        // When max_allowed_restarts is 0, return an error immediately
        if first_err_for_window && self.max_allowed_restarts == 0 {
            return Err(TooManyRestarts {
                supervisor_name: self.supervisor_name,
                first_error: err,
                last_error: None,
            });
        }

        let mut last_error = None;
        if first_err_for_window {
            // Register state when is the first error of a window
            self.window_first_error = Some(err);
            self.last_window_timestamp = Some(time::Instant::now());
            self.restart_count = 1;
        } else {
            // Increase the restart_count
            self.restart_count += 1;
            last_error = Some(err);
        }

        // Verify if the tolerance error has been surpassed.
        if self.restart_count > self.max_allowed_restarts {
            // SAFETY: if statement above guaranteess that a window_first_error
            // is always present.
            let first_error = self.window_first_error.unwrap();

            // To many errors reported on a window of time, finalize this
            // restart manager.
            return Err(TooManyRestarts {
                supervisor_name: self.supervisor_name,
                first_error,
                last_error,
            });
        }

        // Return mutated RestartManager
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{RestartManager, TooManyRestarts};
    use tokio::time;

    #[tokio::test]
    async fn test_restart_manager_too_many_restarts() {
        time::pause();
        let manager = RestartManager::new("root", 2, time::Duration::from_secs(5));
        let first_error = anyhow::anyhow!("error 1");
        let manager = manager
            .register_error(first_error)
            .expect("should not throw an error");
        time::advance(time::Duration::from_secs(1)).await;
        let manager = manager
            .register_error(anyhow::anyhow!("error 2"))
            .expect("should not throw an error");
        time::advance(time::Duration::from_secs(3)).await;
        let last_error = anyhow::anyhow!("error 3");
        let err: TooManyRestarts = manager
            .register_error(last_error)
            .expect_err("should throw an error");

        assert_eq!("to many restarts detected", format!("{}", err));
        assert_eq!("error 1", format!("{}", err.get_first_error()));
        // SAFETY: thet test is setup so that there is more than a single error
        let last_error_result = err.get_last_error().unwrap();
        assert_eq!("error 3", last_error_result.to_string());
    }

    #[tokio::test]
    async fn test_restart_manager_window_reset() {
        time::pause();
        let manager = RestartManager::new("root", 2, time::Duration::from_secs(5));
        let first_error = anyhow::anyhow!("error 1");
        let manager = manager
            .register_error(first_error)
            .expect("should not throw an error 1");
        time::advance(time::Duration::from_secs(1)).await;
        let manager = manager
            .register_error(anyhow::anyhow!("error 2"))
            .expect("should not throw an error 2");
        time::advance(time::Duration::from_secs(5)).await;
        let last_error = anyhow::anyhow!("error 3");
        let manager = manager
            .register_error(last_error)
            .expect("should not throw an error 3");

        // error count is restarted
        assert_eq!(1, manager.get_restart_count());
    }
}

/// Error used to notify a supervisor restarted more times than what is able to
/// tolerate.
#[derive(Debug, Error)]
#[error("to many restarts detected")]
pub struct TooManyRestarts {
    supervisor_name: String,
    first_error: anyhow::Error,
    // In the scenario first_error is the last error of the tolerance window,
    // this attribute is going to be None
    last_error: Option<anyhow::Error>,
}

impl TooManyRestarts {
    pub(crate) fn get_runtime_name(&self) -> &str {
        &self.supervisor_name
    }

    #[cfg(test)]
    pub(crate) fn get_first_error(&self) -> &anyhow::Error {
        &self.first_error
    }

    #[cfg(test)]
    pub(crate) fn get_last_error(&self) -> Option<&anyhow::Error> {
        self.last_error.as_ref()
    }
}
