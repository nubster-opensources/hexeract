//! Opaque handle binding a request client's reply-inbox consumer task to its
//! completion signal.

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Opaque ownership of a request client's reply-inbox supervisor.
///
/// Construct this value with [`Self::spawn`]. The constructor pairs the
/// supervisor task with its completion signal internally, so a
/// [`crate::RequestClient`] cannot be assembled with a task and an unrelated
/// signal that would leave concurrent [`crate::RequestClient::close`] calls
/// waiting forever.
///
/// The doctest below must keep listing every field of this struct: if a
/// field were later added here without updating it, the doctest would fail
/// on a missing-field error (E0063) instead of the private-field error
/// (E0451) it exists to prove, and `compile_fail` would stay green for the
/// wrong reason.
///
/// ```compile_fail
/// use hexeract_bus::RequestClientSupervisor;
///
/// let _ = RequestClientSupervisor {
///     handle: todo!(),
///     task_id: todo!(),
///     finished: todo!(),
/// };
/// ```
#[derive(Debug)]
pub struct RequestClientSupervisor {
    handle: JoinHandle<()>,
    task_id: tokio::task::Id,
    finished: CancellationToken,
}

impl RequestClientSupervisor {
    /// Spawn `task` and bind its actual termination to the returned handle.
    ///
    /// The completion signal fires when the task unwinds, including after a
    /// panic or task cancellation. It is intentionally not supplied by the
    /// caller: the task and the signal can therefore never be mispaired.
    #[must_use]
    pub fn spawn<F>(task: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let finished = CancellationToken::new();
        let signal = finished.clone();
        let handle = tokio::task::spawn(async move {
            let _completion = SupervisorCompletionGuard(signal);
            task.await;
        });
        let task_id = handle.id();
        Self {
            handle,
            task_id,
            finished,
        }
    }

    /// Abort handle for the supervisor task.
    ///
    /// Aborting counts as termination: dropping the aborted task's future
    /// still runs the completion guard's destructor, the same as it would
    /// on a normal return, an error, or a panic, so every
    /// [`crate::RequestClient::close`] caller still wakes.
    #[must_use]
    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.handle.abort_handle()
    }

    pub(crate) fn into_parts(self) -> (JoinHandle<()>, tokio::task::Id, CancellationToken) {
        (self.handle, self.task_id, self.finished)
    }

    /// Build a supervisor from a task and completion signal a test spawned
    /// and paired by hand, bypassing [`Self::spawn`]'s own pairing.
    ///
    /// Reserved for this crate's own unit tests, which sometimes need to
    /// drive the raw completion signal directly instead of through
    /// [`Self::spawn`]'s API; never exported outside `hexeract-bus`.
    #[cfg(test)]
    pub(crate) fn from_task_for_test(handle: JoinHandle<()>, finished: CancellationToken) -> Self {
        let task_id = handle.id();
        Self {
            handle,
            task_id,
            finished,
        }
    }
}

/// Signals supervisor completion whenever its wrapper task unwinds.
struct SupervisorCompletionGuard(CancellationToken);

impl Drop for SupervisorCompletionGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
