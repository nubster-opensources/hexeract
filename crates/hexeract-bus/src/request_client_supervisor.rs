//! Opaque handle binding a request client's reply-inbox consumer task to its
//! completion signal, and owning the [`CancellationToken`] that task
//! observes.

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Opaque ownership of a request client's reply-inbox supervisor.
///
/// Construct this value with [`Self::spawn`], for a client with a real
/// consumer task, or [`Self::detached`], for one with none. Either way, the
/// [`CancellationToken`] a [`crate::RequestClient`] cancels on
/// [`crate::RequestClient::close`] and the token the consumer task can
/// observe are the same value: [`Self::spawn`] hands the task the very
/// token it holds, rather than accepting the task and an independent token
/// as separate arguments the way earlier revisions of this crate did, so a
/// [`crate::RequestClient`] can no longer be assembled with a task that
/// watches a different token than the one `close` cancels.
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
///     cancel: todo!(),
///     handle: todo!(),
///     task_id: todo!(),
///     finished: todo!(),
/// };
/// ```
#[derive(Debug)]
pub struct RequestClientSupervisor {
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
    task_id: Option<tokio::task::Id>,
    finished: Option<CancellationToken>,
}

impl RequestClientSupervisor {
    /// Spawn `task` and bind its actual termination to the returned handle.
    ///
    /// `task` receives a clone of `cancel`, the same token this supervisor
    /// keeps and the one [`crate::RequestClient::close`] later cancels:
    /// the caller cannot mispair the two by handing `task` an unrelated
    /// token, because there is no separate token argument to mismatch. What
    /// this cannot verify is whether `task` actually reacts to the token it
    /// receives: nothing here forces it to return once `cancel` fires, so a
    /// task that ignores its argument keeps running, and
    /// [`crate::RequestClient::close`] then waits for its genuine
    /// termination for as long as that task keeps running, unless the
    /// caller aborts it explicitly through [`Self::abort_handle`].
    ///
    /// The completion signal fires when the task unwinds, including after a
    /// panic or task cancellation.
    #[must_use]
    pub fn spawn<F, Fut>(cancel: CancellationToken, task: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let finished = CancellationToken::new();
        let signal = finished.clone();
        let task_cancel = cancel.clone();
        let handle = tokio::task::spawn(async move {
            let _completion = SupervisorCompletionGuard(signal);
            task(task_cancel).await;
        });
        let task_id = handle.id();
        Self {
            cancel,
            handle: Some(handle),
            task_id: Some(task_id),
            finished: Some(finished),
        }
    }

    /// Build a supervisor for a client with no real consumer task, such as
    /// one built for a unit test.
    ///
    /// [`crate::RequestClient::close`] then has nothing to wait for and
    /// returns as soon as it has closed the registry and cancelled
    /// `cancel`.
    #[must_use]
    pub fn detached(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            handle: None,
            task_id: None,
            finished: None,
        }
    }

    /// Abort handle for the supervisor task, or `None` if this supervisor
    /// was built with [`Self::detached`] and never spawned one.
    ///
    /// Aborting counts as termination: dropping the aborted task's future
    /// still runs the completion guard's destructor, the same as it would
    /// on a normal return, an error, or a panic, so every
    /// [`crate::RequestClient::close`] caller still wakes.
    #[must_use]
    pub fn abort_handle(&self) -> Option<tokio::task::AbortHandle> {
        self.handle.as_ref().map(JoinHandle::abort_handle)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        CancellationToken,
        Option<JoinHandle<()>>,
        Option<tokio::task::Id>,
        Option<CancellationToken>,
    ) {
        (self.cancel, self.handle, self.task_id, self.finished)
    }

    /// Build a supervisor from a task and completion signal a test spawned
    /// and paired by hand, bypassing [`Self::spawn`]'s own pairing.
    ///
    /// Reserved for this crate's own unit tests, which sometimes need to
    /// drive the raw completion signal directly instead of through
    /// [`Self::spawn`]'s API; never exported outside `hexeract-bus`.
    #[cfg(test)]
    pub(crate) fn from_task_for_test(
        cancel: CancellationToken,
        handle: JoinHandle<()>,
        finished: CancellationToken,
    ) -> Self {
        let task_id = handle.id();
        Self {
            cancel,
            handle: Some(handle),
            task_id: Some(task_id),
            finished: Some(finished),
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
