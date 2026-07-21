//! The agent's task supervisor: spawn a fixed set of named workers under one
//! root [`CancellationToken`], run until a shutdown signal fires or a worker
//! exits, then cancel cooperatively and drain within a grace period.
//!
//! This is the topology skeleton for the `hippius-drain-agent` daemon (later slices add
//! the real drain/GC/heartbeat/reconciler workers and wire a SIGTERM future into
//! [`Supervisor::run`]). Shutdown is *designed in*: workers observe a shared
//! cancellation token at each poll and wind down their in-flight work, rather
//! than being aborted mid-`.await` — only a worker that overruns the grace
//! period is force-aborted (axiom `rust_quality_129_async_graceful_shutdown`).
//!
//! Supervision results are returned as data ([`RunReport`] / [`Outcome`]), never
//! as errors or process aborts: a worker panic is *captured* and escalated to an
//! orderly shutdown, mirroring the crate's outcome-as-data style elsewhere.

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;

/// A worker's identity, used in logs and [`WorkerExit`] reporting. The agent's
/// workers are a fixed known set, so a `&'static str` name needs no allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerName(&'static str);

impl WorkerName {
    /// Fallback name for the impossible case where a finished task's id is not
    /// in the supervisor's map (keeps reporting total without an `unwrap`).
    const UNKNOWN: Self = Self("<unknown>");

    /// Names a worker.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The underlying name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl core::fmt::Display for WorkerName {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.0)
    }
}

/// How a worker's task ended. The four modes are distinct so a caller can tell a
/// clean cooperative stop from a fault, rather than inferring it from a flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Returned after observing cancellation — a clean cooperative stop.
    Cancelled,
    /// Returned on its own before any cancellation. Unexpected for a long-lived
    /// daemon worker, so the supervisor treats it as the fault that begins shutdown.
    CompletedEarly,
    /// The task panicked. Captured (not propagated) so one worker's bug escalates
    /// to an orderly shutdown instead of aborting the whole process.
    Panicked,
    /// Force-aborted because it did not wind down within the grace period.
    Aborted,
}

/// How one worker's task ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerExit {
    /// Which worker.
    pub name: WorkerName,
    /// How it ended.
    pub outcome: Outcome,
}

/// What caused the supervisor to begin shutting down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownTrigger {
    /// The injected shutdown future resolved (e.g. SIGTERM).
    Signal,
    /// A worker exited during normal operation (a panic or an early return) — its
    /// exit is the fault that began the shutdown.
    WorkerExited(WorkerName),
    /// There were no workers to supervise; [`Supervisor::run`] returned at once.
    NoWorkers,
}

/// The result of one supervised run.
#[derive(Debug, Clone)]
pub struct RunReport {
    /// What began the shutdown.
    pub trigger: ShutdownTrigger,
    /// Every worker's exit, in the order they were collected.
    pub exits: Vec<WorkerExit>,
    /// Whether every worker wound down within the grace period — `false` if any
    /// had to be force-aborted.
    pub clean: bool,
}

impl RunReport {
    /// The outcome recorded for `name`, if that worker exited during the run.
    #[must_use]
    pub fn outcome_of(&self, name: WorkerName) -> Option<Outcome> {
        self.exits.iter().find(|exit| exit.name == name).map(|exit| exit.outcome)
    }
}

/// Owns a set of worker tasks under one root cancellation token and supervises
/// their lifecycle. Build with [`Supervisor::new`], add workers with
/// [`Supervisor::spawn`], then drive them with [`Supervisor::run`].
#[derive(Debug)]
pub struct Supervisor {
    /// Cancelled once to ask every worker (each holds a child token) to stop.
    root: CancellationToken,
    /// The supervised tasks; each resolves to a [`WorkerExit`] on a non-panic exit.
    tasks: JoinSet<WorkerExit>,
    /// Maps a task id to its name so a panicking/aborted task (which yields a
    /// `JoinError`, not a `WorkerExit`) can still be attributed to its worker.
    names: HashMap<Id, WorkerName>,
    /// How long workers get to wind down after cancellation before being aborted.
    grace: Duration,
}

impl Supervisor {
    /// A supervisor with no workers and the given post-cancellation `grace`.
    #[must_use]
    pub fn new(grace: Duration) -> Self {
        Self {
            root: CancellationToken::new(),
            tasks: JoinSet::new(),
            names: HashMap::new(),
            grace,
        }
    }

    /// Registers and spawns a worker. `worker` receives a child of the root token
    /// and must return once it observes cancellation (`token.cancelled().await`).
    ///
    /// The wrapper classifies the return: if the child token was cancelled by the
    /// time the worker returned it is [`Outcome::Cancelled`], otherwise
    /// [`Outcome::CompletedEarly`] (an unexpected self-exit).
    pub fn spawn<F, Fut>(&mut self, name: WorkerName, worker: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let child = self.root.child_token();
        // A separate handle to read the cancellation state *after* the worker
        // returns; `child` itself is moved into the worker.
        let observe = child.clone();
        let handle = self.tasks.spawn(async move {
            worker(child).await;
            let outcome = if observe.is_cancelled() {
                Outcome::Cancelled
            } else {
                Outcome::CompletedEarly
            };
            WorkerExit { name, outcome }
        });
        self.names.insert(handle.id(), name);
    }

    /// Awaits the next task to finish and maps it to a [`WorkerExit`].
    ///
    /// Cancellation-safe: `join_next_with_id` keeps unfinished tasks in the set if
    /// this future is dropped (e.g. the losing `select!` branch), so no worker is
    /// lost — they are drained in the shutdown phase (axiom 128).
    async fn next_exit(&mut self) -> Option<WorkerExit> {
        match self.tasks.join_next_with_id().await {
            None => None,
            // Normal/cooperative/early exit: the task carried its own WorkerExit.
            Some(Ok((id, exit))) => {
                self.names.remove(&id);
                Some(exit)
            }
            // A panic or an abort yields a JoinError instead of a WorkerExit, so
            // recover the name from the id map and classify by panic-vs-abort.
            Some(Err(join_err)) => {
                let name = self.names.remove(&join_err.id()).unwrap_or(WorkerName::UNKNOWN);
                let outcome = if join_err.is_panic() { Outcome::Panicked } else { Outcome::Aborted };
                Some(WorkerExit { name, outcome })
            }
        }
    }

    /// Runs until `shutdown` resolves or the first worker exits, then cancels the
    /// root token and drains within the grace period (force-aborting stragglers).
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) -> RunReport {
        let mut exits: Vec<WorkerExit> = Vec::new();

        // Phase 1 — normal operation: stop at the shutdown signal, or at the first
        // worker exit (a worker ending here is the fault that begins shutdown).
        // `next_exit` is cancel-safe, so the losing branch drops it without losing
        // any task; the shutdown future is awaited exactly once (no re-poll).
        let trigger = tokio::select! {
            () = shutdown => ShutdownTrigger::Signal,
            maybe = self.next_exit() => match maybe {
                Some(exit) => {
                    let name = exit.name;
                    exits.push(exit);
                    ShutdownTrigger::WorkerExited(name)
                }
                None => {
                    return RunReport { trigger: ShutdownTrigger::NoWorkers, exits, clean: true };
                }
            },
        };

        // Phase 2 — shutdown: ask everyone to stop, then drain within the grace
        // period. Cooperative workers return Cancelled; anyone still running when
        // the grace expires is force-aborted (and reported Aborted).
        self.root.cancel();
        let grace = self.grace;
        let drained = tokio::time::timeout(grace, async {
            while let Some(exit) = self.next_exit().await {
                exits.push(exit);
            }
        })
        .await;

        let clean = drained.is_ok();
        if drained.is_err() {
            self.tasks.abort_all();
            while let Some(exit) = self.next_exit().await {
                exits.push(exit);
            }
        }

        RunReport { trigger, exits, clean }
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, ShutdownTrigger, Supervisor, WorkerName};
    use std::future::{pending, ready};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    const A: WorkerName = WorkerName::new("a");
    const B: WorkerName = WorkerName::new("b");

    /// A worker that loops until cancelled, then returns cleanly.
    async fn cooperative(token: CancellationToken) {
        token.cancelled().await;
    }

    #[tokio::test]
    async fn signal_shutdown_cancels_all_cooperative_workers_cleanly() {
        let mut sup = Supervisor::new(Duration::from_secs(5));
        sup.spawn(A, cooperative);
        sup.spawn(B, cooperative);

        let report = sup.run(ready(())).await;

        assert_eq!(report.trigger, ShutdownTrigger::Signal);
        assert_eq!(report.outcome_of(A), Some(Outcome::Cancelled));
        assert_eq!(report.outcome_of(B), Some(Outcome::Cancelled));
        assert!(report.clean, "cooperative workers wind down within grace");
    }

    #[tokio::test]
    async fn an_early_returning_worker_triggers_shutdown() {
        let mut sup = Supervisor::new(Duration::from_secs(5));
        sup.spawn(A, |_token| async {}); // returns at once, ignoring the token
        sup.spawn(B, cooperative);

        // shutdown never fires on its own; A's early exit must start the shutdown.
        let report = sup.run(pending()).await;

        assert_eq!(report.trigger, ShutdownTrigger::WorkerExited(A));
        assert_eq!(report.outcome_of(A), Some(Outcome::CompletedEarly));
        assert_eq!(report.outcome_of(B), Some(Outcome::Cancelled), "the peer is cancelled cleanly");
        assert!(report.clean);
    }

    #[tokio::test]
    #[expect(clippy::panic, reason = "a worker deliberately panics to exercise panic capture")]
    async fn a_panicking_worker_escalates_to_shutdown() {
        let mut sup = Supervisor::new(Duration::from_secs(5));
        sup.spawn(A, |_token| async { panic!("worker A boom") });
        sup.spawn(B, cooperative);

        let report = sup.run(pending()).await;

        assert_eq!(report.trigger, ShutdownTrigger::WorkerExited(A));
        assert_eq!(report.outcome_of(A), Some(Outcome::Panicked), "the panic is captured, not propagated");
        assert_eq!(report.outcome_of(B), Some(Outcome::Cancelled));
        assert!(report.clean);
    }

    #[tokio::test]
    async fn a_worker_that_ignores_cancellation_is_force_aborted() {
        let mut sup = Supervisor::new(Duration::from_millis(50));
        // Ignores the token entirely: it never observes cancellation, so it can
        // only be stopped by the post-grace abort.
        sup.spawn(A, |_token| async {
            loop {
                tokio::time::sleep(Duration::from_hours(1)).await;
            }
        });

        let report = sup.run(ready(())).await;

        assert_eq!(report.trigger, ShutdownTrigger::Signal);
        assert_eq!(report.outcome_of(A), Some(Outcome::Aborted));
        assert!(!report.clean, "a worker that overran the grace period was force-aborted");
    }

    #[tokio::test]
    async fn no_workers_returns_immediately() {
        let sup = Supervisor::new(Duration::from_secs(5));
        let report = sup.run(pending()).await;
        assert_eq!(report.trigger, ShutdownTrigger::NoWorkers);
        assert!(report.exits.is_empty());
        assert!(report.clean);
    }
}
