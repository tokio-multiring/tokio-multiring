use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use io_uring::{IoUring, opcode, squeue};
use slab::Slab;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc};

use crate::op::{Completion, Operation};

/// `user_data` of submissions whose completion nobody waits for. Operation
/// tokens carry a slab key in their low 32 bits, which never reaches
/// `u32::MAX`, so the two cannot collide.
const DETACHED: u64 = u64::MAX;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Reactor>>> = const { RefCell::new(None) };
}

enum Stage {
    /// The future is waiting for the result.
    Waiting(Waker),
    /// The result is in, and the future has not collected it yet.
    Done(i32),
    /// The future was dropped. The slot is released when the kernel is done
    /// with the operation.
    Abandoned,
}

struct Slot {
    token: u64,
    operation: Operation,
    stage: Stage,
    /// Whether the operation has been queued for the kernel, which from then
    /// on may use its buffers until the completion arrives.
    in_kernel: bool,
}

/// Work handed to a reactor by a thread that does not own its ring. The
/// tokens name slots in that reactor's `Operations`.
enum Remote {
    Submit(u64),
    Cancel(u64),
}

/// A worker's operations, from submission until the future has collected the
/// result. Futures reach their slot from whichever thread polls them.
#[derive(Default)]
struct Operations {
    slots: Slab<Slot>,
    /// Distinguishes successive users of one slab key, so a cancel that
    /// arrives after its target completed cannot hit the key's next user.
    generation: u32,
    remote: Vec<Remote>,
    /// Set when the reactor is gone. Nothing completes after that.
    closed: bool,
}

impl Operations {
    fn insert(&mut self, operation: Operation, waker: &Waker, in_kernel: bool) -> u64 {
        let slot = self.slots.vacant_entry();
        let token = u64::from(self.generation) << 32 | slot.key() as u64;
        self.generation = self.generation.wrapping_add(1);
        slot.insert(Slot {
            token,
            operation,
            stage: Stage::Waiting(waker.clone()),
            in_kernel,
        });
        token
    }

    fn get_mut(&mut self, token: u64) -> Option<&mut Slot> {
        self.slots
            .get_mut(key_of(token))
            .filter(|slot| slot.token == token)
    }

    fn remove(&mut self, token: u64) -> Slot {
        self.slots.remove(key_of(token))
    }

    /// Records the kernel's result for `token`. Waking the future and
    /// dropping an abandoned operation both run foreign code, so they are
    /// left to the caller to do once the lock is released.
    fn finish(
        &mut self,
        token: u64,
        result: i32,
        wakers: &mut Vec<Waker>,
        released: &mut Vec<Slot>,
    ) {
        let slot = self
            .get_mut(token)
            .expect("completion for an operation that is not in flight");
        match std::mem::replace(&mut slot.stage, Stage::Done(result)) {
            Stage::Waiting(waker) => wakers.push(waker),
            Stage::Abandoned => released.push(self.remove(token)),
            Stage::Done(_) => unreachable!("operation completed twice"),
        }
    }
}

/// The part of a reactor that other threads can reach.
#[derive(Default)]
struct Shared {
    operations: Mutex<Operations>,
    /// Wakes the driver task.
    wake: Notify,
    in_flight: AtomicUsize,
}

impl Shared {
    fn operations(&self) -> MutexGuard<'_, Operations> {
        self.operations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Names an in-flight operation: the worker whose ring runs it, and its
/// token there.
#[derive(Clone, Copy)]
pub(crate) struct Ticket {
    worker: usize,
    token: u64,
}

struct Inner {
    workers: Box<[Shared]>,
    next_remote: AtomicUsize,
}

/// Settings for the per-worker rings.
#[derive(Clone, Debug)]
pub struct Config {
    /// Submission queue size of each ring. The completion queue is twice
    /// this.
    pub sq_entries: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self { sq_entries: 256 }
    }
}

/// Handle to the reactors of one runtime. Cheap to clone.
#[derive(Clone)]
pub struct Sidecar {
    inner: Arc<Inner>,
}

impl Sidecar {
    /// Creates one ring per worker of the current multi-threaded runtime and
    /// starts a driver task on each.
    ///
    /// The reactors live until the runtime shuts down. A runtime can host
    /// one sidecar: a second call fails with [`io::ErrorKind::AlreadyExists`]
    /// and leaves the first untouched.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a multi-threaded runtime.
    pub async fn start(config: Config) -> io::Result<Sidecar> {
        let workers = Handle::current().metrics().num_workers();
        let inner = Arc::new(Inner {
            workers: (0..workers).map(|_| Shared::default()).collect(),
            next_remote: AtomicUsize::new(0),
        });

        let (tx, mut rx) = mpsc::unbounded_channel();
        let sidecar = inner.clone();
        tokio::task::run_on_each_worker(move |index| {
            let _ = tx.send(Reactor::install(sidecar.clone(), index, &config));
        });

        let mut result = Ok(());
        for _ in 0..workers {
            match rx.recv().await {
                Some(installed) => result = result.and(installed),
                None => {
                    result = Err(io::Error::other("runtime shut down during sidecar start"));
                    break;
                }
            }
        }
        if let Err(error) = result {
            let sidecar = inner.clone();
            tokio::task::run_on_each_worker(move |_| Reactor::uninstall(&sidecar));
            return Err(error);
        }
        Ok(Sidecar { inner })
    }

    /// Number of operations submitted to the kernel and not yet completed,
    /// across all workers.
    pub fn in_flight(&self) -> usize {
        self.inner
            .workers
            .iter()
            .map(|shared| shared.in_flight.load(Ordering::Relaxed))
            .sum()
    }

    /// Hands `operation` to a ring: the current worker's if this thread has
    /// one, otherwise the next in rotation. `waker` is woken when the result
    /// is ready for [`poll_completion`](Self::poll_completion).
    pub(crate) fn submit(&self, mut operation: Operation, waker: &Waker) -> Ticket {
        match self.local() {
            Some(reactor) => {
                let entry = operation.entry();
                let token = reactor.shared().operations().insert(operation, waker, true);
                reactor.push_operation(token, entry);
                if !reactor.hook_seen.get() {
                    reactor.notify_driver();
                }
                Ticket {
                    worker: reactor.index,
                    token,
                }
            }
            None => {
                let rotation = self.inner.next_remote.fetch_add(1, Ordering::Relaxed);
                let worker = rotation % self.inner.workers.len();
                let shared = &self.inner.workers[worker];
                let token = {
                    let mut operations = shared.operations();
                    let token = operations.insert(operation, waker, false);
                    operations.remote.push(Remote::Submit(token));
                    token
                };
                shared.wake.notify_one();
                Ticket { worker, token }
            }
        }
    }

    /// Collects the result of `ticket`'s operation, or registers the task
    /// to be woken for it. Once this returns `Ready` the ticket is spent.
    ///
    /// Stays pending forever if the reactor has gone away with its runtime.
    pub(crate) fn poll_completion(&self, ticket: Ticket, cx: &mut Context<'_>) -> Poll<Completion> {
        let mut operations = self.inner.workers[ticket.worker].operations();
        if operations.closed {
            return Poll::Pending;
        }
        let slot = operations
            .get_mut(ticket.token)
            .expect("ticket for an operation that is not in flight");
        match &mut slot.stage {
            Stage::Waiting(waker) => {
                let replaced = (!waker.will_wake(cx.waker()))
                    .then(|| std::mem::replace(waker, cx.waker().clone()));
                // A waker's destructor is foreign code: run it unlocked.
                drop(operations);
                drop(replaced);
                Poll::Pending
            }
            Stage::Done(result) => {
                let result = *result;
                let slot = operations.remove(ticket.token);
                Poll::Ready(Completion::new(result, slot.operation))
            }
            Stage::Abandoned => unreachable!("ticket used after it was abandoned"),
        }
    }

    /// Gives up on `ticket`'s operation. If the kernel already has it, it is
    /// asked to cancel, and the operation's buffers are kept until it has
    /// answered.
    pub(crate) fn abandon(&self, ticket: Ticket) {
        let Ticket { worker, token } = ticket;
        let shared = &self.inner.workers[worker];
        let local = self.local().filter(|reactor| reactor.index == worker);

        let mut operations = shared.operations();
        if operations.closed {
            return;
        }
        let slot = operations
            .get_mut(token)
            .expect("ticket for an operation that is not in flight");
        if let Stage::Done(_) = slot.stage {
            let finished = operations.remove(token);
            drop(operations);
            drop(finished);
            return;
        }
        let waiting = std::mem::replace(&mut slot.stage, Stage::Abandoned);
        // Not queued yet: the reactor releases the slot when it gets to the
        // pending `Remote::Submit`.
        let in_kernel = slot.in_kernel;
        if in_kernel && local.is_none() {
            operations.remote.push(Remote::Cancel(token));
        }
        drop(operations);
        drop(waiting);

        if in_kernel {
            match local {
                Some(reactor) => {
                    reactor.push_cancel(token);
                    reactor.notify_driver();
                }
                None => shared.wake.notify_one(),
            }
        }
    }

    /// This thread's reactor, if it has one and it belongs to this sidecar.
    fn local(&self) -> Option<Rc<Reactor>> {
        // The thread-local is gone when a future is dropped during thread
        // teardown; such a thread is treated like any other non-worker.
        CURRENT
            .try_with(|current| current.borrow().clone())
            .ok()
            .flatten()
            .filter(|reactor| Arc::ptr_eq(&reactor.sidecar, &self.inner))
    }
}

/// Flushes the current worker's ring: submits what its tasks have queued and
/// delivers the completions that are ready. Install it with
/// [`Builder::on_before_steal`], or call it from your own before-steal
/// callback if the runtime needs that slot for something else.
///
/// The hook runs when a worker has emptied its local queue, so everything
/// the worker's tasks queued since the last flush goes to the kernel in one
/// `io_uring_enter`, without a trip through the scheduler.
///
/// A worker that never runs out of local work never reaches the hook. A
/// full submission queue is flushed on the spot, so at most
/// [`Config::sq_entries`] operations are left waiting there, until the
/// worker goes idle or a completion on its ring wakes the driver task.
///
/// Without the hook the sidecar still works: each worker's driver task does
/// the flushing, at the cost of one extra task poll per batch.
///
/// Does nothing on a thread that has no reactor.
///
/// [`Builder::on_before_steal`]: tokio::runtime::Builder::on_before_steal
pub fn before_steal() {
    let reactor = CURRENT.with(|current| current.borrow().clone());
    if let Some(reactor) = reactor {
        reactor.hook_seen.set(true);
        reactor.turn();
    }
}

/// One worker's ring. Only ever touched from that worker's thread.
struct Reactor {
    ring: RefCell<IoUring>,
    /// Scratch space for `turn` and `reap`, kept to reuse the allocations.
    remote: RefCell<Vec<Remote>>,
    wakers: RefCell<Vec<Waker>>,
    released: RefCell<Vec<Slot>>,
    /// Whether the driver task has been woken since its last turn.
    driver_notified: Cell<bool>,
    /// Whether [`before_steal`] runs on this thread. Until it has been seen,
    /// local submissions wake the driver task to get flushed.
    hook_seen: Cell<bool>,
    stopped: Cell<bool>,
    sidecar: Arc<Inner>,
    index: usize,
}

impl Reactor {
    fn install(sidecar: Arc<Inner>, index: usize, config: &Config) -> io::Result<()> {
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            if current.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "this runtime already hosts a sidecar",
                ));
            }

            // COOP_TASKRUN, not DEFER_TASKRUN: deferred completions do not
            // raise readiness on the ring's descriptor, so a worker parked
            // in tokio's driver would never learn about them. Cooperative
            // task work wakes the parked thread, and TASKRUN_FLAG lets
            // `turn` see pending work without a syscall.
            let ring = IoUring::builder()
                .setup_single_issuer()
                .setup_coop_taskrun()
                .setup_taskrun_flag()
                .build(config.sq_entries)?;
            // SAFETY: the descriptor is the ring's, and the ring outlives
            // `readiness`: the driver task owns both and drops `readiness`
            // first.
            let readiness = unsafe {
                AsyncFd::register_with_interest(RingFd(ring.as_raw_fd()), Interest::READABLE)
            }
            .map_err(io::Error::from)?;

            let reactor = Rc::new(Reactor {
                ring: RefCell::new(ring),
                remote: RefCell::new(Vec::new()),
                wakers: RefCell::new(Vec::new()),
                released: RefCell::new(Vec::new()),
                driver_notified: Cell::new(false),
                hook_seen: Cell::new(false),
                stopped: Cell::new(false),
                sidecar,
                index,
            });
            tokio::task::spawn_worker_local(drive(reactor.clone(), readiness));
            *current = Some(reactor);
            Ok(())
        })
    }

    /// Removes this thread's reactor if it belongs to `sidecar`.
    fn uninstall(sidecar: &Arc<Inner>) {
        CURRENT.with(|current| {
            let mut current = current.borrow_mut();
            if let Some(reactor) = current.as_ref()
                && Arc::ptr_eq(&reactor.sidecar, sidecar)
            {
                reactor.stopped.set(true);
                reactor.shared().wake.notify_one();
                *current = None;
            }
        });
    }

    fn shared(&self) -> &Shared {
        &self.sidecar.workers[self.index]
    }

    fn notify_driver(&self) {
        if !self.driver_notified.replace(true) {
            self.shared().wake.notify_one();
        }
    }

    /// Queues the submission of an operation whose slot is marked
    /// `in_kernel`. If it cannot be queued, the operation fails with the
    /// error.
    fn push_operation(&self, token: u64, entry: squeue::Entry) {
        match self.push(&entry.user_data(token)) {
            Ok(()) => {
                self.shared().in_flight.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                let result = -error.raw_os_error().unwrap_or(libc::EIO);
                let mut wakers = self.wakers.take();
                let mut released = self.released.take();
                self.shared()
                    .operations()
                    .finish(token, result, &mut wakers, &mut released);
                self.deliver(wakers, released);
            }
        }
    }

    /// Submits an operation that another thread put into `Operations`.
    fn submit_remote(&self, token: u64) {
        let mut operations = self.shared().operations();
        let slot = operations
            .get_mut(token)
            .expect("remote submission without a slot");
        if let Stage::Abandoned = slot.stage {
            let abandoned = operations.remove(token);
            drop(operations);
            drop(abandoned);
            return;
        }
        slot.in_kernel = true;
        let entry = slot.operation.entry();
        drop(operations);
        self.push_operation(token, entry);
    }

    /// Cancels `token` at another thread's request, unless its completion
    /// has arrived in the meantime.
    fn cancel(&self, token: u64) {
        let live = self.shared().operations().get_mut(token).is_some();
        if live {
            self.push_cancel(token);
        }
    }

    fn push_cancel(&self, token: u64) {
        let entry = opcode::AsyncCancel::new(token).build().user_data(DETACHED);
        // A cancel that cannot be queued is dropped: the operation then runs
        // to its natural completion.
        let _ = self.push(&entry);
    }

    /// Wakes the futures whose results are in and drops the operations
    /// nobody is waiting for. Both run foreign code, so the caller must hold
    /// no lock or borrow.
    fn deliver(&self, mut wakers: Vec<Waker>, mut released: Vec<Slot>) {
        released.clear();
        for waker in wakers.drain(..) {
            waker.wake();
        }
        self.wakers.replace(wakers);
        self.released.replace(released);
    }

    /// Queues `entry`, flushing the submission queue to the kernel first if
    /// it is full.
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        loop {
            // SAFETY: every pointer in `entry` refers to heap memory owned by
            // an operation whose slot is marked `in_kernel`. Such a slot is
            // only released once the kernel has posted its completion.
            if unsafe { ring.submission().push(entry) }.is_ok() {
                return Ok(());
            }
            match ring.submit() {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Takes in remote work, submits everything queued, and delivers the
    /// completions that are ready.
    fn turn(&self) {
        let mut remote = self.remote.take();
        std::mem::swap(&mut remote, &mut self.shared().operations().remote);
        for remote in remote.drain(..) {
            match remote {
                Remote::Submit(token) => self.submit_remote(token),
                Remote::Cancel(token) => self.cancel(token),
            }
        }
        self.remote.replace(remote);
        self.driver_notified.set(false);

        loop {
            self.enter();
            self.reap();
            if !self.kernel_holds_completions() {
                break;
            }
        }
    }

    /// Whether the kernel has completions that only an enter will deliver:
    /// ones waiting on task work, or ones that did not fit in the completion
    /// queue.
    fn kernel_holds_completions(&self) -> bool {
        let ring = self.ring.borrow();
        // SAFETY: the submission queue is only accessed from this thread,
        // and no other handle to it is alive here.
        let queue = unsafe { ring.submission_shared() };
        queue.taskrun() || queue.cq_overflow()
    }

    /// Submits queued entries and collects completions held by the kernel.
    fn enter(&self) {
        let ring = self.ring.borrow();
        loop {
            // SAFETY: as in `kernel_holds_completions`.
            let idle = unsafe { ring.submission_shared() }.is_empty();
            if idle && !self.kernel_holds_completions() {
                return;
            }
            match ring.submit() {
                Ok(_) => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                // The kernel is out of completion-queue space or request
                // memory. Reaping frees the former; try again next turn.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ResourceBusy | io::ErrorKind::WouldBlock
                    ) =>
                {
                    self.notify_driver();
                    return;
                }
                // Anything else means the ring itself is unusable. In-flight
                // operations cannot be failed safely, because the kernel may
                // still be using their buffers.
                Err(error) => panic!("io_uring_enter failed: {error}"),
            }
        }
    }

    fn reap(&self) {
        let mut wakers = self.wakers.take();
        let mut released = self.released.take();
        let mut finished = 0;
        {
            let mut ring = self.ring.borrow_mut();
            let completions = ring.completion();
            if !completions.is_empty() {
                let mut operations = self.shared().operations();
                for cqe in completions {
                    let token = cqe.user_data();
                    if token != DETACHED {
                        operations.finish(token, cqe.result(), &mut wakers, &mut released);
                        finished += 1;
                    }
                }
            }
        }
        self.shared()
            .in_flight
            .fetch_sub(finished, Ordering::Relaxed);
        self.deliver(wakers, released);
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // Closing the ring does not wait for in-flight operations to stop,
        // so the kernel may write into their buffers after this point. They
        // are leaked rather than freed.
        let mut operations = self.shared().operations();
        operations.closed = true;
        std::mem::forget(std::mem::take(&mut operations.slots));
    }
}

fn key_of(token: u64) -> usize {
    (token & u64::from(u32::MAX)) as usize
}

/// The ring's descriptor, borrowed for readiness notifications. The ring
/// itself is owned by the [`Reactor`].
struct RingFd(RawFd);

impl AsRawFd for RingFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Arguments drop in reverse order, so `readiness` is deregistered while the
/// ring it watches is still open.
async fn drive(reactor: Rc<Reactor>, readiness: AsyncFd<RingFd>) {
    while !reactor.stopped.get() {
        reactor.turn();
        tokio::select! {
            biased;
            () = reactor.shared().wake.notified() => {}
            ready = readiness.readable() => match ready {
                // Cleared before the next turn reaps, so a completion that
                // lands in between raises readiness again.
                Ok(mut guard) => guard.clear_ready(),
                Err(error) => panic!("ring readiness failed: {error}"),
            },
        }
    }
}
