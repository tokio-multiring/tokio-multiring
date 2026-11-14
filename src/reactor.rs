use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use io_uring::{IoUring, opcode, squeue};
use slab::Slab;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc};

/// `user_data` of submissions whose completion nobody waits for. Operation
/// tokens carry a slab key in their low 32 bits, which never reaches
/// `u32::MAX`, so the two cannot collide.
const DETACHED: u64 = u64::MAX;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Reactor>>> = const { RefCell::new(None) };
}

/// The reactor's view of an operation it has been asked to run.
pub(crate) trait InFlight: Send + Sync {
    /// Called on the ring's thread just before the submission is queued.
    /// Returns `false` if the future is already gone, in which case nothing
    /// is submitted.
    fn submitted(&self, token: u64) -> bool;

    /// Called on the ring's thread with the completion's result.
    fn complete(&self, result: i32);
}

/// Work handed to a reactor by a thread that does not own its ring.
enum Remote {
    Submit {
        entry: squeue::Entry,
        op: Arc<dyn InFlight>,
    },
    Cancel(u64),
}

/// The part of a reactor that other threads can reach.
#[derive(Default)]
struct Shared {
    inject: Mutex<Vec<Remote>>,
    /// Wakes the driver task.
    wake: Notify,
    in_flight: AtomicUsize,
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

    /// Hands `entry` to a ring and returns the index of the worker owning
    /// it: the current worker's if this thread has one, otherwise the next
    /// in rotation.
    pub(crate) fn submit(&self, entry: squeue::Entry, op: Arc<dyn InFlight>) -> usize {
        match self.local() {
            Some(reactor) => {
                reactor.submit(entry, op);
                reactor.notify_driver();
                reactor.index
            }
            None => {
                let rotation = self.inner.next_remote.fetch_add(1, Ordering::Relaxed);
                let index = rotation % self.inner.workers.len();
                self.inject(index, Remote::Submit { entry, op });
                index
            }
        }
    }

    /// Asks the kernel to cancel the operation `token` on `worker`'s ring.
    /// Its completion is still delivered.
    pub(crate) fn cancel(&self, worker: usize, token: u64) {
        match self.local() {
            Some(reactor) if reactor.index == worker => {
                reactor.cancel(token);
                reactor.notify_driver();
            }
            _ => self.inject(worker, Remote::Cancel(token)),
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

    fn inject(&self, worker: usize, remote: Remote) {
        let shared = &self.inner.workers[worker];
        shared.inject.lock().unwrap().push(remote);
        shared.wake.notify_one();
    }
}

struct Submitted {
    token: u64,
    op: Arc<dyn InFlight>,
}

/// One worker's ring. Only ever touched from that worker's thread.
struct Reactor {
    ring: RefCell<IoUring>,
    in_flight: RefCell<Slab<Submitted>>,
    /// Scratch space for `reap`, kept to reuse its allocation.
    completed: RefCell<Vec<(Arc<dyn InFlight>, i32)>>,
    /// Distinguishes successive users of one slab key, so a cancel that
    /// arrives after its target completed cannot hit the key's next user.
    generation: Cell<u32>,
    /// Whether the driver task has been woken since its last turn.
    driver_notified: Cell<bool>,
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
                in_flight: RefCell::new(Slab::new()),
                completed: RefCell::new(Vec::new()),
                generation: Cell::new(0),
                driver_notified: Cell::new(false),
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

    fn submit(&self, entry: squeue::Entry, op: Arc<dyn InFlight>) {
        let token = {
            let mut in_flight = self.in_flight.borrow_mut();
            let slot = in_flight.vacant_entry();
            let generation = self.generation.get();
            self.generation.set(generation.wrapping_add(1));
            let token = u64::from(generation) << 32 | slot.key() as u64;
            if !op.submitted(token) {
                return;
            }
            slot.insert(Submitted {
                token,
                op: op.clone(),
            });
            token
        };

        match self.push(&entry.user_data(token)) {
            Ok(()) => {
                self.shared().in_flight.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                self.in_flight.borrow_mut().remove(key_of(token));
                op.complete(-error.raw_os_error().unwrap_or(libc::EIO));
            }
        }
    }

    fn cancel(&self, token: u64) {
        let live = self
            .in_flight
            .borrow()
            .get(key_of(token))
            .is_some_and(|submitted| submitted.token == token);
        if live {
            let entry = opcode::AsyncCancel::new(token).build().user_data(DETACHED);
            // A cancel that cannot be queued is dropped: the operation then
            // runs to its natural completion.
            let _ = self.push(&entry);
        }
    }

    /// Queues `entry`, flushing the submission queue to the kernel first if
    /// it is full.
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        loop {
            // SAFETY: every pointer in `entry` refers to memory owned by an
            // operation held in `in_flight`, which is only released once the
            // kernel has posted that operation's completion.
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
        let remote = std::mem::take(&mut *self.shared().inject.lock().unwrap());
        for remote in remote {
            match remote {
                Remote::Submit { entry, op } => self.submit(entry, op),
                Remote::Cancel(token) => self.cancel(token),
            }
        }
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
        let mut completed = self.completed.take();
        {
            let mut ring = self.ring.borrow_mut();
            let mut in_flight = self.in_flight.borrow_mut();
            for cqe in ring.completion() {
                let token = cqe.user_data();
                if token == DETACHED {
                    continue;
                }
                let submitted = in_flight.remove(key_of(token));
                debug_assert_eq!(submitted.token, token);
                completed.push((submitted.op, cqe.result()));
            }
        }
        self.shared()
            .in_flight
            .fetch_sub(completed.len(), Ordering::Relaxed);
        // Wakers run arbitrary code, so they are called with no borrow held.
        for (op, result) in completed.drain(..) {
            op.complete(result);
        }
        self.completed.replace(completed);
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        // Closing the ring does not wait for in-flight operations to stop,
        // so the kernel may write into their buffers after this point. They
        // are leaked rather than freed.
        std::mem::forget(std::mem::take(self.in_flight.get_mut()));
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
