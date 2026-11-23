use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::Waker;
use std::time::Duration;

use io_uring::{IoUring, cqueue, opcode, squeue, types};
use tokio::io::{Interest, Ready};
use tokio::runtime::Builder;
use tokio::runtime::reactor::{ParkMode, Reactor, Readiness, RegistrationToken, Timers};

use crate::buffers::BufferPool;
use crate::landing::{Arbiter, CellRef, Claim, TaskRef};
use crate::op::{Completion, Operation};
use crate::plan::Plan;

// What a submission is for is in bits 30 and 31 of its `user_data`. The bits
// below index the worker's table for that kind, and the upper half is the
// generation of the entry there.

/// The bits of a token that index a table.
const INDEX: u64 = (1 << 30) - 1;
/// The bits of a token that say what it is for.
const KIND: u64 = 0b11 << 30;
/// An operation, whose completion goes to a future.
const OPERATION: u64 = 0;
/// A poll that watches a file registered by the runtime.
const WATCH: u64 = 0b01 << 30;
/// The read that completes when another thread wakes the worker.
const WAKE: u64 = 0b10 << 30;
/// The poll that reports completions on the worker's disk ring.
const DISK: u64 = WAKE | 1;
/// Submissions whose completion nobody waits for.
const DETACHED: u64 = u64::MAX;

/// High bit of a [`RegistrationToken`] marking a watch queued from a thread
/// without a ring (resolved on the target worker via `remote_watches`).
const REMOTE_WATCH_BIT: u64 = 1 << 63;
/// Mask for the 15-bit worker index stored in bits 48..63 of a
/// [`RegistrationToken`].
const TOKEN_WORKER_MASK: u64 = (1 << 15) - 1;
/// Mask for the 18-bit generation stored in bits 30..48 of a direct watch
/// [`RegistrationToken`].
const WATCH_GEN_MASK: u64 = (1 << 18) - 1;
/// Mask for the 48-bit monotonic id of a remote watch [`RegistrationToken`].
const REMOTE_ID_MASK: u64 = (1 << 48) - 1;

/// Arbiters are allocated this many at a time.
const CHUNK: usize = 64;

/// How many leases a worker moves between its own free list and its pool at
/// a time.
const BATCH: usize = 32;

/// How many times in a row a worker that the runtime has look at its ring,
/// while it still has tasks to run, only submits before it also takes
/// completions in.
///
/// Completions wake tasks, and a busy worker that is handed more of them at
/// every look never runs out. Tokio then leaves the tasks on its shared
/// queue waiting, since a worker turns to that queue in earnest only when
/// its own is empty. Measured with 2048 echo connections on 8 workers and
/// stealing allowed, taking completions in at every look left one request
/// in a hundred waiting 55 ms, and at every fourth, 4 ms. The bound is for
/// a worker that never runs out by itself.
const LOOKS_PER_REAP: u32 = 4;

// Whether a worker is asleep in its ring.

/// Running, or on its way to sleep.
const AWAKE: u8 = 0;
/// Asleep, or about to be. Waking it takes a write to its descriptor.
const ASLEEP: u8 = 1;
/// Told to wake up since it last slept. It does not go to sleep on that.
const NOTIFIED: u8 = 2;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Worker>>> = const { RefCell::new(None) };
}

/// The right to start an operation in slot `index` of a reactor, with the
/// arbiter that goes with the slot.
#[derive(Clone, Copy)]
struct Lease {
    index: u32,
    cell: CellRef,
}

/// Work handed to a worker by a thread that does not own its ring.
enum Remote {
    Submit {
        lease: Lease,
        generation: u32,
        task: TaskRef,
        operation: Operation,
    },
    Cancel(u64),
    Watch {
        remote_id: u64,
        file: OwnedFd,
        events: u32,
        readiness: Readiness,
    },
    Unwatch(u64),
    UnwatchById(u64),
}

/// What other threads share with a worker: the arbiters, and the way in for
/// threads that have no ring of their own.
#[derive(Default)]
struct Pool {
    chunks: Vec<NonNull<[Arbiter]>>,
    /// Leases not held by the reactor's own free list.
    free: Vec<Lease>,
    queue: Vec<Remote>,
    /// Set when the worker has stopped. Nothing is accepted after that.
    closed: bool,
}

// SAFETY: the chunks are plain heap allocations of atomics, owned by the
// pool.
unsafe impl Send for Pool {}

impl Pool {
    fn lease(&mut self) -> Lease {
        if self.free.is_empty() {
            self.grow();
        }
        self.free.pop().expect("pool was just grown")
    }

    fn grow(&mut self) {
        let chunk: Box<[Arbiter]> = (0..CHUNK).map(|_| Arbiter::new()).collect();
        // Held as a raw pointer: the arbiters are referred to from futures
        // and from the reactor for as long as the pool lives.
        let chunk = NonNull::from(Box::leak(chunk));
        let base = self.chunks.len() * CHUNK;
        self.chunks.push(chunk);
        let first = chunk.cast::<Arbiter>();
        self.free.extend((0..CHUNK).map(|offset| {
            Lease {
                index: u32::try_from(base + offset)
                    .ok()
                    .filter(|&index| u64::from(index) <= INDEX)
                    .expect("more operations in flight than a token can number"),
                // SAFETY: `offset` is within the chunk.
                cell: CellRef::new(unsafe { first.add(offset) }),
            }
        }));
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for chunk in self.chunks.drain(..) {
            // SAFETY: allocated as a `Box` in `grow`. The pool is dropped
            // with the multiring's last handle, when no future and no reactor
            // is left to use an arbiter.
            drop(unsafe { Box::from_raw(chunk.as_ptr()) });
        }
    }
}

/// The part of a worker that other threads can reach.
struct Shared {
    pool: Mutex<Pool>,
    in_flight: AtomicUsize,
    /// One of `AWAKE`, `ASLEEP` and `NOTIFIED`.
    sleep: AtomicU8,
    /// An eventfd. The worker's ring reads from it, so a write brings the
    /// worker out of its sleep there.
    wake: OwnedFd,
    /// The worker's rings, from when the runtime is built until the worker
    /// starts and takes them.
    rings: Mutex<Option<Rings>>,
}

impl Shared {
    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Rings {
    ring: IoUring,
    disk: Option<IoUring>,
}

/// What a multiring has once its runtime is built.
struct Started {
    workers: Box<[Shared]>,
    /// Keeps the kernel submission threads of the disk rings alive.
    _disk: Option<Disk>,
}

struct Inner {
    me: Weak<Inner>,
    sq_entries: u32,
    buffers: Option<BufferPool>,
    disk: Option<DiskConfig>,
    started: OnceLock<Started>,
    next_remote: AtomicUsize,
    next_remote_watch: AtomicU64,
    /// One more than the index of the sleeping worker that wakes for the
    /// runtime's timers. Zero when no worker does, which is never the case
    /// while all of them sleep.
    timekeeper: AtomicUsize,
}

/// Names an in-flight operation: the worker whose ring runs it, its token
/// there, and its arbiter.
#[derive(Clone, Copy)]
pub(crate) struct Ticket {
    worker: usize,
    token: u64,
    cell: CellRef,
}

impl Ticket {
    pub(crate) fn generation(&self) -> u32 {
        generation_of(self.token)
    }
}

/// Settings for the per-worker rings.
#[derive(Clone)]
pub struct Config {
    /// Submission queue size of each ring. The completion queue is twice
    /// this.
    pub sq_entries: u32,
    /// Buffers to register on every ring, for the `*_fixed` and `*_polled`
    /// operations.
    pub buffers: Option<BufferPool>,
    /// Polled rings for `O_DIRECT` file I/O, for the `*_polled` operations.
    /// Needs `buffers`, and a runtime built with [`Plan::build`].
    pub disk: Option<DiskConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sq_entries: 256,
            buffers: None,
            disk: None,
        }
    }
}

/// Settings for the polled disk rings.
///
/// Every worker gets a second ring that the kernel drives without help from
/// the worker: a kernel thread takes submissions off the ring and busy-polls
/// the device for their completions, so a disk operation costs the worker no
/// system call and the device raises no interrupt. One such thread serves
/// the disk rings of all the workers of a domain of the plan, and spins on
/// its CPU while they are busy.
#[derive(Clone, Debug)]
pub struct DiskConfig {
    /// Submission queue size of each worker's disk ring.
    pub sq_entries: u32,
    /// How long a kernel thread keeps spinning with nothing to do before it
    /// goes to sleep. Waking it again takes a system call.
    pub sq_thread_idle: Duration,
    /// The plan that built the runtime.
    pub plan: Plan,
}

/// The kernel submission threads of a multiring, one per domain of the plan.
/// Each is kept alive by a ring that does no I/O, which the disk rings of
/// the domain's workers attach to.
struct Disk {
    anchors: Vec<IoUring>,
    plan: Plan,
    sq_entries: u32,
    idle_ms: u32,
}

impl Disk {
    fn new(config: &DiskConfig) -> io::Result<Disk> {
        let idle_ms = u32::try_from(config.sq_thread_idle.as_millis()).unwrap_or(u32::MAX);
        let anchors = config
            .plan
            .domains()
            .iter()
            .map(|domain| {
                let mut builder = IoUring::builder();
                builder.setup_iopoll().setup_sqpoll(idle_ms);
                if let Some(cpu) = domain.sq_thread {
                    builder.setup_sqpoll_cpu(cpu);
                }
                builder.build(1)
            })
            .collect::<io::Result<_>>()?;

        Ok(Disk {
            anchors,
            plan: config.plan.clone(),
            sq_entries: config.sq_entries,
            idle_ms,
        })
    }

    /// Creates the disk ring of the worker with index `worker`, served by
    /// the kernel thread of the domain the plan places it in.
    fn ring(&self, worker: usize) -> io::Result<IoUring> {
        let domain = self.plan.domain_of_worker(worker);
        IoUring::builder()
            .setup_iopoll()
            .setup_sqpoll(self.idle_ms)
            .setup_attach_wq(self.anchors[domain].as_raw_fd())
            .build(self.sq_entries)
    }
}

/// Handle to the reactor of one runtime. Cheap to clone.
#[derive(Clone)]
pub struct MultiRing {
    inner: Arc<Inner>,
}

impl MultiRing {
    /// Makes the runtime that `builder` builds one that a multiring drives:
    /// every worker gets a ring, sleeps in it when it has nothing to run,
    /// and the runtime's own sockets and other I/O resources are watched by
    /// those rings too. Returns the handle that operations are started on.
    ///
    /// The rings are created when the runtime is built, and an error in
    /// that is returned by the builder's `build`. Operations can be started
    /// from then on. The runtime has to be a multi-threaded one, and must
    /// not use [`block_in_place`](tokio::task::block_in_place): a ring
    /// belongs to the thread its worker started on.
    ///
    /// Runtime shutdown cancels outstanding operations and waits for the
    /// kernel's final completions before releasing their memory. Operations
    /// that cannot be cancelled (including zero-copy buffer release) can
    /// delay worker exit. Tokio's `shutdown_timeout` and `shutdown_background`
    /// bound the caller's wait; the workers retain resources until safe to
    /// reclaim them. Operations first polled after their worker stops fail
    /// with `ECANCELED` and return their buffers normally.
    ///
    /// This takes the builder's `io_reactor` slot (flushing before stealing
    /// runs through [`Reactor::before_steal`], leaving
    /// [`Builder::on_before_steal`] free for callers). A multiring drives one
    /// runtime: building a second one from the same builder fails with
    /// [`io::ErrorKind::AlreadyExists`].
    pub fn install(builder: &mut Builder, config: Config) -> MultiRing {
        let inner = Arc::new_cyclic(|me| Inner {
            me: me.clone(),
            sq_entries: config.sq_entries,
            buffers: config.buffers,
            disk: config.disk,
            started: OnceLock::new(),
            next_remote: AtomicUsize::new(0),
            next_remote_watch: AtomicU64::new(1),
            timekeeper: AtomicUsize::new(0),
        });
        let reactor = inner.clone();
        builder.io_reactor(move |workers| {
            reactor.start(workers)?;
            Ok(reactor.clone() as Arc<dyn Reactor>)
        });
        MultiRing { inner }
    }

    /// The pool registered on the rings, if [`Config::buffers`] named one.
    pub fn buffers(&self) -> Option<&BufferPool> {
        self.inner.buffers.as_ref()
    }

    /// Whether the workers have polled disk rings, for the `*_polled`
    /// operations.
    pub fn has_disk_rings(&self) -> bool {
        self.inner.disk.is_some()
    }

    /// Number of operations handed to a ring that the kernel is not done
    /// with, across all workers. A zero-copy send counts until the kernel
    /// has let go of its buffers.
    pub fn in_flight(&self) -> usize {
        self.inner
            .workers()
            .iter()
            .map(|shared| shared.in_flight.load(Ordering::Relaxed))
            .sum()
    }

    /// Hands `operation` to a ring: the current worker's if this thread has
    /// one, otherwise the next in rotation. The result is written to `task`,
    /// whose waker must already be set.
    pub(crate) fn submit(&self, operation: Operation, task: TaskRef) -> Result<Ticket, Operation> {
        match self.inner.local() {
            Some(worker) => {
                let lease = worker.lease();
                // SAFETY: `self` keeps the pool alive.
                let generation = unsafe { lease.cell.get() }.arm();
                let token = worker.start(lease, generation, task, operation);
                Ok(Ticket {
                    worker: worker.index,
                    token,
                    cell: lease.cell,
                })
            }
            None => {
                let worker = self.inner.next_worker();
                let shared = &self.inner.workers()[worker];

                let mut pool = shared.pool();
                if pool.closed {
                    return Err(operation);
                }
                let lease = pool.lease();
                // SAFETY: `self` keeps the pool alive.
                let generation = unsafe { lease.cell.get() }.arm();
                pool.queue.push(Remote::Submit {
                    lease,
                    generation,
                    task,
                    operation,
                });
                drop(pool);
                self.inner.wake(worker);

                Ok(Ticket {
                    worker,
                    token: token_of(generation, lease.index),
                    cell: lease.cell,
                })
            }
        }
    }

    /// The arbiter of `ticket`'s operation.
    pub(crate) fn arbiter(&self, ticket: &Ticket) -> &Arbiter {
        // SAFETY: the ticket came from this multiring, which keeps the pool
        // alive for as long as it is borrowed.
        unsafe { ticket.cell.get() }
    }

    /// Asks the kernel to cut short an operation whose future is gone.
    pub(crate) fn cancel(&self, ticket: Ticket) {
        match self.inner.local() {
            Some(worker) if worker.index == ticket.worker => worker.cancel(ticket.token),
            _ => {
                let shared = &self.inner.workers()[ticket.worker];
                let mut pool = shared.pool();
                if !pool.closed {
                    pool.queue.push(Remote::Cancel(ticket.token));
                    drop(pool);
                    self.inner.wake(ticket.worker);
                }
            }
        }
    }
}

impl Inner {
    /// The workers of the runtime this multiring drives.
    ///
    /// # Panics
    ///
    /// Panics if the runtime has not been built.
    fn workers(&self) -> &[Shared] {
        let started = self.started.get();
        &started
            .expect("the multiring's runtime is not built")
            .workers
    }

    /// Creates the rings of a runtime with this many workers.
    fn start(&self, workers: usize) -> io::Result<()> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidInput, message);
        if workers as u64 > TOKEN_WORKER_MASK {
            return Err(invalid("too many workers for multiring watch tokens"));
        }
        if self.started.get().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "this multiring already drives a runtime",
            ));
        }
        let disk = match &self.disk {
            Some(_) if self.buffers.is_none() => {
                return Err(invalid("disk rings need a buffer pool"));
            }
            // The disk rings are grouped by the domains of the plan, which
            // only means something if it also places the workers.
            Some(disk) if !disk.plan.has_built() || disk.plan.worker_count() != workers => {
                return Err(invalid("the disk plan is not the one building the runtime"));
            }
            Some(disk) => Some(Disk::new(disk)?),
            None => None,
        };

        let shared = (0..workers)
            .map(|worker| {
                // COOP_TASKRUN, not DEFER_TASKRUN: `turn` relies on any
                // enter bringing in the completions that are due.
                // TASKRUN_FLAG lets it see without a syscall whether there
                // are any. The ring is made disabled so that the worker's
                // thread, which enables it, becomes its one issuer.
                let ring = IoUring::builder()
                    .setup_single_issuer()
                    .setup_coop_taskrun()
                    .setup_taskrun_flag()
                    .setup_r_disabled()
                    .build(self.sq_entries)?;
                let disk = disk.as_ref().map(|disk| disk.ring(worker)).transpose()?;
                if let Some(buffers) = &self.buffers {
                    buffers.register_on(&ring)?;
                    if let Some(disk) = &disk {
                        buffers.register_on(disk)?;
                    }
                }

                // In blocking mode, for the read on the ring to wait.
                // SAFETY: returns a new descriptor or -1.
                let wake = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
                if wake == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Shared {
                    pool: Mutex::default(),
                    in_flight: AtomicUsize::new(0),
                    sleep: AtomicU8::new(AWAKE),
                    // SAFETY: just created, and owned by nothing else.
                    wake: unsafe { OwnedFd::from_raw_fd(wake) },
                    rings: Mutex::new(Some(Rings { ring, disk })),
                })
            })
            .collect::<io::Result<_>>()?;

        let started = Started {
            workers: shared,
            _disk: disk,
        };
        // Tokio builds a runtime on one thread, so nothing races this.
        self.started.set(started).map_err(|_| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "this multiring already drives a runtime",
            )
        })
    }

    /// This thread's worker, if it has one and it belongs to this multiring.
    fn local(&self) -> Option<Rc<Worker>> {
        // The thread-local is gone when a future is dropped during thread
        // teardown; such a thread is treated like any other non-worker.
        CURRENT
            .try_with(|current| current.borrow().clone())
            .ok()
            .flatten()
            .filter(|worker| std::ptr::eq(Arc::as_ptr(&worker.multiring), self))
    }

    /// The worker that takes the next piece of work from a thread without
    /// a ring.
    fn next_worker(&self) -> usize {
        self.next_remote.fetch_add(1, Ordering::Relaxed) % self.workers().len()
    }

    /// Brings `worker` out of its sleep if it is asleep, and keeps it from
    /// going to sleep otherwise.
    fn wake(&self, worker: usize) {
        let shared = &self.workers()[worker];
        if shared.sleep.swap(NOTIFIED, Ordering::SeqCst) == ASLEEP {
            let one = 1u64.to_ne_bytes();
            // SAFETY: writes eight bytes from a buffer of eight. It can only
            // fail with a counter that is at its maximum, which wakes the
            // worker just the same.
            unsafe { libc::write(shared.wake.as_raw_fd(), one.as_ptr().cast(), one.len()) };
        }
    }
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiRing")
            .field("workers", &self.started.get().map(|s| s.workers.len()))
            .finish_non_exhaustive()
    }
}

impl Reactor for Inner {
    /// Has a ring watch the file: the current worker's if this thread has
    /// one, so that a connection is watched where it is used, otherwise the
    /// next in rotation.
    fn register(
        &self,
        fd: BorrowedFd<'_>,
        interest: Interest,
    ) -> io::Result<(Readiness, RegistrationToken)> {
        // The caller may close its descriptor at any time, and the number
        // may then name another file.
        let file = fd.try_clone_to_owned()?;
        let events = poll_events(interest);
        let readiness = Readiness::new();

        let token = match self.local() {
            Some(worker) => {
                let cqe_token = worker.watch(file, events, readiness.clone());
                direct_watch_token(worker.index, cqe_token)
            }
            None => {
                let worker = self.next_worker();
                let remote_id =
                    self.next_remote_watch.fetch_add(1, Ordering::Relaxed) & REMOTE_ID_MASK;
                let mut pool = self.workers()[worker].pool();
                if pool.closed {
                    return Err(io::Error::other("the runtime's I/O reactor has shut down"));
                }
                pool.queue.push(Remote::Watch {
                    remote_id,
                    file,
                    events,
                    readiness: readiness.clone(),
                });
                drop(pool);
                self.wake(worker);
                remote_watch_token(worker, remote_id)
            }
        };
        Ok((readiness, token))
    }

    fn deregister(&self, token: RegistrationToken) -> io::Result<()> {
        let raw = token.0;
        let worker_idx = ((raw >> 48) & TOKEN_WORKER_MASK) as usize;
        if raw & REMOTE_WATCH_BIT == 0 {
            let cqe_token = cqe_token_from_direct(raw);
            match self.local() {
                Some(worker) if worker.index == worker_idx => worker.unwatch(cqe_token),
                _ => {
                    let mut pool = self.workers()[worker_idx].pool();
                    if !pool.closed {
                        pool.queue.push(Remote::Unwatch(cqe_token));
                        drop(pool);
                        self.wake(worker_idx);
                    }
                }
            }
        } else {
            let remote_id = raw & REMOTE_ID_MASK;
            match self.local() {
                Some(worker) if worker.index == worker_idx => {
                    let existing = worker.remote_watches.borrow_mut().remove(&remote_id);
                    if let Some(cqe_token) = existing {
                        worker.unwatch(cqe_token);
                    } else {
                        let mut pool = self.workers()[worker_idx].pool();
                        if !pool.closed {
                            pool.queue.push(Remote::UnwatchById(remote_id));
                            drop(pool);
                            self.wake(worker_idx);
                        }
                    }
                }
                _ => {
                    let mut pool = self.workers()[worker_idx].pool();
                    if !pool.closed {
                        pool.queue.push(Remote::UnwatchById(remote_id));
                        drop(pool);
                        self.wake(worker_idx);
                    }
                }
            }
        }
        Ok(())
    }

    fn park(&self, worker: usize, mode: ParkMode, timers: &Timers<'_>) -> bool {
        match self.local() {
            Some(local) if local.index == worker => local.park(mode, timers),
            _ => panic!(
                "worker {worker} went to sleep on a thread that does not own its ring: \
                 a multiring needs a multi-threaded runtime"
            ),
        }
        true
    }

    fn unpark(&self, worker: usize) {
        self.wake(worker);
    }

    fn unpark_for_timer(&self) {
        if let Some(worker) = self.timekeeper.load(Ordering::SeqCst).checked_sub(1) {
            self.wake(worker);
        }
    }

    fn allow_block_in_place(&self) -> bool {
        false
    }

    fn before_steal(&self, worker: usize) {
        if let Some(local) = self.local() {
            debug_assert_eq!(local.index, worker);
            local.turn();
        }
    }

    fn worker_start(&self, worker: usize) {
        let rings = self.workers()[worker].rings.lock();
        let rings = rings.unwrap_or_else(PoisonError::into_inner).take();
        let Some(rings) = rings else {
            panic!(
                "worker {worker} started a second time: a runtime that a multiring drives \
                 cannot use `block_in_place`"
            );
        };
        let multiring = self.me.upgrade().expect("called through a reference");
        Worker::install(multiring, worker, rings);
    }

    fn worker_stop(&self, _worker: usize) {
        let worker = CURRENT.with(|current| current.borrow_mut().take());
        debug_assert!(
            worker.is_some_and(|worker| std::ptr::eq(Arc::as_ptr(&worker.multiring), self))
        );
    }
}

/// Flushes the current worker's ring: submits what its tasks have queued and
/// delivers the completions that are ready. [`MultiRing::install`] has the
/// runtime call it whenever a worker has run out of local work through
/// [`Reactor::before_steal`], independently of the application's
/// [`Builder::on_before_steal`] callback.
///
/// The hook runs when a worker has emptied its local queue, so everything
/// the worker's tasks queued since the last flush goes to the kernel in one
/// `io_uring_enter`, without a trip through the scheduler.
///
/// A worker that never runs out of local work never reaches the hook. The
/// runtime has every worker look at its ring every so many tasks, which
/// submits what is queued and, every few looks, takes completions in.
///
/// Without the hook the multiring still works: a worker then flushes when it
/// is about to go to sleep, and at those regular looks.
///
/// Does nothing on a thread that has no ring.
pub fn before_steal() {
    let worker = CURRENT.with(|current| current.borrow().clone());
    if let Some(worker) = worker {
        worker.turn();
    }
}

/// An operation the kernel has, or is about to get.
struct InFlight {
    token: u64,
    /// Whether it went to the disk ring.
    polled: bool,
    cell: CellRef,
    task: TaskRef,
    operation: Operation,
    /// Set once the result has been delivered while the kernel still refers
    /// to the operation's buffers. The slot is kept until it says it is done
    /// with them.
    lingering: bool,
    /// What the kernel said when it let go of the buffers, if that came
    /// before the result could be delivered.
    let_go: Option<i32>,
}

/// A file that the runtime registered, watched by a multishot poll.
struct Watch {
    /// The worker's own descriptor for the file.
    file: OwnedFd,
    events: u32,
    readiness: Readiness,
    /// Set when the registration has ended. The entry stays until the
    /// kernel has said that the poll is over.
    ended: bool,
}

/// The watches of one worker, by the index in their token.
#[derive(Default)]
struct Watches {
    /// Each with the generation of the watch that is or will be there.
    entries: Vec<(u32, Option<Watch>)>,
    vacant: Vec<u32>,
}

/// What a watch found out, to be passed on once no borrow is held.
enum Report {
    Ready(Ready),
    /// The file can no longer be watched.
    Failed,
}

/// One worker's rings and what is in flight on them. Only ever touched from
/// that worker's thread.
struct Worker {
    ring: RefCell<IoUring>,
    /// The polled ring, if the multiring has them.
    disk: Option<RefCell<IoUring>>,
    /// In-flight operations, by the index in their token.
    slots: RefCell<Vec<Option<InFlight>>>,
    /// Leases for slots that are empty.
    free: RefCell<Vec<Lease>>,
    watches: RefCell<Watches>,
    /// Maps remote watch ids (queued from non-worker threads) to their local
    /// `cqe_token` in `watches`.
    remote_watches: RefCell<HashMap<u64, u64>>,
    /// Completions whose future was busy with its waker when they came in:
    /// token, result and flags.
    deferred: RefCell<Vec<(u64, i32, u32)>>,
    /// Scratch space for `turn` and `reap`, kept to reuse the allocations.
    remote: RefCell<Vec<Remote>>,
    completions: RefCell<Vec<(u64, i32, u32)>>,
    wakers: RefCell<Vec<Waker>>,
    released: RefCell<Vec<Operation>>,
    reports: RefCell<Vec<(Readiness, Report)>>,
    /// Set when something is left that a turn soon has to pick up. The
    /// worker does not go to sleep on that.
    revisit: Cell<bool>,
    /// Looks at the ring since completions were last taken in.
    looks: Cell<u32>,
    /// Stops rearming watches while shutdown drains their final CQEs.
    stopping: Cell<bool>,
    wake_pending: Cell<bool>,
    /// Stable storage kept alive until the wake read's final CQE.
    woken: Box<UnsafeCell<[u8; 8]>>,
    multiring: Arc<Inner>,
    index: usize,
}

impl Worker {
    /// Makes the calling thread the one that runs the worker with index
    /// `index`, and the only one to use its rings.
    fn install(multiring: Arc<Inner>, index: usize, rings: Rings) {
        if let Err(error) = rings.ring.submitter().register_enable_rings() {
            panic!("worker {index} could not take over its ring: {error}");
        }
        let worker = Rc::new(Worker {
            ring: RefCell::new(rings.ring),
            disk: rings.disk.map(RefCell::new),
            slots: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            watches: RefCell::new(Watches::default()),
            remote_watches: RefCell::new(HashMap::new()),
            deferred: RefCell::new(Vec::new()),
            remote: RefCell::new(Vec::new()),
            completions: RefCell::new(Vec::new()),
            wakers: RefCell::new(Vec::new()),
            released: RefCell::new(Vec::new()),
            reports: RefCell::new(Vec::new()),
            revisit: Cell::new(false),
            looks: Cell::new(0),
            stopping: Cell::new(false),
            wake_pending: Cell::new(false),
            woken: Box::new(UnsafeCell::new([0; 8])),
            multiring,
            index,
        });
        worker.listen_for_wake();
        worker.listen_for_disk();
        let previous = CURRENT.with(|current| current.borrow_mut().replace(worker));
        assert!(previous.is_none(), "this thread already runs a worker");
    }

    fn shared(&self) -> &Shared {
        &self.multiring.workers()[self.index]
    }

    /// Queues the read that a write to the worker's eventfd completes.
    fn listen_for_wake(&self) {
        self.wake_pending.set(true);
        let wake = types::Fd(self.shared().wake.as_raw_fd());
        let entry = opcode::Read::new(wake, self.woken.get().cast(), 8)
            .build()
            .user_data(WAKE);
        if let Err(error) = self.push(&entry) {
            // Nothing could wake the worker from its ring again.
            panic!("io_uring_enter failed: {error}");
        }
    }

    /// Queues the poll that says when the disk ring has completions, which
    /// is what gets a worker asleep in its ring to reap them.
    fn listen_for_disk(&self) {
        let Some(disk) = &self.disk else {
            return;
        };
        let disk = types::Fd(disk.borrow().as_raw_fd());
        let entry = opcode::PollAdd::new(disk, libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(DISK);
        if let Err(error) = self.push(&entry) {
            panic!("io_uring_enter failed: {error}");
        }
    }

    fn lease(&self) -> Lease {
        let mut free = self.free.borrow_mut();
        if let Some(lease) = free.pop() {
            return lease;
        }
        let mut pool = self.shared().pool();
        while pool.free.len() < BATCH {
            pool.grow();
        }
        let keep = pool.free.len() - BATCH;
        free.extend(pool.free.drain(keep..));
        free.pop().expect("a batch was just taken")
    }

    fn release(&self, lease: Lease) {
        let mut free = self.free.borrow_mut();
        free.push(lease);
        // Operations submitted by other threads bring their lease from the
        // pool and leave it here, so the surplus goes back.
        if free.len() > 2 * BATCH {
            self.shared().pool().free.extend(free.drain(BATCH..));
        }
    }

    /// Puts an operation with an armed arbiter into its slot and queues it
    /// for the kernel. Returns its token.
    fn start(&self, lease: Lease, generation: u32, task: TaskRef, mut operation: Operation) -> u64 {
        let token = token_of(generation, lease.index);
        let polled = operation.polled();
        let entry = operation.entry().user_data(token);
        {
            let mut slots = self.slots.borrow_mut();
            let index = lease.index as usize;
            if slots.len() <= index {
                slots.resize_with(index + 1, || None);
            }
            debug_assert!(slots[index].is_none());
            slots[index] = Some(InFlight {
                token,
                polled,
                cell: lease.cell,
                task,
                operation,
                lingering: false,
                let_go: None,
            });
        }
        self.shared().in_flight.fetch_add(1, Ordering::Relaxed);

        let pushed = if polled {
            self.push_disk(&entry)
        } else {
            self.push(&entry)
        };
        if let Err(error) = pushed {
            let result = -error.raw_os_error().unwrap_or(libc::EIO);
            let mut wakers = self.wakers.take();
            let mut released = self.released.take();
            if !self.resolve(token, result, 0, &mut wakers, &mut released) {
                self.deferred.borrow_mut().push((token, result, 0));
                self.revisit.set(true);
            }
            self.wakers.replace(wakers);
            self.released.replace(released);
            self.deliver();
        }
        token
    }

    /// Handles a completion of the operation `token`: writes its result into
    /// the waiting future, or releases the operation if the future is gone.
    /// Returns `false`, leaving everything in place, if the future is busy.
    ///
    /// An operation that completes twice keeps its slot between the two:
    /// the first completion delivers the result, and the second, flagged as
    /// a notification, says that the kernel no longer refers to the
    /// operation's buffers.
    ///
    /// Waking the future and letting go of an operation are left to the
    /// caller, to be done with no borrow held.
    fn resolve(
        &self,
        token: u64,
        result: i32,
        flags: u32,
        wakers: &mut Vec<Waker>,
        released: &mut Vec<Operation>,
    ) -> bool {
        let generation = generation_of(token);
        let cell = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[key_of(token)];
            let in_flight = slot
                .as_mut()
                .expect("completion for an operation that is not in flight");
            debug_assert_eq!(in_flight.token, token);

            if cqueue::notif(flags) {
                if !in_flight.lingering {
                    // The result is still waiting for its future.
                    in_flight.let_go = Some(result);
                    return true;
                }
                let mut finished = slot.take().expect("checked above");
                finished.operation.note_release(result);
                released.push(finished.operation);
                finished.cell
            } else {
                // SAFETY: the reactor keeps the pool alive.
                let cell = unsafe { in_flight.cell.get() };
                let deliver = match cell.claim(generation) {
                    Claim::Deliver => true,
                    Claim::Abandoned => false,
                    Claim::Busy => return false,
                };
                let task = in_flight.task;
                let mut deliver = |completion: Completion| {
                    if deliver {
                        // SAFETY: the claim was granted, so the future is
                        // alive and leaves its landing alone until the
                        // delivery is complete.
                        wakers.extend(unsafe { task.get().deliver(completion) });
                        None
                    } else {
                        Some(completion.operation)
                    }
                };

                if !in_flight.operation.completes_twice() {
                    let finished = slot.take().expect("checked above");
                    released.extend(deliver(Completion::new(result, finished.operation)));
                    cell.retire(generation);
                    finished.cell
                } else {
                    // The future gets the result alone. The operation stays
                    // for as long as the kernel refers to its buffers.
                    deliver(Completion::new(result, Operation::Retained));
                    cell.retire(generation);
                    if cqueue::more(flags) && in_flight.let_go.is_none() {
                        in_flight.lingering = true;
                        return true;
                    }
                    let mut finished = slot.take().expect("checked above");
                    finished
                        .operation
                        .note_release(finished.let_go.unwrap_or(0));
                    released.push(finished.operation);
                    finished.cell
                }
            }
        };

        self.release(Lease {
            index: key_of(token) as u32,
            cell,
        });
        self.shared().in_flight.fetch_sub(1, Ordering::Relaxed);
        true
    }

    /// Cancels `token` if it is still in flight. Polled operations are left
    /// to finish: block I/O that has been issued cannot be called back, and
    /// it is over quickly.
    fn cancel(&self, token: u64) {
        let cancellable = self
            .slots
            .borrow()
            .get(key_of(token))
            .and_then(Option::as_ref)
            .is_some_and(|in_flight| in_flight.token == token && !in_flight.polled);
        if cancellable {
            let entry = opcode::AsyncCancel::new(token).build().user_data(DETACHED);
            // A cancel that cannot be queued is dropped: the operation then
            // runs to its natural completion.
            let _ = self.push(&entry);
        }
    }

    /// Has the ring watch `file` for `events`, reporting to `readiness`.
    /// Returns the token of the watch.
    fn watch(&self, file: OwnedFd, events: u32, readiness: Readiness) -> u64 {
        let token = {
            let mut watches = self.watches.borrow_mut();
            let index = watches.vacant.pop().unwrap_or_else(|| {
                let index = u32::try_from(watches.entries.len())
                    .ok()
                    .filter(|&index| u64::from(index) <= INDEX)
                    .expect("more files registered than a token can number");
                watches.entries.push((0, None));
                index
            });
            let (generation, entry) = &mut watches.entries[index as usize];
            *entry = Some(Watch {
                file,
                events,
                readiness,
                ended: false,
            });
            token_of(*generation, index) | WATCH
        };
        self.poll(token);
        token
    }

    /// Queues the poll of the watch `token`. If that fails, the watch is
    /// over, and whoever waits on the file is told.
    fn poll(&self, token: u64) {
        let entry = {
            let watches = self.watches.borrow();
            let (_, watch) = &watches.entries[key_of(token)];
            let watch = watch.as_ref().expect("a watch that is in place");
            opcode::PollAdd::new(types::Fd(watch.file.as_raw_fd()), watch.events)
                .multi(true)
                .build()
                .user_data(token)
        };
        if self.push(&entry).is_err() {
            let watch = self.unlist(token);
            self.reports
                .borrow_mut()
                .push((watch.readiness, Report::Failed));
            self.revisit.set(true);
        }
    }

    /// Ends the watch `token`, if it is still there.
    fn unwatch(&self, token: u64) {
        let watching = {
            let mut watches = self.watches.borrow_mut();
            match watches.entries.get_mut(key_of(token)) {
                Some((generation, Some(watch))) if *generation == generation_of(token) => {
                    watch.ended = true;
                    true
                }
                _ => false,
            }
        };
        if watching {
            let entry = opcode::PollRemove::new(token).build().user_data(DETACHED);
            // If this cannot be queued, the poll and the worker's descriptor
            // for the file last until the file itself is done for.
            let _ = self.push(&entry);
        }
    }

    /// Takes the watch `token` out of the table, making room for another.
    fn unlist(&self, token: u64) -> Watch {
        let mut watches = self.watches.borrow_mut();
        let (generation, entry) = &mut watches.entries[key_of(token)];
        *generation = generation.wrapping_add(1) & (WATCH_GEN_MASK as u32);
        let watch = entry.take().expect("a watch that is in place");
        watches.vacant.push(key_of(token) as u32);
        watch
    }

    /// Handles a completion of the poll of the watch `token`.
    fn watched(&self, token: u64, result: i32, flags: u32) {
        let (report, ended) = {
            let watches = self.watches.borrow();
            let (generation, watch) = &watches.entries[key_of(token)];
            debug_assert_eq!(*generation, generation_of(token));
            let watch = watch.as_ref().expect("a watch that is in place");
            let report = match result {
                _ if watch.ended => None,
                0.. => Some(Report::Ready(ready(result as u32))),
                _ => Some(Report::Failed),
            };
            let report = report.map(|report| (watch.readiness.clone(), report));
            (report, watch.ended || result < 0)
        };
        self.reports.borrow_mut().extend(report);
        if cqueue::more(flags) {
            return;
        }
        // The kernel ends a multishot poll of its own accord now and then.
        // A new one reports whatever happened in between as it starts.
        match ended {
            true => drop(self.unlist(token)),
            false => self.poll(token),
        }
    }

    /// Wakes the futures whose results are in, passes on what the watches
    /// found, and drops the operations nobody is waiting for. All of these
    /// run foreign code, so the caller must hold no borrow.
    fn deliver(&self) {
        let mut released = self.released.take();
        released.drain(..).for_each(Operation::release);
        self.released.replace(released);

        let mut reports = self.reports.take();
        for (readiness, report) in reports.drain(..) {
            match report {
                Report::Ready(ready) => readiness.set_ready(ready),
                Report::Failed => readiness.shutdown(),
            }
        }
        self.reports.replace(reports);

        let mut wakers = self.wakers.take();
        for waker in wakers.drain(..) {
            waker.wake();
        }
        self.wakers.replace(wakers);
    }

    /// Queues `entry`, flushing the submission queue to the kernel first if
    /// it is full.
    fn push(&self, entry: &squeue::Entry) -> io::Result<()> {
        let mut ring = self.ring.borrow_mut();
        loop {
            // SAFETY: every pointer in `entry` refers to heap memory owned by
            // an operation held in `slots`, which is only released once the
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

    /// Queues `entry` on the disk ring, where the kernel thread picks it up
    /// by itself.
    fn push_disk(&self, entry: &squeue::Entry) -> io::Result<()> {
        let disk = self
            .disk
            .as_ref()
            .expect("polled operations are only accepted with disk rings");
        // SAFETY: as in `push`.
        while unsafe { disk.borrow_mut().submission().push(entry) }.is_err() {
            // The queue is full, and only the kernel thread empties it.
            self.kick_disk();
            match disk.borrow().submitter().squeue_wait() {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        self.kick_disk();
        Ok(())
    }

    /// Wakes the disk ring's kernel thread if it has gone to sleep, and has
    /// the kernel hand over completions that did not fit in the completion
    /// queue. Costs a system call only in those two cases.
    fn kick_disk(&self) {
        let Some(disk) = &self.disk else {
            return;
        };
        let ring = disk.borrow();
        loop {
            match ring.submit() {
                Ok(_) => return,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                // Out of completion-queue space: reaping makes room.
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ResourceBusy | io::ErrorKind::WouldBlock
                    ) =>
                {
                    self.revisit.set(true);
                    return;
                }
                // As in `enter`, there is no safe way to carry on.
                Err(error) => panic!("io_uring_enter failed on the disk ring: {error}"),
            }
        }
    }

    /// Takes in what other threads have handed over.
    fn take_remote(&self) {
        let mut remote = self.remote.take();
        std::mem::swap(&mut remote, &mut self.shared().pool().queue);
        for remote in remote.drain(..) {
            match remote {
                Remote::Submit {
                    lease,
                    generation,
                    task,
                    operation,
                } => {
                    // SAFETY: the worker keeps the pool alive.
                    let cell = unsafe { lease.cell.get() };
                    if cell.is_abandoned(generation) {
                        // Dropped before it got here: not worth a trip to
                        // the kernel.
                        cell.retire(generation);
                        self.release(lease);
                    } else {
                        self.start(lease, generation, task, operation);
                    }
                }
                Remote::Cancel(token) => self.cancel(token),
                Remote::Watch {
                    remote_id,
                    file,
                    events,
                    readiness,
                } => {
                    let token = self.watch(file, events, readiness);
                    self.remote_watches.borrow_mut().insert(remote_id, token);
                }
                Remote::Unwatch(token) => self.unwatch(token),
                Remote::UnwatchById(remote_id) => {
                    let existing = self.remote_watches.borrow_mut().remove(&remote_id);
                    if let Some(token) = existing {
                        self.unwatch(token);
                    }
                }
            }
        }
        self.remote.replace(remote);
    }

    /// Takes in remote work, submits everything queued, and delivers the
    /// completions that are ready. Returns whether any were.
    fn turn(&self) -> bool {
        self.take_remote();
        let mut delivered = false;
        loop {
            self.enter();
            self.kick_disk();
            delivered |= self.reap();
            if !self.kernel_holds_completions() {
                break delivered;
            }
        }
    }

    /// Has a worker that still has tasks to run pass on what they queued,
    /// and every so often take completions in. See [`LOOKS_PER_REAP`].
    fn look(&self) {
        let looks = self.looks.get() + 1;
        if looks >= LOOKS_PER_REAP {
            self.turn();
            return;
        }
        self.looks.set(looks);
        self.take_remote();
        self.enter();
        self.kick_disk();
    }

    /// Puts the worker to sleep in its ring until a completion comes in,
    /// another thread wakes it, or its time is up. Under [`ParkMode::Poll`],
    /// the worker is busy and only looks at its ring.
    fn park(&self, mode: ParkMode, timers: &Timers<'_>) {
        let limit = match mode {
            ParkMode::Poll => return self.look(),
            ParkMode::Park(limit) => limit,
        };
        // Tasks that this woke have to run before the worker may sleep.
        let delivered = self.turn();
        if delivered || self.revisit.take() {
            return;
        }

        let shared = self.shared();
        let asleep =
            shared
                .sleep
                .compare_exchange(AWAKE, ASLEEP, Ordering::SeqCst, Ordering::SeqCst);
        if asleep.is_ok() {
            // One sleeping worker wakes for the timers, the first to get
            // there. Publish `timekeeper` with `SeqCst` *before* calling
            // `timers.next_timeout()` so any concurrent timer insert either
            // wakes us or is observed here.
            let multiring = &*self.multiring;
            let keeper = multiring
                .timekeeper
                .compare_exchange(0, self.index + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok();
            let timeout = if keeper {
                match (limit, timers.next_timeout()) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                }
            } else {
                limit
            };
            if timeout != Some(Duration::ZERO) {
                self.wait(timeout);
            }
            if keeper {
                multiring.timekeeper.store(0, Ordering::SeqCst);
            }
            shared.sleep.store(AWAKE, Ordering::SeqCst);
            self.turn();
        } else {
            // Told to wake up before it slept.
            shared.sleep.store(AWAKE, Ordering::SeqCst);
        }
    }

    /// Submits what is queued and sleeps until the ring has a completion,
    /// for at most `limit`.
    fn wait(&self, limit: Option<Duration>) {
        self.kick_disk();
        let ring = self.ring.borrow();
        let result = match limit {
            None => ring.submit_and_wait(1),
            Some(limit) => {
                let limit = types::Timespec::new()
                    .sec(limit.as_secs())
                    .nsec(limit.subsec_nanos());
                let args = types::SubmitArgs::new().timespec(&limit);
                ring.submitter().submit_with_args(1, &args)
            }
        };
        match result.map_err(|error| (error.raw_os_error(), error)) {
            Ok(_) => {}
            // Out of time, interrupted, or out of completion-queue space,
            // which the reaping that follows makes.
            Err((Some(libc::ETIME | libc::EINTR | libc::EBUSY | libc::EAGAIN), _)) => {}
            // As in `enter`, there is no safe way to carry on.
            Err((_, error)) => panic!("io_uring_enter failed: {error}"),
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
        if queue.taskrun() || queue.cq_overflow() {
            return true;
        }
        self.disk.as_ref().is_some_and(|disk| {
            let ring = disk.borrow();
            // SAFETY: as above. The kernel thread only reads the queue.
            unsafe { ring.submission_shared() }.cq_overflow()
        })
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
                    self.revisit.set(true);
                    return;
                }
                // Anything else means the ring itself is unusable. In-flight
                // operations cannot be failed safely, because the kernel may
                // still be using their buffers.
                Err(error) => panic!("io_uring_enter failed: {error}"),
            }
        }
    }

    /// Takes the completions off the rings and acts on them. Returns whether
    /// any of them was for a future or for a file the runtime watches.
    fn reap(&self) -> bool {
        self.looks.set(0);
        let mut wakers = self.wakers.take();
        let mut released = self.released.take();
        let mut deferred = self.deferred.take();
        let mut completions = self.completions.take();

        let before = deferred.len();
        deferred.retain(|&(token, result, flags)| {
            !self.resolve(token, result, flags, &mut wakers, &mut released)
        });
        let mut delivered = deferred.len() < before;

        for ring in std::iter::once(&self.ring).chain(&self.disk) {
            let mut ring = ring.borrow_mut();
            completions.extend(
                ring.completion()
                    .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags())),
            );
        }
        for (token, result, flags) in completions.drain(..) {
            match token {
                DETACHED => {}
                WAKE => {
                    self.wake_pending.set(false);
                    if !self.stopping.get() {
                        self.listen_for_wake();
                    }
                }
                DISK if self.stopping.get() || cqueue::more(flags) => {}
                DISK => self.listen_for_disk(),
                _ if token & KIND == WATCH => {
                    self.watched(token, result, flags);
                    delivered = true;
                }
                _ => {
                    debug_assert_eq!(token & KIND, OPERATION);
                    if !self.resolve(token, result, flags, &mut wakers, &mut released) {
                        deferred.push((token, result, flags));
                    }
                    delivered = true;
                }
            }
        }
        // A future only keeps its waker locked for a moment: come back soon.
        if !deferred.is_empty() {
            self.revisit.set(true);
        }
        self.deferred.replace(deferred);
        self.completions.replace(completions);
        self.wakers.replace(wakers);
        self.released.replace(released);
        self.deliver();
        delivered
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // A kernel error or a foreign waker panic must not unwind through
        // fields whose memory the kernel can still access.
        struct AbortOnUnwind(bool);
        impl Drop for AbortOnUnwind {
            fn drop(&mut self) {
                if self.0 && std::thread::panicking() {
                    std::process::abort();
                }
            }
        }
        let mut guard = AbortOnUnwind(true);
        self.stopping.set(true);
        let unsent = {
            let mut pool = self.shared().pool();
            pool.closed = true;
            std::mem::take(&mut pool.queue)
        };
        // CURRENT has already been cleared. Foreign wakers therefore cannot
        // reenter this worker; new submissions see the closed queue.
        for remote in unsent {
            match remote {
                Remote::Submit {
                    lease,
                    generation,
                    task,
                    operation,
                } => {
                    // This operation never reached the SQ, so it can be
                    // returned immediately, after arbitration with its future.
                    // SAFETY: the worker keeps the arbiter pool alive.
                    let cell = unsafe { lease.cell.get() };
                    loop {
                        match cell.claim(generation) {
                            Claim::Busy => std::thread::yield_now(),
                            Claim::Abandoned => {
                                operation.release();
                                break;
                            }
                            Claim::Deliver => {
                                let completion = Completion::cancelled(operation);
                                // SAFETY: the claim keeps the future alive.
                                let waker = unsafe { task.get().deliver(completion) };
                                if let Some(waker) = waker {
                                    waker.wake();
                                }
                                break;
                            }
                        }
                    }
                    cell.retire(generation);
                    self.release(lease);
                }
                Remote::Watch { readiness, .. } => readiness.shutdown(),
                _ => {}
            }
        }
        let readiness: Vec<_> = self
            .watches
            .get_mut()
            .entries
            .iter_mut()
            .filter_map(|(_, watch)| watch.as_mut())
            .map(|watch| {
                watch.ended = true;
                watch.readiness.clone()
            })
            .collect();
        for readiness in readiness {
            readiness.shutdown();
        }

        // Closing an io_uring fd is asynchronous. Keep every operation and
        // the wake-read buffer alive until their final CQEs instead. Sync
        // cancellation is retried after flushing/reaping, covering partial
        // submissions and CQ overflow. Disk I/O and zero-copy notifications
        // may not be cancellable: those must complete naturally.
        while self.wake_pending.get() || self.shared().in_flight.load(Ordering::Relaxed) != 0 {
            self.enter();
            self.kick_disk();
            let cancelled = self.ring.borrow().submitter().register_sync_cancel(
                Some(types::Timespec::new().nsec(1_000_000)),
                types::CancelBuilder::any(),
            );
            match cancelled {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.raw_os_error(),
                        Some(libc::ENOENT | libc::EINTR | libc::ETIME | libc::EBUSY | libc::EAGAIN)
                    ) => {}
                Err(error) => {
                    // Unwinding would free memory still owned by the kernel.
                    eprintln!("cannot safely shut down io_uring: {error}");
                    std::process::abort();
                }
            }
            self.reap();
            if self.wake_pending.get() || self.shared().in_flight.load(Ordering::Relaxed) != 0 {
                self.wait(Some(Duration::from_millis(1)));
            }
        }
        guard.0 = false;
    }
}

fn token_of(generation: u32, index: u32) -> u64 {
    u64::from(generation) << 32 | u64::from(index)
}

fn key_of(token: u64) -> usize {
    (token & INDEX) as usize
}

fn generation_of(token: u64) -> u32 {
    (token >> 32) as u32
}

fn direct_watch_token(worker: usize, cqe_token: u64) -> RegistrationToken {
    let generation = u64::from(generation_of(cqe_token)) & WATCH_GEN_MASK;
    let idx = cqe_token & INDEX;
    RegistrationToken(((worker as u64 & TOKEN_WORKER_MASK) << 48) | (generation << 30) | idx)
}

fn cqe_token_from_direct(raw: u64) -> u64 {
    let generation = ((raw >> 30) & WATCH_GEN_MASK) as u32;
    let idx = (raw & INDEX) as u32;
    token_of(generation, idx) | WATCH
}

fn remote_watch_token(worker: usize, remote_id: u64) -> RegistrationToken {
    RegistrationToken(
        REMOTE_WATCH_BIT
            | ((worker as u64 & TOKEN_WORKER_MASK) << 48)
            | (remote_id & REMOTE_ID_MASK),
    )
}

/// The poll events that tell about `interest`. Errors and hang-ups are
/// reported whatever is asked for.
fn poll_events(interest: Interest) -> u32 {
    let mut events = 0;
    if interest.is_readable() {
        events |= libc::POLLIN | libc::POLLRDHUP;
    }
    if interest.is_writable() {
        events |= libc::POLLOUT;
    }
    if interest.is_priority() {
        events |= libc::POLLPRI;
    }
    events as u32
}

/// What a poll that reported `events` says its file is ready for. The same
/// reading as that of tokio's own driver.
fn ready(events: u32) -> Ready {
    let has = |event: libc::c_short| events & event as u32 != 0;
    let mut ready = Ready::EMPTY;
    if has(libc::POLLIN) {
        ready |= Ready::READABLE;
    }
    if has(libc::POLLOUT) {
        ready |= Ready::WRITABLE;
    }
    if has(libc::POLLHUP) || has(libc::POLLIN) && has(libc::POLLRDHUP) {
        ready |= Ready::READ_CLOSED;
    }
    if has(libc::POLLHUP) || has(libc::POLLERR) && (has(libc::POLLOUT) || !has(libc::POLLIN)) {
        ready |= Ready::WRITE_CLOSED;
    }
    if has(libc::POLLERR) {
        ready |= Ready::ERROR;
    }
    if has(libc::POLLPRI) {
        ready |= Ready::PRIORITY;
    }
    ready
}
