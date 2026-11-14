use std::cell::{Cell, RefCell};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Waker;
use std::time::Duration;

use io_uring::{IoUring, opcode, squeue};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::runtime::Handle;
use tokio::sync::{Notify, mpsc};

use crate::buffers::BufferPool;
use crate::landing::{Arbiter, CellRef, Claim, TaskRef};
use crate::op::{Completion, Operation};
use crate::plan::Plan;

/// `user_data` of submissions whose completion nobody waits for. Operation
/// tokens carry a slot index in their low 32 bits, which never reaches
/// `u32::MAX`, so the two cannot collide.
const DETACHED: u64 = u64::MAX;

/// Arbiters are allocated this many at a time.
const CHUNK: usize = 64;

/// How many leases a reactor moves between its own free list and its pool
/// at a time.
const BATCH: usize = 32;

thread_local! {
    static CURRENT: RefCell<Option<Rc<Reactor>>> = const { RefCell::new(None) };
}

/// The right to start an operation in slot `index` of a reactor, with the
/// arbiter that goes with the slot.
#[derive(Clone, Copy)]
struct Lease {
    index: u32,
    cell: CellRef,
}

/// Work handed to a reactor by a thread that does not own its ring.
enum Remote {
    Submit {
        lease: Lease,
        generation: u32,
        task: TaskRef,
        operation: Operation,
    },
    Cancel(u64),
}

/// What other threads share with a worker's reactor: the arbiters, and the
/// way in for threads that have no ring of their own.
#[derive(Default)]
struct Pool {
    chunks: Vec<NonNull<[Arbiter]>>,
    /// Leases not held by the reactor's own free list.
    free: Vec<Lease>,
    queue: Vec<Remote>,
    /// Set when the reactor is gone. Nothing is accepted after that.
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
        self.free.extend((0..CHUNK).map(|offset| Lease {
            index: u32::try_from(base + offset).expect("more than u32::MAX operations in flight"),
            // SAFETY: `offset` is within the chunk.
            cell: CellRef::new(unsafe { first.add(offset) }),
        }));
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        for chunk in self.chunks.drain(..) {
            // SAFETY: allocated as a `Box` in `grow`. The pool is dropped
            // with the sidecar's last handle, when no future and no reactor
            // is left to use an arbiter.
            drop(unsafe { Box::from_raw(chunk.as_ptr()) });
        }
    }
}

/// The part of a reactor that other threads can reach.
#[derive(Default)]
struct Shared {
    pool: Mutex<Pool>,
    /// Wakes the driver task.
    wake: Notify,
    in_flight: AtomicUsize,
}

impl Shared {
    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Inner {
    workers: Box<[Shared]>,
    next_remote: AtomicUsize,
    buffers: Option<BufferPool>,
    disk: Option<Disk>,
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

/// The kernel submission threads of a sidecar, one per domain of the plan.
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

    /// Creates the current worker's disk ring, served by the kernel thread
    /// of the domain the plan placed it in.
    fn ring(&self) -> io::Result<IoUring> {
        let domain = self.plan.domain_of_current_thread().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "the disk plan did not place this runtime's workers",
            )
        })?;
        IoUring::builder()
            .setup_iopoll()
            .setup_sqpoll(self.idle_ms)
            .setup_attach_wq(self.anchors[domain].as_raw_fd())
            .build(self.sq_entries)
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
        let disk = match &config.disk {
            Some(_) if config.buffers.is_none() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "disk rings need a buffer pool",
                ));
            }
            Some(disk) => Some(Disk::new(disk)?),
            None => None,
        };
        let inner = Arc::new(Inner {
            workers: (0..workers).map(|_| Shared::default()).collect(),
            next_remote: AtomicUsize::new(0),
            buffers: config.buffers.clone(),
            disk,
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

    /// The pool registered on the rings, if [`Config::buffers`] named one.
    pub fn buffers(&self) -> Option<&BufferPool> {
        self.inner.buffers.as_ref()
    }

    /// Whether the workers have polled disk rings, for the `*_polled`
    /// operations.
    pub fn has_disk_rings(&self) -> bool {
        self.inner.disk.is_some()
    }

    /// Number of operations handed to a ring and not yet completed, across
    /// all workers.
    pub fn in_flight(&self) -> usize {
        self.inner
            .workers
            .iter()
            .map(|shared| shared.in_flight.load(Ordering::Relaxed))
            .sum()
    }

    /// Hands `operation` to a ring: the current worker's if this thread has
    /// one, otherwise the next in rotation. The result is written to `task`,
    /// whose waker must already be set.
    pub(crate) fn submit(&self, operation: Operation, task: TaskRef) -> Ticket {
        match self.local() {
            Some(reactor) => {
                let lease = reactor.lease();
                // SAFETY: `self` keeps the pool alive.
                let generation = unsafe { lease.cell.get() }.arm();
                let token = reactor.start(lease, generation, task, operation);
                if !reactor.hook_seen.get() {
                    reactor.notify_driver();
                }
                Ticket {
                    worker: reactor.index,
                    token,
                    cell: lease.cell,
                }
            }
            None => {
                let rotation = self.inner.next_remote.fetch_add(1, Ordering::Relaxed);
                let worker = rotation % self.inner.workers.len();
                let shared = &self.inner.workers[worker];

                let mut pool = shared.pool();
                let lease = pool.lease();
                // SAFETY: `self` keeps the pool alive.
                let generation = unsafe { lease.cell.get() }.arm();
                // With the reactor gone the operation is dropped, and its
                // future stays pending.
                let orphan = if pool.closed {
                    Some(operation)
                } else {
                    pool.queue.push(Remote::Submit {
                        lease,
                        generation,
                        task,
                        operation,
                    });
                    None
                };
                drop(pool);
                drop(orphan);
                shared.wake.notify_one();

                Ticket {
                    worker,
                    token: token_of(generation, lease.index),
                    cell: lease.cell,
                }
            }
        }
    }

    /// The arbiter of `ticket`'s operation.
    pub(crate) fn arbiter(&self, ticket: &Ticket) -> &Arbiter {
        // SAFETY: the ticket came from this sidecar, which keeps the pool
        // alive for as long as it is borrowed.
        unsafe { ticket.cell.get() }
    }

    /// Asks the kernel to cut short an operation whose future is gone.
    pub(crate) fn cancel(&self, ticket: Ticket) {
        match self.local() {
            Some(reactor) if reactor.index == ticket.worker => {
                reactor.cancel(ticket.token);
                reactor.notify_driver();
            }
            _ => {
                let shared = &self.inner.workers[ticket.worker];
                let mut pool = shared.pool();
                if !pool.closed {
                    pool.queue.push(Remote::Cancel(ticket.token));
                    drop(pool);
                    shared.wake.notify_one();
                }
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

/// An operation the kernel has, or is about to get.
struct InFlight {
    token: u64,
    /// Whether it went to the disk ring.
    polled: bool,
    cell: CellRef,
    task: TaskRef,
    operation: Operation,
}

/// One worker's ring. Only ever touched from that worker's thread.
struct Reactor {
    ring: RefCell<IoUring>,
    /// The polled ring, if the sidecar has them.
    disk: Option<RefCell<IoUring>>,
    /// In-flight operations, indexed by the low half of their token.
    slots: RefCell<Vec<Option<InFlight>>>,
    /// Leases for slots that are empty.
    free: RefCell<Vec<Lease>>,
    /// Results whose future was busy with its waker when they came in.
    deferred: RefCell<Vec<(u64, i32)>>,
    /// Scratch space for `turn` and `reap`, kept to reuse the allocations.
    remote: RefCell<Vec<Remote>>,
    wakers: RefCell<Vec<Waker>>,
    released: RefCell<Vec<Operation>>,
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
            if let Some(buffers) = &config.buffers {
                buffers.register_on(&ring)?;
            }
            // SAFETY: the descriptor is the ring's, and the ring outlives
            // `readiness`: the driver task owns both and drops `readiness`
            // first.
            let readiness = unsafe {
                AsyncFd::register_with_interest(RingFd(ring.as_raw_fd()), Interest::READABLE)
            }
            .map_err(io::Error::from)?;

            let mut disk_readiness = None;
            let disk = match (&sidecar.disk, &config.buffers) {
                (Some(disk), Some(buffers)) => {
                    let ring = disk.ring()?;
                    buffers.register_on(&ring)?;
                    // SAFETY: as for `readiness`.
                    disk_readiness = Some(
                        unsafe {
                            AsyncFd::register_with_interest(
                                RingFd(ring.as_raw_fd()),
                                Interest::READABLE,
                            )
                        }
                        .map_err(io::Error::from)?,
                    );
                    Some(RefCell::new(ring))
                }
                _ => None,
            };

            let reactor = Rc::new(Reactor {
                ring: RefCell::new(ring),
                disk,
                slots: RefCell::new(Vec::new()),
                free: RefCell::new(Vec::new()),
                deferred: RefCell::new(Vec::new()),
                remote: RefCell::new(Vec::new()),
                wakers: RefCell::new(Vec::new()),
                released: RefCell::new(Vec::new()),
                driver_notified: Cell::new(false),
                hook_seen: Cell::new(false),
                stopped: Cell::new(false),
                sidecar,
                index,
            });
            tokio::task::spawn_worker_local(drive(reactor.clone(), readiness, disk_readiness));
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
            if !self.resolve(token, result, &mut wakers, &mut released) {
                self.deferred.borrow_mut().push((token, result));
                self.notify_driver();
            }
            self.deliver(wakers, released);
        }
        token
    }

    /// Ends the operation `token` with `result`: writes it into the waiting
    /// future, or releases the operation if the future is gone. Returns
    /// `false`, leaving everything in place, if the future is busy.
    ///
    /// Waking the future and dropping an abandoned operation are left to
    /// the caller, to be done with no borrow held.
    fn resolve(
        &self,
        token: u64,
        result: i32,
        wakers: &mut Vec<Waker>,
        released: &mut Vec<Operation>,
    ) -> bool {
        let generation = generation_of(token);
        let finished = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[key_of(token)];
            let in_flight = slot
                .as_ref()
                .expect("completion for an operation that is not in flight");
            debug_assert_eq!(in_flight.token, token);

            // SAFETY: the reactor keeps the pool alive.
            let deliver = match unsafe { in_flight.cell.get() }.claim(generation) {
                Claim::Deliver => true,
                Claim::Abandoned => false,
                Claim::Busy => return false,
            };
            let finished = slot.take().expect("checked above");
            let completion = Completion::new(result, finished.operation);
            if deliver {
                // SAFETY: the claim was granted, so the future is alive and
                // leaves its landing alone until the delivery is complete.
                wakers.extend(unsafe { finished.task.get().deliver(completion) });
            } else {
                released.push(completion.operation);
            }
            finished.cell
        };

        // SAFETY: as above.
        unsafe { finished.get() }.retire(generation);
        self.release(Lease {
            index: key_of(token) as u32,
            cell: finished,
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

    /// Wakes the futures whose results are in and drops the operations
    /// nobody is waiting for. Both run foreign code, so the caller must hold
    /// no borrow.
    fn deliver(&self, mut wakers: Vec<Waker>, mut released: Vec<Operation>) {
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
                    self.notify_driver();
                    return;
                }
                // As in `enter`, there is no safe way to carry on.
                Err(error) => panic!("io_uring_enter failed on the disk ring: {error}"),
            }
        }
    }

    /// Takes in remote work, submits everything queued, and delivers the
    /// completions that are ready.
    fn turn(&self) {
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
                    // SAFETY: the reactor keeps the pool alive.
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
            }
        }
        self.remote.replace(remote);
        self.driver_notified.set(false);

        loop {
            self.enter();
            self.kick_disk();
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
        let mut deferred = self.deferred.take();

        deferred
            .retain(|&(token, result)| !self.resolve(token, result, &mut wakers, &mut released));
        for ring in std::iter::once(&self.ring).chain(&self.disk) {
            for cqe in ring.borrow_mut().completion() {
                let token = cqe.user_data();
                if token != DETACHED
                    && !self.resolve(token, cqe.result(), &mut wakers, &mut released)
                {
                    deferred.push((token, cqe.result()));
                }
            }
        }
        // A future only keeps its waker locked for a moment: come back soon.
        if !deferred.is_empty() {
            self.notify_driver();
        }
        self.deferred.replace(deferred);
        self.deliver(wakers, released);
    }
}

impl Drop for Reactor {
    fn drop(&mut self) {
        let unsent = {
            let mut pool = self.shared().pool();
            pool.closed = true;
            std::mem::take(&mut pool.queue)
        };
        drop(unsent);
        // Closing the ring does not wait for in-flight operations to stop,
        // so the kernel may write into their buffers after this point. They
        // are leaked rather than freed.
        std::mem::forget(std::mem::take(self.slots.get_mut()));
    }
}

fn token_of(generation: u32, index: u32) -> u64 {
    u64::from(generation) << 32 | u64::from(index)
}

fn key_of(token: u64) -> usize {
    (token & u64::from(u32::MAX)) as usize
}

fn generation_of(token: u64) -> u32 {
    (token >> 32) as u32
}

/// The ring's descriptor, borrowed for readiness notifications. The ring
/// itself is owned by the [`Reactor`].
struct RingFd(RawFd);

impl AsRawFd for RingFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Arguments drop in reverse order, so the rings are deregistered from
/// tokio's driver while they are still open.
async fn drive(
    reactor: Rc<Reactor>,
    readiness: AsyncFd<RingFd>,
    disk_readiness: Option<AsyncFd<RingFd>>,
) {
    while !reactor.stopped.get() {
        reactor.turn();
        tokio::select! {
            biased;
            () = reactor.shared().wake.notified() => {}
            () = completions(Some(&readiness)) => {}
            () = completions(disk_readiness.as_ref()) => {}
        }
    }
}

/// Waits until `ring` has completions to reap. Never returns for `None`.
async fn completions(ring: Option<&AsyncFd<RingFd>>) {
    let Some(ring) = ring else {
        return std::future::pending().await;
    };
    match ring.readable().await {
        // Cleared before the next turn reaps, so a completion that lands in
        // between raises readiness again.
        Ok(mut guard) => guard.clear_ready(),
        Err(error) => panic!("ring readiness failed: {error}"),
    }
}
