//! Arbitration between an operation's future and the reactor running it.
//!
//! The reactor keeps a pointer to a [`Landing`] inside the pinned future and
//! writes the result straight into it. The future can be polled or dropped
//! on any thread, so both sides go through an [`Arbiter`] before touching
//! the landing. The arbiter is not part of the future: it lives in its
//! worker's pool, which outlives both sides, so it can be consulted even
//! when the other side is gone.
//!
//! | Stage      | Entered by                  | Landing may be touched by |
//! |------------|-----------------------------|---------------------------|
//! | WAITING    | `arm`, `unlock_waker`       | nobody                    |
//! | POLLING    | future, `lock_waker`        | future (waker only)       |
//! | DELIVERING | reactor, `claim`            | reactor                   |
//! | ABANDONED  | future's drop, `abandon`    | future                    |
//!
//! All four are left only from WAITING, by compare-and-swap, so exactly one
//! side wins. Delivery ends with the landing's `done` flag, the reactor's
//! last access to the future's memory; from then on the future owns the
//! landing outright and never looks at the arbiter again.
//!
//! The reactor recycles an arbiter as soon as it has delivered or seen the
//! abandonment. Every word carries a generation, so a future that is late
//! to the arbiter cannot act on the operation that has taken its place.

use std::cell::UnsafeCell;
use std::marker::PhantomPinned;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Waker;

use crate::op::Completion;

const FREE: u64 = 0;
const WAITING: u64 = 1;
const POLLING: u64 = 2;
const DELIVERING: u64 = 3;
const ABANDONED: u64 = 4;

fn word(generation: u32, stage: u64) -> u64 {
    u64::from(generation) << 8 | stage
}

/// What the reactor may do with a finished operation.
pub(crate) enum Claim {
    /// The future is waiting: write the result into its landing.
    Deliver,
    /// The future is updating its waker. Try again shortly.
    Busy,
    /// The future is gone. Its landing must not be touched.
    Abandoned,
}

pub(crate) struct Arbiter(AtomicU64);

impl Arbiter {
    pub(crate) const fn new() -> Self {
        Self(AtomicU64::new(FREE))
    }

    /// Starts a new operation on a free arbiter and returns its generation.
    /// Only the holder of the arbiter's lease may call this.
    pub(crate) fn arm(&self) -> u32 {
        let generation = (self.0.load(Ordering::Relaxed) >> 8) as u32;
        self.0.store(word(generation, WAITING), Ordering::Release);
        generation
    }

    fn transition(&self, generation: u32, from: u64, to: u64) -> Result<(), u64> {
        self.0
            .compare_exchange(
                word(generation, from),
                word(generation, to),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(drop)
    }

    /// Future: takes exclusive access to the landing's waker. Fails if the
    /// reactor is delivering or has already delivered.
    pub(crate) fn lock_waker(&self, generation: u32) -> bool {
        self.transition(generation, WAITING, POLLING).is_ok()
    }

    pub(crate) fn unlock_waker(&self, generation: u32) {
        self.0.store(word(generation, WAITING), Ordering::Release);
    }

    /// Reactor: decides what happens to a finished operation.
    pub(crate) fn claim(&self, generation: u32) -> Claim {
        match self.transition(generation, WAITING, DELIVERING) {
            Ok(()) => Claim::Deliver,
            Err(current) if current == word(generation, POLLING) => Claim::Busy,
            Err(current) if current == word(generation, ABANDONED) => Claim::Abandoned,
            Err(current) => unreachable!("arbiter in state {current:#x} at completion"),
        }
    }

    /// Future's drop: tells the reactor to keep its hands off the landing.
    /// Fails if the reactor is delivering or has already delivered, in which
    /// case the landing's `done` flag is, or is about to be, set.
    pub(crate) fn abandon(&self, generation: u32) -> bool {
        self.transition(generation, WAITING, ABANDONED).is_ok()
    }

    pub(crate) fn is_abandoned(&self, generation: u32) -> bool {
        self.0.load(Ordering::Acquire) == word(generation, ABANDONED)
    }

    /// Reactor: ends the operation, making the arbiter free for the next.
    pub(crate) fn retire(&self, generation: u32) {
        self.0
            .store(word(generation.wrapping_add(1), FREE), Ordering::Release);
    }
}

/// The part of a future the reactor writes into. Never moves once the
/// future has been polled.
pub(crate) struct Landing {
    done: AtomicBool,
    waker: UnsafeCell<Option<Waker>>,
    completion: UnsafeCell<Option<Completion>>,
    _pinned: PhantomPinned,
}

impl Landing {
    pub(crate) fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            waker: UnsafeCell::new(None),
            completion: UnsafeCell::new(None),
            _pinned: PhantomPinned,
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Registers the waker to call on delivery.
    ///
    /// # Safety
    ///
    /// The caller must have exclusive access to the waker: either no reactor
    /// knows about this landing yet, or the caller holds the waker lock.
    pub(crate) unsafe fn set_waker(&self, waker: &Waker) {
        // SAFETY: exclusive access is the caller's obligation.
        let current = unsafe { &mut *self.waker.get() };
        if !current
            .as_ref()
            .is_some_and(|current| current.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }

    /// Stores the result and returns the waker to call.
    ///
    /// # Safety
    ///
    /// The caller must have claimed the operation with [`Claim::Deliver`].
    /// It must not touch the landing again after this returns.
    pub(crate) unsafe fn deliver(&self, completion: Completion) -> Option<Waker> {
        // SAFETY: the claim excludes the future from both cells until `done`
        // is set, and keeps it from freeing the landing.
        let waker = unsafe {
            *self.completion.get() = Some(completion);
            (*self.waker.get()).take()
        };
        self.done.store(true, Ordering::Release);
        waker
    }

    /// Takes the result, if it has been delivered.
    ///
    /// # Safety
    ///
    /// Only the future that owns the landing may call this.
    pub(crate) unsafe fn take(&self) -> Option<Completion> {
        if !self.is_done() {
            return None;
        }
        // SAFETY: the reactor is done with the landing, and the caller is
        // its only other user.
        unsafe { (*self.completion.get()).take() }
    }
}

/// Pointer to an arbiter in a worker's pool. Valid for as long as the
/// sidecar it came from is alive.
#[derive(Clone, Copy)]
pub(crate) struct CellRef(NonNull<Arbiter>);

// SAFETY: an arbiter is a single atomic, and the pool that owns it outlives
// every holder of the pointer.
unsafe impl Send for CellRef {}

impl CellRef {
    pub(crate) fn new(arbiter: NonNull<Arbiter>) -> Self {
        Self(arbiter)
    }

    /// # Safety
    ///
    /// The sidecar this came from must be alive for `'a`.
    pub(crate) unsafe fn get<'a>(self) -> &'a Arbiter {
        // SAFETY: the caller's obligation.
        unsafe { self.0.as_ref() }
    }
}

/// Pointer to the landing of a future that has been polled.
#[derive(Clone, Copy)]
pub(crate) struct TaskRef(NonNull<Landing>);

// SAFETY: every access through the pointer is serialized with the owning
// future by the arbiter protocol, and what is exchanged (a waker, an
// operation and its result) is `Send`.
unsafe impl Send for TaskRef {}

impl TaskRef {
    pub(crate) fn new(landing: &Landing) -> Self {
        Self(NonNull::from(landing))
    }

    /// # Safety
    ///
    /// The landing must be alive for `'a`: the caller holds a
    /// [`Claim::Deliver`] on its operation.
    pub(crate) unsafe fn get<'a>(self) -> &'a Landing {
        // SAFETY: the caller's obligation.
        unsafe { self.0.as_ref() }
    }
}
