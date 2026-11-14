use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use io_uring::{opcode, squeue, types};

use crate::landing::{Landing, TaskRef};
use crate::reactor::{Sidecar, Ticket};

/// An operation and everything the kernel needs kept alive while it runs.
///
/// The value moves while in flight: into its reactor's slots, which are
/// reallocated as they grow, and from there into the future. A submission
/// may therefore point at heap memory the operation owns, but never at the
/// operation value itself.
pub(crate) enum Operation {
    Nop,
    ReadAt {
        fd: Arc<OwnedFd>,
        buf: Vec<u8>,
        offset: u64,
    },
    WriteAt {
        fd: Arc<OwnedFd>,
        buf: Vec<u8>,
        offset: u64,
    },
}

impl Operation {
    pub(crate) fn entry(&mut self) -> squeue::Entry {
        match self {
            Operation::Nop => opcode::Nop::new().build(),
            Operation::ReadAt { fd, buf, offset } => {
                let spare = buf.spare_capacity_mut();
                opcode::Read::new(
                    types::Fd(fd.as_raw_fd()),
                    spare.as_mut_ptr().cast(),
                    io_len(spare.len()),
                )
                .offset(*offset)
                .build()
            }
            Operation::WriteAt { fd, buf, offset } => {
                opcode::Write::new(types::Fd(fd.as_raw_fd()), buf.as_ptr(), io_len(buf.len()))
                    .offset(*offset)
                    .build()
            }
        }
    }
}

/// io_uring lengths are 32 bits wide.
fn io_len(len: usize) -> u32 {
    len.min(u32::MAX as usize) as u32
}

/// A finished operation, handed back with the kernel's result.
pub(crate) struct Completion {
    pub(crate) result: io::Result<u32>,
    pub(crate) operation: Operation,
}

impl Completion {
    pub(crate) fn new(result: i32, operation: Operation) -> Self {
        let result = if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Ok(result as u32)
        };
        Self { result, operation }
    }
}

enum Progress {
    Idle(Operation),
    InFlight(Ticket),
    Finished,
}

/// Future of a single operation.
///
/// Once polled, the reactor running the operation holds a pointer to
/// `landing`. `Landing` is `!Unpin`, which pins this future in place, and
/// `Drop` makes sure the reactor has let go before the memory is released.
pub(crate) struct Op {
    sidecar: Sidecar,
    progress: Progress,
    landing: Landing,
}

impl Op {
    pub(crate) fn new(sidecar: &Sidecar, operation: Operation) -> Self {
        Self {
            sidecar: sidecar.clone(),
            progress: Progress::Idle(operation),
            landing: Landing::new(),
        }
    }
}

impl Future for Op {
    type Output = Completion;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completion> {
        // SAFETY: nothing is moved out of `this` but `progress`, which
        // nobody points at. The landing is only used through shared
        // references.
        let this = unsafe { self.get_unchecked_mut() };
        match std::mem::replace(&mut this.progress, Progress::Finished) {
            Progress::Idle(operation) => {
                // SAFETY: no reactor knows about the landing yet.
                unsafe { this.landing.set_waker(cx.waker()) };
                let ticket = this.sidecar.submit(operation, TaskRef::new(&this.landing));
                this.progress = Progress::InFlight(ticket);
                Poll::Pending
            }
            Progress::InFlight(ticket) => {
                // SAFETY: this future owns the landing.
                if let Some(completion) = unsafe { this.landing.take() } {
                    return Poll::Ready(completion);
                }
                this.progress = Progress::InFlight(ticket);

                let arbiter = this.sidecar.arbiter(&ticket);
                if arbiter.lock_waker(ticket.generation()) {
                    // SAFETY: the waker lock is held.
                    unsafe { this.landing.set_waker(cx.waker()) };
                    arbiter.unlock_waker(ticket.generation());
                } else {
                    // The reactor is delivering right now. It wakes the
                    // waker it finds, which may not be this one.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
            Progress::Finished => panic!("operation polled after completion"),
        }
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        let Progress::InFlight(ticket) = self.progress else {
            return;
        };
        let arbiter = self.sidecar.arbiter(&ticket);
        let mut spins = 0u32;
        loop {
            if self.landing.is_done() {
                return;
            }
            if arbiter.abandon(ticket.generation()) {
                break;
            }
            // The reactor is in the middle of writing the result here. That
            // is a handful of instructions, and this memory cannot be
            // released until they are done.
            spins += 1;
            if spins % 64 == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
        // The kernel may be using the operation's buffers. They stay with
        // the reactor, which is asked to cut the operation short.
        self.sidecar.cancel(ticket);
    }
}
