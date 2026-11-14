use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use io_uring::{opcode, squeue, types};

use crate::reactor::{Sidecar, Ticket};

/// An operation and everything the kernel needs kept alive while it runs.
///
/// While in flight the value lives in its reactor's slab, which moves its
/// entries when it grows. A submission may therefore point at heap memory
/// the operation owns, but never at the operation value itself.
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
pub(crate) struct Op {
    sidecar: Sidecar,
    progress: Progress,
}

impl Op {
    pub(crate) fn new(sidecar: &Sidecar, operation: Operation) -> Self {
        Self {
            sidecar: sidecar.clone(),
            progress: Progress::Idle(operation),
        }
    }
}

impl Future for Op {
    type Output = Completion;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Completion> {
        let this = &mut *self;
        match std::mem::replace(&mut this.progress, Progress::Finished) {
            Progress::Idle(operation) => {
                let ticket = this.sidecar.submit(operation, cx.waker());
                this.progress = Progress::InFlight(ticket);
                Poll::Pending
            }
            Progress::InFlight(ticket) => match this.sidecar.poll_completion(ticket, cx) {
                Poll::Ready(completion) => Poll::Ready(completion),
                Poll::Pending => {
                    this.progress = Progress::InFlight(ticket);
                    Poll::Pending
                }
            },
            Progress::Finished => panic!("operation polled after completion"),
        }
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        if let Progress::InFlight(ticket) = self.progress {
            self.sidecar.abandon(ticket);
        }
    }
}
