use std::ffi::CString;
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use io_uring::{opcode, squeue, types};
use tokio::sync::oneshot;

use crate::buffers::FixedBuf;
use crate::landing::{Landing, TaskRef};
use crate::ops::{Reclaimed, Segment};
use crate::reactor::{MultiRing, Ticket};

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
    ReadFixedAt {
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        /// The most bytes to read.
        limit: usize,
        /// Whether it runs on the polled disk ring.
        polled: bool,
    },
    WriteFixedAt {
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        /// Whether it runs on the polled disk ring.
        polled: bool,
    },
    Accept {
        listener: Arc<OwnedFd>,
        /// The new connection, once the operation has succeeded.
        accepted: Option<OwnedFd>,
    },
    Recv {
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        /// The most bytes to take.
        limit: usize,
    },
    Send {
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        /// Where in the contents to start.
        start: usize,
    },
    SendBytes {
        fd: Arc<OwnedFd>,
        buf: Bytes,
    },
    SendBytesZc {
        fd: Arc<OwnedFd>,
        buf: Bytes,
        /// Whether the kernel reported copying the data after all.
        copied: bool,
        /// Fires once the kernel has let go of `buf`.
        reclaim: oneshot::Sender<bool>,
    },
    SendMsg {
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        message: Message,
    },
    RecvMsg {
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        message: Message,
    },
    /// A send that the kernel completes twice: once with the result, and
    /// once more when it no longer refers to the buffers.
    SendMsgZc {
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        message: Message,
        /// The registered buffer that all of the message lies in, if it is
        /// to be sent as one.
        registered: Option<u16>,
        /// Whether the kernel reported copying the data after all.
        copied: bool,
        /// Where the buffers go when the kernel is done with them.
        reclaim: oneshot::Sender<Reclaimed>,
    },
    /// What a completion carries in place of an operation that the reactor
    /// keeps, because the kernel is not done with it.
    Retained,
    Shutdown {
        fd: Arc<OwnedFd>,
        how: i32,
    },
    OpenAt {
        dir: Arc<OwnedFd>,
        path: CString,
        flags: i32,
        mode: u32,
        /// The new descriptor, once the operation has succeeded.
        opened: Option<OwnedFd>,
    },
    /// With an empty `path`, about `dir` itself.
    Statx {
        dir: Arc<OwnedFd>,
        path: CString,
        stat: Box<libc::statx>,
    },
    RenameAt {
        dir: Arc<OwnedFd>,
        from: CString,
        to: CString,
    },
    UnlinkAt {
        dir: Arc<OwnedFd>,
        path: CString,
        flags: i32,
    },
    MkdirAt {
        dir: Arc<OwnedFd>,
        path: CString,
        mode: u32,
    },
    Fdatasync {
        fd: Arc<OwnedFd>,
    },
    Truncate {
        fd: Arc<OwnedFd>,
        len: u64,
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
            Operation::ReadFixedAt {
                fd,
                buf,
                offset,
                limit,
                ..
            } => opcode::ReadFixed::new(
                types::Fd(fd.as_raw_fd()),
                buf.spare_ptr(),
                io_len(buf.spare_len().min(*limit)),
                buf.registered_index(),
            )
            .offset(*offset)
            .build(),
            Operation::WriteFixedAt {
                fd, buf, offset, ..
            } => opcode::WriteFixed::new(
                types::Fd(fd.as_raw_fd()),
                buf.as_ptr(),
                io_len(buf.len()),
                buf.registered_index(),
            )
            .offset(*offset)
            .build(),
            Operation::Accept { listener, .. } => opcode::Accept::new(
                types::Fd(listener.as_raw_fd()),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
            .flags(libc::SOCK_CLOEXEC)
            .build(),
            Operation::Recv { fd, buf, limit } => opcode::Recv::new(
                types::Fd(fd.as_raw_fd()),
                buf.spare_ptr(),
                io_len(buf.spare_len().min(*limit)),
            )
            .build(),
            // WAITALL has the kernel keep sending until the whole buffer is
            // out, where it would otherwise stop at a full socket buffer.
            Operation::Send { fd, buf, start } => {
                let rest = &buf[*start..];
                opcode::Send::new(types::Fd(fd.as_raw_fd()), rest.as_ptr(), io_len(rest.len()))
                    .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
                    .build()
            }
            Operation::SendBytes { fd, buf } => {
                opcode::Send::new(types::Fd(fd.as_raw_fd()), buf.as_ptr(), io_len(buf.len()))
                    .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
                    .build()
            }
            Operation::SendBytesZc { fd, buf, .. } => {
                opcode::SendZc::new(types::Fd(fd.as_raw_fd()), buf.as_ptr(), io_len(buf.len()))
                    .flags(libc::MSG_NOSIGNAL | libc::MSG_WAITALL)
                    .zc_flags(SEND_ZC_REPORT_USAGE)
                    .build()
            }
            Operation::SendMsg { fd, message, .. } => {
                opcode::SendMsg::new(types::Fd(fd.as_raw_fd()), &raw const *message.header)
                    .flags((libc::MSG_NOSIGNAL | libc::MSG_WAITALL) as u32)
                    .build()
            }
            Operation::RecvMsg { fd, message, .. } => {
                opcode::RecvMsg::new(types::Fd(fd.as_raw_fd()), &raw mut *message.header)
                    .flags(libc::MSG_WAITALL as u32)
                    .build()
            }
            Operation::SendMsgZc {
                fd,
                message,
                registered,
                ..
            } => opcode::SendMsgZc::new(types::Fd(fd.as_raw_fd()), &raw const *message.header)
                .flags((libc::MSG_NOSIGNAL | libc::MSG_WAITALL) as u32)
                .ioprio(SEND_ZC_REPORT_USAGE)
                .buf_index(*registered)
                .build(),
            Operation::Retained => unreachable!("never submitted"),
            Operation::Shutdown { fd, how } => {
                opcode::Shutdown::new(types::Fd(fd.as_raw_fd()), *how).build()
            }
            Operation::OpenAt {
                dir,
                path,
                flags,
                mode,
                ..
            } => opcode::OpenAt::new(types::Fd(dir.as_raw_fd()), path.as_ptr())
                .flags(*flags | libc::O_CLOEXEC)
                .mode(*mode)
                .build(),
            Operation::Statx { dir, path, stat } => opcode::Statx::new(
                types::Fd(dir.as_raw_fd()),
                path.as_ptr(),
                std::ptr::from_mut::<libc::statx>(stat).cast(),
            )
            .flags(libc::AT_EMPTY_PATH)
            .mask(libc::STATX_TYPE | libc::STATX_SIZE | libc::STATX_MTIME)
            .build(),
            Operation::RenameAt { dir, from, to } => {
                let dir = types::Fd(dir.as_raw_fd());
                opcode::RenameAt::new(dir, from.as_ptr(), dir, to.as_ptr()).build()
            }
            Operation::UnlinkAt { dir, path, flags } => {
                opcode::UnlinkAt::new(types::Fd(dir.as_raw_fd()), path.as_ptr())
                    .flags(*flags)
                    .build()
            }
            Operation::MkdirAt { dir, path, mode } => {
                opcode::MkDirAt::new(types::Fd(dir.as_raw_fd()), path.as_ptr())
                    .mode(*mode)
                    .build()
            }
            Operation::Fdatasync { fd } => opcode::Fsync::new(types::Fd(fd.as_raw_fd()))
                .flags(types::FsyncFlags::DATASYNC)
                .build(),
            Operation::Truncate { fd, len } => {
                opcode::Ftruncate::new(types::Fd(fd.as_raw_fd()), *len).build()
            }
        }
    }
}

/// Has the kernel say, when it lets go of a zero-copy send's buffers,
/// whether it copied the data.
const SEND_ZC_REPORT_USAGE: u16 = 1 << 3;

/// Set in what the kernel says then, if it did.
const NOTIF_USAGE_ZC_COPIED: u32 = 1 << 31;

/// The most regions one vectored operation can name.
const MAX_SEGMENTS: usize = 1024;

/// What `sendmsg` and `recvmsg` take: a list of memory regions, here ones
/// inside buffers that the operation owns.
pub(crate) struct Message {
    header: Box<libc::msghdr>,
    /// What `header` points at.
    _regions: Vec<libc::iovec>,
}

// SAFETY: the pointers lead to the heap allocation of `_regions` and into
// pool buffers, both owned by the operation the message is part of.
unsafe impl Send for Message {}

impl Message {
    /// Names the regions `segments` of `bufs`. With `sending`, they must lie
    /// within the buffers' contents, otherwise within their capacity.
    ///
    /// # Panics
    ///
    /// Panics if a segment does not.
    pub(crate) fn new(bufs: &[FixedBuf], segments: &[Segment], sending: bool) -> Message {
        assert!(segments.len() <= MAX_SEGMENTS, "too many segments");
        let mut regions: Vec<libc::iovec> = segments
            .iter()
            .map(|segment| {
                let buf = &bufs[segment.buf];
                let bound = if sending { buf.len() } else { buf.capacity() };
                let end = segment.start.checked_add(segment.len);
                assert!(end.is_some_and(|end| end <= bound), "segment out of bounds");
                libc::iovec {
                    // SAFETY: the segment lies within the buffer.
                    iov_base: unsafe { buf.ptr().add(segment.start) }.cast(),
                    iov_len: segment.len,
                }
            })
            .collect();

        // SAFETY: `msghdr` is plain data, for which all zeroes is valid.
        let mut header: Box<libc::msghdr> = Box::new(unsafe { std::mem::zeroed() });
        header.msg_iov = regions.as_mut_ptr();
        header.msg_iovlen = regions.len();
        Message {
            header,
            _regions: regions,
        }
    }
}

impl Operation {
    /// Whether the kernel completes the operation a second time, when it
    /// lets go of the operation's buffers.
    pub(crate) fn completes_twice(&self) -> bool {
        matches!(
            self,
            Operation::SendMsgZc { .. } | Operation::SendBytesZc { .. }
        )
    }

    /// Records what the kernel said in the second completion.
    pub(crate) fn note_release(&mut self, notice: i32) {
        let is_copied = notice as u32 & NOTIF_USAGE_ZC_COPIED != 0;
        match self {
            Operation::SendMsgZc { copied, .. } | Operation::SendBytesZc { copied, .. } => {
                *copied = is_copied;
            }
            _ => {}
        }
    }

    /// Disposes of an operation that the kernel is done with and that no
    /// future is going to collect. Runs foreign code: buffers go back to
    /// their pool or on to whoever waits for them.
    pub(crate) fn release(self) {
        match self {
            Operation::SendMsgZc {
                bufs,
                copied,
                reclaim,
                ..
            } => {
                // With nobody waiting, the buffers return to their pool.
                let _ = reclaim.send(Reclaimed { bufs, copied });
            }
            Operation::SendBytesZc {
                buf,
                copied,
                reclaim,
                ..
            } => {
                drop(buf);
                let _ = reclaim.send(copied);
            }
            _ => {}
        }
    }

    /// Whether the operation belongs on the polled disk ring.
    pub(crate) fn polled(&self) -> bool {
        matches!(
            self,
            Operation::ReadFixedAt { polled: true, .. }
                | Operation::WriteFixedAt { polled: true, .. }
        )
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
    /// Returns an operation that was never submitted to the kernel.
    pub(crate) fn cancelled(operation: Operation) -> Self {
        Self::new(-libc::ECANCELED, operation)
    }

    pub(crate) fn new(result: i32, mut operation: Operation) -> Self {
        let new_fd = match &mut operation {
            Operation::Accept { accepted, .. } => Some(accepted),
            Operation::OpenAt { opened, .. } => Some(opened),
            _ => None,
        };
        if let (Some(new_fd), 0..) = (new_fd, result) {
            // SAFETY: a successful accept or open returns a new descriptor
            // that nothing else owns. Taking it here, before anyone can
            // drop the completion, is what keeps it from leaking.
            *new_fd = Some(unsafe { OwnedFd::from_raw_fd(result) });
        }
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
    multiring: MultiRing,
    progress: Progress,
    landing: Landing,
}

impl Op {
    pub(crate) fn new(multiring: &MultiRing, operation: Operation) -> Self {
        Self {
            multiring: multiring.clone(),
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
                let ticket = this
                    .multiring
                    .submit(operation, TaskRef::new(&this.landing));
                match ticket {
                    Ok(ticket) => {
                        this.progress = Progress::InFlight(ticket);
                        Poll::Pending
                    }
                    Err(operation) => Poll::Ready(Completion::cancelled(operation)),
                }
            }
            Progress::InFlight(ticket) => {
                // SAFETY: this future owns the landing.
                if let Some(completion) = unsafe { this.landing.take() } {
                    return Poll::Ready(completion);
                }
                this.progress = Progress::InFlight(ticket);

                let arbiter = this.multiring.arbiter(&ticket);
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
        let arbiter = self.multiring.arbiter(&ticket);
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
        self.multiring.cancel(ticket);
    }
}
