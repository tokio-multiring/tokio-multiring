use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use io_uring::{opcode, squeue, types};

use crate::op::{Op, Operation};
use crate::reactor::Sidecar;

impl Sidecar {
    /// Completes after one round trip through a ring, doing no I/O.
    pub fn nop(&self) -> impl Future<Output = io::Result<()>> + Send + use<> {
        Op::new(self, Nop)
    }

    /// Reads from `fd` at `offset` into the spare capacity of `buf`,
    /// extending its length by the number of bytes read.
    ///
    /// The buffer is handed back whether or not the read succeeds. If the
    /// future is dropped before it completes, the read is cancelled and the
    /// buffer is freed once the kernel is done with it.
    pub fn read_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send + use<> {
        Op::new(self, ReadAt { fd, buf, offset })
    }

    /// Writes the contents of `buf` to `fd` at `offset`. A short write is
    /// reported as is, not retried.
    ///
    /// The buffer is handed back whether or not the write succeeds.
    pub fn write_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: Vec<u8>,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, Vec<u8>)> + Send + use<> {
        Op::new(self, WriteAt { fd, buf, offset })
    }
}

/// io_uring lengths are 32 bits wide.
fn io_len(len: usize) -> u32 {
    len.min(u32::MAX as usize) as u32
}

struct Nop;

impl Operation for Nop {
    type Output = io::Result<()>;

    fn entry(&mut self) -> squeue::Entry {
        opcode::Nop::new().build()
    }

    fn finish(self, result: io::Result<u32>) -> Self::Output {
        result.map(drop)
    }
}

struct ReadAt {
    fd: Arc<OwnedFd>,
    buf: Vec<u8>,
    offset: u64,
}

impl Operation for ReadAt {
    type Output = (io::Result<usize>, Vec<u8>);

    fn entry(&mut self) -> squeue::Entry {
        let spare = self.buf.spare_capacity_mut();
        opcode::Read::new(
            types::Fd(self.fd.as_raw_fd()),
            spare.as_mut_ptr().cast(),
            io_len(spare.len()),
        )
        .offset(self.offset)
        .build()
    }

    fn finish(mut self, result: io::Result<u32>) -> Self::Output {
        let result = result.map(|read| {
            let read = read as usize;
            // SAFETY: the kernel initialized `read` bytes of the spare
            // capacity the entry pointed at.
            unsafe { self.buf.set_len(self.buf.len() + read) };
            read
        });
        (result, self.buf)
    }
}

struct WriteAt {
    fd: Arc<OwnedFd>,
    buf: Vec<u8>,
    offset: u64,
}

impl Operation for WriteAt {
    type Output = (io::Result<usize>, Vec<u8>);

    fn entry(&mut self) -> squeue::Entry {
        opcode::Write::new(
            types::Fd(self.fd.as_raw_fd()),
            self.buf.as_ptr(),
            io_len(self.buf.len()),
        )
        .offset(self.offset)
        .build()
    }

    fn finish(self, result: io::Result<u32>) -> Self::Output {
        (result.map(|written| written as usize), self.buf)
    }
}
