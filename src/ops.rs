use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use crate::op::{Op, Operation};
use crate::reactor::Sidecar;

impl Sidecar {
    /// Completes after one round trip through a ring, doing no I/O.
    pub fn nop(&self) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let op = Op::new(self, Operation::Nop);
        async move { op.await.result.map(drop) }
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
        let op = Op::new(self, Operation::ReadAt { fd, buf, offset });
        async move {
            let completion = op.await;
            let Operation::ReadAt { mut buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            let result = completion.result.map(|read| {
                let read = read as usize;
                // SAFETY: the kernel initialized `read` bytes of the spare
                // capacity the submission pointed at.
                unsafe { buf.set_len(buf.len() + read) };
                read
            });
            (result, buf)
        }
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
        let op = Op::new(self, Operation::WriteAt { fd, buf, offset });
        async move {
            let completion = op.await;
            let Operation::WriteAt { buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|written| written as usize), buf)
        }
    }
}
