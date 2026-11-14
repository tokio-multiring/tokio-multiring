use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use crate::buffers::FixedBuf;
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

    /// Reads from `fd` at `offset` into the spare capacity of `buf`, like
    /// [`read_at`](Self::read_at), but without the kernel having to map and
    /// pin the buffer for the operation.
    ///
    /// `buf` must come from the pool in [`Config::buffers`]. With an
    /// `O_DIRECT` file, the buffer's contents so far, the offset and the
    /// buffer size must all be multiples of the device's block size; an
    /// empty buffer always is.
    ///
    /// The buffer is handed back whether or not the read succeeds. If the
    /// future is dropped before it completes, the read is cancelled and the
    /// buffer returns to its pool once the kernel is done with it.
    ///
    /// [`Config::buffers`]: crate::Config::buffers
    pub fn read_fixed_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = match self.check_registered(&buf) {
            Ok(()) => Ok(Op::new(self, Operation::ReadFixedAt { fd, buf, offset })),
            Err(error) => Err((error, buf)),
        };
        async move {
            let completion = match op {
                Ok(op) => op.await,
                Err((error, buf)) => return (Err(error), buf),
            };
            let Operation::ReadFixedAt { mut buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            let result = completion.result.map(|read| {
                let read = read as usize;
                buf.advance(read);
                read
            });
            (result, buf)
        }
    }

    /// Writes the contents of `buf` to `fd` at `offset`, like
    /// [`write_at`](Self::write_at), but without the kernel having to map
    /// and pin the buffer for the operation. A short write is reported as
    /// is, not retried.
    ///
    /// `buf` must come from the pool in [`Config::buffers`]. With an
    /// `O_DIRECT` file, the length of the contents and the offset must be
    /// multiples of the device's block size.
    ///
    /// The buffer is handed back whether or not the write succeeds.
    ///
    /// [`Config::buffers`]: crate::Config::buffers
    pub fn write_fixed_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = match self.check_registered(&buf) {
            Ok(()) => Ok(Op::new(self, Operation::WriteFixedAt { fd, buf, offset })),
            Err(error) => Err((error, buf)),
        };
        async move {
            let completion = match op {
                Ok(op) => op.await,
                Err((error, buf)) => return (Err(error), buf),
            };
            let Operation::WriteFixedAt { buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|written| written as usize), buf)
        }
    }

    /// Waits for a connection on `listener` and returns it, in blocking
    /// mode and with close-on-exec set.
    ///
    /// Sockets used with a sidecar must be in blocking mode: on a
    /// non-blocking one the kernel reports `WouldBlock` where it would
    /// otherwise wait. Listeners and streams from `std` are fine as they
    /// come; ones from tokio are not.
    ///
    /// If the future is dropped, a connection that had already been
    /// accepted for it is closed.
    pub fn accept(
        &self,
        listener: Arc<OwnedFd>,
    ) -> impl Future<Output = io::Result<OwnedFd>> + Send + use<> {
        let op = Op::new(
            self,
            Operation::Accept {
                listener,
                accepted: None,
            },
        );
        async move {
            let completion = op.await;
            completion.result?;
            let Operation::Accept { accepted, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            Ok(accepted.expect("a successful accept carries its connection"))
        }
    }

    /// Receives from the socket `fd` into the spare capacity of `buf`,
    /// extending its contents by the number of bytes received. Completes as
    /// soon as any bytes have arrived; `Ok(0)` means the peer has shut down
    /// its side, or that `buf` had no spare capacity.
    ///
    /// The buffer is handed back whether or not the receive succeeds. If the
    /// future is dropped before it completes, the receive is cancelled and
    /// the buffer returns to its pool once the kernel is done with it. Bytes
    /// that arrived in between are lost with it.
    pub fn recv(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = Op::new(self, Operation::Recv { fd, buf });
        async move {
            let completion = op.await;
            let Operation::Recv { mut buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            let result = completion.result.map(|received| {
                let received = received as usize;
                buf.advance(received);
                received
            });
            (result, buf)
        }
    }

    /// Sends the contents of `buf` on the socket `fd`. The kernel keeps
    /// going until everything is sent, so a count short of the contents
    /// means the connection failed part of the way through.
    ///
    /// A peer that has gone away is reported as `BrokenPipe`, without
    /// raising `SIGPIPE`. The buffer is handed back whether or not the send
    /// succeeds.
    pub fn send(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = Op::new(self, Operation::Send { fd, buf });
        async move {
            let completion = op.await;
            let Operation::Send { buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|sent| sent as usize), buf)
        }
    }

    /// A buffer's registered index only means something on rings that have
    /// its pool.
    fn check_registered(&self, buf: &FixedBuf) -> io::Result<()> {
        if self.buffers().is_some_and(|pool| pool.owns(buf)) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "buffer is not from the pool registered on this sidecar",
            ))
        }
    }
}
