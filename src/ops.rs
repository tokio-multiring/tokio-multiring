use std::ffi::CString;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use crate::buffers::FixedBuf;
use crate::op::{Message, Op, Operation};
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

    /// Reads up to `limit` bytes from `fd` at `offset` into the spare
    /// capacity of `buf`, like [`read_at`](Self::read_at), but without the
    /// kernel having to map and pin the buffer for the operation.
    ///
    /// `buf` must come from the pool in [`Config::buffers`]. With an
    /// `O_DIRECT` file, the buffer's contents so far, the offset and the
    /// length asked for must all be multiples of the device's block size.
    /// Asking for much more than the file holds is wasteful there: the
    /// kernel fills what lies past the end of the file with zeroes before
    /// it reports the shorter count.
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
        limit: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        self.read_fixed(fd, buf, offset, limit, false)
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
        self.write_fixed(fd, buf, offset, false)
    }

    /// Like [`read_fixed_at`](Self::read_fixed_at), on the worker's polled
    /// disk ring: submitting takes no system call and the device raises no
    /// interrupt. Needs [`Config::disk`].
    ///
    /// `fd` must be open with `O_DIRECT`, with everything aligned as that
    /// requires; the kernel refuses anything else. On a device without
    /// polling support the operation still works, completed by interrupt.
    ///
    /// A polled operation cannot be cancelled once the kernel has it. If the
    /// future is dropped the read runs to its end, and the buffer returns to
    /// its pool then.
    ///
    /// [`Config::disk`]: crate::Config::disk
    pub fn read_polled_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        limit: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        self.read_fixed(fd, buf, offset, limit, true)
    }

    /// Like [`write_fixed_at`](Self::write_fixed_at), on the worker's polled
    /// disk ring. See [`read_polled_at`](Self::read_polled_at) for what that
    /// asks of `fd`. If the future is dropped once the kernel has the
    /// operation, the write still happens.
    pub fn write_polled_at(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        self.write_fixed(fd, buf, offset, true)
    }

    fn read_fixed(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        limit: usize,
        polled: bool,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = match self.check_fixed(&buf, polled) {
            Ok(()) => Ok(Op::new(
                self,
                Operation::ReadFixedAt {
                    fd,
                    buf,
                    offset,
                    limit,
                    polled,
                },
            )),
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
                buf.grow(read);
                read
            });
            (result, buf)
        }
    }

    fn write_fixed(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        polled: bool,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = match self.check_fixed(&buf, polled) {
            Ok(()) => Ok(Op::new(
                self,
                Operation::WriteFixedAt {
                    fd,
                    buf,
                    offset,
                    polled,
                },
            )),
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

    /// A buffer's registered index only means something on rings that have
    /// its pool, and a polled operation needs a disk ring to run on.
    fn check_fixed(&self, buf: &FixedBuf, polled: bool) -> io::Result<()> {
        let invalid = |message| Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        if !self.buffers().is_some_and(|pool| pool.owns(buf)) {
            return invalid("buffer is not from the pool registered on this sidecar");
        }
        if polled && !self.has_disk_rings() {
            return invalid("this sidecar has no disk rings");
        }
        Ok(())
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
    /// extending its contents by the number of bytes received, which is at
    /// most `limit`. Completes as soon as any bytes have arrived; `Ok(0)`
    /// means the peer has shut down its side, or that there was no room.
    ///
    /// The buffer is handed back whether or not the receive succeeds. If the
    /// future is dropped before it completes, the receive is cancelled and
    /// the buffer returns to its pool once the kernel is done with it. Bytes
    /// that arrived in between are lost with it.
    pub fn recv(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        limit: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        let op = Op::new(self, Operation::Recv { fd, buf, limit });
        async move {
            let completion = op.await;
            let Operation::Recv { mut buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            let result = completion.result.map(|received| {
                let received = received as usize;
                buf.grow(received);
                received
            });
            (result, buf)
        }
    }

    /// Sends the contents of `buf` from position `start` on, on the socket
    /// `fd`. The kernel keeps going until everything is sent, so a count
    /// short of that means the connection failed part of the way through.
    ///
    /// A peer that has gone away is reported as `BrokenPipe`, without
    /// raising `SIGPIPE`. The buffer is handed back whether or not the send
    /// succeeds.
    ///
    /// # Panics
    ///
    /// Panics if `start` is past the end of the contents.
    pub fn send(
        &self,
        fd: Arc<OwnedFd>,
        buf: FixedBuf,
        start: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        assert!(start <= buf.len());
        let op = Op::new(self, Operation::Send { fd, buf, start });
        async move {
            let completion = op.await;
            let Operation::Send { buf, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|sent| sent as usize), buf)
        }
    }

    /// Sends the regions `segments` of the buffers' contents on the socket
    /// `fd`, one after the other, as a single stream of bytes. Like
    /// [`send`](Self::send), it keeps going until everything is sent.
    ///
    /// The buffers are handed back in the order they came, whether or not
    /// the send succeeds.
    ///
    /// # Panics
    ///
    /// Panics if a segment reaches past the contents of its buffer, or if
    /// there are more than 1024 segments.
    pub fn send_vectored(
        &self,
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Vec<FixedBuf>)> + Send + use<> {
        let message = Message::new(&bufs, segments, true);
        let op = Op::new(self, Operation::SendMsg { fd, bufs, message });
        async move {
            let completion = op.await;
            let Operation::SendMsg { bufs, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|sent| sent as usize), bufs)
        }
    }

    /// Receives from the socket `fd` into the regions `segments` of the
    /// buffers, filling them one after the other. Unlike
    /// [`recv`](Self::recv), it waits until all of them are full: a count
    /// short of that means the peer has shut down its side.
    ///
    /// The regions may lie anywhere within the buffers' capacity. The
    /// lengths of the buffers' contents are left as they are, for the caller
    /// to [`grow`](FixedBuf::grow). The buffers are handed back in the order
    /// they came, whether or not the receive succeeds.
    ///
    /// # Panics
    ///
    /// Panics if a segment reaches past the capacity of its buffer, or if
    /// there are more than 1024 segments.
    pub fn recv_vectored(
        &self,
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Vec<FixedBuf>)> + Send + use<> {
        let message = Message::new(&bufs, segments, false);
        let op = Op::new(self, Operation::RecvMsg { fd, bufs, message });
        async move {
            let completion = op.await;
            let Operation::RecvMsg { bufs, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            (completion.result.map(|received| received as usize), bufs)
        }
    }

    /// Shuts down one or both directions of the socket `fd`. Shutting down
    /// writing tells the peer that nothing more is coming, while what it
    /// sends can still be received.
    pub fn shutdown(
        &self,
        fd: Arc<OwnedFd>,
        how: std::net::Shutdown,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        self.unit(Operation::Shutdown { fd, how })
    }

    /// Opens `path`, relative to the directory `dir`, as `openat(2)` does.
    /// Close-on-exec is always set.
    pub fn open_at(
        &self,
        dir: Arc<OwnedFd>,
        path: CString,
        flags: i32,
        mode: u32,
    ) -> impl Future<Output = io::Result<OwnedFd>> + Send + use<> {
        let op = Op::new(
            self,
            Operation::OpenAt {
                dir,
                path,
                flags,
                mode,
                opened: None,
            },
        );
        async move {
            let completion = op.await;
            completion.result?;
            let Operation::OpenAt { opened, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            Ok(opened.expect("a successful open carries its descriptor"))
        }
    }

    /// Looks up the file `path`, relative to the directory `dir`, following
    /// symbolic links.
    pub fn stat_at(
        &self,
        dir: Arc<OwnedFd>,
        path: CString,
    ) -> impl Future<Output = io::Result<FileStat>> + Send + use<> {
        // SAFETY: `statx` is plain data, for which all zeroes is valid.
        let stat = Box::new(unsafe { std::mem::zeroed() });
        let op = Op::new(self, Operation::Statx { dir, path, stat });
        async move {
            let completion = op.await;
            completion.result?;
            let Operation::Statx { stat, .. } = completion.operation else {
                unreachable!("a completion carries the operation it was submitted with")
            };
            Ok(FileStat {
                size: stat.stx_size,
                modified: (stat.stx_mtime.tv_sec, stat.stx_mtime.tv_nsec),
                is_dir: u32::from(stat.stx_mode) & libc::S_IFMT == libc::S_IFDIR,
            })
        }
    }

    /// Looks up the open file `fd`.
    pub fn stat(
        &self,
        fd: Arc<OwnedFd>,
    ) -> impl Future<Output = io::Result<FileStat>> + Send + use<> {
        self.stat_at(fd, CString::default())
    }

    /// Renames `from` to `to`, both relative to the directory `dir`,
    /// replacing `to` if it exists.
    pub fn rename_at(
        &self,
        dir: Arc<OwnedFd>,
        from: CString,
        to: CString,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::RenameAt { dir, from, to })
    }

    /// Removes the file `path`, relative to the directory `dir`.
    pub fn unlink_at(
        &self,
        dir: Arc<OwnedFd>,
        path: CString,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::UnlinkAt {
            dir,
            path,
            flags: 0,
        })
    }

    /// Removes the empty directory `path`, relative to the directory `dir`.
    pub fn rmdir_at(
        &self,
        dir: Arc<OwnedFd>,
        path: CString,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::UnlinkAt {
            dir,
            path,
            flags: libc::AT_REMOVEDIR,
        })
    }

    /// Creates the directory `path`, relative to the directory `dir`.
    pub fn mkdir_at(
        &self,
        dir: Arc<OwnedFd>,
        path: CString,
        mode: u32,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::MkdirAt { dir, path, mode })
    }

    /// Has the data of `fd`, and the metadata needed to read it back, reach
    /// the device, as `fdatasync(2)` does.
    pub fn fdatasync(
        &self,
        fd: Arc<OwnedFd>,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::Fdatasync { fd })
    }

    /// Sets the size of the file `fd` to `len` bytes.
    pub fn truncate(
        &self,
        fd: Arc<OwnedFd>,
        len: u64,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.unit(Operation::Truncate { fd, len })
    }

    /// Runs an operation whose result carries nothing but success.
    fn unit(&self, operation: Operation) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let op = Op::new(self, operation);
        async move { op.await.result.map(drop) }
    }
}

/// A region of one of the buffers given to a vectored operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segment {
    /// Index of the buffer.
    pub buf: usize,
    /// Where in the buffer the region starts.
    pub start: usize,
    pub len: usize,
}

/// What [`Sidecar::stat`] reports about a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStat {
    /// Size in bytes.
    pub size: u64,
    /// Time of the last change to the contents, as seconds and nanoseconds
    /// since the Unix epoch.
    pub modified: (i64, u32),
    pub is_dir: bool,
}
