use std::ffi::CString;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::buffers::FixedBuf;
use crate::op::{Message, Op, Operation};
use crate::reactor::MultiRing;

impl MultiRing {
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
            return invalid("buffer is not from the pool registered on this multiring");
        }
        if polled && !self.has_disk_rings() {
            return invalid("this multiring has no disk rings");
        }
        Ok(())
    }

    /// Waits for a connection on `listener` and returns it, in blocking
    /// mode and with close-on-exec set.
    ///
    /// Sockets used with a multiring must be in blocking mode: on a
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

    /// Like [`send_vectored`](Self::send_vectored), without copying: the
    /// network stack sends from the buffers themselves.
    ///
    /// The send is complete when everything is queued on the socket, as any
    /// send is. The kernel goes on referring to the buffers after that, for
    /// as long as it may have to transmit their contents again, which is
    /// until the peer has acknowledged them. So the buffers do not come back
    /// with the result. In their place comes a [`Reclaim`], which yields
    /// them once the kernel has let go.
    ///
    /// That makes the buffers a sender holds the measure of what it has in
    /// flight. A caller that sends only from buffers it has reclaimed can
    /// never have more outstanding than it owns buffers, however fast it is
    /// and however slow its peer.
    ///
    /// Avoiding the copy pays for sends of some size: tens of kilobytes and
    /// up. Whether the copy is in fact avoided is up to the route the data
    /// takes. Loopback, for one, copies. See [`Reclaimed::copied`].
    ///
    /// Buffers from this multiring's pool are sent as registered buffers if
    /// the pool is on ordinary pages. For a pool on huge pages, and for
    /// other buffers, the kernel pins the pages of each send as it goes.
    /// The reason is a mistake in Linux, in every release up to 7.0 at
    /// least: a region of a registered buffer on a huge page is charged to
    /// the socket as if it began at the start of that page, so a socket
    /// counts as full after a small fraction of what it could hold, and
    /// sends crawl.
    ///
    /// # Panics
    ///
    /// Panics if a segment reaches past the contents of its buffer, or if
    /// there are more than 1024 segments.
    pub fn send_zc_vectored(
        &self,
        fd: Arc<OwnedFd>,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Reclaim)> + Send + use<> {
        let message = Message::new(&bufs, segments, true);
        // One send names one registered buffer, which all of it must be in.
        let registered = self
            .buffers()
            .filter(|pool| !pool.huge_pages() && bufs.iter().all(|buf| pool.owns(buf)))
            .and_then(|_| bufs.first().map(FixedBuf::registered_index))
            .filter(|&index| bufs.iter().all(|buf| buf.registered_index() == index));
        let (reclaim, reclaimed) = oneshot::channel();
        let op = Op::new(
            self,
            Operation::SendMsgZc {
                fd,
                bufs,
                message,
                registered,
                copied: false,
                reclaim,
            },
        );
        async move {
            let sent = op.await.result.map(|sent| sent as usize);
            (sent, Reclaim(reclaimed))
        }
    }

    /// Sends the contents of `buf` on the socket `fd`, without copying into a
    /// pool buffer first. Like [`send`](Self::send), the kernel keeps going
    /// until everything is sent.
    ///
    /// At most `u32::MAX` bytes are sent in a single call.
    pub fn send_bytes(
        &self,
        fd: Arc<OwnedFd>,
        buf: Bytes,
    ) -> impl Future<Output = io::Result<usize>> + Send + use<> {
        let op = Op::new(self, Operation::SendBytes { fd, buf });
        async move { op.await.result.map(|sent| sent as usize) }
    }

    /// Like [`send_bytes`](Self::send_bytes), without copying in the kernel
    /// (`IORING_OP_SEND_ZC`).
    ///
    /// The `Bytes` handle is held by the reactor until the kernel confirms it
    /// no longer refers to the backing memory. The returned [`Release`] future
    /// resolves once that happens, reporting whether the kernel fell back to
    /// copying.
    pub fn send_bytes_zc(
        &self,
        fd: Arc<OwnedFd>,
        buf: Bytes,
    ) -> impl Future<Output = (io::Result<usize>, Release)> + Send + use<> {
        let (reclaim, reclaimed) = oneshot::channel();
        let op = Op::new(
            self,
            Operation::SendBytesZc {
                fd,
                buf,
                copied: false,
                reclaim,
            },
        );
        async move {
            let sent = op.await.result.map(|sent| sent as usize);
            (sent, Release(reclaimed))
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

/// The buffers of a zero-copy send, on their way back from the kernel. A
/// future that yields them when the kernel has let go of them.
///
/// Dropping it does not lose the buffers: they return to their pool when
/// the kernel is done.
pub struct Reclaim(oneshot::Receiver<Reclaimed>);

impl Reclaim {
    /// Takes the buffers if the kernel has already let go of them.
    pub fn try_take(&mut self) -> Option<Reclaimed> {
        self.0.try_recv().ok()
    }
}

impl Future for Reclaim {
    type Output = Reclaimed;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Reclaimed> {
        Pin::new(&mut self.0).poll(cx).map(|reclaimed| {
            // An operation that never reached the kernel is dropped whole,
            // and its buffers are back in their pool.
            reclaimed.unwrap_or(Reclaimed {
                bufs: Vec::new(),
                copied: false,
            })
        })
    }
}

/// What a [`Reclaim`] yields.
pub struct Reclaimed {
    /// The buffers, in the order they were given. None if the send was
    /// given up before it reached the kernel: they are in their pool then.
    pub bufs: Vec<FixedBuf>,
    /// Whether the kernel could not confirm that it sent without copying.
    /// It copies when the data's route calls for it, as loopback does.
    pub copied: bool,
}

/// Notification that the kernel has finished with the memory of a
/// [`MultiRing::send_bytes_zc`] operation and its [`Bytes`] handle has been
/// dropped.
///
/// Resolves to `true` if the kernel reported falling back to copying, or
/// `false` otherwise. Dropping it does not free the buffer early: the reactor
/// keeps it alive until the kernel lets go.
pub struct Release(oneshot::Receiver<bool>);

impl Release {
    /// Returns the result if the kernel has already let go of the buffer.
    pub fn try_take(&mut self) -> Option<bool> {
        self.0.try_recv().ok()
    }
}

impl Future for Release {
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|res| res.unwrap_or(false))
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

/// What [`MultiRing::stat`] reports about a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileStat {
    /// Size in bytes.
    pub size: u64,
    /// Time of the last change to the contents, as seconds and nanoseconds
    /// since the Unix epoch.
    pub modified: (i64, u32),
    pub is_dir: bool,
}
