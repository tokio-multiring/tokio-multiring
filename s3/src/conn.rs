//! One client connection: buffered input and output over a socket.

use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use tokio_multiring::{FixedBuf, MultiRing, Reclaim, Segment};

/// How long a connection being closed is kept open for the client to finish
/// sending.
const LINGER: Duration = Duration::from_secs(2);

/// What a call to [`Conn::fill`] found.
pub(crate) enum Filled {
    /// More input is pending.
    Data,
    /// The client has closed its side.
    Closed,
    /// The input buffer is full of bytes nobody has consumed.
    Full,
}

pub(crate) struct Conn {
    multiring: MultiRing,
    fd: Arc<OwnedFd>,
    /// Bytes received. Those before `consumed` have been dealt with.
    input: Option<FixedBuf>,
    consumed: usize,
    output: Option<FixedBuf>,
    /// Whether downloads on this connection are sent without copying.
    zero_copy: bool,
}

fn lost() -> io::Error {
    io::Error::other("an earlier operation on the connection was cut short")
}

impl Conn {
    pub(crate) fn new(
        multiring: MultiRing,
        fd: OwnedFd,
        input: FixedBuf,
        output: FixedBuf,
        zero_copy: bool,
    ) -> Conn {
        Conn {
            multiring,
            fd: Arc::new(fd),
            input: Some(input),
            consumed: 0,
            output: Some(output),
            zero_copy,
        }
    }

    /// Whether downloads on this connection are sent without copying.
    pub(crate) fn zero_copy(&self) -> bool {
        self.zero_copy
    }

    /// Input that has been received and not consumed.
    pub(crate) fn pending(&self) -> &[u8] {
        self.input
            .as_deref()
            .map_or(&[], |input| &input[self.consumed..])
    }

    pub(crate) fn consume(&mut self, count: usize) {
        assert!(count <= self.pending().len());
        self.consumed += count;
    }

    /// Receives more input, after what is pending.
    pub(crate) async fn fill(&mut self) -> io::Result<Filled> {
        let mut input = self.input.take().ok_or_else(lost)?;
        if self.consumed > 0 {
            // Make room by moving what is pending to the front.
            let pending = input.len() - self.consumed;
            input.copy_within(self.consumed.., 0);
            input.truncate(pending);
            self.consumed = 0;
        }
        if input.len() == input.capacity() {
            self.input = Some(input);
            return Ok(Filled::Full);
        }
        let (received, input) = self
            .multiring
            .recv(self.fd.clone(), input, usize::MAX)
            .await;
        self.input = Some(input);
        Ok(match received? {
            0 => Filled::Closed,
            _ => Filled::Data,
        })
    }

    /// Receives straight into the regions `segments` of `bufs`, past the
    /// input buffer, which must be empty. Waits for every region to fill: a
    /// count short of that means the client has closed its side.
    pub(crate) async fn recv_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<usize>, Vec<FixedBuf>) {
        assert!(self.pending().is_empty());
        let received = self
            .multiring
            .recv_vectored(self.fd.clone(), bufs, segments);
        received.await
    }

    /// The output buffer, for appending to.
    pub(crate) fn output(&mut self) -> io::Result<&mut FixedBuf> {
        self.output.as_mut().ok_or_else(lost)
    }

    /// Sends the output buffer and empties it.
    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        let output = self.output.take().ok_or_else(lost)?;
        if output.is_empty() {
            self.output = Some(output);
            return Ok(());
        }
        let (sent, mut output) = self.send(output, 0).await;
        output.clear();
        self.output = Some(output);
        sent
    }

    /// Sends the contents of `buf` from `start` on. The output buffer must
    /// have been flushed.
    pub(crate) async fn send(&mut self, buf: FixedBuf, start: usize) -> (io::Result<()>, FixedBuf) {
        let expected = buf.len() - start;
        let (sent, buf) = self.multiring.send(self.fd.clone(), buf, start).await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, buf)
    }

    /// Sends the regions `segments` of `bufs`, one after the other. The
    /// output buffer must have been flushed.
    pub(crate) async fn send_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        let expected: usize = segments.iter().map(|segment| segment.len).sum();
        let sent = self
            .multiring
            .send_vectored(self.fd.clone(), bufs, segments);
        let (sent, bufs) = sent.await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, bufs)
    }

    /// Like [`send_vectored`](Self::send_vectored), without copying. The
    /// buffers stay with the kernel until the client has acknowledged what
    /// is in them, and are had back through the [`Reclaim`].
    pub(crate) async fn send_zero_copy(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Reclaim) {
        let expected: usize = segments.iter().map(|segment| segment.len).sum();
        let sent = self
            .multiring
            .send_zc_vectored(self.fd.clone(), bufs, segments);
        let (sent, reclaim) = sent.await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, reclaim)
    }

    /// Closes the connection in a way that lets the client read the last
    /// response.
    ///
    /// Closing a socket that still has unread input makes the kernel reset
    /// the connection, and a client busy sending may then never see what
    /// was sent to it. So this announces the end, and keeps taking what the
    /// client sends until it has noticed or [`LINGER`] has passed.
    pub(crate) async fn close(mut self) {
        let announced = self
            .multiring
            .shutdown(self.fd.clone(), std::net::Shutdown::Write);
        if announced.await.is_err() {
            return;
        }
        let drain = async {
            while let Some(mut input) = self.input.take() {
                input.clear();
                let (received, input) = self
                    .multiring
                    .recv(self.fd.clone(), input, usize::MAX)
                    .await;
                if !matches!(received, Ok(1..)) {
                    break;
                }
                self.input = Some(input);
            }
        };
        let _ = tokio::time::timeout(LINGER, drain).await;
    }
}
