//! One client connection: buffered input and output over a socket driven by
//! a [`Sidecar`].

use std::collections::VecDeque;
use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio_sidecar::{FixedBuf, Reclaim, Release, Segment, Sidecar};

/// How long a connection being closed is kept open for the client to finish
/// sending.
const LINGER: Duration = Duration::from_secs(2);

/// Maximum unacknowledged zero-copy `Bytes` chunks in flight per connection.
const MAX_INFLIGHT_ZC: usize = 4;

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
    sidecar: Sidecar,
    fd: Arc<OwnedFd>,
    /// Bytes received. Those before `consumed` have been dealt with.
    input: Option<FixedBuf>,
    consumed: usize,
    output: Option<FixedBuf>,
    /// Whether downloads on this connection are sent without copying.
    zero_copy: bool,
    /// Unacknowledged zero-copy `Bytes` sends on this connection, bounded to
    /// [`MAX_INFLIGHT_ZC`] to keep pinned memory per connection small.
    in_flight_zc: VecDeque<Release>,
}

fn lost() -> io::Error {
    io::Error::other("an earlier operation on the connection was cut short")
}

impl Conn {
    pub(crate) fn new(
        sidecar: Sidecar,
        fd: OwnedFd,
        input: FixedBuf,
        output: FixedBuf,
        zero_copy: bool,
    ) -> Conn {
        Conn {
            sidecar,
            fd: Arc::new(fd),
            input: Some(input),
            consumed: 0,
            output: Some(output),
            zero_copy,
            in_flight_zc: VecDeque::new(),
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
            let pending = input.len() - self.consumed;
            input.copy_within(self.consumed.., 0);
            input.truncate(pending);
            self.consumed = 0;
        }
        if input.len() == input.capacity() {
            self.input = Some(input);
            return Ok(Filled::Full);
        }
        let (received, input) = self.sidecar.recv(self.fd.clone(), input, usize::MAX).await;
        self.input = Some(input);
        Ok(match received? {
            0 => Filled::Closed,
            _ => Filled::Data,
        })
    }

    /// Receives straight into `segments` of `bufs`, bypassing the input buffer,
    /// which must be empty.
    pub(crate) async fn recv_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<usize>, Vec<FixedBuf>) {
        assert!(self.pending().is_empty());
        self.sidecar
            .recv_vectored(self.fd.clone(), bufs, segments)
            .await
    }

    /// The output buffer, for appending to.
    pub(crate) fn output(&mut self) -> io::Result<&mut FixedBuf> {
        self.output.as_mut().ok_or_else(lost)
    }

    /// Whether the output buffer currently has pending bytes.
    pub(crate) fn has_output(&self) -> bool {
        self.output.as_ref().is_some_and(|buf| !buf.is_empty())
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

    /// Sends the contents of `buf` from `start` on.
    pub(crate) async fn send(&mut self, buf: FixedBuf, start: usize) -> (io::Result<()>, FixedBuf) {
        let expected = buf.len() - start;
        let (sent, buf) = self.sidecar.send(self.fd.clone(), buf, start).await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, buf)
    }

    /// Sends `segments` of `bufs`. If the output buffer still holds unflushed
    /// response headers, prepends them into the same vectored send so headers
    /// and body go out in one io_uring operation.
    pub(crate) async fn send_vectored(
        &mut self,
        mut bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        if self.has_output() {
            let Some(head) = self.output.take() else {
                return (Err(lost()), bufs);
            };
            let head_len = head.len();
            let head_index = bufs.len();
            bufs.push(head);
            let mut combined = Vec::with_capacity(segments.len() + 1);
            combined.push(Segment {
                buf: head_index,
                start: 0,
                len: head_len,
            });
            combined.extend_from_slice(segments);
            let expected: usize = combined.iter().map(|s| s.len).sum();
            let (sent, mut bufs) = self
                .sidecar
                .send_vectored(self.fd.clone(), bufs, &combined)
                .await;
            let mut head = bufs.pop().expect("head buffer was pushed above");
            head.clear();
            self.output = Some(head);
            let result = match sent {
                Ok(sent) if sent == expected => Ok(()),
                Ok(_) => Err(io::ErrorKind::WriteZero.into()),
                Err(error) => Err(error),
            };
            return (result, bufs);
        }

        let expected: usize = segments.iter().map(|segment| segment.len).sum();
        let (sent, bufs) = self
            .sidecar
            .send_vectored(self.fd.clone(), bufs, segments)
            .await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, bufs)
    }

    /// Like [`send_vectored`](Self::send_vectored), without copying. The
    /// output buffer must be flushed first.
    pub(crate) async fn send_zero_copy(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Reclaim) {
        debug_assert!(!self.has_output());
        let expected: usize = segments.iter().map(|segment| segment.len).sum();
        let (sent, reclaim) = self
            .sidecar
            .send_zc_vectored(self.fd.clone(), bufs, segments)
            .await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, reclaim)
    }

    /// Sends `buf` directly via io_uring without copying into the per-connection
    /// output buffer. The output buffer must be flushed first.
    pub(crate) async fn send_bytes(&mut self, buf: Bytes) -> io::Result<()> {
        debug_assert!(!self.has_output());
        let expected = buf.len();
        match self.sidecar.send_bytes(self.fd.clone(), buf).await {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        }
    }

    /// Sends `buf` via `IORING_OP_SEND_ZC`, bounding unacknowledged chunks in
    /// flight on this connection to [`MAX_INFLIGHT_ZC`]. The output buffer must
    /// be flushed first.
    pub(crate) async fn send_bytes_zc(&mut self, buf: Bytes) -> io::Result<()> {
        debug_assert!(!self.has_output());
        while self
            .in_flight_zc
            .front_mut()
            .and_then(Release::try_take)
            .is_some()
        {
            self.in_flight_zc.pop_front();
        }
        if self.in_flight_zc.len() >= MAX_INFLIGHT_ZC
            && let Some(oldest) = self.in_flight_zc.pop_front()
        {
            oldest.await;
        }
        let expected = buf.len();
        let (sent, release) = self.sidecar.send_bytes_zc(self.fd.clone(), buf).await;
        self.in_flight_zc.push_back(release);
        match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        }
    }

    /// Closes the connection cleanly so the client can read the last response.
    pub(crate) async fn close(mut self) {
        let announced = self
            .sidecar
            .shutdown(self.fd.clone(), std::net::Shutdown::Write);
        if announced.await.is_err() {
            return;
        }
        let drain = async {
            while let Some(mut input) = self.input.take() {
                input.clear();
                let (received, input) = self.sidecar.recv(self.fd.clone(), input, usize::MAX).await;
                if !matches!(received, Ok(1..)) {
                    break;
                }
                self.input = Some(input);
            }
        };
        let _ = tokio::time::timeout(LINGER, drain).await;
    }
}
