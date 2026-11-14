//! One client connection: buffered input and output over a socket.

use std::io;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use tokio_sidecar::{FixedBuf, Sidecar};

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
    sidecar: Sidecar,
    fd: Arc<OwnedFd>,
    /// Bytes received. Those before `consumed` have been dealt with.
    input: Option<FixedBuf>,
    consumed: usize,
    output: Option<FixedBuf>,
}

fn lost() -> io::Error {
    io::Error::other("an earlier operation on the connection was cut short")
}

impl Conn {
    pub(crate) fn new(sidecar: Sidecar, fd: OwnedFd, input: FixedBuf, output: FixedBuf) -> Conn {
        Conn {
            sidecar,
            fd: Arc::new(fd),
            input: Some(input),
            consumed: 0,
            output: Some(output),
        }
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
        let (received, input) = self.sidecar.recv(self.fd.clone(), input, usize::MAX).await;
        self.input = Some(input);
        Ok(match received? {
            0 => Filled::Closed,
            _ => Filled::Data,
        })
    }

    /// Receives up to `limit` bytes straight into the spare capacity of
    /// `buf`, past the input buffer, which must be empty. Returns how many
    /// arrived; none means the client has closed its side.
    pub(crate) async fn recv_into(
        &mut self,
        buf: FixedBuf,
        limit: usize,
    ) -> (io::Result<usize>, FixedBuf) {
        assert!(self.pending().is_empty());
        self.sidecar.recv(self.fd.clone(), buf, limit).await
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
        let (sent, buf) = self.sidecar.send(self.fd.clone(), buf, start).await;
        let result = match sent {
            Ok(sent) if sent == expected => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, buf)
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
