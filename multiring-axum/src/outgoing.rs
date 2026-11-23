//! Registered-buffer response bodies with a standard streaming fallback.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use tokio::sync::mpsc;
use tokio_multiring::{FixedBuf, Reclaim, Segment};
use ureq_proto::server::Reply;
use ureq_proto::server::state::SendBody;

use crate::conn::Conn;

const FALLBACK_CHUNK: usize = 16 * 1024;
const FALLBACK_QUEUE: usize = 2;

enum Sink<'a> {
    Direct {
        conn: &'a mut Conn,
        body: &'a mut Reply<SendBody>,
    },
    Stream(mpsc::Sender<Bytes>),
}

/// Sink passed to [`DirectWrite::write_to`]. On the native connection it sends
/// registered buffers directly; under a body wrapper or another HTTP server it
/// yields bounded, copied `Bytes` frames through the normal Body interface.
pub struct BodySink<'a> {
    inner: Sink<'a>,
}

impl<'a> BodySink<'a> {
    pub(crate) fn new(conn: &'a mut Conn, body: &'a mut Reply<SendBody>) -> Self {
        Self {
            inner: Sink::Direct { conn, body },
        }
    }

    /// Whether this sink supports zero-copy sends for the current client.
    pub fn zero_copy(&self) -> bool {
        match &self.inner {
            Sink::Direct { conn, .. } => conn.zero_copy(),
            Sink::Stream(_) => false,
        }
    }

    /// Whether the native response uses chunked transfer encoding.
    /// A standard-body fallback has no access to its eventual wire framing.
    pub fn is_chunked(&self) -> bool {
        match &self.inner {
            Sink::Direct { body, .. } => body.is_chunked(),
            Sink::Stream(_) => false,
        }
    }

    /// Flushes buffered response headers or bytes. Stream frames are handed to
    /// the consumer as they are written, so the fallback has nothing to flush.
    pub async fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            Sink::Direct { conn, .. } => conn.flush().await,
            Sink::Stream(_) => Ok(()),
        }
    }

    /// Sends buffer segments, coalescing native response headers where possible.
    /// Body wrappers receive copied frames and retain their normal semantics.
    pub async fn send_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        if let Sink::Direct { conn, body } = &mut self.inner
            && !body.is_chunked()
        {
            let len: usize = segments.iter().map(|s| s.len).sum();
            // Validate framing BEFORE putting any bytes on the socket.
            if let Err(e) = body.consume_direct_write(len) {
                return (Err(io::Error::new(io::ErrorKind::InvalidData, e)), bufs);
            }
            return conn.send_vectored(bufs, segments).await;
        }
        for seg in segments {
            let slice = &bufs[seg.buf][seg.start..seg.start + seg.len];
            if let Err(e) = self.write_bytes(slice).await {
                return (Err(e), bufs);
            }
        }
        (Ok(()), bufs)
    }

    /// Sends registered buffers without copying. Call only when
    /// [`Self::zero_copy`] is true and the response is not chunked.
    pub async fn send_zero_copy(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Option<Reclaim>) {
        let Sink::Direct { conn, body } = &mut self.inner else {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "zero-copy is unavailable through a body wrapper",
                )),
                None,
            );
        };
        if body.is_chunked() {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "send_zero_copy requires a non-chunked response",
                )),
                None,
            );
        }
        let len: usize = segments.iter().map(|s| s.len).sum();
        if let Err(e) = body.consume_direct_write(len) {
            return (Err(io::Error::new(io::ErrorKind::InvalidData, e)), None);
        }
        if let Err(e) = conn.flush().await {
            return (Err(e), None);
        }
        let (sent, reclaim) = conn.send_zero_copy(bufs, segments).await;
        (sent, Some(reclaim))
    }

    /// Writes bytes through the HTTP encoder, or yields bounded Body frames.
    pub async fn write_bytes(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            Sink::Stream(sender) => {
                for chunk in bytes.chunks(FALLBACK_CHUNK) {
                    sender
                        .send(Bytes::copy_from_slice(chunk))
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
                }
            }
            Sink::Direct { conn, body } => {
                while !bytes.is_empty() {
                    let output = conn.output()?;
                    let (used, produced) = body
                        .write(bytes, output.spare_capacity_mut())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    output.grow(produced);
                    bytes = &bytes[used..];
                    if used == 0 {
                        conn.flush().await?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// A response producer that can use registered-buffer and zero-copy sends on
/// the native server, with ordinary streaming semantics under Body middleware.
pub trait DirectWrite: Send + 'static {
    fn write_to<'a>(
        self: Box<Self>,
        sink: &'a mut BodySink<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// The capability is owned by the actual Body. A response extension cannot
/// prove middleware left that body intact, so no side channel is used.
pub(crate) struct DirectResponseBody {
    writer: Mutex<Option<Box<dyn DirectWrite>>>,
    receiver: Option<mpsc::Receiver<Bytes>>,
    producer: Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send>>>,
    result: Option<io::Result<()>>,
    remaining: Option<u64>,
    done: bool,
}

impl DirectResponseBody {
    pub(crate) fn take_writer(&self) -> Option<Box<dyn DirectWrite>> {
        self.writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

impl http_body::Body for DirectResponseBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.receiver.is_none() {
            let Some(writer) = self.take_writer() else {
                self.done = true;
                return Poll::Ready(None);
            };
            let (sender, receiver) = mpsc::channel(FALLBACK_QUEUE);
            self.receiver = Some(receiver);
            self.producer = Some(Box::pin(async move {
                let mut sink = BodySink {
                    inner: Sink::Stream(sender),
                };
                writer.write_to(&mut sink).await
            }));
        }
        // Run on the consumer's task. The bounded queue makes the producer
        // yield; dropping this Body cancels it synchronously, without a runtime.
        if let Some(producer) = &mut self.producer
            && let Poll::Ready(result) = producer.as_mut().poll(cx)
        {
            self.producer = None;
            self.result = Some(result);
        }
        match self.receiver.as_mut().expect("created above").poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(bytes)) => {
                if let Some(remaining) = &mut self.remaining {
                    if bytes.len() as u64 > *remaining {
                        self.done = true;
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "response exceeds Content-Length",
                        ))));
                    }
                    *remaining -= bytes.len() as u64;
                }
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            Poll::Ready(None) => {
                if let Some(Err(error)) = self.result.take() {
                    self.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                self.done = true;
                if self.remaining.is_some_and(|n| n != 0) {
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "response ended before Content-Length",
                    ))))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.remaining {
            Some(remaining) => SizeHint::with_exact(remaining),
            None => SizeHint::default(),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

/// Builds a response whose actual Body owns the producer. Header-only
/// middleware preserves the native fast path; body wrappers use its bounded
/// standard-body fallback, and replacing the body drops the producer.
pub fn direct_response(head: http::Response<()>, writer: impl DirectWrite) -> Response {
    let (parts, ()) = head.into_parts();
    let remaining = parts
        .headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    Response::from_parts(
        parts,
        Body::new(DirectResponseBody {
            writer: Mutex::new(Some(Box::new(writer))),
            receiver: None,
            producer: None,
            result: None,
            remaining,
            done: false,
        }),
    )
}

/// Convenience wrapper implementing [`IntoResponse`] for a producer and head.
pub struct DirectResponse {
    response: Response,
}

impl DirectResponse {
    pub fn new(head: http::Response<()>, writer: impl DirectWrite) -> Self {
        Self {
            response: direct_response(head, writer),
        }
    }
}

impl IntoResponse for DirectResponse {
    fn into_response(self) -> Response {
        self.response
    }
}
