//! Request-body bridge between `ureq-proto` + `tokio-sidecar` and Axum.
//!
//! Two interfaces share the same underlying connection handoff slot:
//!
//! - [`SidecarIncoming`] implements [`http_body::Body`] so every standard Axum
//!   extractor (`Bytes`, `String`, `Json`, `Body`, Tower middleware) works
//!   unmodified.
//! - [`DirectBody`] implements [`axum::extract::FromRequest`] so handlers that
//!   want direct io_uring `recv_vectored` into registered [`FixedBuf`]s (and
//!   explicit `100 Continue` timing) can claim the connection without copying.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use bytes::{Bytes, BytesMut};
use http_body::{Frame, SizeHint};
use tokio_sidecar::{FixedBuf, Segment};
use ureq_proto::server::Reply;
use ureq_proto::server::state::{RecvBody, Send100};

use crate::conn::{Conn, Filled};

/// The protocol state while a request body may still be unread.
pub(crate) enum IncomingReply {
    Send100(Reply<Send100>),
    RecvBody(Reply<RecvBody>),
    /// Transient sentinel while transitioning between typestates.
    Taken,
}

/// Live connection state loaned out while the handler runs.
pub(crate) struct ActiveConn {
    pub(crate) conn: Conn,
    pub(crate) reply: IncomingReply,
    /// Set if a fatal transport error occurred while reading the body.
    pub(crate) io_error: bool,
}

/// Shared handoff slot between the connection driver, [`SidecarIncoming`],
/// and [`DirectBody`]. Uncontended during normal HTTP/1.1 execution.
#[derive(Clone)]
pub(crate) struct ConnSlot {
    inner: Arc<Mutex<Option<Box<ActiveConn>>>>,
}

impl ConnSlot {
    pub(crate) fn new(active: ActiveConn) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(Box::new(active)))),
        }
    }

    pub(crate) fn take(&self) -> Option<Box<ActiveConn>> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn put(&self, active: Box<ActiveConn>) {
        *self.inner.lock().unwrap_or_else(PoisonError::into_inner) = Some(active);
    }
}

/// Request extension inserted by `sidecar-axum` when a request has a body.
#[derive(Clone)]
pub(crate) struct DirectBodySlot(pub(crate) ConnSlot);

/// Why reading a request body failed.
#[derive(Debug)]
pub enum BodyError {
    /// Transport-level I/O error or unexpected EOF.
    Io(io::Error),
    /// Malformed HTTP framing (e.g. invalid chunked encoding).
    Malformed(ureq_proto::Error),
}

impl std::fmt::Display for BodyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodyError::Io(e) => write!(f, "body I/O error: {e}"),
            BodyError::Malformed(e) => write!(f, "malformed request body: {e}"),
        }
    }
}

impl std::error::Error for BodyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BodyError::Io(e) => Some(e),
            BodyError::Malformed(e) => Some(e),
        }
    }
}

impl From<io::Error> for BodyError {
    fn from(error: io::Error) -> Self {
        BodyError::Io(error)
    }
}

/// Sends `100 Continue` if `active.reply` is in [`IncomingReply::Send100`].
async fn ensure_recv_body(active: &mut ActiveConn) -> Result<(), BodyError> {
    if !matches!(active.reply, IncomingReply::Send100(_)) {
        return Ok(());
    }
    let IncomingReply::Send100(reply) = std::mem::replace(&mut active.reply, IncomingReply::Taken)
    else {
        unreachable!();
    };
    let output = active.conn.output()?;
    let (used, body) = reply.accept(output.spare_capacity_mut()).map_err(|e| {
        active.io_error = true;
        BodyError::Malformed(e)
    })?;
    output.grow(used);
    if let Err(e) = active.conn.flush().await {
        active.io_error = true;
        return Err(BodyError::Io(e));
    }
    active.reply = IncomingReply::RecvBody(body);
    Ok(())
}

/// Decodes body bytes from `conn` into `output` (which must be non-empty).
/// Returns `0` only once the body has ended.
pub(crate) async fn read_body_into(
    conn: &mut Conn,
    body: &mut Reply<RecvBody>,
    output: &mut [u8],
) -> Result<usize, BodyError> {
    while !body.is_ended() {
        let (used, produced) = body
            .read(conn.pending(), output)
            .map_err(BodyError::Malformed)?;
        conn.consume(used);
        if produced > 0 {
            return Ok(produced);
        }
        if used == 0 {
            match conn.fill().await? {
                Filled::Data => {}
                Filled::Closed => {
                    return Err(BodyError::Io(io::Error::from(io::ErrorKind::UnexpectedEof)));
                }
                Filled::Full => {
                    return Err(BodyError::Malformed(ureq_proto::Error::HttpParseFail(
                        "request body framing did not fit input buffer".into(),
                    )));
                }
            }
        }
    }
    Ok(0)
}

/// Extractor for direct io_uring request body reception.
///
/// When running on `sidecar-axum`, this claims the underlying connection and
/// `ureq-proto` body state directly — enabling zero-copy `recv_vectored` into
/// registered [`FixedBuf`]s and explicit `Expect: 100-continue` approval.
///
/// When running on another server (such as standard `axum::serve` / Hyper),
/// [`DirectBody::from_request`] transparently captures the standard
/// [`axum::body::Body`] in [`DirectBody::Fallback`], so a single Axum handler
/// compiles and runs on both runtimes.
pub enum DirectBody {
    /// Request has no body (e.g. `Content-Length: 0` or GET).
    Empty,
    /// Live io_uring connection + `ureq-proto` body state.
    Direct(SidecarDirectBody),
    /// Standard Axum body (when served by Hyper / `axum::serve` or after a
    /// body-wrapping middleware).
    Fallback(Body),
}

impl<S: Send + Sync> FromRequest<S> for DirectBody {
    type Rejection = Infallible;

    async fn from_request(mut req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(DirectBodySlot(slot)) = req.extensions_mut().remove::<DirectBodySlot>()
            && let Some(active) = slot.take()
        {
            return Ok(DirectBody::Direct(SidecarDirectBody {
                slot,
                active: Some(active),
            }));
        }
        let body = req.into_body();
        if http_body::Body::is_end_stream(&body) {
            Ok(DirectBody::Empty)
        } else {
            Ok(DirectBody::Fallback(body))
        }
    }
}

/// Direct io_uring request body handle returned inside [`DirectBody::Direct`].
///
/// Returns the connection to the server automatically on drop so the server
/// can send the response (or reject/discard/abandon an unread body).
pub struct SidecarDirectBody {
    slot: ConnSlot,
    active: Option<Box<ActiveConn>>,
}

impl SidecarDirectBody {
    fn active(&self) -> &ActiveConn {
        self.active
            .as_deref()
            .expect("active connection is present")
    }

    fn active_mut(&mut self) -> &mut ActiveConn {
        self.active
            .as_deref_mut()
            .expect("active connection is present")
    }

    /// Sends `100 Continue` if the client requested it. Call this *after*
    /// validating the request (e.g. opening the target upload) so rejections
    /// can be answered without receiving the body.
    pub async fn accept_100(&mut self) -> Result<(), BodyError> {
        ensure_recv_body(self.active_mut()).await
    }

    /// Remaining bytes for a length-delimited body, or `None` if chunked or
    /// still waiting on `accept_100`.
    pub fn remaining(&self) -> Option<u64> {
        match &self.active().reply {
            IncomingReply::RecvBody(body) => body.remaining(),
            IncomingReply::Send100(_) | IncomingReply::Taken => None,
        }
    }

    /// Whether the entire request body has been consumed.
    pub fn is_ended(&self) -> bool {
        match &self.active().reply {
            IncomingReply::RecvBody(body) => body.is_ended(),
            IncomingReply::Send100(_) | IncomingReply::Taken => false,
        }
    }

    /// Whether unread bytes are still buffered in the connection's head buffer.
    pub fn has_pending(&self) -> bool {
        !self.active().conn.pending().is_empty()
    }

    /// Receives directly from the socket into `segments` of `bufs` (bypassing
    /// the head buffer) for a length-delimited body with no buffered bytes.
    ///
    /// Caller must ensure `self.remaining() >= Some(total_segment_bytes)` and
    /// `!self.has_pending()`.
    pub async fn recv_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (Result<usize, BodyError>, Vec<FixedBuf>) {
        if let Err(err) = ensure_recv_body(self.active_mut()).await {
            return (Err(err), bufs);
        }
        let expected: usize = segments.iter().map(|s| s.len).sum();
        let active = self.active_mut();
        let (received, bufs) = active.conn.recv_vectored(bufs, segments).await;
        let received = match received {
            Ok(n) if n < expected => {
                active.io_error = true;
                return (
                    Err(BodyError::Io(io::Error::from(io::ErrorKind::UnexpectedEof))),
                    bufs,
                );
            }
            Ok(n) => n,
            Err(e) => {
                active.io_error = true;
                return (Err(BodyError::Io(e)), bufs);
            }
        };
        let IncomingReply::RecvBody(body) = &mut active.reply else {
            unreachable!("ensure_recv_body transitions to RecvBody");
        };
        if let Err(e) = body.consume_direct_read(received) {
            return (Err(BodyError::Malformed(e)), bufs);
        }
        (Ok(received), bufs)
    }

    /// Reads decoded body bytes into `output`. Returns `0` once the body ends.
    pub async fn read_into(&mut self, output: &mut [u8]) -> Result<usize, BodyError> {
        ensure_recv_body(self.active_mut()).await?;
        let active = self.active_mut();
        let IncomingReply::RecvBody(body) = &mut active.reply else {
            return Ok(0);
        };
        match read_body_into(&mut active.conn, body, output).await {
            Ok(n) => Ok(n),
            Err(BodyError::Io(e)) => {
                active.io_error = true;
                Err(BodyError::Io(e))
            }
            Err(other) => Err(other),
        }
    }
}

impl Drop for SidecarDirectBody {
    fn drop(&mut self) {
        if let Some(active) = self.active.take() {
            self.slot.put(active);
        }
    }
}

/// Chunk size used when decoding a request body through standard
/// [`http_body::Body`] (for `Bytes`, `Json`, `Body::into_data_stream`, etc.).
const STREAM_CHUNK: usize = 16 * 1024;

type ReadFut = Pin<Box<dyn Future<Output = (Option<Result<Bytes, io::Error>>, ConnSlot)> + Send>>;

enum IncomingState {
    Idle(ConnSlot),
    Reading(ReadFut),
    Done,
}

/// [`http_body::Body`] adapter for standard Axum request body extractors.
pub(crate) struct SidecarIncoming {
    state: IncomingState,
    remaining_hint: Option<u64>,
}

impl SidecarIncoming {
    pub(crate) fn new(slot: ConnSlot, remaining_hint: Option<u64>) -> Self {
        Self {
            state: IncomingState::Idle(slot),
            remaining_hint,
        }
    }
}

impl http_body::Body for SidecarIncoming {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            match std::mem::replace(&mut self.state, IncomingState::Done) {
                IncomingState::Done => return Poll::Ready(None),
                IncomingState::Idle(slot) => {
                    let Some(mut active) = slot.take() else {
                        return Poll::Ready(None);
                    };

                    // Fast synchronous path when bytes are already buffered in
                    // the head buffer and we don't need to send 100-continue.
                    if let IncomingReply::RecvBody(ref mut body) = active.reply {
                        if body.is_ended() {
                            slot.put(active);
                            return Poll::Ready(None);
                        }
                        if !active.conn.pending().is_empty() {
                            let mut buf = BytesMut::zeroed(STREAM_CHUNK);
                            match body.read(active.conn.pending(), &mut buf) {
                                Ok((used, produced)) => {
                                    active.conn.consume(used);
                                    if produced > 0 {
                                        buf.truncate(produced);
                                        if let Some(rem) = &mut self.remaining_hint {
                                            *rem = rem.saturating_sub(produced as u64);
                                        }
                                        slot.put(active);
                                        self.state = IncomingState::Idle(slot);
                                        return Poll::Ready(Some(Ok(Frame::data(buf.freeze()))));
                                    }
                                    if body.is_ended() {
                                        slot.put(active);
                                        return Poll::Ready(None);
                                    }
                                }
                                Err(e) => {
                                    slot.put(active);
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        e,
                                    ))));
                                }
                            }
                        }
                    }

                    // Need async I/O (accept 100-continue and/or recv from socket).
                    let fut: ReadFut = Box::pin(async move {
                        if let Err(err) = ensure_recv_body(&mut active).await {
                            slot.put(active);
                            let io_err = match err {
                                BodyError::Io(e) => e,
                                BodyError::Malformed(e) => {
                                    io::Error::new(io::ErrorKind::InvalidData, e)
                                }
                            };
                            return (Some(Err(io_err)), slot);
                        }
                        let IncomingReply::RecvBody(ref mut body) = active.reply else {
                            slot.put(active);
                            return (None, slot);
                        };
                        let mut buf = BytesMut::zeroed(STREAM_CHUNK);
                        let res = match read_body_into(&mut active.conn, body, &mut buf).await {
                            Ok(0) => None,
                            Ok(n) => {
                                buf.truncate(n);
                                Some(Ok(buf.freeze()))
                            }
                            Err(BodyError::Io(e)) => {
                                active.io_error = true;
                                Some(Err(e))
                            }
                            Err(BodyError::Malformed(e)) => {
                                Some(Err(io::Error::new(io::ErrorKind::InvalidData, e)))
                            }
                        };
                        slot.put(active);
                        (res, slot)
                    });
                    self.state = IncomingState::Reading(fut);
                }
                IncomingState::Reading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = IncomingState::Reading(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((item, slot)) => match item {
                        Some(Ok(bytes)) => {
                            if let Some(rem) = &mut self.remaining_hint {
                                *rem = rem.saturating_sub(bytes.len() as u64);
                            }
                            self.state = IncomingState::Idle(slot);
                            return Poll::Ready(Some(Ok(Frame::data(bytes))));
                        }
                        Some(Err(err)) => {
                            return Poll::Ready(Some(Err(err)));
                        }
                        None => {
                            return Poll::Ready(None);
                        }
                    },
                },
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.remaining_hint {
            Some(n) => SizeHint::with_exact(n),
            None => SizeHint::default(),
        }
    }
}
