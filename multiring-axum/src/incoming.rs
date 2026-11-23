//! Request-body bridge between `ureq-proto` + `tokio-multiring` and Axum.
//!
//! Two interfaces share the same underlying connection handoff slot:
//!
//! - [`MultiRingIncoming`] implements [`http_body::Body`] so every standard Axum
//!   extractor (`Bytes`, `String`, `Json`, `Body`, Tower middleware) works
//!   unmodified.
//! - [`DirectBody`] implements [`axum::extract::FromRequest`] so handlers that
//!   want direct io_uring `recv_vectored` into registered [`FixedBuf`]s (and
//!   explicit `100 Continue` timing) can claim the connection without copying.

use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{FromRequest, Request};
use bytes::{Bytes, BytesMut};
use http_body::{Frame, SizeHint};
use tokio_multiring::{FixedBuf, Segment};

use crate::conn::{Conn, Filled};

/// Live read half loaned out while a handler or response consumes its body.
pub(crate) struct ActiveConn {
    pub(crate) conn: Conn,
    pub(crate) body: ureq_proto::server::RequestBody,
    pub(crate) io_error: bool,
}

enum ContinueState {
    Pending(Bytes),
    Sending {
        expected: usize,
        operation: Pin<Box<dyn Future<Output = io::Result<usize>> + Send>>,
    },
    Accepted,
    Rejected,
    Failed,
}

/// Serializes the optional interim response against the final response head.
struct ContinueGate {
    state: tokio::sync::Mutex<ContinueState>,
    multiring: tokio_multiring::MultiRing,
    fd: Arc<std::os::fd::OwnedFd>,
}

impl ContinueGate {
    /// Complete any started interim send before permitting final output. Its
    /// future lives in the gate, so cancelling an individual body read neither
    /// cancels nor duplicates a partially submitted 100 response.
    async fn prepare(&self, receive: bool) -> io::Result<bool> {
        let mut state = self.state.lock().await;
        if let ContinueState::Pending(bytes) = &*state {
            if !receive {
                *state = ContinueState::Rejected;
                return Ok(true);
            }
            let bytes = bytes.clone();
            let expected = bytes.len();
            let multiring = self.multiring.clone();
            let fd = self.fd.clone();
            *state = ContinueState::Sending {
                expected,
                operation: Box::pin(async move { multiring.send_bytes(fd, bytes).await }),
            };
        }
        if let ContinueState::Sending {
            expected,
            operation,
        } = &mut *state
        {
            let result = match operation.as_mut().await {
                Ok(sent) if sent == *expected => Ok(()),
                Ok(_) => Err(io::ErrorKind::WriteZero.into()),
                Err(error) => Err(error),
            };
            *state = if result.is_ok() {
                ContinueState::Accepted
            } else {
                ContinueState::Failed
            };
            result?;
        }
        match *state {
            ContinueState::Accepted => Ok(false),
            ContinueState::Rejected if !receive => Ok(true),
            ContinueState::Rejected => {
                Err(io::Error::other("request body rejected by final response"))
            }
            ContinueState::Failed => Err(io::Error::other("interim response failed")),
            _ => unreachable!("pending interim state resolved above"),
        }
    }

    async fn accept(&self) -> io::Result<()> {
        self.prepare(true).await.map(drop)
    }
}

#[derive(Default)]
struct Readers {
    count: AtomicUsize,
    changed: tokio::sync::Notify,
}

/// Counts actual body consumers, independently of temporary ConnSlot clones.
/// Decrement BEFORE notification, so a concurrent waiter cannot miss the drop.
struct ReaderLease(Arc<Readers>);

impl Clone for ReaderLease {
    fn clone(&self) -> Self {
        self.0.count.fetch_add(1, Ordering::AcqRel);
        Self(self.0.clone())
    }
}

impl Drop for ReaderLease {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

/// Shared ownership of the request read half. The response driver owns one
/// reference; an incoming Body (or its pending read) owns another.
#[derive(Clone)]
pub(crate) struct ConnSlot {
    inner: Arc<Mutex<Option<Box<ActiveConn>>>>,
    gate: Arc<ContinueGate>,
    readers: Arc<Readers>,
}

impl ConnSlot {
    pub(crate) fn new(mut active: ActiveConn) -> Result<Self, BodyError> {
        let (multiring, fd) = active.conn.transport();
        let mut interim = [0; 64];
        let used = active
            .body
            .accept_continue(&mut interim)
            .map_err(BodyError::Malformed)?;
        let state = if used == 0 {
            ContinueState::Accepted
        } else {
            ContinueState::Pending(Bytes::copy_from_slice(&interim[..used]))
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(Box::new(active)))),
            readers: Arc::new(Readers::default()),
            gate: Arc::new(ContinueGate {
                state: tokio::sync::Mutex::new(state),
                multiring,
                fd,
            }),
        })
    }

    pub(crate) fn take(&self) -> Option<Box<ActiveConn>> {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn put(&self, active: Box<ActiveConn>) {
        *self.inner.lock().unwrap_or_else(PoisonError::into_inner) = Some(active);
        self.readers.changed.notify_waiters();
    }

    fn lease(&self) -> ReaderLease {
        self.readers.count.fetch_add(1, Ordering::AcqRel);
        ReaderLease(self.readers.clone())
    }

    /// A consumer may outlive the response and still legitimately read input.
    /// Wait for EOF/error or for the last Body owner to release its lease before
    /// taking over its framing state to drain and reuse the connection.
    pub(crate) async fn wait_for_reader(&self) {
        loop {
            let changed = self.readers.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let finished = self
                .inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .is_some_and(|active| active.io_error || active.body.is_ended());
            if finished || self.readers.count.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }

    pub(crate) async fn accept_continue(&self) -> Result<(), BodyError> {
        self.gate.accept().await.map_err(BodyError::Io)
    }

    /// If the source body is still alive, a streaming response may need it.
    /// Otherwise a pending Expect request is rejected without sending 100.
    /// Returns whether rejection requires closing after the final response.
    pub(crate) async fn before_response(&self, discard_limit: usize) -> io::Result<bool> {
        if self.readers.count.load(Ordering::Acquire) != 0 {
            self.gate.accept().await?;
            return Ok(false);
        }
        if self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
        {
            return Err(io::Error::other("request body read was cancelled"));
        }
        let rejected = self.gate.prepare(false).await?;
        let unread_is_large = self
            .inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_none_or(|active| {
                active.io_error
                    || (!active.body.is_ended()
                        && active
                            .body
                            .remaining()
                            .is_none_or(|n| n > discard_limit as u64))
            });
        Ok(rejected || unread_is_large)
    }
}

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

/// Decodes body bytes from `conn` into `output` (which must be non-empty).
/// Returns `0` only once the body has ended.
pub(crate) async fn read_body_into(
    conn: &mut Conn,
    body: &mut ureq_proto::server::RequestBody,
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
/// When running on `multiring-axum`, this claims the underlying connection and
/// `ureq-proto` body state directly — enabling `recv_vectored` into
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
    Direct(MultiRingDirectBody),
    /// Standard Axum body (when served by Hyper / `axum::serve` or after a
    /// body-wrapping middleware).
    Fallback(Body),
}

impl<S: Send + Sync> FromRequest<S> for DirectBody {
    type Rejection = Infallible;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        // The capability belongs to the concrete Body, not the request's
        // extensions: replacing or wrapping that Body must select the fallback.
        if let Some(incoming) = req.body().downcast_ref::<MultiRingIncoming>()
            && let IncomingState::Idle(slot) = &incoming.state
            && let Some(active) = slot.take()
        {
            return Ok(DirectBody::Direct(MultiRingDirectBody {
                slot: slot.clone(),
                active: Some(active),
                _lease: incoming.lease.clone(),
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
/// Returns the request read half to the server automatically on drop so it
/// can drain unread input and decide whether the connection can be reused.
pub struct MultiRingDirectBody {
    slot: ConnSlot,
    active: Option<Box<ActiveConn>>,
    _lease: ReaderLease,
}

impl MultiRingDirectBody {
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
        self.slot.accept_continue().await
    }

    /// Remaining bytes for a length-delimited body, or `None` for chunked framing.
    pub fn remaining(&self) -> Option<u64> {
        self.active().body.remaining()
    }

    /// Whether the entire request body has been consumed.
    pub fn is_ended(&self) -> bool {
        self.active().body.is_ended()
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
        if let Err(err) = self.slot.accept_continue().await {
            return (Err(err), bufs);
        }
        let expected: usize = segments.iter().map(|s| s.len).sum();
        let active = self.active_mut();
        if active.io_error {
            return (
                Err(BodyError::Io(io::Error::other(
                    "an earlier body read was interrupted",
                ))),
                bufs,
            );
        }
        // A cancelled receive may have consumed bytes without returning their
        // count. Poison until both transport and framing have been accounted.
        active.io_error = true;
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
        let body = &mut active.body;
        if let Err(e) = body.consume_direct_read(received) {
            return (Err(BodyError::Malformed(e)), bufs);
        }
        active.io_error = false;
        (Ok(received), bufs)
    }

    /// Reads decoded body bytes into `output`. Returns `0` once the body ends.
    pub async fn read_into(&mut self, output: &mut [u8]) -> Result<usize, BodyError> {
        self.slot.accept_continue().await?;
        let active = self.active_mut();
        if active.io_error {
            return Err(BodyError::Io(io::Error::other(
                "an earlier body read was interrupted",
            )));
        }
        active.io_error = true;
        let body = &mut active.body;
        match read_body_into(&mut active.conn, body, output).await {
            Ok(n) => {
                active.io_error = false;
                Ok(n)
            }
            Err(BodyError::Io(e)) => {
                active.io_error = true;
                Err(BodyError::Io(e))
            }
            Err(other) => Err(other),
        }
    }
}

impl Drop for MultiRingDirectBody {
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
pub(crate) struct MultiRingIncoming {
    state: IncomingState,
    remaining_hint: Option<u64>,
    // Declared last so a pending read is cancelled before the lease wakes the
    // connection driver during Drop.
    lease: ReaderLease,
}

impl MultiRingIncoming {
    pub(crate) fn new(slot: ConnSlot, remaining_hint: Option<u64>) -> Self {
        let lease = slot.lease();
        Self {
            state: IncomingState::Idle(slot),
            remaining_hint,
            lease,
        }
    }
}

impl http_body::Body for MultiRingIncoming {
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
                    if slot
                        .gate
                        .state
                        .try_lock()
                        .is_ok_and(|state| matches!(*state, ContinueState::Accepted))
                    {
                        let body = &mut active.body;
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
                                    active.io_error = true;
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
                        if let Err(err) = slot.accept_continue().await {
                            slot.put(active);
                            let io_err = match err {
                                BodyError::Io(e) => e,
                                BodyError::Malformed(e) => {
                                    io::Error::new(io::ErrorKind::InvalidData, e)
                                }
                            };
                            return (Some(Err(io_err)), slot);
                        }
                        let body = &mut active.body;
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
                                active.io_error = true;
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
