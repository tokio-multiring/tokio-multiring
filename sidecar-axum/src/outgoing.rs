//! Zero-copy / registered-buffer response body fast path through Axum's
//! [`http::Response<axum::body::Body>`].

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use tokio_sidecar::{FixedBuf, Reclaim, Segment};
use ureq_proto::server::Reply;
use ureq_proto::server::state::SendBody;

use crate::conn::Conn;

/// Sink passed to [`DirectWrite::write_to`] for writing a response body
/// directly onto the worker's io_uring connection.
pub struct BodySink<'a> {
    conn: &'a mut Conn,
    body: &'a mut Reply<SendBody>,
}

impl<'a> BodySink<'a> {
    pub(crate) fn new(conn: &'a mut Conn, body: &'a mut Reply<SendBody>) -> Self {
        Self { conn, body }
    }

    /// Whether zero-copy sends are enabled for this client.
    pub fn zero_copy(&self) -> bool {
        self.conn.zero_copy()
    }

    /// Whether this response is using chunked transfer encoding.
    pub fn is_chunked(&self) -> bool {
        self.body.is_chunked()
    }

    /// Flushes any buffered response headers or bytes.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.conn.flush().await
    }

    /// Sends `segments` of `bufs` in one vectored io_uring send.
    ///
    /// If the response is non-chunked and the response headers are still in
    /// the connection's output buffer, headers and body are coalesced into a
    /// single vectored send operation.
    pub async fn send_vectored(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        if self.body.is_chunked() {
            for seg in segments {
                let slice = &bufs[seg.buf][seg.start..seg.start + seg.len];
                if let Err(e) = self.write_bytes(slice).await {
                    return (Err(e), bufs);
                }
            }
            return (Ok(()), bufs);
        }
        let len: usize = segments.iter().map(|s| s.len).sum();
        let (sent, bufs) = self.conn.send_vectored(bufs, segments).await;
        if let Err(e) = sent {
            return (Err(e), bufs);
        }
        if let Err(e) = self.body.consume_direct_write(len) {
            return (Err(io::Error::new(io::ErrorKind::InvalidData, e)), bufs);
        }
        (Ok(()), bufs)
    }

    /// Sends `segments` of `bufs` without copying (`IORING_OP_SEND_ZC`).
    ///
    /// Only valid for non-chunked responses (e.g. when `Content-Length` is set).
    /// Flushes any pending response headers before issuing the zero-copy send.
    pub async fn send_zero_copy(
        &mut self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> (io::Result<()>, Option<Reclaim>) {
        if self.body.is_chunked() {
            return (
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "send_zero_copy requires a known Content-Length (non-chunked response)",
                )),
                None,
            );
        }
        if let Err(e) = self.conn.flush().await {
            return (Err(e), None);
        }
        let len: usize = segments.iter().map(|s| s.len).sum();
        let (sent, reclaim) = self.conn.send_zero_copy(bufs, segments).await;
        if let Err(e) = sent {
            return (Err(e), Some(reclaim));
        }
        if let Err(e) = self.body.consume_direct_write(len) {
            return (
                Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                Some(reclaim),
            );
        }
        (Ok(()), Some(reclaim))
    }

    /// Writes `bytes` through the HTTP response encoder (handles chunked or
    /// content-length framing automatically).
    pub async fn write_bytes(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let output = self.conn.output()?;
            let (used, produced) = self
                .body
                .write(bytes, output.spare_capacity_mut())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            output.grow(produced);
            bytes = &bytes[used..];
            if used == 0 {
                self.conn.flush().await?;
            }
        }
        Ok(())
    }
}

/// A response body producer that writes straight to a [`BodySink`] via
/// io_uring (registered buffers, vectored sends, `send_zc`) without boxing
/// chunks into [`bytes::Bytes`].
pub trait DirectWrite: Send + 'static {
    fn write_to<'a>(
        self: Box<Self>,
        sink: &'a mut BodySink<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// Extension stored on `http::Response<axum::body::Body>` to carry a
/// [`DirectWrite`] through Axum routers and Tower middleware.
#[derive(Clone)]
pub(crate) struct DirectBodySlot(Arc<Mutex<Option<Box<dyn DirectWrite>>>>);

impl DirectBodySlot {
    pub(crate) fn take(&self) -> Option<Box<dyn DirectWrite>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

/// Builds an [`axum::response::Response`] whose body is produced directly via
/// [`DirectWrite`] when served by `sidecar-axum`.
///
/// The caller should set `Content-Length` on `head` if known (required for
/// zero-copy sends). Any Tower middleware that inspects or adds response
/// headers continues to work transparently.
pub fn direct_response(head: http::Response<()>, writer: impl DirectWrite) -> Response {
    let (parts, ()) = head.into_parts();
    let mut response = Response::from_parts(parts, Body::empty());
    response
        .extensions_mut()
        .insert(DirectBodySlot(Arc::new(Mutex::new(Some(Box::new(writer))))));
    response
}

/// Convenience wrapper implementing [`IntoResponse`] for a [`DirectWrite`]
/// paired with response status and headers.
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
