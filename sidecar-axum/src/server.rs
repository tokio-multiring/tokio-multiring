//! HTTP/1.1 connection driver for `axum::Router` / `tower_service::Service`
//! on top of `tokio-sidecar` and `ureq-proto`.

use std::convert::Infallible;
use std::future::poll_fn;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use http::{HeaderValue, Request, Response, StatusCode, header};
use http_body::Body as HttpBody;
use http_body_util::BodyExt;
use tokio::sync::Semaphore;
use tokio_sidecar::{BufferPool, Sidecar};
use tower_service::Service;
use ureq_proto::server::state::{ProvideResponse, RecvBody};
use ureq_proto::server::{RecvRequestResult, Reply, SendResponseResult};

use crate::conn::{Conn, Filled};
use crate::incoming::{
    ActiveConn, BodyError, ConnSlot, DirectBodySlot as IncomingDirectSlot, IncomingReply,
    SidecarIncoming, read_body_into,
};
use crate::outgoing::{BodySink, DirectBodySlot as OutgoingDirectSlot};

/// The most bytes of an unread request body that are drained and discarded so
/// a keep-alive connection can be reused. Past this, the connection closes.
const DISCARD_LIMIT: usize = 64 * 1024;

/// Minimum payload size for `IORING_OP_SEND_ZC` on standard `axum::body::Body`
/// `Bytes` frames. Below this, copying in the kernel is cheaper than page
/// pinning and the extra completion notification.
const SEND_BYTES_ZC_THRESHOLD: usize = 64 * 1024;

/// Maximum slice size per `IORING_OP_SEND_ZC` submission on standard `Bytes`
/// frames, capping pinned unacknowledged memory per connection.
const SEND_BYTES_ZC_CHUNK: usize = 1 << 20;

/// Maximum slice size per copying `IORING_OP_SEND` submission (stays well
/// within io_uring's 32-bit length field).
const SEND_BYTES_MAX_CHUNK: usize = 1 << 30;

const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nconnection: close\r\ncontent-length: 0\r\n\r\n";
const HEAD_TOO_LARGE: &[u8] = b"HTTP/1.1 431 Request Header Fields Too Large\r\n\
    connection: close\r\ncontent-length: 0\r\n\r\n";

/// Which clients get zero-copy sends on [`BodySink::send_zero_copy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZeroCopy {
    Never,
    /// Clients on other machines (non-loopback peers).
    Remote,
    /// Loopback clients too. Useful for testing.
    Always,
}

/// Settings for [`Server`].
#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum concurrent connections served at a time.
    pub connections: usize,
    /// Size of each per-connection head buffer (input + output), in bytes.
    /// Must be a multiple of 4096.
    pub head_buffer: usize,
    /// Which clients enable zero-copy sends.
    pub zero_copy: ZeroCopy,
    /// Whether accepted connection tasks are spawned worker-local (pinned to
    /// the accepting worker's io_uring ring) or regular work-stealing tasks.
    pub worker_local_connections: bool,
    /// Optional timeout while waiting for the next request head on a keep-alive
    /// or newly accepted connection.
    pub idle_timeout: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connections: 1024,
            head_buffer: 16 * 1024,
            zero_copy: ZeroCopy::Remote,
            worker_local_connections: true,
            idle_timeout: None,
        }
    }
}

struct Shared {
    sidecar: Sidecar,
    heads: BufferPool,
    slots: Arc<Semaphore>,
    zero_copy: ZeroCopy,
    worker_local_connections: bool,
    idle_timeout: Option<Duration>,
}

/// Serves an Axum `Router` (or any compatible `tower_service::Service`) over a
/// [`Sidecar`].
#[derive(Clone)]
pub struct Server {
    shared: Arc<Shared>,
}

impl Server {
    /// Creates a new server backed by `sidecar`.
    pub fn new(sidecar: Sidecar, config: Config) -> io::Result<Self> {
        let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
        if config.connections == 0 {
            return Err(invalid("a server needs room for at least one connection"));
        }
        let heads = BufferPool::new(config.head_buffer, 2 * config.connections)?;
        Ok(Self {
            shared: Arc::new(Shared {
                sidecar,
                heads,
                slots: Arc::new(Semaphore::new(config.connections)),
                zero_copy: config.zero_copy,
                worker_local_connections: config.worker_local_connections,
                idle_timeout: config.idle_timeout,
            }),
        })
    }

    /// Starts accepting connections from `listener` on every worker of the
    /// current multi-threaded runtime, driving `service` for every request.
    ///
    /// # Panics
    ///
    /// Panics if called outside a multi-threaded Tokio runtime.
    pub fn listen<S>(&self, listener: TcpListener, service: S)
    where
        S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
            + Clone
            + Send
            + 'static,
        S::Future: Send,
    {
        let listener = Arc::new(OwnedFd::from(listener));
        let shared = self.shared.clone();
        tokio::task::run_on_each_worker(|_| {
            let shared = shared.clone();
            let listener = listener.clone();
            let service = service.clone();
            move || {
                tokio::task::spawn_worker_local(accept_loop(shared, listener, service));
            }
        });
    }
}

async fn accept_loop<S>(shared: Arc<Shared>, listener: Arc<OwnedFd>, service: S)
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    loop {
        let slot = shared
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore is never closed");
        let socket = match shared.sidecar.accept(listener.clone()).await {
            Ok(socket) => socket,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                return log::error!("cannot accept on listener: {error}");
            }
            Err(error) => {
                log::warn!("accept failed: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };

        let socket = TcpStream::from(socket);
        if let Err(error) = socket.set_nodelay(true) {
            log::debug!("could not disable Nagle's algorithm: {error}");
        }
        let zero_copy = match shared.zero_copy {
            ZeroCopy::Never => false,
            ZeroCopy::Always => true,
            ZeroCopy::Remote => socket
                .peer_addr()
                .is_ok_and(|peer| !peer.ip().to_canonical().is_loopback()),
        };

        let buf = || {
            shared
                .heads
                .try_acquire()
                .expect("a connection slot guarantees two head buffers")
        };
        let conn = Conn::new(
            shared.sidecar.clone(),
            socket.into(),
            buf(),
            buf(),
            zero_copy,
        );
        let svc = service.clone();
        let idle_timeout = shared.idle_timeout;
        let fut = async move {
            serve_connection(conn, svc, idle_timeout).await;
            drop(slot);
        };
        if shared.worker_local_connections {
            tokio::task::run_on_any_worker(move || {
                tokio::task::spawn_worker_local(fut);
            });
        } else {
            tokio::spawn(fut);
        }
    }
}

enum Fatal {
    Io(io::Error),
    Protocol(ureq_proto::Error),
}

impl From<io::Error> for Fatal {
    fn from(error: io::Error) -> Self {
        Fatal::Io(error)
    }
}

impl From<ureq_proto::Error> for Fatal {
    fn from(error: ureq_proto::Error) -> Self {
        Fatal::Protocol(error)
    }
}

async fn serve_connection<S>(mut conn: Conn, mut service: S, idle_timeout: Option<Duration>)
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
{
    loop {
        match exchange(conn, &mut service, idle_timeout).await {
            Ok(Some(next_conn)) => conn = next_conn,
            Ok(None) => return,
            Err(Fatal::Io(error)) => return log::debug!("connection lost: {error}"),
            Err(Fatal::Protocol(error)) => {
                return log::debug!("connection protocol error: {error}");
            }
        }
    }
}

fn wants_close(request: &Request<()>) -> bool {
    request
        .headers()
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|option| option.trim().eq_ignore_ascii_case("close"))
}

fn has_connection_close(headers: &http::HeaderMap) -> bool {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|option| option.trim().eq_ignore_ascii_case("close"))
}

enum Discarded {
    All(Reply<ProvideResponse>),
    Abandoned(Reply<ProvideResponse>),
}

async fn discard(conn: &mut Conn, mut body: Reply<RecvBody>) -> Result<Discarded, Fatal> {
    let mut scratch = [0u8; 1024];
    let mut discarded = 0usize;
    while discarded <= DISCARD_LIMIT {
        match read_body_into(conn, &mut body, &mut scratch).await {
            Ok(0) => return Ok(Discarded::All(body.proceed()?)),
            Ok(produced) => discarded += produced,
            Err(BodyError::Io(err)) => return Err(Fatal::Io(err)),
            Err(BodyError::Malformed(_)) => break,
        }
    }
    Ok(Discarded::Abandoned(body.abandon()))
}

/// Runs one HTTP/1.1 exchange. Returns `Ok(Some(conn))` when the connection
/// can be reused for another request, or `Ok(None)` once closed cleanly.
async fn exchange<S>(
    mut conn: Conn,
    service: &mut S,
    idle_timeout: Option<Duration>,
) -> Result<Option<Conn>, Fatal>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
{
    let mut reply = Reply::new()?;
    let head = loop {
        match reply.try_request(conn.pending()) {
            Ok((used, Some(request))) => {
                conn.consume(used);
                break request;
            }
            Ok((_, None)) => {
                let filled = match idle_timeout {
                    Some(limit) => match tokio::time::timeout(limit, conn.fill()).await {
                        Ok(res) => res?,
                        Err(_) => {
                            conn.close().await;
                            return Ok(None);
                        }
                    },
                    None => conn.fill().await?,
                };
                match filled {
                    Filled::Data => {}
                    Filled::Closed => {
                        conn.close().await;
                        return Ok(None);
                    }
                    Filled::Full => {
                        refuse(&mut conn, HEAD_TOO_LARGE).await?;
                        conn.close().await;
                        return Ok(None);
                    }
                }
            }
            Err(_) => {
                refuse(&mut conn, BAD_REQUEST).await?;
                conn.close().await;
                return Ok(None);
            }
        }
    };

    let client_wants_close = wants_close(&head);
    let next = reply.proceed().expect("a parsed request can proceed");

    // Fast path: no request body (GET, HEAD, DELETE, Content-Length: 0).
    // Avoids allocating a ConnSlot or Request body wrapper altogether.
    let (mut conn, response, reply, force_close) = match next {
        RecvRequestResult::ProvideResponse(reply) => {
            let (parts, ()) = head.into_parts();
            let req = Request::from_parts(parts, Body::empty());
            let Ok(()) = poll_fn(|cx| service.poll_ready(cx)).await;
            let Ok(response) = service.call(req).await;
            (conn, response, reply, false)
        }
        RecvRequestResult::Send100(send100) => {
            let remaining_hint = head
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            run_with_body(
                conn,
                head,
                IncomingReply::Send100(send100),
                remaining_hint,
                service,
            )
            .await?
        }
        RecvRequestResult::RecvBody(recv_body) => {
            let remaining_hint = recv_body.remaining();
            run_with_body(
                conn,
                head,
                IncomingReply::RecvBody(recv_body),
                remaining_hint,
                service,
            )
            .await?
        }
    };

    let keep_alive = respond(
        &mut conn,
        reply,
        response,
        force_close || client_wants_close,
    )
    .await?;
    Ok(if keep_alive {
        Some(conn)
    } else {
        conn.close().await;
        None
    })
}

async fn run_with_body<S>(
    conn: Conn,
    head: Request<()>,
    incoming: IncomingReply,
    remaining_hint: Option<u64>,
    service: &mut S,
) -> Result<(Conn, Response<Body>, Reply<ProvideResponse>, bool), Fatal>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
{
    let slot = ConnSlot::new(ActiveConn {
        conn,
        reply: incoming,
        io_error: false,
    });
    let (mut parts, ()) = head.into_parts();
    parts.extensions.insert(IncomingDirectSlot(slot.clone()));
    let body = Body::new(SidecarIncoming::new(slot.clone(), remaining_hint));
    let req = Request::from_parts(parts, body);

    let Ok(()) = poll_fn(|cx| service.poll_ready(cx)).await;
    let Ok(response) = service.call(req).await;

    let Some(mut active) = slot.take() else {
        // Handler retained the body beyond returning a response (e.g. moved it
        // to another task). We cannot safely reuse the connection.
        return Err(Fatal::Io(io::Error::other(
            "handler did not release request body before returning response",
        )));
    };
    if active.io_error {
        return Err(Fatal::Io(io::Error::from(io::ErrorKind::ConnectionReset)));
    }

    let (reply, force_close) = match active.reply {
        IncomingReply::Send100(reply) => {
            // Answering without sending 100-continue closes the connection:
            // the client may still send the body after a timeout.
            (reply.reject(), true)
        }
        IncomingReply::RecvBody(body) if body.is_ended() => (body.proceed()?, false),
        IncomingReply::RecvBody(body) => match discard(&mut active.conn, body).await? {
            Discarded::All(reply) => (reply, false),
            Discarded::Abandoned(reply) => (reply, true),
        },
        IncomingReply::Taken => {
            return Err(Fatal::Io(io::Error::other(
                "request body state machine was interrupted",
            )));
        }
    };

    Ok((active.conn, response, reply, force_close))
}

/// Sends `response` over `conn`. Returns `Ok(true)` if the connection can
/// carry another request, or `Ok(false)` if it must close.
async fn respond(
    conn: &mut Conn,
    reply: Reply<ProvideResponse>,
    response: Response<Body>,
    force_close: bool,
) -> Result<bool, Fatal> {
    let (mut parts, mut body) = response.into_parts();

    let direct = parts
        .extensions
        .remove::<OutgoingDirectSlot>()
        .and_then(|slot| slot.take())
        .filter(|_| body.is_end_stream());

    if force_close && !has_connection_close(&parts.headers) {
        parts
            .headers
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
    }

    // Match Hyper's behavior: if the handler didn't specify Content-Length or
    // Transfer-Encoding, populate Content-Length from Body::size_hint().exact()
    // so sized bodies (String, Vec<u8>, Json, empty) don't fall back to chunked.
    if direct.is_none()
        && !parts.headers.contains_key(header::CONTENT_LENGTH)
        && !parts.headers.contains_key(header::TRANSFER_ENCODING)
        && !parts.status.is_informational()
        && parts.status != StatusCode::NO_CONTENT
        && parts.status != StatusCode::NOT_MODIFIED
        && let Some(exact) = body.size_hint().exact()
    {
        parts
            .headers
            .insert(header::CONTENT_LENGTH, HeaderValue::from(exact));
    }

    let head = Response::from_parts(parts, ());
    let mut head_writer = reply.provide(head)?;
    while !head_writer.is_finished() {
        let output = conn.output()?;
        let stuck = output.is_empty();
        let used = head_writer.write(output.spare_capacity_mut())?;
        output.grow(used);
        if used == 0 {
            if stuck {
                return Err(io::Error::other("response head does not fit output buffer").into());
            }
            conn.flush().await?;
        }
    }

    let cleanup = match head_writer.proceed() {
        SendResponseResult::Cleanup(cleanup) => {
            conn.flush().await?;
            cleanup
        }
        SendResponseResult::SendBody(mut send_body) => {
            if let Some(writer) = direct {
                let mut sink = BodySink::new(conn, &mut send_body);
                writer.write_to(&mut sink).await?;
            } else {
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|e| Fatal::Io(io::Error::other(e)))?;
                    if let Ok(data) = frame.into_data() {
                        if data.is_empty() {
                            continue;
                        }
                        let fits_output = conn.output()?.spare_capacity_mut().len() >= data.len();
                        if !send_body.is_chunked() && !fits_output {
                            conn.flush().await?;
                            let use_zc = conn.zero_copy() && data.len() >= SEND_BYTES_ZC_THRESHOLD;
                            let max_chunk = if use_zc {
                                SEND_BYTES_ZC_CHUNK
                            } else {
                                SEND_BYTES_MAX_CHUNK
                            };
                            if data.len() <= max_chunk {
                                send_body.consume_direct_write(data.len())?;
                                if use_zc {
                                    conn.send_bytes_zc(data).await?;
                                } else {
                                    conn.send_bytes(data).await?;
                                }
                            } else {
                                let mut offset = 0;
                                while offset < data.len() {
                                    let end = (offset + max_chunk).min(data.len());
                                    let chunk = data.slice(offset..end);
                                    send_body.consume_direct_write(chunk.len())?;
                                    if use_zc && chunk.len() >= SEND_BYTES_ZC_THRESHOLD {
                                        conn.send_bytes_zc(chunk).await?;
                                    } else {
                                        conn.send_bytes(chunk).await?;
                                    }
                                    offset = end;
                                }
                            }
                            continue;
                        }
                        let mut rest = data.as_ref();
                        while !rest.is_empty() {
                            let output = conn.output()?;
                            let (used, produced) =
                                send_body.write(rest, output.spare_capacity_mut())?;
                            output.grow(produced);
                            rest = &rest[used..];
                            if used == 0 {
                                conn.flush().await?;
                            }
                        }
                    }
                }
            }

            // Finish chunked or close-delimited body (writes `0\r\n\r\n` for chunked).
            while !send_body.is_finished() {
                let output = conn.output()?;
                let stuck = output.is_empty();
                let (_, produced) = send_body.write(&[], output.spare_capacity_mut())?;
                output.grow(produced);
                if !send_body.is_finished() {
                    if produced == 0 && stuck {
                        return Err(io::Error::other("response body ended short").into());
                    }
                    conn.flush().await?;
                }
            }

            conn.flush().await?;
            send_body.proceed()
        }
    };

    Ok(!cleanup.must_close_connection())
}

async fn refuse(conn: &mut Conn, response: &[u8]) -> Result<(), Fatal> {
    conn.output()?.extend_from_slice(response);
    conn.flush().await?;
    Ok(())
}
