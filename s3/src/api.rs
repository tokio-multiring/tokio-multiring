//! The HTTP exchange: one request in, one response out, as often as the
//! client keeps the connection.

use std::collections::VecDeque;
use std::io;
use std::ops::Range;

use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use tokio_sidecar::FixedBuf;
use ureq_proto::server::state::{ProvideResponse, RecvBody, Send100, SendBody};
use ureq_proto::server::{RecvRequestResult, Reply, SendResponseResult};

use crate::Inner;
use crate::conn::{Conn, Filled};
use crate::date;
use crate::error::ApiError;
use crate::join::Started;
use crate::name::{Bucket, Object, Target};
use crate::store::{Meta, StoreError, Stored, Upload};

/// The most bytes of an unwanted request body that are read and thrown
/// away to keep the connection usable. Past that the connection is closed.
const DISCARD_LIMIT: usize = 64 * 1024;

// Sent when there is no request to build an answer to.
const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nconnection: close\r\ncontent-length: 0\r\n\r\n";
const HEAD_TOO_LARGE: &[u8] = b"HTTP/1.1 431 Request Header Fields Too Large\r\n\
    connection: close\r\ncontent-length: 0\r\n\r\n";

/// Why a connection cannot go on.
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

fn closed() -> Fatal {
    Fatal::Io(io::ErrorKind::UnexpectedEof.into())
}

/// What a request asks for.
enum Action {
    CreateBucket(Bucket),
    DeleteBucket(Bucket),
    HeadBucket(Bucket),
    PutObject(Object),
    GetObject {
        object: Object,
        range: Option<HeaderValue>,
        /// Whether the client only wants the headers.
        head: bool,
    },
    DeleteObject(Object),
}

impl Action {
    fn of(request: &Request<()>) -> Result<Action, ApiError> {
        let uri = request.uri();
        let target = Target::parse(uri.path())?;
        // Queries select the operations this does not have: listings,
        // multipart uploads, tagging and the like.
        if uri.query().is_some_and(|query| !query.is_empty()) {
            return Err(ApiError::NOT_IMPLEMENTED);
        }

        let method = request.method();
        let get_object = |object, head| Action::GetObject {
            object,
            range: request.headers().get(header::RANGE).cloned(),
            head,
        };
        Ok(match target {
            Target::Bucket(bucket) if method == Method::PUT => Action::CreateBucket(bucket),
            Target::Bucket(bucket) if method == Method::DELETE => Action::DeleteBucket(bucket),
            Target::Bucket(bucket) if method == Method::HEAD => Action::HeadBucket(bucket),
            Target::Object(object) if method == Method::PUT => Action::PutObject(object),
            Target::Object(object) if method == Method::GET => get_object(object, false),
            Target::Object(object) if method == Method::HEAD => get_object(object, true),
            Target::Object(object) if method == Method::DELETE => Action::DeleteObject(object),
            // Listing buckets and objects.
            Target::Service | Target::Bucket(_) if method == Method::GET => {
                return Err(ApiError::NOT_IMPLEMENTED);
            }
            _ => return Err(ApiError::METHOD_NOT_ALLOWED),
        })
    }
}

/// A response, and where its body comes from.
struct Answer {
    response: Response<()>,
    payload: Payload,
}

enum Payload {
    None,
    Bytes(Vec<u8>),
    Object { stored: Stored, range: Range<u64> },
}

fn response(status: StatusCode) -> http::response::Builder {
    Response::builder()
        .status(status)
        .header(header::DATE, date::now())
}

fn entity_tag(stat: &Meta) -> String {
    let (seconds, nanoseconds) = stat.modified;
    format!("\"{seconds:x}{nanoseconds:08x}-{:x}\"", stat.size)
}

impl Answer {
    fn new(response: http::response::Builder, payload: Payload) -> Answer {
        let response = response
            .body(())
            .expect("status and headers are built from valid parts");
        Answer { response, payload }
    }

    /// A response without a body.
    fn empty(status: StatusCode) -> Answer {
        let mut response = response(status);
        if status != StatusCode::NO_CONTENT {
            response = response.header(header::CONTENT_LENGTH, 0);
        }
        Answer::new(response, Payload::None)
    }

    fn error(error: ApiError) -> Answer {
        let body = error.body();
        let response = response(error.status)
            .header(header::CONTENT_TYPE, "application/xml")
            .header(header::CONTENT_LENGTH, body.len());
        Answer::new(response, Payload::Bytes(body))
    }

    fn failed(error: StoreError) -> Answer {
        match error {
            StoreError::Api(error) => Answer::error(error),
            StoreError::Io(error) => {
                log::error!("storage failed: {error}");
                Answer::error(ApiError::INTERNAL)
            }
        }
    }

    /// Tells the client that this is the last response on the connection.
    fn closing(mut self) -> Answer {
        self.response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        self
    }

    /// The object was stored.
    fn stored(stat: &Meta) -> Answer {
        let response = response(StatusCode::OK)
            .header(header::ETAG, entity_tag(stat))
            .header(header::CONTENT_LENGTH, 0);
        Answer::new(response, Payload::None)
    }

    /// The object, or the part of it that `range` asks for. Without
    /// `stored`, the headers alone.
    fn object(stat: Meta, range: Option<&HeaderValue>, stored: Option<Stored>) -> Answer {
        let (status, range) = match Requested::parse(range, stat.size) {
            Requested::Whole => (StatusCode::OK, 0..stat.size),
            Requested::Part(range) => (StatusCode::PARTIAL_CONTENT, range),
            Requested::Unsatisfiable => {
                let mut answer = Answer::error(ApiError::INVALID_RANGE);
                let total = HeaderValue::try_from(format!("bytes */{}", stat.size));
                answer.response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    total.expect("digits make a valid header value"),
                );
                return answer;
            }
        };

        let mut response = response(status)
            .header(header::CONTENT_TYPE, "binary/octet-stream")
            .header(header::CONTENT_LENGTH, range.end - range.start)
            .header(header::ETAG, entity_tag(&stat))
            .header(header::LAST_MODIFIED, date::http_date(stat.modified.0))
            .header(header::ACCEPT_RANGES, "bytes");
        if status == StatusCode::PARTIAL_CONTENT {
            let part = format!("bytes {}-{}/{}", range.start, range.end - 1, stat.size);
            response = response.header(header::CONTENT_RANGE, part);
        }
        let payload = match stored {
            Some(stored) => Payload::Object { stored, range },
            None => Payload::None,
        };
        Answer::new(response, payload)
    }
}

/// What a `Range` header asks for, of an object of a known size.
#[derive(Debug, PartialEq, Eq)]
enum Requested {
    /// Everything: there is no header, or not one this understands, which
    /// HTTP says to treat the same.
    Whole,
    Part(Range<u64>),
    Unsatisfiable,
}

impl Requested {
    fn parse(header: Option<&HeaderValue>, size: u64) -> Requested {
        let spec = header
            .and_then(|header| header.to_str().ok())
            .and_then(|header| header.strip_prefix("bytes="));
        let Some((first, last)) = spec.and_then(|spec| spec.split_once('-')) else {
            return Requested::Whole;
        };

        if first.is_empty() {
            // The last so many bytes.
            return match last.parse::<u64>() {
                Ok(0) => Requested::Unsatisfiable,
                Ok(_) if size == 0 => Requested::Unsatisfiable,
                Ok(count) => Requested::Part(size.saturating_sub(count)..size),
                Err(_) => Requested::Whole,
            };
        }
        let Ok(start) = first.parse::<u64>() else {
            return Requested::Whole;
        };
        let end = if last.is_empty() {
            size
        } else {
            match last.parse::<u64>() {
                Ok(last) if last >= start => last.saturating_add(1).min(size),
                _ => return Requested::Whole,
            }
        };
        if start >= size {
            return Requested::Unsatisfiable;
        }
        Requested::Part(start..end)
    }
}

/// A request's body, if it has one.
enum Incoming {
    Body(Reply<RecvBody>),
    Empty(Reply<ProvideResponse>),
}

/// A request that has been dealt with, and what to answer it.
struct Outcome {
    reply: Reply<ProvideResponse>,
    answer: Answer,
}

impl Outcome {
    fn new(reply: Reply<ProvideResponse>, answer: Answer) -> Outcome {
        Outcome { reply, answer }
    }

    /// For a request whose body was left unread, after which the connection
    /// cannot carry another.
    fn closing(reply: Reply<ProvideResponse>, answer: Answer) -> Outcome {
        Outcome::new(reply, answer.closing())
    }

    /// For a request whose body, if it had one, has been got out of the way
    /// with [`discard`].
    fn after(discarded: Discarded, answer: Answer) -> Outcome {
        match discarded {
            Discarded::All(reply) => Outcome::new(reply, answer),
            Discarded::Abandoned(reply) => Outcome::closing(reply, answer),
        }
    }
}

/// What became of a request body nobody wanted.
enum Discarded {
    All(Reply<ProvideResponse>),
    /// It was too long to read to the end.
    Abandoned(Reply<ProvideResponse>),
}

/// Serves `conn` until the client is done with it.
pub(crate) async fn serve(server: &Inner, mut conn: Conn) {
    loop {
        match exchange(server, &mut conn).await {
            Ok(true) => {}
            Ok(false) => return conn.close().await,
            Err(Fatal::Io(error)) => return log::debug!("connection lost: {error}"),
            Err(Fatal::Protocol(error)) => return log::debug!("connection dropped: {error}"),
        }
    }
}

/// Whether the client asked for the connection to be closed after this
/// request.
fn wants_close(request: &Request<()>) -> bool {
    let mut options = request
        .headers()
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','));
    options.any(|option| option.trim().eq_ignore_ascii_case("close"))
}

/// Handles one request. Returns whether the connection can take another.
async fn exchange(server: &Inner, conn: &mut Conn) -> Result<bool, Fatal> {
    let mut reply = Reply::new()?;
    let request = loop {
        match reply.try_request(conn.pending()) {
            Ok((used, Some(request))) => {
                conn.consume(used);
                break request;
            }
            Ok((_, None)) => match conn.fill().await? {
                Filled::Data => {}
                Filled::Closed => return Ok(false),
                Filled::Full => return refuse(conn, HEAD_TOO_LARGE).await,
            },
            Err(_) => return refuse(conn, BAD_REQUEST).await,
        }
    };

    let action = Action::of(&request);
    let next = reply.proceed().expect("a parsed request can proceed");
    let outcome = match (next, action) {
        // Answering anything but "go ahead" ends the connection: the client
        // may or may not send the body after it.
        (RecvRequestResult::Send100(reply), Err(error)) => {
            Outcome::closing(reply.reject(), Answer::error(error))
        }
        // The client is waiting to hear whether to send the object. Find
        // out whether there is somewhere to put it before saying yes.
        (RecvRequestResult::Send100(reply), Ok(Action::PutObject(object))) => {
            match server.store.begin(&object).await {
                Err(error) => Outcome::closing(reply.reject(), Answer::failed(error)),
                Ok(upload) => {
                    let body = go_ahead(conn, reply).await?;
                    store_object(server, conn, upload, Incoming::Body(body)).await?
                }
            }
        }
        (RecvRequestResult::Send100(reply), Ok(action)) => {
            let body = go_ahead(conn, reply).await?;
            perform(server, conn, action, Incoming::Body(body)).await?
        }
        (RecvRequestResult::RecvBody(body), Err(error)) => {
            Outcome::after(discard(conn, body).await?, Answer::error(error))
        }
        (RecvRequestResult::RecvBody(body), Ok(action)) => {
            perform(server, conn, action, Incoming::Body(body)).await?
        }
        (RecvRequestResult::ProvideResponse(reply), Err(error)) => {
            Outcome::new(reply, Answer::error(error))
        }
        (RecvRequestResult::ProvideResponse(reply), Ok(action)) => {
            perform(server, conn, action, Incoming::Empty(reply)).await?
        }
    };

    let Outcome { reply, mut answer } = outcome;
    if wants_close(&request) {
        answer = answer.closing();
    }
    respond(server, conn, reply, answer).await
}

/// Tells a client that asked first to go ahead and send its request body.
async fn go_ahead(conn: &mut Conn, reply: Reply<Send100>) -> Result<Reply<RecvBody>, Fatal> {
    let output = conn.output()?;
    let (used, body) = reply.accept(output.spare_capacity_mut())?;
    output.grow(used);
    conn.flush().await?;
    Ok(body)
}

/// Answers a request that could not be read, and gives up on the
/// connection.
async fn refuse(conn: &mut Conn, response: &[u8]) -> Result<bool, Fatal> {
    conn.output()?.extend_from_slice(response);
    conn.flush().await?;
    Ok(false)
}

async fn perform(
    server: &Inner,
    conn: &mut Conn,
    action: Action,
    incoming: Incoming,
) -> Result<Outcome, Fatal> {
    let action = match action {
        Action::PutObject(object) => return put_object(server, conn, &object, incoming).await,
        other => other,
    };
    let discarded = match incoming {
        Incoming::Body(body) => discard(conn, body).await?,
        Incoming::Empty(reply) => Discarded::All(reply),
    };

    let store = &server.store;
    let done = |status| move |()| Answer::empty(status);
    let answer = match action {
        Action::CreateBucket(bucket) => {
            store.create_bucket(&bucket).await.map(done(StatusCode::OK))
        }
        Action::DeleteBucket(bucket) => store
            .delete_bucket(&bucket)
            .await
            .map(done(StatusCode::NO_CONTENT)),
        Action::HeadBucket(bucket) => store.head_bucket(&bucket).await.map(done(StatusCode::OK)),
        Action::DeleteObject(object) => store
            .delete_object(&object)
            .await
            .map(done(StatusCode::NO_CONTENT)),
        Action::GetObject {
            object,
            range,
            head: true,
        } => store
            .head_object(&object)
            .await
            .map(|stat| Answer::object(stat, range.as_ref(), None)),
        Action::GetObject {
            object,
            range,
            head: false,
        } => store
            .open_object(&object)
            .await
            .map(|stored| Answer::object(stored.meta, range.as_ref(), Some(stored))),
        Action::PutObject(_) => unreachable!("handled above"),
    };
    Ok(Outcome::after(
        discarded,
        answer.unwrap_or_else(Answer::failed),
    ))
}

/// Why a request body could not be taken in.
enum BodyError {
    Fatal(Fatal),
    /// The body's framing is broken.
    Malformed,
    Store(io::Error),
}

impl From<io::Error> for BodyError {
    fn from(error: io::Error) -> Self {
        BodyError::Fatal(Fatal::Io(error))
    }
}

/// Decodes body bytes from the connection's input into `output`, which must
/// not be empty. Returns how many it produced, which is none only once the
/// body has ended.
async fn read_body(
    conn: &mut Conn,
    body: &mut Reply<RecvBody>,
    output: &mut [u8],
) -> Result<usize, BodyError> {
    while !body.is_ended() {
        let (used, produced) = body
            .read(conn.pending(), output)
            .map_err(|_| BodyError::Malformed)?;
        conn.consume(used);
        if produced > 0 {
            return Ok(produced);
        }
        // Framing alone, or not even a whole piece of it.
        if used == 0 {
            match conn.fill().await? {
                Filled::Data => {}
                Filled::Closed => return Err(BodyError::Fatal(closed())),
                Filled::Full => return Err(BodyError::Malformed),
            }
        }
    }
    Ok(0)
}

/// Gets a request body nobody wants out of the way.
async fn discard(conn: &mut Conn, mut body: Reply<RecvBody>) -> Result<Discarded, Fatal> {
    let mut scratch = [0; 1024];
    let mut discarded = 0;
    while discarded <= DISCARD_LIMIT {
        match read_body(conn, &mut body, &mut scratch).await {
            Ok(0) => return Ok(Discarded::All(body.proceed()?)),
            Ok(produced) => discarded += produced,
            Err(BodyError::Fatal(fatal)) => return Err(fatal),
            Err(BodyError::Malformed | BodyError::Store(_)) => break,
        }
    }
    Ok(Discarded::Abandoned(body.abandon()))
}

async fn put_object(
    server: &Inner,
    conn: &mut Conn,
    object: &Object,
    incoming: Incoming,
) -> Result<Outcome, Fatal> {
    match server.store.begin(object).await {
        Ok(upload) => store_object(server, conn, upload, incoming).await,
        Err(error) => {
            let discarded = match incoming {
                Incoming::Body(body) => discard(conn, body).await?,
                Incoming::Empty(reply) => Discarded::All(reply),
            };
            Ok(Outcome::after(discarded, Answer::failed(error)))
        }
    }
}

/// Takes the request body in as the object that `upload` is for.
async fn store_object(
    server: &Inner,
    conn: &mut Conn,
    mut upload: Upload,
    incoming: Incoming,
) -> Result<Outcome, Fatal> {
    // Finishing an upload takes a buffer to write the header through. One
    // that carried the body serves, since asking for another while holding
    // those could wait forever.
    let (reply, scratch) = match incoming {
        Incoming::Empty(reply) => (reply, server.bodies.acquire().await),
        Incoming::Body(mut body) => match receive(server, conn, &mut body, &mut upload).await {
            Ok(mut bufs) => (body.proceed()?, bufs.swap_remove(0)),
            Err(error) => {
                server.store.abort(upload).await;
                let answer = match error {
                    BodyError::Fatal(fatal) => return Err(fatal),
                    BodyError::Malformed => Answer::error(ApiError::BAD_REQUEST),
                    BodyError::Store(error) => Answer::failed(StoreError::Io(error)),
                };
                return Ok(Outcome::closing(body.abandon(), answer));
            }
        },
    };
    let answer = match server.store.finish(upload, scratch).await {
        Ok(meta) => Answer::stored(&meta),
        Err(error) => Answer::failed(StoreError::Io(error)),
    };
    Ok(Outcome::new(reply, answer))
}

/// Takes the buffers for a transfer that moves through `per_round` buffers
/// at a time and could keep `rounds` rounds going at once. One round's worth
/// is waited for. More are taken only if they are free, up to the server's
/// window, so a lone transfer gets a deep pipeline and a crowd of them
/// shares the pool.
async fn acquire_window(server: &Inner, per_round: usize, rounds: u64) -> Vec<Vec<FixedBuf>> {
    let wanted = rounds.min(server.window as u64) as usize;
    let mut window = vec![server.bodies.acquire_many(per_round).await];
    while window.len() < wanted {
        match server.bodies.try_acquire_many(per_round) {
            Some(bufs) => window.push(bufs),
            None => break,
        }
    }
    window
}

/// Moves a request body into `upload`, a round at a time: from the socket
/// into one buffer per drive, and from each buffer onto its drive. A round
/// is written while the next ones are received. Hands back the buffers it
/// used.
async fn receive(
    server: &Inner,
    conn: &mut Conn,
    body: &mut Reply<RecvBody>,
    upload: &mut Upload,
) -> Result<Vec<FixedBuf>, BodyError> {
    let geometry = server.store.geometry();
    let round_len = geometry.round_len();
    // An object of known size takes no more buffers than it has parts, and
    // no more rounds than it has.
    let (parts, rounds) = match body.remaining() {
        Some(left) => (geometry.parts(left), left.div_ceil(round_len as u64)),
        None => (geometry.drives(), u64::MAX),
    };
    let mut free = acquire_window(server, parts, rounds).await;
    let mut writing = VecDeque::new();

    loop {
        let mut bufs = match free.pop() {
            Some(bufs) => bufs,
            None => {
                let oldest: Started<_> = writing.pop_front().expect("every buffer is in use");
                let (written, bufs): (io::Result<()>, Vec<FixedBuf>) = oldest.finish().await;
                written.map_err(BodyError::Store)?;
                bufs
            }
        };
        bufs.iter_mut().for_each(FixedBuf::clear);

        let mut filled = 0;
        while filled < round_len && !body.is_ended() {
            match body.remaining() {
                // A body of known length, with none of it in the input
                // buffer: it goes from the socket to where it is written
                // from, without a stop on the way.
                Some(left) if conn.pending().is_empty() => {
                    let left = usize::try_from(left).unwrap_or(usize::MAX);
                    let upto = round_len.min(filled.saturating_add(left));
                    let segments = geometry.fill(filled..upto);
                    let (received, returned) = conn.recv_vectored(bufs, &segments).await;
                    bufs = returned;
                    let received = received?;
                    if received < upto - filled {
                        return Err(BodyError::Fatal(closed()));
                    }
                    body.consume_direct_read(received)
                        .map_err(|_| BodyError::Malformed)?;
                    filled += received;
                }
                _ => {
                    let room = geometry.fill(filled..round_len);
                    let next = room.first().expect("the round is not full");
                    let output = &mut bufs[next.buf].spare_capacity_mut()[next.start..][..next.len];
                    filled += read_body(conn, body, output).await?;
                }
            }
        }
        if filled == 0 {
            free.push(bufs);
            break;
        }

        let round = server.store.begin_round(upload, filled).await;
        let round = round.map_err(BodyError::Store)?;
        writing.push_back(Started::start(server.store.write(round, bufs)).await);
        if body.is_ended() {
            break;
        }
    }

    // The upload cannot be finished before everything is written.
    for write in writing {
        let (written, bufs) = write.finish().await;
        written.map_err(BodyError::Store)?;
        free.push(bufs);
    }
    Ok(free.into_iter().flatten().collect())
}

/// Sends `answer`. Returns whether the connection can take another request.
async fn respond(
    server: &Inner,
    conn: &mut Conn,
    reply: Reply<ProvideResponse>,
    answer: Answer,
) -> Result<bool, Fatal> {
    let Answer { response, payload } = answer;
    let mut head = reply.provide(response)?;
    while !head.is_finished() {
        let output = conn.output()?;
        let stuck = output.is_empty();
        let used = head.write(output.spare_capacity_mut())?;
        output.grow(used);
        if used == 0 {
            if stuck {
                return Err(io::Error::other("response head does not fit the buffer").into());
            }
            conn.flush().await?;
        }
    }

    let cleanup = match head.proceed() {
        SendResponseResult::Cleanup(cleanup) => {
            conn.flush().await?;
            cleanup
        }
        SendResponseResult::SendBody(mut body) => {
            match payload {
                Payload::None => {}
                Payload::Bytes(bytes) => {
                    let mut rest = &bytes[..];
                    while !rest.is_empty() {
                        let output = conn.output()?;
                        let (used, produced) = body.write(rest, output.spare_capacity_mut())?;
                        output.grow(produced);
                        rest = &rest[used..];
                        if used == 0 {
                            conn.flush().await?;
                        }
                    }
                }
                Payload::Object { stored, range } => {
                    conn.flush().await?;
                    send_object(server, conn, &mut body, &stored, range).await?;
                }
            }
            conn.flush().await?;
            if !body.is_finished() {
                return Err(io::Error::other("response body ended short").into());
            }
            body.proceed()
        }
    };
    Ok(!cleanup.must_close_connection())
}

/// Sends the bytes `range` of an object, a round at a time: from every
/// drive into a buffer of its own, and from the buffers to the socket in the
/// order the bytes have in the object. The rounds to come are read while one
/// is sent.
async fn send_object(
    server: &Inner,
    conn: &mut Conn,
    body: &mut Reply<SendBody>,
    stored: &Stored,
    range: Range<u64>,
) -> Result<(), Fatal> {
    let geometry = server.store.geometry();
    let mut rounds = geometry.rounds(&range);
    if rounds.is_empty() {
        return Ok(());
    }
    let window = acquire_window(server, stored.parts(), rounds.end - rounds.start).await;
    let mut read = |bufs| {
        let round = rounds.next()?;
        let plan = geometry.plan(round, &range);
        Some(server.store.read_round(stored, plan, bufs))
    };

    let mut reading = VecDeque::new();
    for bufs in window {
        let next = read(bufs).expect("the window is no deeper than the rounds");
        reading.push_back(Started::start(next).await);
    }
    while let Some(oldest) = reading.pop_front() {
        let (got, bufs, plan) = oldest.finish().await;
        got?;
        let (sent, bufs) = conn.send_vectored(bufs, &plan.segments).await;
        sent?;
        body.consume_direct_write(plan.len)?;
        // The buffers go straight back to work on the round after the ones
        // already under way.
        if let Some(next) = read(bufs) {
            reading.push_back(Started::start(next).await);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requested(header: &str, size: u64) -> Requested {
        Requested::parse(Some(&HeaderValue::from_str(header).unwrap()), size)
    }

    #[test]
    fn ranges() {
        assert_eq!(Requested::parse(None, 10), Requested::Whole);
        assert_eq!(requested("bytes=0-0", 10), Requested::Part(0..1));
        assert_eq!(requested("bytes=2-5", 10), Requested::Part(2..6));
        assert_eq!(requested("bytes=2-", 10), Requested::Part(2..10));
        assert_eq!(requested("bytes=2-99", 10), Requested::Part(2..10));
        assert_eq!(requested("bytes=9-9", 10), Requested::Part(9..10));
        assert_eq!(requested("bytes=-3", 10), Requested::Part(7..10));
        assert_eq!(requested("bytes=-99", 10), Requested::Part(0..10));
        assert_eq!(
            requested("bytes=0-18446744073709551615", 10),
            Requested::Part(0..10)
        );
    }

    #[test]
    fn ranges_past_the_end() {
        assert_eq!(requested("bytes=10-", 10), Requested::Unsatisfiable);
        assert_eq!(requested("bytes=10-12", 10), Requested::Unsatisfiable);
        assert_eq!(requested("bytes=-0", 10), Requested::Unsatisfiable);
        assert_eq!(requested("bytes=0-", 0), Requested::Unsatisfiable);
        assert_eq!(requested("bytes=-5", 0), Requested::Unsatisfiable);
    }

    #[test]
    fn ranges_that_are_ignored() {
        for header in [
            "bytes=5-2",
            "bytes=0-1,4-5",
            "bytes=a-b",
            "bytes=-",
            "bytes=",
            "bytes=1",
            "items=0-1",
            "bytes=-x",
            "bytes=1-x",
            "bytes= 1-2",
        ] {
            assert_eq!(requested(header, 10), Requested::Whole, "{header}");
        }
    }
}
