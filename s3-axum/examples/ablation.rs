//! Ablation benchmark isolating:
//!   1. `hyper`          — standard Tokio (mio) + `axum::serve` (Hyper)
//!   2. `tokio-ureq`     — standard Tokio (mio) + `ureq-proto` over `tokio::net::TcpStream` (just drop Hyper!)
//!   3. `multiring-body`   — `tokio-multiring` (`io_uring`) + `multiring-axum` (`ureq-proto`), standard `axum::body::Body`
//!   4. `multiring-direct` — `tokio-multiring` (`io_uring`) + `multiring-axum` (`ureq-proto`), `DirectWrite` + `send_zc`
//!
//! Supports both pure in-memory HTTP (`--mode mem`, serving static 4K/16M
//! responses compatible with `s3/examples/load`) and real S3 (`--mode s3`,
//! using `TokioStore` across modes 1-3 and `MultiRingStore` for mode 4).

use std::convert::Infallible;
use std::future::{Future, poll_fn};
use std::io;
use std::net::TcpListener;
use std::path::PathBuf;
use std::pin::Pin;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::serve::ListenerExt;
use bytes::{Bytes, BytesMut};
use http::{HeaderValue, Request, StatusCode, header};
use http_body::Body as HttpBody;
use http_body_util::BodyExt;
use multiring_axum::{
    BodySink, Config as HttpConfig, DirectResponse, DirectWrite, Server, ZeroCopy,
};
use s3_axum::{Backend, MultiRingStore, TokioStore, router as s3_router};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_multiring::{BufferPool, Domain, FixedBuf, Hyperthreads, MultiRing, Plan, Segment};
use tower_service::Service;
use ureq_proto::server::{RecvRequestResult, Reply, SendResponseResult};

fn build_plan(domains_limit: usize) -> io::Result<Plan> {
    let detected = Plan::detect(Hyperthreads::Skip)?;
    let domains = detected.domains();
    let count = domains_limit.min(domains.len());
    let selected: Vec<Domain> = domains[..count].to_vec();
    Plan::new(selected)
}

/// Minimal HTTP/1.1 server driving any `axum::Router` over `ureq-proto` and
/// stock `tokio::net::TcpStream` (mio/epoll) — isolates the effect of dropping
/// Hyper without changing the I/O reactor or adding `io_uring`.
async fn serve_tokio_ureq<S>(listener: tokio::net::TcpListener, service: S) -> io::Result<()>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    loop {
        let (stream, _) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let svc = service.clone();
        tokio::spawn(async move {
            let _ = serve_tokio_ureq_conn(stream, svc).await;
        });
    }
}

async fn serve_tokio_ureq_conn<S>(mut stream: TcpStream, mut service: S) -> io::Result<()>
where
    S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>,
{
    let mut in_buf = vec![0u8; 16 * 1024];
    let mut in_start = 0usize;
    let mut in_end = 0usize;
    let mut out_buf = vec![0u8; 64 * 1024];
    let mut out_len = 0usize;

    loop {
        let mut reply = Reply::new().map_err(io::Error::other)?;
        let head = loop {
            match reply.try_request(&in_buf[in_start..in_end]) {
                Ok((used, Some(req))) => {
                    in_start += used;
                    break req;
                }
                Ok((_, None)) => {
                    if in_start > 0 {
                        in_buf.copy_within(in_start..in_end, 0);
                        in_end -= in_start;
                        in_start = 0;
                    }
                    if in_end == in_buf.len() {
                        return Ok(());
                    }
                    let n = stream.read(&mut in_buf[in_end..]).await?;
                    if n == 0 {
                        return Ok(());
                    }
                    in_end += n;
                }
                Err(_) => return Ok(()),
            }
        };

        let next = reply.proceed().expect("parsed request can proceed");
        let (req_body, reply) = match next {
            RecvRequestResult::ProvideResponse(reply) => (Body::empty(), reply),
            RecvRequestResult::Send100(send100) => {
                let (used, mut recv_body) = send100
                    .accept(&mut out_buf[out_len..])
                    .map_err(io::Error::other)?;
                out_len += used;
                stream.write_all(&out_buf[..out_len]).await?;
                out_len = 0;
                let bytes = drain_body(
                    &mut stream,
                    &mut in_buf,
                    &mut in_start,
                    &mut in_end,
                    &mut recv_body,
                )
                .await?;
                let reply = recv_body.proceed().map_err(io::Error::other)?;
                (Body::from(bytes), reply)
            }
            RecvRequestResult::RecvBody(mut recv_body) => {
                let bytes = drain_body(
                    &mut stream,
                    &mut in_buf,
                    &mut in_start,
                    &mut in_end,
                    &mut recv_body,
                )
                .await?;
                let reply = recv_body.proceed().map_err(io::Error::other)?;
                (Body::from(bytes), reply)
            }
        };

        let (parts, ()) = head.into_parts();
        let req = Request::from_parts(parts, req_body);
        let Ok(()) = poll_fn(|cx| service.poll_ready(cx)).await;
        let Ok(resp) = service.call(req).await;

        let (mut parts, mut body) = resp.into_parts();
        if !parts.headers.contains_key(header::CONTENT_LENGTH)
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

        let mut head_w = reply
            .provide(Response::from_parts(parts, ()))
            .map_err(io::Error::other)?;
        while !head_w.is_finished() {
            let used = head_w
                .write(&mut out_buf[out_len..])
                .map_err(io::Error::other)?;
            out_len += used;
            if used == 0 {
                stream.write_all(&out_buf[..out_len]).await?;
                out_len = 0;
            }
        }

        let cleanup = match head_w.proceed() {
            SendResponseResult::Cleanup(c) => {
                if out_len > 0 {
                    stream.write_all(&out_buf[..out_len]).await?;
                    out_len = 0;
                }
                c
            }
            SendResponseResult::SendBody(mut send_body) => {
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(io::Error::other)?;
                    if let Ok(data) = frame.into_data() {
                        if !send_body.is_chunked() && data.len() >= 16 * 1024 {
                            // Direct write straight from `Bytes` without copying into out_buf!
                            if out_len > 0 {
                                stream.write_all(&out_buf[..out_len]).await?;
                                out_len = 0;
                            }
                            stream.write_all(&data).await?;
                            send_body
                                .consume_direct_write(data.len())
                                .map_err(io::Error::other)?;
                        } else {
                            let mut rest = data.as_ref();
                            while !rest.is_empty() {
                                let (used, prod) = send_body
                                    .write(rest, &mut out_buf[out_len..])
                                    .map_err(io::Error::other)?;
                                out_len += prod;
                                rest = &rest[used..];
                                if used == 0 {
                                    stream.write_all(&out_buf[..out_len]).await?;
                                    out_len = 0;
                                }
                            }
                        }
                    }
                }
                while !send_body.is_finished() {
                    let (_, prod) = send_body
                        .write(&[], &mut out_buf[out_len..])
                        .map_err(io::Error::other)?;
                    out_len += prod;
                    if !send_body.is_finished() {
                        stream.write_all(&out_buf[..out_len]).await?;
                        out_len = 0;
                    }
                }
                if out_len > 0 {
                    stream.write_all(&out_buf[..out_len]).await?;
                    out_len = 0;
                }
                send_body.proceed()
            }
        };

        if cleanup.must_close_connection() {
            return Ok(());
        }
    }
}

async fn drain_body(
    stream: &mut TcpStream,
    in_buf: &mut [u8],
    in_start: &mut usize,
    in_end: &mut usize,
    recv: &mut Reply<ureq_proto::server::state::RecvBody>,
) -> io::Result<Bytes> {
    let cap = recv.remaining().unwrap_or(0).min(64 * 1024 * 1024) as usize;
    let mut out = BytesMut::with_capacity(cap);
    let mut tmp = vec![0u8; 64 * 1024];
    while !recv.is_ended() {
        let (used, prod) = recv
            .read(&in_buf[*in_start..*in_end], &mut tmp)
            .map_err(io::Error::other)?;
        *in_start += used;
        if prod > 0 {
            out.extend_from_slice(&tmp[..prod]);
            continue;
        }
        if used == 0 {
            if *in_start > 0 {
                in_buf.copy_within(*in_start..*in_end, 0);
                *in_end -= *in_start;
                *in_start = 0;
            }
            let n = stream.read(&mut in_buf[*in_end..]).await?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            *in_end += n;
        }
    }
    Ok(out.freeze())
}

#[derive(Clone)]
struct MemState {
    payload: Bytes,
    pool: Option<BufferPool>,
    size: usize,
    use_direct: bool,
}

struct MemDirectWriter {
    pool: BufferPool,
    total: usize,
}

impl DirectWrite for MemDirectWriter {
    fn write_to<'a>(
        self: Box<Self>,
        sink: &'a mut BodySink<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let chunk = self.pool.buf_size();
            let mut left = self.total;
            let mut free: Vec<FixedBuf> = if left <= chunk {
                vec![self.pool.acquire().await]
            } else {
                self.pool.acquire_many(4).await
            };
            let mut unack = std::collections::VecDeque::new();
            while left > 0 {
                let mut buf = match free.pop() {
                    Some(b) => b,
                    None => {
                        let r: tokio_multiring::Reclaim = unack.pop_front().unwrap();
                        r.await.bufs.into_iter().next().unwrap()
                    }
                };
                let n = left.min(chunk);
                buf.clear();
                buf.grow(n);
                let seg = [Segment {
                    buf: 0,
                    start: 0,
                    len: n,
                }];
                if sink.zero_copy() && n >= 64 * 1024 {
                    let (res, reclaim) = sink.send_zero_copy(vec![buf], &seg).await;
                    if let Some(r) = reclaim {
                        unack.push_back(r);
                    }
                    res?;
                } else {
                    let (res, mut bufs) = sink.send_vectored(vec![buf], &seg).await;
                    res?;
                    free.push(bufs.pop().unwrap());
                }
                left -= n;
            }
            for r in unack {
                let _ = r.await;
            }
            Ok(())
        })
    }
}

fn mem_router(state: MemState) -> Router {
    Router::new()
        .route("/bench", put(|| async { StatusCode::OK }))
        .route(
            "/bench/{key}",
            get(
                |State(st): State<MemState>, Path(_key): Path<String>| async move {
                    if st.use_direct
                        && let Some(pool) = st.pool
                    {
                        let head = Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_LENGTH, st.size)
                            .body(())
                            .unwrap();
                        return DirectResponse::new(
                            head,
                            MemDirectWriter {
                                pool,
                                total: st.size,
                            },
                        )
                        .into_response();
                    }
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_LENGTH, st.payload.len())
                        .body(Body::from(st.payload))
                        .unwrap()
                },
            ),
        )
        .with_state(state)
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut stack = String::from("hyper");
    let mut mode = String::from("mem");
    let mut mem_size: usize = 4096;
    let mut listen = String::from("127.0.0.1:19123");
    let mut domains: usize = 2;
    let mut roots: Vec<PathBuf> = Vec::new();
    let mut direct_disk = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stack" => stack = args.next().unwrap(),
            "--mode" => mode = args.next().unwrap(),
            "--size" => mem_size = args.next().unwrap().parse().unwrap(),
            "--listen" => listen = args.next().unwrap(),
            "--domains" => domains = args.next().unwrap().parse().unwrap(),
            "--root" => roots.push(PathBuf::from(args.next().unwrap())),
            "--direct" => direct_disk = true,
            other => panic!("unknown arg: {other}"),
        }
    }

    let plan = build_plan(domains)?;
    let listener = TcpListener::bind(&listen)?;

    match stack.as_str() {
        "hyper" | "tokio-ureq" => {
            listener.set_nonblocking(true)?;
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            let rt = plan.build(&mut builder)?;
            rt.block_on(async move {
                let app = if mode == "mem" {
                    mem_router(MemState {
                        payload: Bytes::from(vec![0x42u8; mem_size]),
                        pool: None,
                        size: mem_size,
                        use_direct: false,
                    })
                } else {
                    let store = TokioStore::open(&roots, 128 * 1024, 1 << 20, direct_disk)?;
                    s3_router(Backend::Tokio(store))
                };
                let listener = tokio::net::TcpListener::from_std(listener)?;
                if stack == "hyper" {
                    let listener = listener.tap_io(|s| {
                        let _ = s.set_nodelay(true);
                    });
                    axum::serve(listener, app).await
                } else {
                    serve_tokio_ureq(listener, app).await
                }
            })
        }
        "multiring-body" | "multiring-direct" => {
            let pool = BufferPool::new(1 << 20, 256)?;
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_time();
            let multiring = MultiRing::install(
                &mut builder,
                tokio_multiring::Config {
                    buffers: Some(pool.clone()),
                    ..Default::default()
                },
            );
            let rt = plan.build(&mut builder)?;
            rt.block_on(async move {
                let app = if mode == "mem" {
                    mem_router(MemState {
                        payload: Bytes::from(vec![0x42u8; mem_size]),
                        pool: Some(pool),
                        size: mem_size,
                        use_direct: stack == "multiring-direct",
                    })
                } else if stack == "multiring-body" {
                    // `multiring-axum` (ureq-proto + io_uring sockets) driving `TokioStore`!
                    let store = TokioStore::open(&roots, 128 * 1024, 1 << 20, direct_disk)?;
                    s3_router(Backend::Tokio(store))
                } else {
                    let store = MultiRingStore::open(multiring.clone(), &roots, 128 * 1024, 4)?;
                    s3_router(Backend::MultiRing(store))
                };
                Server::new(
                    multiring,
                    HttpConfig {
                        connections: 1024,
                        zero_copy: ZeroCopy::Remote,
                        worker_local_connections: true,
                        ..HttpConfig::default()
                    },
                )?
                .listen(listener, app);
                std::future::pending().await
            })
        }
        other => panic!("unknown stack: {other}"),
    }
}
