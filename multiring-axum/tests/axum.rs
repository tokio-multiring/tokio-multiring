use std::collections::HashMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, Response, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use bytes::Bytes;
use futures_util::stream;
use multiring_axum::{BodySink, Config, DirectBody, DirectResponse, DirectWrite, Server, ZeroCopy};
use tokio::runtime::{Builder, Runtime};
use tokio_multiring::{BufferPool, MultiRing, Segment};

struct Harness {
    addr: SocketAddr,
    _rt: Runtime,
}

impl Harness {
    fn start(router: Router, zero_copy: ZeroCopy, pool: Option<BufferPool>) -> Self {
        Self::start_with_connections(router, zero_copy, pool, 32)
    }

    fn start_with_connections(
        router: Router,
        zero_copy: ZeroCopy,
        pool: Option<BufferPool>,
        connections: usize,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_time();
        let multiring = MultiRing::install(
            &mut builder,
            tokio_multiring::Config {
                buffers: pool,
                ..Default::default()
            },
        );
        let rt = builder.build().unwrap();
        rt.block_on(async {
            let server = Server::new(
                multiring,
                Config {
                    connections,
                    head_buffer: 4096,
                    zero_copy,
                    worker_local_connections: true,
                    idle_timeout: None,
                },
            )
            .unwrap();
            server.listen(listener, router);
        });
        Self { addr, _rt: rt }
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(self.addr).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
    }
}

fn read_some(stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    loop {
        match stream.read(buf) {
            Ok(n) => return n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

fn read_response(stream: &mut TcpStream) -> (u16, String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = read_some(stream, &mut chunk);
        assert!(n > 0, "unexpected EOF waiting for response headers");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8(raw[..head_end].to_vec()).unwrap();
    let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[head_end..].to_vec();

    let lower = head.to_ascii_lowercase();
    if let Some(line) = lower
        .lines()
        .find(|line| line.starts_with("content-length:"))
    {
        let len: usize = line["content-length:".len()..].trim().parse().unwrap();
        while body.len() < len {
            let n = read_some(stream, &mut chunk);
            assert!(n > 0, "unexpected EOF reading content-length body");
            body.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(body.len(), len);
    } else if lower.contains("transfer-encoding: chunked") {
        // Decode chunked body until 0\r\n\r\n
        let mut decoded = Vec::new();
        let mut pos = 0;
        loop {
            while !body[pos..].windows(2).any(|w| w == b"\r\n") {
                let n = read_some(stream, &mut chunk);
                assert!(n > 0, "unexpected EOF reading chunk size");
                body.extend_from_slice(&chunk[..n]);
            }
            let crlf = body[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
            let size_str = std::str::from_utf8(&body[pos..pos + crlf]).unwrap().trim();
            let size = usize::from_str_radix(size_str, 16).unwrap();
            pos += crlf + 2;
            if size == 0 {
                while body.len() < pos + 2 {
                    let n = read_some(stream, &mut chunk);
                    assert!(n > 0);
                    body.extend_from_slice(&chunk[..n]);
                }
                break;
            }
            while body.len() < pos + size + 2 {
                let n = read_some(stream, &mut chunk);
                assert!(n > 0, "unexpected EOF reading chunk data");
                body.extend_from_slice(&chunk[..n]);
            }
            decoded.extend_from_slice(&body[pos..pos + size]);
            pos += size + 2;
        }
        body = decoded;
    } else if status == 204 || status == 304 || head.starts_with("HTTP/1.0") {
        // Read until EOF if HTTP/1.0 close-delimited and status allows a body
        if head.starts_with("HTTP/1.0") && status != 204 && status != 304 {
            loop {
                let n = read_some(stream, &mut chunk);
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&chunk[..n]);
            }
        }
    }

    (status, head, body)
}

#[test]
fn routing_extractors_keep_alive_and_head() {
    let router = Router::new()
        .route(
            "/hello/{name}",
            get(
                |Path(name): Path<String>, Query(q): Query<HashMap<String, String>>| async move {
                    let suffix = q.get("bang").map(String::as_str).unwrap_or("");
                    format!("hi {name}{suffix}")
                },
            ),
        )
        .route("/empty", get(|| async { StatusCode::NO_CONTENT }));

    let h = Harness::start(router, ZeroCopy::Never, None);
    let mut s = h.connect();

    s.write_all(b"GET /hello/louis?bang=! HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(head.to_ascii_lowercase().contains("content-length: 9"));
    assert_eq!(body, b"hi louis!");

    // Reuse same keep-alive connection for HEAD and 204.
    s.write_all(b"HEAD /hello/louis HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let mut head_only = [0u8; 1024];
    let n = read_some(&mut s, &mut head_only);
    let head_str = std::str::from_utf8(&head_only[..n]).unwrap();
    assert!(head_str.starts_with("HTTP/1.1 200 OK"));
    assert!(head_str.to_ascii_lowercase().contains("content-length: 8"));
    assert!(head_str.ends_with("\r\n\r\n"));

    s.write_all(b"GET /empty HTTP/1.1\r\nhost: test\r\nconnection: close\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 204);
    assert!(body.is_empty());
}

#[test]
fn standard_body_extractors_and_chunked_streaming() {
    let router = Router::new()
        .route(
            "/echo",
            post(|body: Bytes| async move { (StatusCode::OK, body) }),
        )
        .route(
            "/stream",
            get(|| async {
                let chunks = vec![
                    Ok::<_, std::io::Error>(Bytes::from_static(b"chunk-1-")),
                    Ok(Bytes::from_static(b"chunk-2-")),
                    Ok(Bytes::from_static(b"chunk-3")),
                ];
                Body::from_stream(stream::iter(chunks))
            }),
        );

    let h = Harness::start(router, ZeroCopy::Never, None);
    let mut s = h.connect();

    // Content-Length upload
    let payload = vec![b'x'; 50_000];
    let req = format!(
        "POST /echo HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        payload.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(&payload).unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, payload);

    // Chunked upload on same connection
    s.write_all(
        b"POST /echo HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\n\
          5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    )
    .unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");

    // Chunked streaming response on same connection
    s.write_all(b"GET /stream HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(
        head.to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
    );
    assert_eq!(body, b"chunk-1-chunk-2-chunk-3");
}

#[test]
fn expect_100_continue_accept_and_reject() {
    let router = Router::new()
        .route(
            "/ok",
            put(|body: Bytes| async move { format!("got {}", body.len()) }),
        )
        .route("/deny", put(|| async { StatusCode::FORBIDDEN }));

    let h = Harness::start(router, ZeroCopy::Never, None);

    // Accepted lazily when handler reads Bytes
    let mut s = h.connect();
    s.write_all(
        b"PUT /ok HTTP/1.1\r\nhost: test\r\nexpect: 100-continue\r\ncontent-length: 5\r\n\r\n",
    )
    .unwrap();
    let mut interim = [0u8; 64];
    let n = read_some(&mut s, &mut interim);
    assert_eq!(&interim[..n], b"HTTP/1.1 100 Continue\r\n\r\n");
    s.write_all(b"abcde").unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, b"got 5");

    // Rejected before reading body
    let mut s2 = h.connect();
    s2.write_all(
        b"PUT /deny HTTP/1.1\r\nhost: test\r\nexpect: 100-continue\r\ncontent-length: 1000\r\n\r\n",
    )
    .unwrap();
    let (status, head, _) = read_response(&mut s2);
    assert_eq!(status, 403);
    assert!(head.to_ascii_lowercase().contains("connection: close"));
}

#[test]
fn direct_body_and_direct_response_with_middleware() {
    struct PoolWriter {
        pool: BufferPool,
        data: Arc<Vec<u8>>,
        use_zc: bool,
    }

    impl DirectWrite for PoolWriter {
        fn write_to<'a>(
            self: Box<Self>,
            sink: &'a mut BodySink<'a>,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let mut buf = self.pool.acquire().await;
                let mut off = 0;
                while off < self.data.len() {
                    buf.clear();
                    let n = buf.extend_from_slice(&self.data[off..]);
                    let seg = [Segment {
                        buf: 0,
                        start: 0,
                        len: n,
                    }];
                    if self.use_zc && sink.zero_copy() {
                        let (res, reclaim) = sink.send_zero_copy(vec![buf], &seg).await;
                        res?;
                        let mut reclaimed = reclaim.unwrap().await.bufs;
                        buf = reclaimed.pop().unwrap();
                    } else {
                        let (res, mut returned) = sink.send_vectored(vec![buf], &seg).await;
                        res?;
                        buf = returned.pop().unwrap();
                    }
                    off += n;
                }
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct AppState {
        pool: BufferPool,
    }

    async fn add_header(req: axum::extract::Request, next: Next) -> axum::response::Response {
        let mut resp = next.run(req).await;
        resp.headers_mut()
            .insert("x-middleware", HeaderValue::from_static("preserved"));
        resp
    }

    let pool = BufferPool::without_huge_pages(64 * 1024, 16).unwrap();
    let state = AppState { pool: pool.clone() };

    let router = Router::new()
        .route(
            "/upload",
            put(|State(st): State<AppState>, body: DirectBody| async move {
                let DirectBody::Direct(mut direct) = body else {
                    panic!("expected DirectBody::Direct on multiring-axum");
                };
                direct.accept_100().await.unwrap();
                let mut total = 0usize;
                let mut buf = st.pool.acquire().await;
                while !direct.is_ended() {
                    buf.clear();
                    if let Some(rem) = direct.remaining()
                        && !direct.has_pending()
                    {
                        let want = (rem as usize).min(buf.capacity());
                        let seg = [Segment {
                            buf: 0,
                            start: 0,
                            len: want,
                        }];
                        let (res, mut bufs) = direct.recv_vectored(vec![buf], &seg).await;
                        buf = bufs.pop().unwrap();
                        total += res.unwrap();
                    } else {
                        let n = direct.read_into(buf.spare_capacity_mut()).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        total += n;
                    }
                }
                format!("direct:{total}")
            }),
        )
        .route(
            "/download/{mode}",
            get(
                |State(st): State<AppState>, Path(mode): Path<String>| async move {
                    let payload =
                        Arc::new((0..200_000u32).map(|i| (i % 251) as u8).collect::<Vec<_>>());
                    let head = Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_LENGTH, payload.len())
                        .body(())
                        .unwrap();
                    DirectResponse::new(
                        head,
                        PoolWriter {
                            pool: st.pool,
                            data: payload,
                            use_zc: mode == "zc",
                        },
                    )
                    .into_response()
                },
            ),
        )
        .layer(middleware::from_fn(add_header))
        .with_state(state);

    let h = Harness::start(router, ZeroCopy::Always, Some(pool));
    let mut s = h.connect();

    // DirectBody upload with 100-continue
    let up = vec![0x5au8; 150_000];
    let head_req = format!(
        "PUT /upload HTTP/1.1\r\nhost: test\r\nexpect: 100-continue\r\ncontent-length: {}\r\n\r\n",
        up.len()
    );
    s.write_all(head_req.as_bytes()).unwrap();
    let mut interim = [0u8; 64];
    let n = read_some(&mut s, &mut interim);
    assert_eq!(&interim[..n], b"HTTP/1.1 100 Continue\r\n\r\n");
    s.write_all(&up).unwrap();
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(
        head.to_ascii_lowercase()
            .contains("x-middleware: preserved")
    );
    assert_eq!(body, b"direct:150000");

    // DirectResponse with vectored send (coalesced with headers!)
    let expected: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    s.write_all(b"GET /download/vec HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(
        head.to_ascii_lowercase()
            .contains("x-middleware: preserved")
    );
    assert_eq!(body, expected);

    // DirectResponse with zero-copy send_zc on the same connection
    s.write_all(b"GET /download/zc HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(
        head.to_ascii_lowercase()
            .contains("x-middleware: preserved")
    );
    assert_eq!(body, expected);
}

#[test]
fn unread_body_discard_abandon_and_error_responses() {
    let router = Router::new()
        .route("/ignore", post(|| async { "ignored" }))
        .route("/ping", get(|| async { "pong" }));

    let h = Harness::start(router, ZeroCopy::Never, None);

    // Small unread body is discarded and connection stays reusable.
    let mut s = h.connect();
    s.write_all(b"POST /ignore HTTP/1.1\r\nhost: test\r\ncontent-length: 10\r\n\r\n0123456789")
        .unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, b"ignored");

    s.write_all(b"GET /ping HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, b"pong");

    // Large unread body (> 64 KiB) is abandoned and connection closes cleanly.
    let big = vec![b'a'; 128 * 1024];
    let req = format!(
        "POST /ignore HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        big.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    let _ = s.write_all(&big);
    let (status, head, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert!(head.to_ascii_lowercase().contains("connection: close"));
    assert_eq!(body, b"ignored");

    // Malformed request -> 400
    let mut s2 = h.connect();
    s2.write_all(b"NOT A VALID REQUEST\r\n\r\n").unwrap();
    let (status, _, _) = read_response(&mut s2);
    assert_eq!(status, 400);

    // Oversized headers (> 4096 head buffer) -> 431
    let mut s3 = h.connect();
    let mut big_head = String::from("GET /ping HTTP/1.1\r\nhost: test\r\n");
    while big_head.len() < 5000 {
        big_head.push_str("x-pad: 0123456789abcdef0123456789abcdef\r\n");
    }
    big_head.push_str("\r\n");
    let _ = s3.write_all(big_head.as_bytes());
    let (status, _, _) = read_response(&mut s3);
    assert_eq!(status, 431);
}

#[test]
fn idle_timeout_closes_quiet_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(2).enable_time();
    let multiring = MultiRing::install(&mut builder, tokio_multiring::Config::default());
    let rt = builder.build().unwrap();
    rt.block_on(async {
        let router = Router::new().route("/ping", get(|| async { "pong" }));
        let server = Server::new(
            multiring,
            Config {
                connections: 8,
                head_buffer: 4096,
                zero_copy: ZeroCopy::Never,
                worker_local_connections: true,
                idle_timeout: Some(Duration::from_millis(120)),
            },
        )
        .unwrap();
        server.listen(listener, router);
    });

    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    // First request succeeds
    s.write_all(b"GET /ping HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut s);
    assert_eq!(status, 200);
    assert_eq!(body, b"pong");

    // Sit idle past the timeout -> server closes cleanly (EOF)
    let mut buf = [0u8; 16];
    let n = read_some(&mut s, &mut buf);
    assert_eq!(n, 0, "idle connection should be closed by server");
}

#[test]
fn large_standard_body_direct_and_zc_paths() {
    let big: bytes::Bytes = (0..2_500_000usize)
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>()
        .into();
    let big_single = big.clone();
    let big_stream = big.clone();

    let make_router = move || {
        let b1 = big_single.clone();
        let b2 = big_stream.clone();
        Router::new()
            .route("/single", get(move || async move { b1.clone() }))
            .route(
                "/streamed",
                get(move || async move {
                    // Stream several 256 KiB Bytes chunks with an explicit Content-Length.
                    let chunks: Vec<Result<bytes::Bytes, std::io::Error>> = (0..b2.len())
                        .step_by(256 * 1024)
                        .map(|off| Ok(b2.slice(off..(off + 256 * 1024).min(b2.len()))))
                        .collect();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_LENGTH, b2.len())
                        .body(Body::from_stream(futures_util::stream::iter(chunks)))
                        .unwrap()
                }),
            )
            .route("/small", get(|| async { "ok" }))
    };

    for zc in [ZeroCopy::Never, ZeroCopy::Always] {
        let h = Harness::start(make_router(), zc, None);
        let mut s = h.connect();

        s.write_all(b"GET /single HTTP/1.1\r\nhost: test\r\n\r\n")
            .unwrap();
        let (status, _, body) = read_response(&mut s);
        assert_eq!(status, 200);
        assert_eq!(body.len(), big.len());
        assert_eq!(body, big.as_ref());

        // Same keep-alive connection: streamed multi-chunk > 2 MiB + small follow-up.
        s.write_all(b"GET /streamed HTTP/1.1\r\nhost: test\r\n\r\n")
            .unwrap();
        let (status, _, body) = read_response(&mut s);
        assert_eq!(status, 200);
        assert_eq!(body, big.as_ref());

        s.write_all(b"GET /small HTTP/1.1\r\nhost: test\r\n\r\n")
            .unwrap();
        let (status, _, body) = read_response(&mut s);
        assert_eq!(status, 200);
        assert_eq!(body, b"ok");
    }
}

async fn echo_direct_body(body: DirectBody) -> String {
    let bytes = match body {
        DirectBody::Direct(mut direct) => {
            let mut bytes = Vec::new();
            let mut chunk = [0; 64];
            loop {
                let count = direct.read_into(&mut chunk).await.unwrap();
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            bytes
        }
        DirectBody::Fallback(body) => axum::body::to_bytes(body, 1024).await.unwrap().to_vec(),
        DirectBody::Empty => Vec::new(),
    };
    String::from_utf8(bytes).unwrap()
}

#[test]
fn direct_extractor_respects_replaced_request_body() {
    async fn replace(req: axum::extract::Request, next: Next) -> axum::response::Response {
        let (parts, _) = req.into_parts();
        next.run(axum::extract::Request::from_parts(
            parts,
            Body::from("replacement"),
        ))
        .await
    }
    let router = Router::new()
        .route("/", post(echo_direct_body))
        .layer(middleware::from_fn(replace));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\noriginal")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"replacement");
}

#[test]
fn direct_extractor_respects_wrapped_request_body() {
    async fn uppercase(req: axum::extract::Request, next: Next) -> axum::response::Response {
        use http_body_util::BodyExt;
        let (parts, body) = req.into_parts();
        let body =
            body.map_frame(|frame| frame.map_data(|bytes| Bytes::from(bytes.to_ascii_uppercase())));
        next.run(axum::extract::Request::from_parts(parts, Body::new(body)))
            .await
    }
    let router = Router::new()
        .route("/", post(echo_direct_body))
        .layer(middleware::from_fn(uppercase));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\noriginal")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"ORIGINAL");
}

#[test]
fn streaming_request_body_can_be_returned_as_response() {
    let router = Router::new()
        .route("/", post(|body: Body| async move { body }))
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\noriginal")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"original");
    stream
        .write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"next");
}

struct TextWriter(&'static [u8]);

impl DirectWrite for TextWriter {
    fn write_to<'a>(
        self: Box<Self>,
        sink: &'a mut BodySink<'a>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        Box::pin(async move { sink.write_bytes(self.0).await })
    }
}

fn text_response() -> axum::response::Response {
    DirectResponse::new(
        Response::builder()
            .header(header::CONTENT_LENGTH, 8)
            .body(())
            .unwrap(),
        TextWriter(b"original"),
    )
    .into_response()
}

#[test]
fn direct_response_respects_body_replacement() {
    async fn replace(req: axum::extract::Request, next: Next) -> axum::response::Response {
        let mut response = next.run(req).await;
        *response.body_mut() = Body::empty();
        response
            .headers_mut()
            .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        response
    }
    let router = Router::new()
        .route("/", get(|| async { text_response() }))
        .layer(middleware::from_fn(replace));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert!(body.is_empty());
}

#[test]
fn direct_response_respects_body_wrapper() {
    async fn uppercase(req: axum::extract::Request, next: Next) -> axum::response::Response {
        use http_body_util::BodyExt;
        next.run(req).await.map(|body| {
            Body::new(
                body.map_frame(|frame| {
                    frame.map_data(|bytes| Bytes::from(bytes.to_ascii_uppercase()))
                }),
            )
        })
    }
    let router = Router::new()
        .route("/", get(|| async { text_response() }))
        .layer(middleware::from_fn(uppercase));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"GET / HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    let (status, _, body) = read_response(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"ORIGINAL");
}

fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        assert_eq!(read_some(stream, &mut byte), 1);
        head.push(byte[0]);
    }
    String::from_utf8(head).unwrap()
}

#[test]
fn streaming_prefix_and_expect_continue_precede_upload() {
    use futures_util::StreamExt;
    let router = Router::new()
        .route(
            "/",
            post(|body: Body| async move {
                let prefix =
                    stream::once(async { Ok::<_, axum::Error>(Bytes::from_static(b"prefix:")) });
                Body::from_stream(prefix.chain(body.into_data_stream()))
            }),
        )
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(
            b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\nexpect: 100-continue\r\n\r\n",
        )
        .unwrap();
    assert!(read_head(&mut stream).starts_with("HTTP/1.1 100 "));
    assert!(read_head(&mut stream).starts_with("HTTP/1.1 200 "));
    let mut prefix = [0; 12];
    stream.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"7\r\nprefix:\r\n");
    // The response has already streamed data before the request body arrives.
    stream.write_all(b"original").unwrap();
    let mut rest = Vec::new();
    while !rest.ends_with(b"0\r\n\r\n") {
        let mut byte = [0];
        assert_eq!(read_some(&mut stream, &mut byte), 1);
        rest.push(byte[0]);
    }
    assert_eq!(rest, b"8\r\noriginal\r\n0\r\n\r\n");
    stream
        .write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut stream).2, b"next");
}

#[test]
fn ignored_request_body_does_not_delay_final_response() {
    let router = Router::new()
        .route("/", post(|| async { StatusCode::PAYLOAD_TOO_LARGE }))
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut stream).0, 413);
    // The ignored input is drained only after the final response is available.
    stream
        .write_all(b"originalGET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut stream).2, b"next");
}

#[test]
fn direct_extraction_follows_body_when_middleware_swaps_requests() {
    type Waiting = (Body, tokio::sync::oneshot::Sender<Body>);
    let waiting = Arc::new(tokio::sync::Mutex::new(None::<Waiting>));
    let router = Router::new()
        .route("/", post(echo_direct_body))
        .layer(middleware::from_fn({
            let waiting = waiting.clone();
            move |req: axum::extract::Request, next: Next| {
                let waiting = waiting.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let mut guard = waiting.lock().await;
                    let body = if let Some((other, sender)) = guard.take() {
                        sender
                            .send(body)
                            .unwrap_or_else(|_| panic!("other request went away"));
                        drop(guard);
                        other
                    } else {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        *guard = Some((body, sender));
                        drop(guard);
                        receiver.await.unwrap()
                    };
                    next.run(axum::extract::Request::from_parts(parts, body))
                        .await
                }
            }
        }));
    let harness = Harness::start(router, ZeroCopy::Never, None);
    std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            let mut s = harness.connect();
            s.write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 5\r\n\r\nfirst")
                .unwrap();
            assert_eq!(read_response(&mut s).2, b"second");
        });
        let second = scope.spawn(|| {
            let mut s = harness.connect();
            s.write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 6\r\n\r\nsecond")
                .unwrap();
            assert_eq!(read_response(&mut s).2, b"first");
        });
        first.join().unwrap();
        second.join().unwrap();
    });
}

#[test]
fn direct_response_works_as_a_standard_body_without_a_runtime() {
    let mut body = std::pin::pin!(axum::body::to_bytes(text_response().into_body(), 1024));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let std::task::Poll::Ready(bytes) = body.as_mut().poll(&mut cx) else {
        panic!("an immediately ready producer should finish without a runtime");
    };
    assert_eq!(bytes.unwrap(), "original");
}

#[tokio::test]
async fn direct_response_fallback_is_bounded_and_cancelled_on_drop() {
    use http_body_util::BodyExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Writer {
        sent: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Writer {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl DirectWrite for Writer {
        fn write_to<'a>(
            self: Box<Self>,
            sink: &'a mut BodySink<'a>,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
            Box::pin(async move {
                assert!(!sink.zero_copy());
                for _ in 0..1024 {
                    sink.write_bytes(&[7; 16 * 1024]).await?;
                    self.sent.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            })
        }
    }
    let sent = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let mut body = DirectResponse::new(
        Response::new(()),
        Writer {
            sent: sent.clone(),
            dropped: dropped.clone(),
        },
    )
    .into_response()
    .into_body();
    assert_eq!(
        body.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .len(),
        16 * 1024
    );
    // No producer task runs independently: two chunks were queued while the
    // first poll made progress, and without polling the Body nothing advances.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(sent.load(Ordering::SeqCst), 2);
    drop(body);
    assert!(
        dropped.load(Ordering::SeqCst),
        "cancellation must drop the producer synchronously"
    );
}

#[test]
fn retained_idle_body_can_read_after_response() {
    let release = Arc::new(tokio::sync::Notify::new());
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let router = Router::new()
        .route(
            "/",
            post({
                let release = release.clone();
                move |body: Body| {
                    let release = release.clone();
                    let result_tx = result_tx.clone();
                    async move {
                        tokio::spawn(async move {
                            release.notified().await;
                            let bytes = axum::body::to_bytes(body, 100).await.unwrap();
                            result_tx.send(bytes).unwrap();
                        });
                        "accepted"
                    }
                }
            }),
        )
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start_with_connections(router, ZeroCopy::Never, None, 1);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\noriginal")
        .unwrap();
    assert_eq!(read_response(&mut stream).2, b"accepted");
    release.notify_one();
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "original"
    );
    stream
        .write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut stream).2, b"next");
}

#[test]
fn cancelled_retained_read_returns_head_buffers_before_reuse() {
    use http_body_util::BodyExt;
    let release = Arc::new(tokio::sync::Notify::new());
    let router = Router::new()
        .route(
            "/",
            post({
                let release = release.clone();
                move |mut body: Body| {
                    let release = release.clone();
                    async move {
                        let (started, ready) = tokio::sync::oneshot::channel();
                        tokio::spawn(async move {
                            let mut read = Box::pin(body.frame());
                            std::future::poll_fn(|cx| {
                                assert!(read.as_mut().poll(cx).is_pending());
                                std::task::Poll::Ready(())
                            })
                            .await;
                            started.send(()).unwrap();
                            release.notified().await;
                            drop(read);
                            drop(body);
                        });
                        ready.await.unwrap();
                        "accepted"
                    }
                }
            }),
        )
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start_with_connections(router, ZeroCopy::Never, None, 1);
    let mut first = harness.connect();
    first
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut first).2, b"accepted");
    let mut next = harness.connect();
    next.write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    next.set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let error = next
        .read(&mut [0; 1])
        .expect_err("retained input still owns the connection slot");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
    release.notify_one();
    next.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    assert_eq!(read_response(&mut next).2, b"next");
}

#[test]
fn cancelled_direct_receive_poisoned_connection_is_not_reused() {
    use std::task::Poll;
    let pool = BufferPool::new(4096, 1).unwrap();
    let handler_pool = pool.clone();
    let router = Router::new()
        .route(
            "/",
            post(move |body: DirectBody| {
                let pool = handler_pool.clone();
                async move {
                    let DirectBody::Direct(mut body) = body else {
                        panic!("native direct body expected")
                    };
                    let buf = pool.acquire().await;
                    let segments = [Segment {
                        buf: 0,
                        start: 0,
                        len: 8,
                    }];
                    let mut read = Box::pin(body.recv_vectored(vec![buf], &segments));
                    std::future::poll_fn(|cx| {
                        assert!(read.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    drop(read);
                    drop(body);
                    "cancelled"
                }
            }),
        )
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start_with_connections(router, ZeroCopy::Never, Some(pool), 1);
    let mut first = harness.connect();
    first
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut first).2, b"cancelled");
    first
        .write_all(b"originalGET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(
        read_some(&mut first, &mut [0; 1]),
        0,
        "stale request framing must not be reused"
    );
    drop(first);
    let mut next = harness.connect();
    next.write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut next).2, b"next");
}

#[test]
fn closing_response_reports_eof_before_waiting_for_retained_body() {
    let release = Arc::new(tokio::sync::Notify::new());
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let router = Router::new().route(
        "/",
        post({
            let release = release.clone();
            move |body: Body| {
                let release = release.clone();
                let result_tx = result_tx.clone();
                async move {
                    tokio::spawn(async move {
                        release.notified().await;
                        result_tx
                            .send(axum::body::to_bytes(body, 100).await.unwrap())
                            .unwrap();
                    });
                    "accepted"
                }
            }
        }),
    );
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream.write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 8\r\nconnection: close\r\n\r\noriginal").unwrap();
    assert_eq!(read_response(&mut stream).2, b"accepted");
    assert_eq!(read_some(&mut stream, &mut [0; 1]), 0);
    release.notify_one();
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "original"
    );
}

#[test]
fn caught_direct_send_cancellation_closes_without_serving_pipeline() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CancelWriter {
        pool: BufferPool,
        zc: bool,
    }
    impl DirectWrite for CancelWriter {
        fn write_to<'a>(
            self: Box<Self>,
            sink: &'a mut BodySink<'a>,
        ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
            Box::pin(async move {
                sink.flush().await?;
                let mut buf = self.pool.acquire().await;
                buf.extend_from_slice(b"original");
                let segments = [Segment {
                    buf: 0,
                    start: 0,
                    len: 8,
                }];
                async fn cancel<F: Future>(future: F) {
                    let mut future = Box::pin(future);
                    std::future::poll_fn(|cx| {
                        assert!(future.as_mut().poll(cx).is_pending());
                        std::task::Poll::Ready(())
                    })
                    .await;
                    drop(future);
                }
                if self.zc {
                    cancel(sink.send_zero_copy(vec![buf], &segments)).await;
                } else {
                    cancel(sink.send_vectored(vec![buf], &segments)).await;
                }
                // Deliberately catch cancellation, as a timeout/select user can.
                Ok(())
            })
        }
    }
    for zc in [false, true] {
        let pool = BufferPool::without_huge_pages(4096, 4).unwrap();
        let writer_pool = pool.clone();
        let next_calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                "/",
                get(move || {
                    let pool = writer_pool.clone();
                    async move {
                        DirectResponse::new(
                            Response::builder()
                                .header(header::CONTENT_LENGTH, 8)
                                .body(())
                                .unwrap(),
                            CancelWriter { pool, zc },
                        )
                    }
                }),
            )
            .route(
                "/next",
                get({
                    let next_calls = next_calls.clone();
                    move || {
                        next_calls.fetch_add(1, Ordering::SeqCst);
                        async { "must not run" }
                    }
                }),
            );
        let harness = Harness::start(router, ZeroCopy::Always, Some(pool));
        let mut stream = harness.connect();
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nhost: test\r\n\r\nGET /next HTTP/1.1\r\nhost: test\r\n\r\n",
            )
            .unwrap();
        let mut response = Vec::new();
        if let Err(error) = stream.read_to_end(&mut response) {
            assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
        }
        assert_eq!(next_calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn cancelled_continue_wait_still_finishes_interim_before_final_response() {
    let router = Router::new().route(
        "/",
        post(|body: DirectBody| async move {
            let DirectBody::Direct(mut body) = body else {
                panic!("direct body expected")
            };
            let mut accepting = Box::pin(body.accept_100());
            std::future::poll_fn(|cx| {
                assert!(accepting.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(accepting);
            drop(body);
            StatusCode::PAYLOAD_TOO_LARGE
        }),
    );
    let harness = Harness::start(router, ZeroCopy::Never, None);
    let mut stream = harness.connect();
    stream.write_all(b"POST / HTTP/1.1\r\nhost: test\r\ncontent-length: 100000\r\nexpect: 100-continue\r\n\r\n").unwrap();
    assert!(read_head(&mut stream).starts_with("HTTP/1.1 100 "));
    assert_eq!(read_response(&mut stream).0, 413);
    assert_eq!(read_some(&mut stream, &mut [0; 1]), 0);
}

#[test]
fn terminal_request_error_closes_even_if_failed_body_is_retained() {
    use http_body_util::BodyExt;
    let release = Arc::new(tokio::sync::Notify::new());
    let router = Router::new()
        .route(
            "/",
            post({
                let release = release.clone();
                move |mut body: Body| {
                    let release = release.clone();
                    async move {
                        let (failed, failure) = tokio::sync::oneshot::channel();
                        tokio::spawn(async move {
                            assert!(body.frame().await.unwrap().is_err());
                            failed.send(()).unwrap();
                            release.notified().await;
                            drop(body);
                        });
                        failure.await.unwrap();
                        StatusCode::BAD_REQUEST
                    }
                }
            }),
        )
        .route("/next", get(|| async { "next" }));
    let harness = Harness::start_with_connections(router, ZeroCopy::Never, None, 1);
    let mut stream = harness.connect();
    stream
        .write_all(b"POST / HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\nZ\r\n")
        .unwrap();
    assert_eq!(read_response(&mut stream).0, 400);
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    assert_eq!(read_some(&mut stream, &mut [0; 1]), 0);
    let mut next = harness.connect();
    next.write_all(b"GET /next HTTP/1.1\r\nhost: test\r\n\r\n")
        .unwrap();
    assert_eq!(read_response(&mut next).2, b"next");
    release.notify_one();
}
