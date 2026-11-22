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
use sidecar_axum::{BodySink, Config, DirectBody, DirectResponse, DirectWrite, Server, ZeroCopy};
use tokio::runtime::{Builder, Runtime};
use tokio_sidecar::{BufferPool, Segment, Sidecar};

struct Harness {
    addr: SocketAddr,
    _rt: Runtime,
}

impl Harness {
    fn start(router: Router, zero_copy: ZeroCopy, pool: Option<BufferPool>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_time();
        let sidecar = Sidecar::install(
            &mut builder,
            tokio_sidecar::Config {
                buffers: pool,
                ..Default::default()
            },
        );
        let rt = builder.build().unwrap();
        rt.block_on(async {
            let server = Server::new(
                sidecar,
                Config {
                    connections: 32,
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

fn read_response(stream: &mut TcpStream) -> (u16, String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk).unwrap();
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
            let n = stream.read(&mut chunk).unwrap();
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
                let n = stream.read(&mut chunk).unwrap();
                assert!(n > 0, "unexpected EOF reading chunk size");
                body.extend_from_slice(&chunk[..n]);
            }
            let crlf = body[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
            let size_str = std::str::from_utf8(&body[pos..pos + crlf]).unwrap().trim();
            let size = usize::from_str_radix(size_str, 16).unwrap();
            pos += crlf + 2;
            if size == 0 {
                while body.len() < pos + 2 {
                    let n = stream.read(&mut chunk).unwrap();
                    assert!(n > 0);
                    body.extend_from_slice(&chunk[..n]);
                }
                break;
            }
            while body.len() < pos + size + 2 {
                let n = stream.read(&mut chunk).unwrap();
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
                let n = stream.read(&mut chunk).unwrap();
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
    let n = s.read(&mut head_only).unwrap();
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
    let n = s.read(&mut interim).unwrap();
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
                    panic!("expected DirectBody::Direct on sidecar-axum");
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
    let n = s.read(&mut interim).unwrap();
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
    let sidecar = Sidecar::install(&mut builder, tokio_sidecar::Config::default());
    let rt = builder.build().unwrap();
    rt.block_on(async {
        let router = Router::new().route("/ping", get(|| async { "pong" }));
        let server = Server::new(
            sidecar,
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
    let n = s.read(&mut buf).unwrap();
    assert_eq!(n, 0, "idle connection should be closed by server");
}
