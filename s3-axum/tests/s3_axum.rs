use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::serve::ListenerExt;
use s3_axum::{Backend, SidecarStore, TokioStore, router};
use sidecar_axum::{Config as HttpConfig, Server, ZeroCopy};
use tokio::runtime::{Builder, Runtime};
use tokio_sidecar::{BufferPool, Sidecar};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn scratch_roots(name: &str, drives: usize) -> Vec<PathBuf> {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/tmp")
        .join(format!("s3-axum-{name}-{pid}-{id}"));
    (0..drives)
        .map(|d| {
            let dir = base.join(format!("d{d}"));
            std::fs::create_dir_all(&dir).unwrap();
            dir
        })
        .collect()
}

struct RunningServer {
    addr: SocketAddr,
    _rt: Runtime,
}

impl RunningServer {
    fn start_sidecar(roots: &[PathBuf], zero_copy: ZeroCopy) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = BufferPool::without_huge_pages(64 * 1024, 32).unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_time();
        let sidecar = Sidecar::install(
            &mut builder,
            tokio_sidecar::Config {
                buffers: Some(pool),
                ..Default::default()
            },
        );
        let rt = builder.build().unwrap();
        let roots = roots.to_vec();
        rt.block_on(async move {
            let store = SidecarStore::open(sidecar.clone(), &roots, 16 * 1024, 2).unwrap();
            let app = router(Backend::Sidecar(store));
            let server = Server::new(
                sidecar,
                HttpConfig {
                    connections: 32,
                    head_buffer: 4096,
                    zero_copy,
                    worker_local_connections: true,
                    idle_timeout: None,
                },
            )
            .unwrap();
            server.listen(listener, app);
        });
        Self { addr, _rt: rt }
    }

    fn start_tokio(roots: &[PathBuf], direct: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_all();
        let rt = builder.build().unwrap();
        let roots = roots.to_vec();
        rt.block_on(async move {
            let store = TokioStore::open(&roots, 16 * 1024, 64 * 1024, direct).unwrap();
            let app = router(Backend::Tokio(store));
            let listener = tokio::net::TcpListener::from_std(listener)
                .unwrap()
                .tap_io(|s| {
                    let _ = s.set_nodelay(true);
                });
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
        });
        Self { addr, _rt: rt }
    }

    fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(self.addr).unwrap();
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    }
}

fn send_request(stream: &mut TcpStream, head: &str, body: &[u8]) -> (u16, String, Vec<u8>) {
    stream.write_all(head.as_bytes()).unwrap();
    if !body.is_empty() {
        stream.write_all(body).unwrap();
    }
    read_response(stream, head.starts_with("HEAD "))
}

fn read_response(stream: &mut TcpStream, head_only: bool) -> (u16, String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = stream.read(&mut chunk).unwrap();
        assert!(n > 0, "unexpected EOF waiting for headers");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8(raw[..head_end].to_vec()).unwrap();
    let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[head_end..].to_vec();
    if head_only || status == 204 || status == 304 {
        return (status, head, body);
    }

    let lower = head.to_ascii_lowercase();
    if let Some(line) = lower
        .lines()
        .find(|line| line.starts_with("content-length:"))
    {
        let len: usize = line["content-length:".len()..].trim().parse().unwrap();
        while body.len() < len {
            let n = stream.read(&mut chunk).unwrap();
            assert!(n > 0, "unexpected EOF reading body");
            body.extend_from_slice(&chunk[..n]);
        }
        assert_eq!(body.len(), len);
    }
    (status, head, body)
}

fn run_suite(server: &RunningServer) {
    let mut s = server.connect();

    // Create bucket -> 200, duplicate -> 409, HEAD -> 200
    let (st, _, _) = send_request(
        &mut s,
        "PUT /photos HTTP/1.1\r\nhost: test\r\ncontent-length: 0\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    let (st, _, _) = send_request(
        &mut s,
        "PUT /photos HTTP/1.1\r\nhost: test\r\ncontent-length: 0\r\n\r\n",
        b"",
    );
    assert_eq!(st, 409);
    let (st, _, _) = send_request(&mut s, "HEAD /photos HTTP/1.1\r\nhost: test\r\n\r\n", b"");
    assert_eq!(st, 200);

    // PUT & GET objects of empty, small, and awkward multi-stripe sizes
    for (idx, size) in [0usize, 17, 4096, 16_384, 16_385, 210_003]
        .into_iter()
        .enumerate()
    {
        let payload: Vec<u8> = (0..size)
            .map(|i| ((i * 31 + idx * 7) & 0xff) as u8)
            .collect();
        let req = format!(
            "PUT /photos/obj-{idx} HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
            payload.len()
        );
        let (st, head, _) = send_request(&mut s, &req, &payload);
        assert_eq!(st, 200, "put size {size}");
        assert!(head.to_ascii_lowercase().contains("etag:"));

        let req = format!("GET /photos/obj-{idx} HTTP/1.1\r\nhost: test\r\n\r\n");
        let (st, _, body) = send_request(&mut s, &req, b"");
        assert_eq!(st, 200, "get size {size}");
        assert_eq!(body, payload, "mismatch for size {size}");

        let req = format!("HEAD /photos/obj-{idx} HTTP/1.1\r\nhost: test\r\n\r\n");
        let (st, head, body) = send_request(&mut s, &req, b"");
        assert_eq!(st, 200);
        assert!(body.is_empty());
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("content-length: {size}"))
        );
    }

    // Range GET on the 210_003 byte object
    let big: Vec<u8> = (0..210_003usize)
        .map(|i| ((i * 31 + 5 * 7) & 0xff) as u8)
        .collect();
    let (st, head, body) = send_request(
        &mut s,
        "GET /photos/obj-5 HTTP/1.1\r\nhost: test\r\nrange: bytes=15000-50000\r\n\r\n",
        b"",
    );
    assert_eq!(st, 206);
    assert!(
        head.to_ascii_lowercase()
            .contains("content-range: bytes 15000-50000/210003")
    );
    assert_eq!(body, &big[15000..=50000]);

    // Unsatisfiable range -> 416
    let (st, _, _) = send_request(
        &mut s,
        "GET /photos/obj-5 HTTP/1.1\r\nhost: test\r\nrange: bytes=999999-\r\n\r\n",
        b"",
    );
    assert_eq!(st, 416);

    // Chunked upload
    let (st, _, _) = send_request(
        &mut s,
        "PUT /photos/chunked HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\n",
        b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
    );
    assert_eq!(st, 200);
    let (st, head, body) = send_request(
        &mut s,
        "GET /photos/chunked HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert!(head.to_ascii_lowercase().contains("x-amz-request-id:"));
    assert!(head.to_ascii_lowercase().contains("x-s3-runtime:"));
    assert_eq!(body, b"hello world");

    // ListBuckets (GET /)
    let (st, _, body) = send_request(&mut s, "GET / HTTP/1.1\r\nhost: test\r\n\r\n", b"");
    assert_eq!(st, 200);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<Name>photos</Name>"), "unexpected xml: {xml}");

    // ListObjectsV2 with prefix and pagination
    let (st, _, body) = send_request(
        &mut s,
        "GET /photos?list-type=2&prefix=obj-&max-keys=3 HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<KeyCount>3</KeyCount>"), "{xml}");
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"), "{xml}");
    assert!(xml.contains("<Key>obj-0</Key>"), "{xml}");
    assert!(xml.contains("<Key>obj-2</Key>"), "{xml}");
    assert!(
        xml.contains("<NextContinuationToken>obj-2</NextContinuationToken>"),
        "{xml}"
    );
    assert!(!xml.contains(".part-"), "internal parts leaked: {xml}");

    let (st, _, body) = send_request(
        &mut s,
        "GET /photos?list-type=2&prefix=obj-&max-keys=10&continuation-token=obj-2 HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("<KeyCount>3</KeyCount>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    assert!(xml.contains("<Key>obj-3</Key>"), "{xml}");
    assert!(xml.contains("<Key>obj-5</Key>"), "{xml}");

    // Delete objects and bucket
    for name in [
        "obj-0", "obj-1", "obj-2", "obj-3", "obj-4", "obj-5", "chunked",
    ] {
        let req = format!("DELETE /photos/{name} HTTP/1.1\r\nhost: test\r\n\r\n");
        let (st, _, _) = send_request(&mut s, &req, b"");
        assert_eq!(st, 204);
    }
    let (st, _, _) = send_request(&mut s, "DELETE /photos HTTP/1.1\r\nhost: test\r\n\r\n", b"");
    assert_eq!(st, 204);
}

fn cleanup(roots: &[PathBuf]) {
    if let Some(parent) = roots.first().and_then(|p| Path::parent(p)) {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[test]
fn sidecar_runtime_suite() {
    let roots = scratch_roots("sidecar", 2);
    let server = RunningServer::start_sidecar(&roots, ZeroCopy::Always);
    run_suite(&server);
    drop(server);
    cleanup(&roots);
}

#[test]
fn tokio_runtime_buffered_suite() {
    let roots = scratch_roots("tokio-buf", 2);
    let server = RunningServer::start_tokio(&roots, false);
    run_suite(&server);
    drop(server);
    cleanup(&roots);
}

#[test]
fn tokio_runtime_direct_suite() {
    let roots = scratch_roots("tokio-direct", 2);
    let server = RunningServer::start_tokio(&roots, true);
    run_suite(&server);
    drop(server);
    cleanup(&roots);
}

#[test]
fn cross_runtime_on_disk_interoperability() {
    let roots = scratch_roots("interop", 2);
    let sidecar = RunningServer::start_sidecar(&roots, ZeroCopy::Always);
    let tokio_srv = RunningServer::start_tokio(&roots, false);

    let mut s_sidecar = sidecar.connect();
    let mut s_tokio = tokio_srv.connect();

    // Create bucket via Tokio, PUT via Tokio, GET via Sidecar!
    let (st, _, _) = send_request(
        &mut s_tokio,
        "PUT /shared HTTP/1.1\r\nhost: test\r\ncontent-length: 0\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);

    let from_tokio: Vec<u8> = (0..180_000usize).map(|i| (i % 199) as u8).collect();
    let req = format!(
        "PUT /shared/from-tokio HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        from_tokio.len()
    );
    assert_eq!(send_request(&mut s_tokio, &req, &from_tokio).0, 200);

    let (st, _, got) = send_request(
        &mut s_sidecar,
        "GET /shared/from-tokio HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(got, from_tokio);

    // PUT via Sidecar, GET via Tokio!
    let from_sidecar: Vec<u8> = (0..220_000usize).map(|i| ((i * 13) % 251) as u8).collect();
    let req = format!(
        "PUT /shared/from-sidecar HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        from_sidecar.len()
    );
    assert_eq!(send_request(&mut s_sidecar, &req, &from_sidecar).0, 200);

    let (st, _, got) = send_request(
        &mut s_tokio,
        "GET /shared/from-sidecar HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(got, from_sidecar);

    drop(sidecar);
    drop(tokio_srv);
    cleanup(&roots);
}
