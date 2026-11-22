use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::serve::ListenerExt;
use hmac::{Hmac, Mac};
use multiring_axum::{Config as HttpConfig, Server, ZeroCopy};
use s3_axum::{
    AppOptions, AuthConfig, Backend, MultiRingStore, TokioStore, TokioUringStore, router,
    router_with_options,
};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};
use tokio_multiring::{BufferPool, MultiRing};

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
    fn start_multiring(roots: &[PathBuf], zero_copy: ZeroCopy) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let pool = BufferPool::without_huge_pages(64 * 1024, 32).unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_time();
        let multiring = MultiRing::install(
            &mut builder,
            tokio_multiring::Config {
                buffers: Some(pool),
                ..Default::default()
            },
        );
        let rt = builder.build().unwrap();
        let roots = roots.to_vec();
        rt.block_on(async move {
            let store = MultiRingStore::open(multiring.clone(), &roots, 16 * 1024, 2).unwrap();
            let app = router(Backend::MultiRing(store));
            let server = Server::new(
                multiring,
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

    fn start_tokio_uring(roots: &[PathBuf]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(2).enable_all().enable_io_uring();
        let rt = builder.build().unwrap();
        let roots = roots.to_vec();
        rt.block_on(async move {
            let store = TokioUringStore::open(&roots, 16 * 1024, 64 * 1024).unwrap();
            let app = router(Backend::TokioUring(store));
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

fn read_some(stream: &mut TcpStream, buf: &mut [u8]) -> usize {
    loop {
        match stream.read(buf) {
            Ok(n) => return n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read failed: {e}"),
        }
    }
}

fn read_response(stream: &mut TcpStream, head_only: bool) -> (u16, String, Vec<u8>) {
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = read_some(stream, &mut chunk);
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
            let n = read_some(stream, &mut chunk);
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

    // Range GET on a small (inline-read) 4096-byte object
    let small: Vec<u8> = (0..4096usize)
        .map(|i| ((i * 31 + 2 * 7) & 0xff) as u8)
        .collect();
    let (st, _, body) = send_request(
        &mut s,
        "GET /photos/obj-2 HTTP/1.1\r\nhost: test\r\nrange: bytes=100-599\r\n\r\n",
        b"",
    );
    assert_eq!(st, 206);
    assert_eq!(body, &small[100..=599]);

    // Overwrite a multi-part object (210 KiB) with a small single-part object (4 KiB),
    // then overwrite back to a multi-part object, verifying exact data both times.
    let small_replacement = vec![0xabu8; 4096];
    let req = format!(
        "PUT /photos/obj-5 HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        small_replacement.len()
    );
    assert_eq!(send_request(&mut s, &req, &small_replacement).0, 200);
    let (st, _, body) = send_request(
        &mut s,
        "GET /photos/obj-5 HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(body, small_replacement);

    let req = format!(
        "PUT /photos/obj-5 HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        big.len()
    );
    assert_eq!(send_request(&mut s, &req, &big).0, 200);
    let (st, _, body) = send_request(
        &mut s,
        "GET /photos/obj-5 HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(body, big);

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
fn multiring_runtime_suite() {
    let roots = scratch_roots("multiring", 2);
    let server = RunningServer::start_multiring(&roots, ZeroCopy::Always);
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
fn tokio_uring_runtime_suite() {
    let roots = scratch_roots("tokio-uring", 2);
    let server = RunningServer::start_tokio_uring(&roots);
    run_suite(&server);
    drop(server);
    cleanup(&roots);
}

#[test]
fn cross_runtime_on_disk_interoperability() {
    let roots = scratch_roots("interop", 2);
    let multiring = RunningServer::start_multiring(&roots, ZeroCopy::Always);
    let tokio_srv = RunningServer::start_tokio(&roots, false);

    let mut s_multiring = multiring.connect();
    let mut s_tokio = tokio_srv.connect();

    // Create bucket via Tokio, PUT via Tokio, GET via MultiRing!
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
        &mut s_multiring,
        "GET /shared/from-tokio HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(got, from_tokio);

    // PUT via MultiRing, GET via Tokio!
    let from_multiring: Vec<u8> = (0..220_000usize).map(|i| ((i * 13) % 251) as u8).collect();
    let req = format!(
        "PUT /shared/from-multiring HTTP/1.1\r\nhost: test\r\ncontent-length: {}\r\n\r\n",
        from_multiring.len()
    );
    assert_eq!(send_request(&mut s_multiring, &req, &from_multiring).0, 200);

    let (st, _, got) = send_request(
        &mut s_tokio,
        "GET /shared/from-multiring HTTP/1.1\r\nhost: test\r\n\r\n",
        b"",
    );
    assert_eq!(st, 200);
    assert_eq!(got, from_multiring);

    drop(multiring);
    drop(tokio_srv);
    cleanup(&roots);
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sigv4_headers(
    method: &str,
    path: &str,
    query: &str,
    host: &str,
    payload_hash: &str,
    extra: &[(&str, &str)],
    auth: &AuthConfig,
) -> (String, String, [u8; 32]) {
    let amz_date = "20261122T040000Z";
    let date = &amz_date[..8];
    let scope = format!("{date}/us-east-1/s3/aws4_request");

    let mut hdrs: Vec<(&str, &str)> = vec![
        ("host", host),
        ("x-amz-content-sha256", payload_hash),
        ("x-amz-date", amz_date),
    ];
    hdrs.extend_from_slice(extra);
    hdrs.sort_by_key(|(k, _)| *k);

    let mut canon_hdrs = String::new();
    let mut signed_names = Vec::new();
    for (k, v) in &hdrs {
        canon_hdrs.push_str(k);
        canon_hdrs.push(':');
        canon_hdrs.push_str(v.trim());
        canon_hdrs.push('\n');
        signed_names.push(*k);
    }
    let signed_str = signed_names.join(";");
    let canon_req =
        format!("{method}\n{path}\n{query}\n{canon_hdrs}\n{signed_str}\n{payload_hash}");
    let canon_hash = hex::encode(Sha256::digest(canon_req.as_bytes()));
    let sts = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canon_hash}");

    let k_date = hmac_sha256(
        format!("AWS4{}", auth.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, b"us-east-1");
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&k_signing, sts.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_str}, Signature={sig}",
        auth.access_key
    );
    let mut raw = format!(
        "{method} {}{} HTTP/1.1\r\n",
        path,
        if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        }
    );
    for (k, v) in &hdrs {
        raw.push_str(&format!("{k}: {v}\r\n"));
    }
    raw.push_str(&format!("authorization: {auth_header}\r\n"));
    (raw, sig, k_signing)
}

fn run_sigv4_suite(addr: SocketAddr, auth: &AuthConfig) {
    let host = addr.to_string();
    let empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let connect = || {
        let s = TcpStream::connect(addr).unwrap();
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s
    };

    // Unauthenticated request -> 403 AccessDenied
    let mut s = connect();
    let (st, _, _) = send_request(&mut s, "GET / HTTP/1.1\r\nhost: test\r\n\r\n", b"");
    assert_eq!(st, 403);

    // Bad signature -> 403 SignatureDoesNotMatch
    let wrong = AuthConfig {
        access_key: auth.access_key.clone(),
        secret_key: "wrong-secret".into(),
    };
    let (mut req, _, _) = sigv4_headers("GET", "/", "", &host, empty_hash, &[], &wrong);
    req.push_str("\r\n");
    let (st, _, body) = send_request(&mut s, &req, b"");
    assert_eq!(st, 403);
    assert!(String::from_utf8_lossy(&body).contains("SignatureDoesNotMatch"));

    // Create bucket with valid SigV4
    let (mut req, _, _) = sigv4_headers("PUT", "/secure", "", &host, empty_hash, &[], auth);
    req.push_str("content-length: 0\r\n\r\n");
    let (st, _, _) = send_request(&mut s, &req, b"");
    assert_eq!(st, 200);

    // PUT with exact SHA-256 hash verification
    let payload: Vec<u8> = (0..100_000usize).map(|i| ((i * 17) & 0xff) as u8).collect();
    let good_hash = hex::encode(Sha256::digest(&payload));
    let (mut req, _, _) = sigv4_headers("PUT", "/secure/hashed", "", &host, &good_hash, &[], auth);
    req.push_str(&format!("content-length: {}\r\n\r\n", payload.len()));
    let (st, _, _) = send_request(&mut s, &req, &payload);
    assert_eq!(st, 200);

    // PUT with mismatched SHA-256 -> 400 XAmzContentSHA256Mismatch
    let mut bad_payload = payload.clone();
    bad_payload[42] ^= 0xff;
    let (mut req, _, _) = sigv4_headers("PUT", "/secure/badhash", "", &host, &good_hash, &[], auth);
    req.push_str(&format!("content-length: {}\r\n\r\n", bad_payload.len()));
    let (st, _, body) = send_request(&mut s, &req, &bad_payload);
    assert_eq!(st, 400);
    assert!(String::from_utf8_lossy(&body).contains("XAmzContentSHA256Mismatch"));

    // Signed aws-chunked upload with trailing checksum header (AWS CLI v2 / SDK style)
    let chunked_data: Vec<u8> = (0..120_000usize).map(|i| ((i * 29) & 0xff) as u8).collect();
    let decoded_len_str = chunked_data.len().to_string();
    let (mut req, mut prev_sig, key) = sigv4_headers(
        "PUT",
        "/secure/streamed",
        "",
        &host,
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        &[
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", &decoded_len_str),
        ],
        auth,
    );
    let mut wire = Vec::new();
    for piece in chunked_data.chunks(32_768).chain(std::iter::once(&[][..])) {
        let piece_hash = hex::encode(Sha256::digest(piece));
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n20261122T040000Z\n20261122/us-east-1/s3/aws4_request\n{prev_sig}\n{empty_hash}\n{piece_hash}"
        );
        prev_sig = hex::encode(hmac_sha256(&key, sts.as_bytes()));
        wire.extend_from_slice(
            format!("{:x};chunk-signature={prev_sig}\r\n", piece.len()).as_bytes(),
        );
        wire.extend_from_slice(piece);
        if !piece.is_empty() {
            wire.extend_from_slice(b"\r\n");
        }
    }
    wire.extend_from_slice(b"x-amz-checksum-crc32:AAAAAA==\r\n\r\n");
    req.push_str(&format!("content-length: {}\r\n\r\n", wire.len()));
    let (st, _, _) = send_request(&mut s, &req, &wire);
    assert_eq!(st, 200);

    // GET back both objects with valid SigV4 and verify exact bytes
    let (mut req, _, _) = sigv4_headers("GET", "/secure/hashed", "", &host, empty_hash, &[], auth);
    req.push_str("\r\n");
    let (st, _, body) = send_request(&mut s, &req, b"");
    assert_eq!(st, 200);
    assert_eq!(body, payload);

    let (mut req, _, _) =
        sigv4_headers("GET", "/secure/streamed", "", &host, empty_hash, &[], auth);
    req.push_str("\r\n");
    let (st, _, body) = send_request(&mut s, &req, b"");
    assert_eq!(st, 200);
    assert_eq!(body, chunked_data);
}

#[test]
fn sigv4_and_aws_chunked_on_both_runtimes() {
    let auth = AuthConfig {
        access_key: "AKIAEXAMPLEKEY".into(),
        secret_key: "supersecretkey1234567890".into(),
    };

    // 1. MultiRing runtime
    let roots_sc = scratch_roots("sigv4-multiring", 2);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let pool = BufferPool::without_huge_pages(64 * 1024, 32).unwrap();
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(2).enable_time();
    let multiring = MultiRing::install(
        &mut builder,
        tokio_multiring::Config {
            buffers: Some(pool),
            ..Default::default()
        },
    );
    let rt_sc = builder.build().unwrap();
    let auth_sc = auth.clone();
    let roots_clone = roots_sc.clone();
    rt_sc.block_on(async move {
        let store = MultiRingStore::open(multiring.clone(), &roots_clone, 16 * 1024, 2).unwrap();
        let app = router_with_options(
            Backend::MultiRing(store),
            AppOptions {
                trace: false,
                auth: Some(auth_sc),
            },
        );
        Server::new(
            multiring,
            HttpConfig {
                connections: 16,
                head_buffer: 4096,
                zero_copy: ZeroCopy::Always,
                worker_local_connections: true,
                idle_timeout: None,
            },
        )
        .unwrap()
        .listen(listener, app);
    });
    run_sigv4_suite(addr, &auth);
    drop(rt_sc);
    cleanup(&roots_sc);

    // 2. Standard Tokio runtime
    let roots_tk = scratch_roots("sigv4-tokio", 2);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut builder = Builder::new_multi_thread();
    builder.worker_threads(2).enable_all();
    let rt_tk = builder.build().unwrap();
    let auth_tk = auth.clone();
    let roots_clone = roots_tk.clone();
    rt_tk.block_on(async move {
        let store = TokioStore::open(&roots_clone, 16 * 1024, 64 * 1024, false).unwrap();
        let app = router_with_options(
            Backend::Tokio(store),
            AppOptions {
                trace: false,
                auth: Some(auth_tk),
            },
        );
        let listener = tokio::net::TcpListener::from_std(listener)
            .unwrap()
            .tap_io(|s| {
                let _ = s.set_nodelay(true);
            });
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
    });
    run_sigv4_suite(addr, &auth);
    drop(rt_tk);
    cleanup(&roots_tk);
}
