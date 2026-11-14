//! The server, driven over real sockets with a client that writes HTTP by
//! hand.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use sidecar_s3::{Config, Server};
use tokio_sidecar::{BufferPool, DiskConfig, Domain, Hyperthreads, Plan, Sidecar};

/// Size of the buffers object data moves through. Small, so that modest
/// objects span several.
const BUFFER: usize = 64 * 1024;

struct Fixture {
    address: SocketAddr,
    root: PathBuf,
    /// Keeps the server running.
    _runtime: tokio::runtime::Runtime,
}

fn start(name: &str) -> Fixture {
    start_with(name, false)
}

fn start_with(name: &str, polled: bool) -> Fixture {
    let root = root(name);
    std::fs::create_dir(&root).unwrap();
    serve(root, polled)
}

/// Where the server called `name` keeps its buckets.
fn root(name: &str) -> PathBuf {
    std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-{}", std::process::id()))
}

/// Starts a server on the directory `root`.
fn serve(root: PathBuf, polled: bool) -> Fixture {
    // Two workers on whichever CPUs this process may use.
    let detected = Plan::detect(Hyperthreads::Use).unwrap();
    let cpus = detected
        .domains()
        .iter()
        .flat_map(|domain| domain.workers.iter().chain(&domain.sq_thread).copied());
    let plan = Plan::new(vec![Domain {
        workers: cpus.take(2).collect(),
        sq_thread: None,
    }])
    .unwrap();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal);
    let runtime = plan.build(&mut builder).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    runtime.block_on(async {
        let sidecar = Sidecar::start(tokio_sidecar::Config {
            buffers: Some(BufferPool::new(BUFFER, 3).unwrap()),
            disk: polled.then(|| DiskConfig {
                sq_entries: 32,
                sq_thread_idle: Duration::from_millis(50),
                plan,
            }),
            ..tokio_sidecar::Config::default()
        })
        .await
        .unwrap();

        let mut config = Config::new(&root);
        config.connections = 4;
        config.head_buffer = 4096;
        Server::new(sidecar, config).unwrap().listen(listener);
    });
    Fixture {
        address,
        root,
        _runtime: runtime,
    }
}

impl Fixture {
    fn connect(&self) -> Client {
        let stream = TcpStream::connect(self.address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.set_nodelay(true).unwrap();
        Client(BufReader::new(stream))
    }

    /// A client, with the bucket `bucket` made.
    fn with_bucket(&self) -> Client {
        let mut client = self.connect();
        assert_eq!(client.request("PUT", "/bucket", &[], b"").status, 200);
        client
    }

    /// Names of the files in the bucket's directory.
    fn files(&self) -> Vec<String> {
        let mut files: Vec<String> = std::fs::read_dir(self.root.join("bucket"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        files
    }
}

struct Client(BufReader<TcpStream>);

struct Reply {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> &str {
        self.headers.get(name).map_or("", String::as_str)
    }

    fn text(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap()
    }
}

impl Client {
    fn send(&mut self, bytes: &[u8]) {
        self.0.get_mut().write_all(bytes).unwrap();
    }

    fn head(method: &str, path: &str, headers: &[(&str, &str)]) -> Vec<u8> {
        let mut head = format!("{method} {path} HTTP/1.1\r\nhost: test\r\n");
        for (name, value) in headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.into_bytes()
    }

    /// Sends a request with a body of known length, and reads the reply.
    fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Reply {
        let mut request = Client::head(method, path, headers);
        if method == "PUT" || !body.is_empty() {
            request.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        request.extend_from_slice(body);
        self.send(&request);
        self.reply(method == "HEAD")
    }

    fn get(&mut self, path: &str) -> Reply {
        self.request("GET", path, &[], b"")
    }

    fn put(&mut self, path: &str, body: &[u8]) -> Reply {
        self.request("PUT", path, &[], body)
    }

    fn line(&mut self) -> String {
        let mut line = String::new();
        self.0.read_line(&mut line).unwrap();
        line.trim_end().to_owned()
    }

    fn reply(&mut self, to_head: bool) -> Reply {
        let status_line = self.line();
        let status: u16 = status_line
            .split(' ')
            .nth(1)
            .unwrap_or_else(|| panic!("not a status line: {status_line:?}"))
            .parse()
            .unwrap();
        let mut headers = HashMap::new();
        loop {
            let line = self.line();
            if line.is_empty() {
                break;
            }
            let (name, value) = line.split_once(": ").unwrap();
            assert!(headers.insert(name.to_owned(), value.to_owned()).is_none());
        }

        let mut body = Vec::new();
        if !to_head && status >= 200 && status != 204 {
            body.resize(headers["content-length"].parse().unwrap(), 0);
            self.0.read_exact(&mut body).unwrap();
        }
        Reply {
            status,
            headers,
            body,
        }
    }

    /// Whether the server has closed the connection.
    fn is_closed(&mut self) -> bool {
        matches!(self.0.read(&mut [0]), Ok(0))
    }
}

/// Bytes that differ from block to block and buffer to buffer.
fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index % 251 + index / 4096) as u8)
        .collect()
}

fn error_code(reply: &Reply) -> &str {
    let text = reply.text();
    let start = text.find("<Code>").expect("an error body") + 6;
    &text[start..text.find("</Code>").unwrap()]
}

#[test]
fn bucket_lifecycle() {
    let server = start("buckets");
    let mut client = server.connect();
    assert_eq!(client.request("HEAD", "/bucket", &[], b"").status, 404);

    let created = client.request("PUT", "/bucket", &[], b"");
    assert_eq!(
        (created.status, created.header("content-length")),
        (200, "0")
    );
    assert!(server.root.join("bucket").is_dir());
    let again = client.request("PUT", "/bucket", &[], b"");
    assert_eq!(
        (again.status, error_code(&again)),
        (409, "BucketAlreadyOwnedByYou")
    );
    assert_eq!(client.request("HEAD", "/bucket", &[], b"").status, 200);

    assert_eq!(client.put("/bucket/key", b"x").status, 200);
    let full = client.request("DELETE", "/bucket", &[], b"");
    assert_eq!((full.status, error_code(&full)), (409, "BucketNotEmpty"));
    assert_eq!(
        client.request("DELETE", "/bucket/key", &[], b"").status,
        204
    );

    assert_eq!(client.request("DELETE", "/bucket", &[], b"").status, 204);
    let gone = client.request("DELETE", "/bucket", &[], b"");
    assert_eq!((gone.status, error_code(&gone)), (404, "NoSuchBucket"));
    assert!(!server.root.join("bucket").exists());
}

#[test]
fn object_round_trip() {
    let server = start("objects");
    let mut client = server.with_bucket();

    let stored = client.put("/bucket/greeting", b"hello world");
    assert_eq!(stored.status, 200);
    let tag = stored.header("etag").to_owned();
    assert!(tag.starts_with('"') && tag.ends_with('"') && tag.len() > 2);

    let fetched = client.get("/bucket/greeting");
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.body, b"hello world");
    assert_eq!(fetched.header("content-length"), "11");
    assert_eq!(fetched.header("etag"), tag);
    assert_eq!(fetched.header("accept-ranges"), "bytes");
    assert_eq!(fetched.header("content-type"), "binary/octet-stream");
    assert!(fetched.header("last-modified").ends_with(" GMT"));
    assert!(fetched.header("date").ends_with(" GMT"));

    let looked_up = client.request("HEAD", "/bucket/greeting", &[], b"");
    assert_eq!(looked_up.status, 200);
    assert_eq!(looked_up.header("content-length"), "11");
    assert_eq!(looked_up.header("etag"), tag);
    assert_eq!(
        looked_up.header("last-modified"),
        fetched.header("last-modified")
    );

    assert_eq!(
        client
            .request("DELETE", "/bucket/greeting", &[], b"")
            .status,
        204
    );
    // Deleting what is not there succeeds too.
    assert_eq!(
        client
            .request("DELETE", "/bucket/greeting", &[], b"")
            .status,
        204
    );
    let missing = client.get("/bucket/greeting");
    assert_eq!((missing.status, error_code(&missing)), (404, "NoSuchKey"));
    assert_eq!(missing.header("content-type"), "application/xml");
    assert_eq!(
        client.request("HEAD", "/bucket/greeting", &[], b"").status,
        404
    );
}

/// Sizes around the block and buffer boundaries, where padding, truncation
/// and the hand-over between buffers happen.
#[test]
fn objects_of_awkward_sizes() {
    let server = start("sizes");
    let mut client = server.with_bucket();
    for size in [
        0,
        1,
        4095,
        4096,
        4097,
        BUFFER - 1,
        BUFFER,
        BUFFER + 1,
        3 * BUFFER,
        300_001,
    ] {
        let path = format!("/bucket/size-{size}");
        let body = pattern(size);
        assert_eq!(client.put(&path, &body).status, 200, "{size}");

        let on_disk = server.root.join(format!("bucket/size-{size}"));
        assert_eq!(on_disk.metadata().unwrap().len(), size as u64);
        assert_eq!(std::fs::read(on_disk).unwrap(), body, "{size}");

        let fetched = client.get(&path);
        assert_eq!(fetched.status, 200, "{size}");
        assert_eq!(fetched.header("content-length"), size.to_string());
        assert!(fetched.body == body, "{size}");
    }
    assert_eq!(server.files().len(), 10);
}

#[test]
fn byte_ranges() {
    let server = start("ranges");
    let mut client = server.with_bucket();
    let size = 300_001;
    let body = pattern(size);
    assert_eq!(client.put("/bucket/object", &body).status, 200);

    for (range, expected) in [
        ("bytes=0-0", 0..1),
        ("bytes=5-9", 5..10),
        ("bytes=4095-4096", 4095..4097),
        ("bytes=65530-65545", 65530..65546),
        ("bytes=1000-200000", 1000..200_001),
        ("bytes=300000-", 300_000..size),
        ("bytes=-7", size - 7..size),
        ("bytes=299990-999999", 299_990..size),
    ] {
        let part = client.request("GET", "/bucket/object", &[("range", range)], b"");
        assert_eq!(part.status, 206, "{range}");
        assert!(part.body == body[expected.clone()], "{range}");
        let described = format!("bytes {}-{}/{size}", expected.start, expected.end - 1);
        assert_eq!(part.header("content-range"), described);
    }

    let whole = client.request("GET", "/bucket/object", &[("range", "bytes=9-1")], b"");
    assert_eq!((whole.status, whole.body.len()), (200, size));

    let past = client.request("GET", "/bucket/object", &[("range", "bytes=300001-")], b"");
    assert_eq!((past.status, error_code(&past)), (416, "InvalidRange"));
    assert_eq!(past.header("content-range"), "bytes */300001");

    let looked_up = client.request("HEAD", "/bucket/object", &[("range", "bytes=5-9")], b"");
    assert_eq!(
        (looked_up.status, looked_up.header("content-length")),
        (206, "5")
    );
}

#[test]
fn polled_disk_rings() {
    let server = start_with("polled", true);
    let mut client = server.with_bucket();
    let body = pattern(300_001);
    assert_eq!(client.put("/bucket/object", &body).status, 200);
    assert_eq!(
        std::fs::read(server.root.join("bucket/object")).unwrap(),
        body
    );
    assert!(client.get("/bucket/object").body == body);
    let part = client.request(
        "GET",
        "/bucket/object",
        &[("range", "bytes=65530-65545")],
        b"",
    );
    assert_eq!(part.body, body[65530..65546]);
}

#[test]
fn chunked_upload() {
    let server = start("chunked");
    let mut client = server.with_bucket();
    let body = pattern(150_000);

    let mut request = Client::head("PUT", "/bucket/object", &[("transfer-encoding", "chunked")]);
    request.extend_from_slice(b"\r\n");
    for chunk in body.chunks(7_777) {
        request.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
        request.extend_from_slice(chunk);
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"0\r\n\r\n");
    // In pieces that cut chunk headers in two.
    for piece in request.chunks(1_009) {
        client.send(piece);
    }
    assert_eq!(client.reply(false).status, 200);
    assert!(client.get("/bucket/object").body == body);

    client.send(b"PUT /bucket/broken HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\nnot hex\r\n");
    let refused = client.reply(false);
    assert_eq!((refused.status, error_code(&refused)), (400, "BadRequest"));
    assert_eq!(refused.header("connection"), "close");
    assert!(client.is_closed());
    assert_eq!(server.files(), ["object"]);
}

#[test]
fn pipelined_requests() {
    let server = start("pipelined");
    let mut client = server.with_bucket();
    let body = pattern(100_000);

    let mut requests = Client::head("PUT", "/bucket/object", &[]);
    requests.extend_from_slice(format!("content-length: {}\r\n\r\n", body.len()).as_bytes());
    requests.extend_from_slice(&body);
    requests.extend_from_slice(b"GET /bucket/object HTTP/1.1\r\nrange: bytes=10-19\r\n\r\n");
    requests.extend_from_slice(b"HEAD /bucket/object HTTP/1.1\r\n\r\n");
    requests.extend_from_slice(b"DELETE /bucket/object HTTP/1.1\r\n\r\n");
    client.send(&requests);

    assert_eq!(client.reply(false).status, 200);
    assert_eq!(client.reply(false).body, body[10..20]);
    assert_eq!(client.reply(true).header("content-length"), "100000");
    assert_eq!(client.reply(false).status, 204);
    assert!(server.files().is_empty());
}

#[test]
fn overwriting_replaces_the_object() {
    let server = start("overwrite");
    let mut client = server.with_bucket();
    let first = client.put("/bucket/object", &pattern(200_000));
    let second = client.put("/bucket/object", b"short");
    assert_eq!((first.status, second.status), (200, 200));
    assert_ne!(first.header("etag"), second.header("etag"));

    let fetched = client.get("/bucket/object");
    assert_eq!(fetched.body, b"short");
    assert_eq!(fetched.header("etag"), second.header("etag"));
    assert_eq!(server.files(), ["object"]);
}

/// A reader that has started on an object gets all of it, whatever happens
/// to the key meanwhile.
#[test]
fn reads_see_one_version() {
    let server = start("versions");
    let mut reader = server.with_bucket();
    let mut writer = server.connect();
    let old = pattern(4 * 1024 * 1024);
    assert_eq!(writer.put("/bucket/object", &old).status, 200);

    reader.send(b"GET /bucket/object HTTP/1.1\r\n\r\n");
    assert_eq!(reader.line(), "HTTP/1.1 200 OK");
    assert_eq!(writer.put("/bucket/object", b"new").status, 200);
    assert_eq!(
        writer.request("DELETE", "/bucket/object", &[], b"").status,
        204
    );

    let mut rest = Vec::new();
    reader
        .0
        .get_mut()
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let _ = reader.0.read_to_end(&mut rest);
    assert!(rest.ends_with(&old), "{} bytes read", rest.len());
}

#[test]
fn keys_with_unusual_characters() {
    let server = start("keys");
    let mut client = server.with_bucket();
    for (path, file) in [
        ("/bucket/a/b/c.txt", "a%2Fb%2Fc.txt"),
        ("/bucket/..", "%2E."),
        ("/bucket/../../escape", "%2E.%2F..%2Fescape"),
        ("/bucket/.upload-0", "%2Eupload-0"),
        ("/bucket/with%20space%25", "with%20space%25"),
        ("/bucket/caf%C3%A9", "caf%C3%A9"),
        ("/bucket/a%2Fb", "a%2Fb"),
    ] {
        assert_eq!(client.put(path, path.as_bytes()).status, 200, "{path}");
        assert_eq!(client.get(path).body, path.as_bytes(), "{path}");
        assert!(server.root.join("bucket").join(file).is_file(), "{path}");
    }
    assert_eq!(server.files().len(), 7);
    assert_eq!(std::fs::read_dir(&server.root).unwrap().count(), 1);
}

#[test]
fn requests_that_are_refused() {
    let server = start("refused");
    let mut client = server.with_bucket();
    for (method, path, status, code) in [
        ("GET", "/", 501, "NotImplemented"),
        ("GET", "/bucket", 501, "NotImplemented"),
        ("GET", "/bucket/key?acl", 501, "NotImplemented"),
        ("PUT", "/bucket/key?uploads", 501, "NotImplemented"),
        ("POST", "/bucket/key", 405, "MethodNotAllowed"),
        ("PATCH", "/bucket", 405, "MethodNotAllowed"),
        ("PUT", "/", 405, "MethodNotAllowed"),
        ("PUT", "/Bucket", 400, "InvalidBucketName"),
        ("GET", "/ab/key", 400, "InvalidBucketName"),
        ("GET", "/bucket/%zz", 400, "InvalidURI"),
        ("GET", "/nowhere/key", 404, "NoSuchBucket"),
        ("DELETE", "/nowhere/key", 404, "NoSuchBucket"),
        ("HEAD", "/nowhere", 404, ""),
    ] {
        let reply = client.request(method, path, &[], b"");
        assert_eq!(reply.status, status, "{method} {path}");
        if !code.is_empty() {
            assert_eq!(error_code(&reply), code, "{method} {path}");
        }
    }
    let long = format!("/bucket/{}", "k".repeat(256));
    let reply = client.get(&long);
    assert_eq!((reply.status, error_code(&reply)), (400, "KeyTooLongError"));
    assert!(server.files().is_empty());
}

/// A small body that nobody wants is read and dropped, and the connection
/// carries on.
#[test]
fn unwanted_bodies() {
    let server = start("unwanted");
    let mut client = server.with_bucket();
    let reply = client.put("/nowhere/key", b"a few bytes");
    assert_eq!((reply.status, error_code(&reply)), (404, "NoSuchBucket"));
    assert_eq!(
        client
            .request("DELETE", "/bucket/key", &[], b"with a body")
            .status,
        204
    );
    assert_eq!(
        client
            .request("PUT", "/other", &[], b"<Configuration/>")
            .status,
        200
    );
    assert_eq!(client.put("/bucket/key", b"still here").status, 200);
    assert_eq!(client.get("/bucket/key").body, b"still here");
}

#[test]
fn expect_continue() {
    let server = start("expect");
    let mut client = server.with_bucket();
    let expect = [("expect", "100-continue"), ("content-length", "5")];

    let mut request = Client::head("PUT", "/bucket/object", &expect);
    request.extend_from_slice(b"\r\n");
    client.send(&request);
    assert_eq!(client.reply(false).status, 100);
    client.send(b"hello");
    assert_eq!(client.reply(false).status, 200);
    assert_eq!(client.get("/bucket/object").body, b"hello");

    // Told no before it sends anything, the client keeps its body.
    let mut request = Client::head("PUT", "/nowhere/object", &expect);
    request.extend_from_slice(b"\r\n");
    client.send(&request);
    let refused = client.reply(false);
    assert_eq!(
        (refused.status, error_code(&refused)),
        (404, "NoSuchBucket")
    );
    assert_eq!(refused.header("connection"), "close");
    assert!(client.is_closed());
    assert_eq!(server.files(), ["object"]);
}

#[test]
fn abandoned_upload_leaves_nothing() {
    let server = start("abandoned");
    let mut client = server.with_bucket();
    assert_eq!(client.put("/bucket/object", b"before").status, 200);

    let wait_for = |expected: &dyn Fn(&[String]) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !expected(&server.files()) {
            assert!(Instant::now() < deadline, "files: {:?}", server.files());
            std::thread::sleep(Duration::from_millis(2));
        }
    };

    let mut request = Client::head("PUT", "/bucket/object", &[("content-length", "500000")]);
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(&pattern(200_000));
    client.send(&request);
    // The upload is under way, in a file of its own.
    wait_for(&|files| files.len() == 2 && files[0].starts_with(".upload-"));
    drop(client);

    wait_for(&|files| files == ["object"]);
    assert_eq!(server.connect().get("/bucket/object").body, b"before");
}

#[test]
fn malformed_requests_close_the_connection() {
    let server = start("malformed");
    let mut client = server.connect();
    client.send(b"this is not http\r\n\r\n");
    assert_eq!(client.reply(false).status, 400);
    assert!(client.is_closed());

    let mut client = server.connect();
    let padding = "x".repeat(5000);
    client.send(&Client::head(
        "GET",
        "/bucket/key",
        &[("padding", &padding)],
    ));
    assert_eq!(client.reply(false).status, 431);
    assert!(client.is_closed());

    // Both connections gave their slots back.
    for _ in 0..8 {
        assert_eq!(
            server.connect().request("HEAD", "/bucket", &[], b"").status,
            404
        );
    }
}

#[test]
fn http_1_0_and_connection_close() {
    let server = start("versions-of-http");
    let mut client = server.with_bucket();
    assert_eq!(client.put("/bucket/object", b"hello").status, 200);

    client.send(b"GET /bucket/object HTTP/1.1\r\nconnection: close\r\n\r\n");
    let reply = client.reply(false);
    assert_eq!(
        (reply.body.as_slice(), reply.header("connection")),
        (&b"hello"[..], "close")
    );
    assert!(client.is_closed());

    let mut client = server.connect();
    client.send(b"GET /bucket/object HTTP/1.0\r\n\r\n");
    assert_eq!(client.line(), "HTTP/1.0 200 OK");
    let mut rest = Vec::new();
    client.0.read_to_end(&mut rest).unwrap();
    assert!(rest.ends_with(b"\r\n\r\nhello"));
}

/// More clients than the server takes at a time. The ones over the limit
/// wait their turn.
#[test]
fn more_clients_than_slots() {
    let server = start("crowd");
    drop(server.with_bucket());
    let body = pattern(100_000);

    std::thread::scope(|scope| {
        for client in 0..12 {
            let (server, body) = (&server, &body);
            scope.spawn(move || {
                let path = format!("/bucket/object-{client}");
                for _ in 0..5 {
                    let mut client = server.connect();
                    assert_eq!(client.put(&path, body).status, 200);
                    assert!(client.get(&path).body == *body);
                }
            });
        }
    });
    assert_eq!(server.files().len(), 12);
}

/// What an interrupted run left in a bucket is cleared when the next one
/// starts.
#[test]
fn leftovers_are_swept_at_start() {
    let root = root("sweep");
    std::fs::create_dir_all(root.join("bucket")).unwrap();
    std::fs::write(root.join("bucket/.upload-7"), b"half an object").unwrap();
    std::fs::write(root.join("bucket/%2Eupload-7"), b"a real object").unwrap();

    let server = serve(root, false);
    assert_eq!(server.files(), ["%2Eupload-7"]);
    assert_eq!(
        server.connect().get("/bucket/.upload-7").body,
        b"a real object"
    );
}

/// A client still sending a large body it was told nobody wants gets to
/// read the answer, where an abrupt close would reset its connection.
#[test]
fn refusal_reaches_a_client_that_is_still_sending() {
    let server = start("still-sending");
    let mut client = server.connect();
    let body = pattern(8 * 1024 * 1024);

    let mut request = Client::head("PUT", "/nowhere/object", &[]);
    request.extend_from_slice(format!("content-length: {}\r\n\r\n", body.len()).as_bytes());
    client.send(&request);
    let mut sender = client.0.get_ref().try_clone().unwrap();
    let sending = std::thread::spawn(move || sender.write_all(&body));

    let refused = client.reply(false);
    assert_eq!(
        (refused.status, error_code(&refused)),
        (404, "NoSuchBucket")
    );
    assert_eq!(refused.header("connection"), "close");
    assert!(client.is_closed());
    let _ = sending.join().unwrap();
}
