//! The server, driven over real sockets with a client that writes HTTP by
//! hand.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use sidecar_s3::{Config, Server};
use tokio_sidecar::{BufferPool, DiskConfig, Domain, Hyperthreads, Plan, Sidecar};

/// Size of the units objects are cut into.
const UNIT: usize = 128 * 1024;

/// Size of the buffers object data moves through: two units, so that modest
/// objects take several rounds.
const BUFFER: usize = 2 * UNIT;

/// Space the header takes at the start of an object's first part.
const HEADER: usize = 4096;

struct Fixture {
    address: SocketAddr,
    /// The directory of each drive.
    roots: Vec<PathBuf>,
    runtime: tokio::runtime::Runtime,
}

/// A server with two drives.
fn start(name: &str) -> Fixture {
    start_with(name, 2, false)
}

fn start_with(name: &str, drives: usize, polled: bool) -> Fixture {
    let roots: Vec<PathBuf> = (0..drives)
        .map(|drive| {
            std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join(format!("{name}-{}-{drive}", std::process::id()))
        })
        .collect();
    for root in &roots {
        std::fs::create_dir(root).unwrap();
    }
    serve(roots, polled)
}

/// Starts a server on the directories `roots`.
fn serve(roots: Vec<PathBuf>, polled: bool) -> Fixture {
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
            buffers: Some(BufferPool::new(BUFFER, 8).unwrap()),
            disk: polled.then(|| DiskConfig {
                sq_entries: 32,
                sq_thread_idle: Duration::from_millis(50),
                plan,
            }),
            ..tokio_sidecar::Config::default()
        })
        .await
        .unwrap();

        let mut config = Config::new(&roots);
        config.connections = 4;
        config.head_buffer = 4096;
        Server::new(sidecar, config).unwrap().listen(listener);
    });
    Fixture {
        address,
        roots,
        runtime,
    }
}

/// The hash that picks the drive of an object's first part.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
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

    /// Shuts the server down, leaving what it stored.
    fn stop(self) -> Vec<PathBuf> {
        drop(self.runtime);
        self.roots
    }

    /// Names of all the files in the bucket's directories, on every drive.
    fn files(&self) -> Vec<String> {
        let mut files: Vec<String> = self
            .roots
            .iter()
            .flat_map(|root| std::fs::read_dir(root.join("bucket")).unwrap())
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        files.sort();
        files
    }

    /// Names of the files that name objects.
    fn objects(&self) -> Vec<String> {
        let mut files = self.files();
        files.retain(|file| !file.starts_with('.'));
        files
    }

    /// Names of the files that are not there for an object to be found by:
    /// parts other than the first, and uploads under way.
    fn hidden(&self) -> Vec<String> {
        let mut files = self.files();
        files.retain(|file| file.starts_with('.'));
        files
    }

    /// Puts the object whose key is stored under the file name `file` back
    /// together from what is on the drives, by the documented layout alone.
    fn reassemble(&self, file: &str) -> Vec<u8> {
        let drives = self.roots.len();
        let first = (fnv1a(format!("bucket/{file}").as_bytes()) % drives as u64) as usize;
        let first_part = std::fs::read(self.roots[first].join("bucket").join(file)).unwrap();

        let field = |index: usize| {
            u64::from_le_bytes(first_part[8 * index..8 * index + 8].try_into().unwrap())
        };
        assert_eq!(&first_part[..8], b"sidecar1");
        assert_eq!((field(1), field(2)), (drives as u64, UNIT as u64));
        let size = field(3) as usize;
        let id = u128::from(field(6)) | u128::from(field(7)) << 64;
        assert!(first_part[64..HEADER].iter().all(|&byte| byte == 0));

        let mut parts = vec![first_part[HEADER..].to_vec()];
        for part in 1..drives {
            let name = format!(".part-{id:032x}-{part}");
            let path = self.roots[(first + part) % drives]
                .join("bucket")
                .join(name);
            // A part exists exactly when the object reaches it.
            assert_eq!(path.exists(), size > part * UNIT, "{path:?}");
            parts.push(std::fs::read(path).unwrap_or_default());
        }

        let mut object = Vec::with_capacity(size);
        let mut taken = vec![0; drives];
        for unit in 0.. {
            if object.len() == size {
                break;
            }
            let part = unit % drives;
            let len = UNIT.min(size - object.len());
            object.extend_from_slice(&parts[part][taken[part]..taken[part] + len]);
            taken[part] += len;
        }
        for (part, taken) in parts.iter().zip(taken) {
            assert_eq!(part.len(), taken, "a part is longer than its share");
        }
        object
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
    assert!(server.roots.iter().all(|root| root.join("bucket").is_dir()));
    let again = client.request("PUT", "/bucket", &[], b"");
    assert_eq!(
        (again.status, error_code(&again)),
        (409, "BucketAlreadyOwnedByYou")
    );
    assert_eq!(client.request("HEAD", "/bucket", &[], b"").status, 200);

    // One object is on one drive only, and holds the bucket on both.
    assert_eq!(client.put("/bucket/key", b"x").status, 200);
    let full = client.request("DELETE", "/bucket", &[], b"");
    assert_eq!((full.status, error_code(&full)), (409, "BucketNotEmpty"));
    assert!(server.roots.iter().all(|root| root.join("bucket").is_dir()));
    assert_eq!(client.get("/bucket/key").body, b"x");
    assert_eq!(
        client.request("DELETE", "/bucket/key", &[], b"").status,
        204
    );

    assert_eq!(client.request("DELETE", "/bucket", &[], b"").status, 204);
    let gone = client.request("DELETE", "/bucket", &[], b"");
    assert_eq!((gone.status, error_code(&gone)), (404, "NoSuchBucket"));
    assert!(
        server
            .roots
            .iter()
            .all(|root| !root.join("bucket").exists())
    );
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

/// Sizes around the block, unit and round boundaries, where padding,
/// truncation and the hand-over between drives and buffers happen. What
/// comes back is checked, and so is what is on the drives.
fn awkward_sizes(name: &str, drives: usize) {
    let server = start_with(name, drives, false);
    let mut client = server.with_bucket();
    let round = drives * BUFFER;
    // Some of these coincide when there is one drive.
    let sizes: std::collections::BTreeSet<usize> = [
        0,
        1,
        4095,
        4096,
        4097,
        UNIT - 1,
        UNIT,
        UNIT + 1,
        2 * UNIT - 1,
        2 * UNIT,
        2 * UNIT + 1,
        3 * UNIT + 5,
        round - 1,
        round,
        round + 1,
        2 * round + UNIT + 4097,
    ]
    .into();
    for &size in &sizes {
        let file = format!("size-{size}");
        let path = format!("/bucket/{file}");
        let body = pattern(size);
        assert_eq!(client.put(&path, &body).status, 200, "{size}");
        assert!(server.reassemble(&file) == body, "{size} on the drives");

        let fetched = client.get(&path);
        assert_eq!(fetched.status, 200, "{size}");
        assert_eq!(fetched.header("content-length"), size.to_string());
        assert!(fetched.body == body, "{size}");
    }
    assert_eq!(server.objects().len(), sizes.len());
    // Every object larger than a unit has a part on each drive it reaches.
    let other_parts: usize = sizes
        .iter()
        .map(|size| size.div_ceil(UNIT).clamp(1, drives) - 1)
        .sum();
    assert_eq!(server.hidden().len(), other_parts);
}

#[test]
fn objects_of_awkward_sizes_on_one_drive() {
    awkward_sizes("sizes-1", 1);
}

#[test]
fn objects_of_awkward_sizes_on_two_drives() {
    awkward_sizes("sizes-2", 2);
}

#[test]
fn objects_of_awkward_sizes_on_three_drives() {
    awkward_sizes("sizes-3", 3);
}

/// Small objects are spread over the drives, by the hash of their names.
#[test]
fn small_objects_use_every_drive() {
    let server = start("spread");
    let mut client = server.with_bucket();
    for object in 0..32 {
        assert_eq!(
            client
                .put(&format!("/bucket/object-{object}"), b"small")
                .status,
            200
        );
    }
    for root in &server.roots {
        let on_this_drive = std::fs::read_dir(root.join("bucket")).unwrap().count();
        assert!(
            (6..=26).contains(&on_this_drive),
            "{on_this_drive} of 32 on {root:?}"
        );
    }
    for object in 0..32 {
        assert_eq!(server.reassemble(&format!("object-{object}")), b"small");
    }
}

#[test]
fn byte_ranges() {
    let server = start("ranges");
    let mut client = server.with_bucket();
    let size = 5 * UNIT + 44_641;
    let body = pattern(size);
    assert_eq!(client.put("/bucket/object", &body).status, 200);

    let round = 2 * BUFFER;
    for range in [
        0..1,
        5..10,
        4095..4097,
        // Across a unit, from one drive to the other.
        UNIT - 3..UNIT + 3,
        // On the second drive alone.
        UNIT + 5..UNIT + 10,
        2 * UNIT - 1..2 * UNIT,
        // Across a round.
        round - 5..round + 5,
        round..round + 1,
        1000..size - 1000,
        size - 1..size,
        0..size,
    ] {
        let header = format!("bytes={}-{}", range.start, range.end - 1);
        let part = client.request("GET", "/bucket/object", &[("range", &header)], b"");
        assert_eq!(part.status, 206, "{header}");
        assert!(part.body == body[range.clone()], "{header}");
        let described = format!("bytes {}-{}/{size}", range.start, range.end - 1);
        assert_eq!(part.header("content-range"), described);
    }

    let last = client.request("GET", "/bucket/object", &[("range", "bytes=-7")], b"");
    assert_eq!((last.status, &last.body[..]), (206, &body[size - 7..]));
    let open_ended = format!("bytes={}-", size - 100_000);
    let tail = client.request("GET", "/bucket/object", &[("range", &open_ended)], b"");
    assert!(tail.body == body[size - 100_000..]);
    let past = format!("bytes={}-9999999", size - 10);
    let clipped = client.request("GET", "/bucket/object", &[("range", &past)], b"");
    assert!(clipped.body == body[size - 10..]);

    let whole = client.request("GET", "/bucket/object", &[("range", "bytes=9-1")], b"");
    assert_eq!((whole.status, whole.body.len()), (200, size));

    let beyond = format!("bytes={size}-");
    let refused = client.request("GET", "/bucket/object", &[("range", &beyond)], b"");
    assert_eq!(
        (refused.status, error_code(&refused)),
        (416, "InvalidRange")
    );
    assert_eq!(refused.header("content-range"), format!("bytes */{size}"));

    let looked_up = client.request("HEAD", "/bucket/object", &[("range", "bytes=5-9")], b"");
    assert_eq!(
        (looked_up.status, looked_up.header("content-length")),
        (206, "5")
    );
}

#[test]
fn polled_disk_rings() {
    let server = start_with("polled", 2, true);
    let mut client = server.with_bucket();
    let body = pattern(5 * UNIT + 44_641);
    assert_eq!(client.put("/bucket/object", &body).status, 200);
    assert!(server.reassemble("object") == body);
    assert!(client.get("/bucket/object").body == body);
    let part = client.request(
        "GET",
        "/bucket/object",
        &[("range", "bytes=131070-131080")],
        b"",
    );
    assert_eq!(part.body, body[131_070..131_081]);
}

#[test]
fn chunked_upload() {
    let server = start("chunked");
    let mut client = server.with_bucket();
    let body = pattern(5 * UNIT + 1234);

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
    assert!(server.reassemble("object") == body);
    assert!(client.get("/bucket/object").body == body);

    client.send(b"PUT /bucket/broken HTTP/1.1\r\ntransfer-encoding: chunked\r\n\r\nnot hex\r\n");
    let refused = client.reply(false);
    assert_eq!((refused.status, error_code(&refused)), (400, "BadRequest"));
    assert_eq!(refused.header("connection"), "close");
    assert!(client.is_closed());
    assert_eq!(server.objects(), ["object"]);
    assert_eq!(server.hidden().len(), 1);
}

#[test]
fn pipelined_requests() {
    let server = start("pipelined");
    let mut client = server.with_bucket();
    let body = pattern(3 * UNIT + 77);

    let mut requests = Client::head("PUT", "/bucket/object", &[]);
    requests.extend_from_slice(format!("content-length: {}\r\n\r\n", body.len()).as_bytes());
    requests.extend_from_slice(&body);
    requests
        .extend_from_slice(b"GET /bucket/object HTTP/1.1\r\nrange: bytes=131070-131079\r\n\r\n");
    requests.extend_from_slice(b"HEAD /bucket/object HTTP/1.1\r\n\r\n");
    requests.extend_from_slice(b"DELETE /bucket/object HTTP/1.1\r\n\r\n");
    client.send(&requests);

    assert_eq!(client.reply(false).status, 200);
    assert_eq!(client.reply(false).body, body[131_070..131_080]);
    assert_eq!(
        client.reply(true).header("content-length"),
        body.len().to_string()
    );
    assert_eq!(client.reply(false).status, 204);
    assert!(server.files().is_empty());
}

/// Replacing or removing an object takes its parts on other drives with it.
#[test]
fn overwriting_and_deleting_remove_old_parts() {
    let server = start("overwrite");
    let mut client = server.with_bucket();
    let first = client.put("/bucket/object", &pattern(5 * UNIT));
    assert_eq!((first.status, server.hidden().len()), (200, 1));
    let old_part = server.hidden();

    let second = client.put("/bucket/object", &pattern(3 * UNIT));
    assert_eq!(second.status, 200);
    assert_ne!(first.header("etag"), second.header("etag"));
    assert_eq!(server.hidden().len(), 1);
    assert_ne!(server.hidden(), old_part);
    assert!(server.reassemble("object") == pattern(3 * UNIT));

    let third = client.put("/bucket/object", b"short");
    assert_eq!(third.status, 200);
    let fetched = client.get("/bucket/object");
    assert_eq!(fetched.body, b"short");
    assert_eq!(fetched.header("etag"), third.header("etag"));
    assert_eq!(server.files(), ["object"]);

    assert_eq!(
        client.put("/bucket/object", &pattern(2 * UNIT + 1)).status,
        200
    );
    assert_eq!(server.files().len(), 2);
    assert_eq!(
        client.request("DELETE", "/bucket/object", &[], b"").status,
        204
    );
    assert!(server.files().is_empty());
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
    assert!(server.files().is_empty());

    let mut rest = Vec::new();
    reader
        .0
        .get_mut()
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    let _ = reader.0.read_to_end(&mut rest);
    assert!(rest.ends_with(&old), "{} bytes read", rest.len());
}

/// Writers replace one key with objects of different shapes while readers
/// fetch it. Every read gets one of the objects, whole, and when the dust
/// settles the drives hold the last one and nothing else.
#[test]
fn overwrites_race_with_reads() {
    let server = start("race");
    drop(server.with_bucket());
    let bodies = [
        pattern(50_000),
        pattern(2 * UNIT + 9),
        pattern(5 * UNIT + 4097),
    ];
    assert_eq!(
        server.connect().put("/bucket/object", &bodies[0]).status,
        200
    );

    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writers: Vec<_> = bodies
            .iter()
            .map(|body| {
                let server = &server;
                scope.spawn(move || {
                    let mut client = server.connect();
                    for _ in 0..25 {
                        assert_eq!(client.put("/bucket/object", body).status, 200);
                    }
                })
            })
            .collect();
        for _ in 0..2 {
            let (server, bodies, done) = (&server, &bodies, &done);
            scope.spawn(move || {
                let mut client = server.connect();
                let mut seen = [0; 3];
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    let fetched = client.get("/bucket/object");
                    assert_eq!(fetched.status, 200);
                    let which = bodies.iter().position(|body| *body == fetched.body);
                    seen[which.expect("a mixture of two objects")] += 1;
                }
                assert!(seen.iter().sum::<usize>() > 0);
            });
        }
        for writer in writers {
            writer.join().unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    let last = server.connect().get("/bucket/object").body;
    assert!(bodies.contains(&last));
    assert!(server.reassemble("object") == last);
    assert_eq!(server.objects(), ["object"]);
    assert_eq!(server.hidden().len(), usize::from(last.len() > UNIT));
}

/// Opening an object is two steps: its first part, then the part that one
/// names. Readers that only ask for a byte spend their time doing that,
/// while writers keep replacing the object and removing the part named.
#[test]
fn opening_races_with_replacement() {
    let server = start("reopen");
    drop(server.with_bucket());
    let bodies = [vec![b'a'; UNIT + 1], vec![b'b'; UNIT + 1]];
    assert_eq!(
        server.connect().put("/bucket/object", &bodies[0]).status,
        200
    );

    let done = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writers: Vec<_> = bodies
            .iter()
            .map(|body| {
                let server = &server;
                scope.spawn(move || {
                    let mut client = server.connect();
                    for _ in 0..150 {
                        assert_eq!(client.put("/bucket/object", body).status, 200);
                    }
                })
            })
            .collect();
        for _ in 0..2 {
            let (server, done) = (&server, &done);
            scope.spawn(move || {
                let mut client = server.connect();
                let last = [("range", "bytes=-1")];
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    let fetched = client.request("GET", "/bucket/object", &last, b"");
                    assert_eq!(fetched.status, 206);
                    assert!(fetched.body == b"a" || fetched.body == b"b");
                }
            });
        }
        for writer in writers {
            writer.join().unwrap();
        }
        done.store(true, std::sync::atomic::Ordering::Relaxed);
    });
    assert_eq!(server.files().len(), 2);
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
        ("/bucket/.part-0-1", "%2Epart-0-1"),
        ("/bucket/with%20space%25", "with%20space%25"),
        ("/bucket/caf%C3%A9", "caf%C3%A9"),
        ("/bucket/a%2Fb", "a%2Fb"),
    ] {
        assert_eq!(client.put(path, path.as_bytes()).status, 200, "{path}");
        assert_eq!(client.get(path).body, path.as_bytes(), "{path}");
        assert_eq!(server.reassemble(file), path.as_bytes(), "{path}");
    }
    assert_eq!(server.files().len(), 8);
    for root in &server.roots {
        assert_eq!(std::fs::read_dir(root).unwrap().count(), 1);
    }
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

/// An upload that reached both drives and then lost its client.
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

    // More than a round, of which a round and a bit arrive.
    let size = (2 * BUFFER + 300_000).to_string();
    let mut request = Client::head("PUT", "/bucket/object", &[("content-length", &size)]);
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(&pattern(2 * BUFFER + 50_000));
    client.send(&request);
    // The first round is on the drives, in two files of its own.
    wait_for(&|files| {
        files
            .iter()
            .filter(|file| file.starts_with(".upload-"))
            .count()
            == 2
    });
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

/// More clients than the server takes at a time, moving objects that take
/// two buffers each through a pool that cannot serve them all at once.
#[test]
fn more_clients_than_slots() {
    let server = start("crowd");
    drop(server.with_bucket());
    let body = pattern(3 * UNIT + 5);

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
    assert_eq!(server.objects().len(), 12);
    assert_eq!(server.hidden().len(), 12);
}

/// What an interrupted run left in a bucket is cleared when the next one
/// starts, and what it had stored is still there.
#[test]
fn leftovers_are_swept_at_start() {
    let server = start("sweep");
    let mut client = server.with_bucket();
    let body = pattern(3 * UNIT);
    assert_eq!(client.put("/bucket/.upload-7", &body).status, 200);
    drop(client);
    let kept = server.files();
    let roots = server.stop();

    for root in &roots {
        std::fs::write(root.join("bucket/.upload-7"), b"half an object").unwrap();
    }
    let server = serve(roots, false);
    assert_eq!(server.files(), kept);
    assert!(server.connect().get("/bucket/.upload-7").body == body);
}

/// Objects are tied to the arrangement of drives they were stored under.
/// Under another, the server says so and serves nothing in their place.
#[test]
fn another_arrangement_of_drives_is_refused() {
    let server = start_with("rearranged", 1, false);
    let mut client = server.with_bucket();
    assert_eq!(client.put("/bucket/object", &pattern(1000)).status, 200);
    drop(client);
    let mut roots = server.stop();

    // The same drive, as the first of three.
    for extra in ["rearranged-x", "rearranged-y"] {
        let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("{extra}-{}", std::process::id()));
        std::fs::create_dir_all(root.join("bucket")).unwrap();
        roots.push(root);
    }
    let server = serve(roots, false);
    // Where the key hashes to now may or may not be where the object is.
    let reply = server.connect().get("/bucket/object");
    assert!(
        reply.status == 500 || reply.status == 404,
        "{}",
        reply.status
    );
    assert!(reply.text().contains("<Error>"));
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
