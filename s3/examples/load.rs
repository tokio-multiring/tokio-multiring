//! Closed-loop HTTP load generator. `prepare` writes COUNT objects per connection;
//! `verify` reads and validates them once. `get` and `put` warm up for WARMUP
//! seconds, then measure for SECONDS. Connection and payload setup are untimed.
//! VERIFY=1 (default) checks GET bytes; PAYLOAD_BYTE=66 matches ablation mem mode.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::FromStr;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn env<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} has an invalid value")),
        Err(_) => default,
    }
}

struct Client {
    stream: TcpStream,
    scratch: Vec<u8>,
}

impl Client {
    fn connect(address: &str) -> Client {
        let address = address
            .parse()
            .expect("ADDR must be an IP:port socket address");
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(30)).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        Client {
            stream,
            scratch: vec![0; 1 << 20],
        }
    }

    fn request(&mut self, head: &str, body: &[u8], expected: Option<&[u8]>) -> (u16, usize) {
        self.stream.write_all(head.as_bytes()).unwrap();
        self.stream.write_all(body).unwrap();
        let mut filled = 0;
        let head_end = loop {
            assert!(filled < self.scratch.len(), "response head is too large");
            let read = self.stream.read(&mut self.scratch[filled..]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            filled += read;
            if let Some(end) = self.scratch[..filled]
                .windows(4)
                .position(|w| w == b"\r\n\r\n")
            {
                break end + 4;
            }
        };
        let head = std::str::from_utf8(&self.scratch[..head_end]).unwrap();
        let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .expect("load generator requires Content-Length responses");
        let mut received = filled - head_end;
        assert!(received <= length, "unexpected bytes after response");
        if let Some(expected) = expected {
            assert_eq!(length, expected.len(), "wrong object length");
            assert_eq!(
                &self.scratch[head_end..filled],
                &expected[..received],
                "corrupt payload"
            );
        }
        while received < length {
            let want = (length - received).min(self.scratch.len());
            let read = self.stream.read(&mut self.scratch[..want]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            if let Some(expected) = expected {
                assert_eq!(
                    &self.scratch[..read],
                    &expected[received..received + read],
                    "corrupt payload"
                );
            }
            received += read;
        }
        (status, length)
    }
}

fn payload(conn: usize, size: usize, byte: Option<u8>) -> Vec<u8> {
    if let Some(byte) = byte {
        return vec![byte; size];
    }
    let mut random = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(conn as u64 + 1);
    (0..size)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random as u8
        })
        .collect()
}

fn main() {
    let address = env("ADDR", String::from("127.0.0.1:9000"));
    let mode = env("MODE", String::from("get"));
    let conns = env("CONNS", 32usize);
    let size = env("SIZE", 1usize << 20);
    let count = env("COUNT", 100usize);
    let seconds = env("SECONDS", 10u64);
    let warmup = env("WARMUP", 2u64);
    let verify = mode == "verify" || env("VERIFY", 1u8) != 0;
    let byte = std::env::var("PAYLOAD_BYTE")
        .ok()
        .map(|s| s.parse::<u8>().expect("invalid PAYLOAD_BYTE"));
    let put = match mode.as_str() {
        "prepare" | "put" => true,
        "get" | "verify" => false,
        _ => panic!("MODE must be prepare, verify, put, or get"),
    };
    let finite = mode == "prepare" || mode == "verify";
    assert!(conns > 0 && count > 0 && seconds > 0);
    let (status, _) = Client::connect(&address).request(
        "PUT /bench HTTP/1.1\r\nhost: bench\r\ncontent-length: 0\r\n\r\n",
        b"",
        None,
    );
    assert!(
        status == 200 || status == 409,
        "creating the bucket: {status}"
    );

    let (mut latencies, elapsed) = std::thread::scope(|scope| {
        let mut controls = Vec::new();
        let mut workers = Vec::new();
        for conn in 0..conns {
            // Separate channels let a worker panic disconnect its coordinator
            // without stranding other workers at a barrier.
            let (ready_tx, ready_rx) = mpsc::channel();
            let (start_tx, start_rx) = mpsc::channel::<Instant>();
            controls.push((ready_rx, start_tx));
            let address = &address;
            workers.push(scope.spawn(move || {
                let mut client = Client::connect(address);
                let data = payload(conn, if put || verify { size } else { 0 }, byte);
                let heads: Vec<_> = (0..count).map(|object| {
                    if put {
                        format!("PUT /bench/c{conn}-{object} HTTP/1.1\r\nhost: bench\r\ncontent-length: {size}\r\n\r\n")
                    } else {
                        format!("GET /bench/c{conn}-{object} HTTP/1.1\r\nhost: bench\r\n\r\n")
                    }
                }).collect();
                let mut request = |object: usize| {
                    let (status, length) = client.request(&heads[object], if put { &data } else { b"" },
                        (!put && verify).then_some(data.as_slice()));
                    assert_eq!(status, 200);
                    assert!(put || length == size, "object is {length} bytes");
                };
                ready_tx.send(()).unwrap();
                let warm_start = start_rx.recv().expect("coordinator stopped");
                if !finite {
                    for object in (0..count).cycle() {
                        if Instant::now() >= warm_start + Duration::from_secs(warmup) { break; }
                        request(object);
                    }
                }
                ready_tx.send(()).unwrap();
                let started = start_rx.recv().expect("coordinator stopped");
                let mut latencies = Vec::new();
                for object in (0..count).cycle() {
                    if finite {
                        if latencies.len() == count { break; }
                    } else if Instant::now() >= started + Duration::from_secs(seconds) { break; }
                    let sent = Instant::now();
                    request(object);
                    latencies.push(sent.elapsed().as_micros() as u64);
                }
                (latencies, Instant::now())
            }));
        }
        for (ready, _) in &controls {
            ready.recv().expect("worker failed during setup");
        }
        let warm_start = Instant::now();
        for (_, start) in &controls {
            start.send(warm_start).unwrap();
        }
        for (ready, _) in &controls {
            ready.recv().expect("worker failed during warmup");
        }
        let started = Instant::now();
        eprintln!("MEASURE_START");
        for (_, start) in &controls {
            start.send(started).unwrap();
        }
        let mut latencies = Vec::new();
        let mut ended = started;
        for worker in workers {
            let (samples, finish) = worker.join().unwrap();
            latencies.extend(samples);
            ended = ended.max(finish);
        }
        eprintln!("MEASURE_END");
        (latencies, ended.duration_since(started).as_secs_f64())
    });
    assert!(!latencies.is_empty(), "no measured requests");
    latencies.sort_unstable();
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let rate = latencies.len() as f64 / elapsed;
    println!(
        "mode={mode} size={size} conns={conns} count={count} requests={} elapsed_s={elapsed:.6} warmup_s={} verify={} req_s={rate:.2} mib_s={:.2} p50_us={} p99_us={}",
        latencies.len(),
        if finite { 0 } else { warmup },
        !put && verify,
        rate * size as f64 / (1 << 20) as f64,
        percentile(0.50),
        percentile(0.99)
    );
}
