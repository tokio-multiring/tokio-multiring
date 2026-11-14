//! Load generator for a running server.
//!
//! ```text
//! ADDR=127.0.0.1:9000 MODE=put CONNS=32 SIZE=1048576 COUNT=200 load
//! ADDR=127.0.0.1:9000 MODE=get CONNS=32 SIZE=1048576 COUNT=200 SECONDS=5 load
//! ```
//!
//! `put` stores `COUNT` objects of `SIZE` bytes over each of `CONNS`
//! connections, in the bucket `bench`. `get` fetches those objects over and
//! over for `SECONDS`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::FromStr;
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
        let stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        Client {
            stream,
            scratch: vec![0; 1 << 20],
        }
    }

    /// Sends a request and reads the whole response. Returns its status and
    /// the length of its body.
    fn request(&mut self, head: &str, body: &[u8]) -> (u16, usize) {
        self.stream.write_all(head.as_bytes()).unwrap();
        self.stream.write_all(body).unwrap();

        let mut filled = 0;
        let head_end = loop {
            let read = self.stream.read(&mut self.scratch[filled..]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            filled += read;
            let end = self.scratch[..filled]
                .windows(4)
                .position(|window| window == b"\r\n\r\n");
            if let Some(end) = end {
                break end + 4;
            }
        };

        let head = std::str::from_utf8(&self.scratch[..head_end]).unwrap();
        let status = head[9..12].parse().unwrap();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length: "))
            .map_or(0, |length| length.parse::<usize>().unwrap());

        let mut left = length - (filled - head_end);
        while left > 0 {
            let want = left.min(self.scratch.len());
            let read = self.stream.read(&mut self.scratch[..want]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            left -= read;
        }
        (status, length)
    }
}

fn main() {
    let address = env("ADDR", String::from("127.0.0.1:9000"));
    let mode = env("MODE", String::from("get"));
    let conns = env("CONNS", 32usize);
    let size = env("SIZE", 1usize << 20);
    let count = env("COUNT", 100usize);
    let seconds = env("SECONDS", 5u64);
    let put = match mode.as_str() {
        "put" => true,
        "get" => false,
        other => panic!("MODE is `put` or `get`, not `{other}`"),
    };

    let (status, _) = Client::connect(&address).request(
        "PUT /bench HTTP/1.1\r\nhost: bench\r\ncontent-length: 0\r\n\r\n",
        b"",
    );
    assert!(
        status == 200 || status == 409,
        "creating the bucket: {status}"
    );

    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);
    let mut latencies: Vec<u32> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..conns)
            .map(|conn| {
                let address = &address;
                scope.spawn(move || {
                    let mut client = Client::connect(address);
                    let mut random = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(conn as u64 + 1);
                    let body: Vec<u8> = (0..if put { size } else { 0 })
                        .map(|_| {
                            random ^= random << 13;
                            random ^= random >> 7;
                            random ^= random << 17;
                            random as u8
                        })
                        .collect();

                    let mut latencies = Vec::new();
                    for object in (0..count).cycle() {
                        if put && latencies.len() == count {
                            break;
                        }
                        if !put && Instant::now() >= deadline {
                            break;
                        }
                        let head = if put {
                            format!(
                                "PUT /bench/c{conn}-{object} HTTP/1.1\r\nhost: bench\r\n\
                                 content-length: {size}\r\n\r\n"
                            )
                        } else {
                            format!("GET /bench/c{conn}-{object} HTTP/1.1\r\nhost: bench\r\n\r\n")
                        };
                        let sent = Instant::now();
                        let (status, length) = client.request(&head, &body);
                        latencies.push(sent.elapsed().as_micros() as u32);
                        assert_eq!(status, 200);
                        assert!(put || length == size, "object is {length} bytes");
                    }
                    latencies
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    });
    let elapsed = started.elapsed().as_secs_f64();

    latencies.sort_unstable();
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let rate = latencies.len() as f64 / elapsed;
    println!(
        "{mode} size={size:<8} conns={conns:<3} {rate:>8.0} req/s {:>7.0} MiB/s  p50 {:>6} us  p99 {:>6} us",
        rate * size as f64 / (1 << 20) as f64,
        percentile(0.50),
        percentile(0.99),
    );
}
