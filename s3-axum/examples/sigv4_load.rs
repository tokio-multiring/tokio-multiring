//! SigV4-authenticated load generator for `s3-axum`.
//!
//! Pre-signs request headers (and `aws-chunked` framing for `put-chunked`) on
//! the client so the client is never CPU-bound while the server runs full
//! AWS4-HMAC-SHA256 verification and streaming SHA-256 payload hashing on every
//! request.
//!
//! Modes (`MODE` env var):
//!   - `put-sha256`  : SigV4 PUT with `x-amz-content-sha256: <hex>` (full-body SHA-256)
//!   - `put-chunked` : SigV4 PUT with `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (64 KiB chunks)
//!   - `get-sigv4`   : SigV4 GET with `x-amz-content-sha256: <empty-sha256>`

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::FromStr;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const AMZ_DATE: &str = "20261122T060000Z";
const DATE_STAMP: &str = "20261122";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const CHUNK_SIZE: usize = 64 * 1024;

fn env<T: FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn signing_key(secret: &str) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), DATE_STAMP.as_bytes());
    let k_region = hmac_sha256(&k_date, REGION.as_bytes());
    let k_service = hmac_sha256(&k_region, SERVICE.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn sign_request(
    method: &str,
    path: &str,
    host: &str,
    payload_hash: &str,
    extra_headers: &[(&str, String)],
    access_key: &str,
    key: &[u8; 32],
) -> (String, String) {
    let mut hdrs: Vec<(&str, String)> = vec![
        ("host", host.to_owned()),
        ("x-amz-content-sha256", payload_hash.to_owned()),
        ("x-amz-date", AMZ_DATE.to_owned()),
    ];
    hdrs.extend(extra_headers.iter().cloned());
    hdrs.sort_by(|a, b| a.0.cmp(b.0));

    let mut canon_hdrs = String::new();
    let mut signed_names = String::new();
    for (i, (k, v)) in hdrs.iter().enumerate() {
        canon_hdrs.push_str(k);
        canon_hdrs.push(':');
        canon_hdrs.push_str(v);
        canon_hdrs.push('\n');
        if i > 0 {
            signed_names.push(';');
        }
        signed_names.push_str(k);
    }

    let canonical = format!("{method}\n{path}\n\n{canon_hdrs}\n{signed_names}\n{payload_hash}");
    let scope = format!("{DATE_STAMP}/{REGION}/{SERVICE}/aws4_request");
    let sts = format!(
        "AWS4-HMAC-SHA256\n{AMZ_DATE}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let sig = hex::encode(hmac_sha256(key, sts.as_bytes()));

    let mut wire = format!("{method} {path} HTTP/1.1\r\n");
    for (k, v) in &hdrs {
        wire.push_str(k);
        wire.push_str(": ");
        wire.push_str(v);
        wire.push_str("\r\n");
    }
    wire.push_str(&format!(
        "authorization: AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_names}, Signature={sig}\r\n"
    ));
    (wire, sig)
}

fn encode_aws_chunked(body: &[u8], seed_sig: &str, key: &[u8; 32]) -> Vec<u8> {
    let scope = format!("{DATE_STAMP}/{REGION}/{SERVICE}/aws4_request");
    let mut out = Vec::with_capacity(body.len() + (body.len() / CHUNK_SIZE + 2) * 128);
    let mut prev = seed_sig.to_owned();
    for chunk in body.chunks(CHUNK_SIZE) {
        let sts = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{AMZ_DATE}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{}",
            sha256_hex(chunk)
        );
        let sig = hex::encode(hmac_sha256(key, sts.as_bytes()));
        out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).as_bytes());
        out.extend_from_slice(chunk);
        out.extend_from_slice(b"\r\n");
        prev = sig;
    }
    let sts = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{AMZ_DATE}\n{scope}\n{prev}\n{EMPTY_SHA256}\n{EMPTY_SHA256}"
    );
    let sig = hex::encode(hmac_sha256(key, sts.as_bytes()));
    out.extend_from_slice(format!("0;chunk-signature={sig}\r\n\r\n").as_bytes());
    out
}

struct Client {
    stream: TcpStream,
    head: Vec<u8>,
    scratch: Vec<u8>,
}

impl Client {
    fn connect(address: &str) -> Self {
        let stream = TcpStream::connect(address).unwrap();
        stream.set_nodelay(true).unwrap();
        Self {
            stream,
            head: Vec::with_capacity(4096),
            scratch: vec![0; 1 << 20],
        }
    }

    fn request(&mut self, head: &[u8], body: &[u8]) -> (u16, usize) {
        self.stream.write_all(head).unwrap();
        if !body.is_empty() {
            self.stream.write_all(body).unwrap();
        }
        self.head.clear();
        let end = loop {
            let start = self.head.len();
            self.head.resize(start + 4096, 0);
            let read = self.stream.read(&mut self.head[start..]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            self.head.truncate(start + read);
            if let Some(pos) = self.head.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let header_str = std::str::from_utf8(&self.head[..end]).unwrap();
        let status: u16 = header_str
            .split_ascii_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let length: usize = header_str
            .lines()
            .find_map(|line| {
                let (k, v) = line.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let mut left = length.saturating_sub(self.head.len() - end);
        while left > 0 {
            let want = left.min(self.scratch.len());
            let read = self.stream.read(&mut self.scratch[..want]).unwrap();
            assert_ne!(read, 0, "server closed the connection");
            left -= read;
        }
        (status, length)
    }
}

struct PreparedConn {
    heads: Vec<Vec<u8>>,
    bodies: Vec<Vec<u8>>,
}

fn main() {
    let address = env("ADDR", String::from("127.0.0.1:9000"));
    let mode = env("MODE", String::from("put-sha256"));
    let conns = env("CONNS", 32usize);
    let size = env("SIZE", 1usize << 20);
    let count = env("COUNT", 8usize);
    let seconds = env("SECONDS", 5u64);
    let seed_only = env("SEED", 0usize) != 0;
    let access_key = env("ACCESS_KEY", String::from("benchkey"));
    let secret_key = env("SECRET_KEY", String::from("benchsecret"));
    let key = signing_key(&secret_key);

    // Create bucket with signed PUT /bench.
    {
        let (mut head, _) = sign_request(
            "PUT",
            "/bench",
            "bench",
            EMPTY_SHA256,
            &[],
            &access_key,
            &key,
        );
        head.push_str("content-length: 0\r\n\r\n");
        let (status, _) = Client::connect(&address).request(head.as_bytes(), b"");
        assert!(status == 200 || status == 409, "create bucket: {status}");
    }

    // Pre-build per-connection signed requests before starting the timer.
    let prepared: Vec<PreparedConn> = (0..conns)
        .map(|conn| {
            let mut random = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(conn as u64 + 1);
            let raw_body: Vec<u8> = if mode.starts_with("put") {
                (0..size)
                    .map(|_| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        random as u8
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let body_sha = if mode == "put-sha256" {
                sha256_hex(&raw_body)
            } else {
                String::new()
            };

            let mut heads = Vec::with_capacity(count);
            let mut bodies = Vec::with_capacity(count);
            for obj in 0..count {
                let path = format!("/bench/c{conn}-{obj}");
                match mode.as_str() {
                    "put-sha256" => {
                        let (mut h, _) =
                            sign_request("PUT", &path, "bench", &body_sha, &[], &access_key, &key);
                        h.push_str(&format!("content-length: {size}\r\n\r\n"));
                        heads.push(h.into_bytes());
                        if bodies.is_empty() {
                            bodies.push(raw_body.clone());
                        }
                    }
                    "put-chunked" => {
                        let (mut h, seed_sig) = sign_request(
                            "PUT",
                            &path,
                            "bench",
                            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
                            &[
                                ("content-encoding", "aws-chunked".to_owned()),
                                ("x-amz-decoded-content-length", size.to_string()),
                            ],
                            &access_key,
                            &key,
                        );
                        let encoded = encode_aws_chunked(&raw_body, &seed_sig, &key);
                        h.push_str(&format!("content-length: {}\r\n\r\n", encoded.len()));
                        heads.push(h.into_bytes());
                        bodies.push(encoded);
                    }
                    "get-sigv4" => {
                        let (mut h, _) = sign_request(
                            "GET",
                            &path,
                            "bench",
                            EMPTY_SHA256,
                            &[],
                            &access_key,
                            &key,
                        );
                        h.push_str("\r\n");
                        heads.push(h.into_bytes());
                    }
                    other => panic!("unknown MODE `{other}`"),
                }
            }
            PreparedConn { heads, bodies }
        })
        .collect();

    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);
    let mut latencies: Vec<u32> = std::thread::scope(|scope| {
        let workers: Vec<_> = prepared
            .iter()
            .map(|PreparedConn { heads, bodies }| {
                let address = &address;
                let mode = &mode;
                scope.spawn(move || {
                    let mut client = Client::connect(address);
                    let mut lats = Vec::new();
                    for obj in (0..count).cycle() {
                        if seed_only && lats.len() == count {
                            break;
                        }
                        if !seed_only && Instant::now() >= deadline {
                            break;
                        }
                        let body: &[u8] = match mode.as_str() {
                            "put-sha256" => &bodies[0],
                            "put-chunked" => &bodies[obj],
                            _ => b"",
                        };
                        let t0 = Instant::now();
                        let (status, len) = client.request(&heads[obj], body);
                        lats.push(t0.elapsed().as_micros() as u32);
                        assert_eq!(status, 200, "unexpected status {status}");
                        if mode == "get-sigv4" {
                            assert_eq!(len, size, "object is {len} bytes");
                        }
                    }
                    lats
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect()
    });
    let elapsed = started.elapsed().as_secs_f64();

    latencies.sort_unstable();
    let pct = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let rate = latencies.len() as f64 / elapsed;
    println!(
        "{mode:<11} size={size:<8} conns={conns:<3} {rate:>8.0} req/s {:>7.0} MiB/s  p50 {:>6} us  p99 {:>6} us",
        rate * size as f64 / (1 << 20) as f64,
        pct(0.50),
        pct(0.99),
    );
}
