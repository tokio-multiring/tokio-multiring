//! Asymmetric / mixed-size SigV4 S3 load generator for `s3-axum`.
//!
//! Exercises concurrent mixes of large and small GETs and PUTs (with full
//! SigV4 auth and SHA-256 payload verification on PUTs) and reports per-class
//! throughput and p50/p95/p99 latency so noisy-neighbor / head-of-line
//! interference is directly visible.
//!
//! Modes (`MODE` env var):
//!   - `classes`: dedicated connection pools running concurrently:
//!       * 4 conns  × 16 MiB PUT (SigV4 + SHA-256)
//!       * 8 conns  × 16 MiB GET (SigV4)
//!       * 16 conns ×  4 KiB GET (SigV4)
//!       * 4 conns  ×  4 KiB PUT (SigV4 + SHA-256)
//!   - `bimodal`: 32 connections, each mixing request sizes/verbs on the same
//!     keep-alive socket (60% 4K GET, 15% 64K GET, 10% 1M GET, 5% 16M GET,
//!     5% 4K PUT, 5% 4M PUT).

use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const AMZ_DATE: &str = "20261122T060000Z";
const DATE_STAMP: &str = "20261122";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";

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

fn sign_head(
    method: &str,
    path: &str,
    payload_hash: &str,
    content_length: Option<usize>,
    access_key: &str,
    key: &[u8; 32],
) -> Vec<u8> {
    let hdrs = [
        ("host", "bench"),
        ("x-amz-content-sha256", payload_hash),
        ("x-amz-date", AMZ_DATE),
    ];
    let canon_hdrs =
        format!("host:bench\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{AMZ_DATE}\n");
    let signed_names = "host;x-amz-content-sha256;x-amz-date";
    let canonical = format!("{method}\n{path}\n\n{canon_hdrs}\n{signed_names}\n{payload_hash}");
    let scope = format!("{DATE_STAMP}/{REGION}/{SERVICE}/aws4_request");
    let sts = format!(
        "AWS4-HMAC-SHA256\n{AMZ_DATE}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let sig = hex::encode(hmac_sha256(key, sts.as_bytes()));

    let mut wire = format!("{method} {path} HTTP/1.1\r\n");
    for (k, v) in hdrs {
        wire.push_str(k);
        wire.push_str(": ");
        wire.push_str(v);
        wire.push_str("\r\n");
    }
    wire.push_str(&format!(
        "authorization: AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_names}, Signature={sig}\r\n"
    ));
    if let Some(len) = content_length {
        wire.push_str(&format!("content-length: {len}\r\n"));
    }
    wire.push_str("\r\n");
    wire.into_bytes()
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

fn make_payload(seed: u64, size: usize) -> (Arc<Vec<u8>>, String) {
    let mut r = seed;
    let data: Vec<u8> = (0..size)
        .map(|_| {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r as u8
        })
        .collect();
    let hash = sha256_hex(&data);
    (Arc::new(data), hash)
}

#[derive(Clone, Copy)]
enum Class {
    Get4k = 0,
    Get64k = 1,
    Get1m = 2,
    Get16m = 3,
    Put4k = 4,
    Put4m = 5,
    Put16m = 6,
}

const CLASS_NAMES: [&str; 7] = [
    "GET  4K", "GET 64K", "GET  1M", "GET 16M", "PUT  4K", "PUT  4M", "PUT 16M",
];

const CLASS_SIZES: [usize; 7] = [4096, 64 * 1024, 1 << 20, 16 << 20, 4096, 4 << 20, 16 << 20];

#[derive(Default)]
struct ClassStats {
    lats_us: Vec<u32>,
}

fn seed_objects(
    address: &str,
    prefix: &str,
    count: usize,
    payload: &[u8],
    payload_sha: &str,
    access_key: &str,
    key: &[u8; 32],
) {
    let mut client = Client::connect(address);
    for i in 0..count {
        let path = format!("/bench/{prefix}-{i}");
        let head = sign_head(
            "PUT",
            &path,
            payload_sha,
            Some(payload.len()),
            access_key,
            key,
        );
        let (status, _) = client.request(&head, payload);
        assert_eq!(status, 200, "seed {path}: {status}");
    }
}

fn print_summary(stats: &mut [ClassStats; 7], elapsed: f64) {
    let mut total_reqs = 0usize;
    let mut total_bytes = 0u64;
    for (idx, st) in stats.iter_mut().enumerate() {
        if st.lats_us.is_empty() {
            continue;
        }
        st.lats_us.sort_unstable();
        let n = st.lats_us.len();
        total_reqs += n;
        total_bytes += n as u64 * CLASS_SIZES[idx] as u64;
        let pct = |p: f64| st.lats_us[((n - 1) as f64 * p) as usize];
        let rps = n as f64 / elapsed;
        let mib = rps * CLASS_SIZES[idx] as f64 / (1 << 20) as f64;
        println!(
            "  {} | {:>7.0} req/s | {:>6.0} MiB/s | p50 {:>7} us | p95 {:>7} us | p99 {:>7} us",
            CLASS_NAMES[idx],
            rps,
            mib,
            pct(0.50),
            pct(0.95),
            pct(0.99),
        );
    }
    println!(
        "  TOTAL   | {:>7.0} req/s | {:>6.0} MiB/s",
        total_reqs as f64 / elapsed,
        total_bytes as f64 / elapsed / (1 << 20) as f64,
    );
}

fn main() {
    let address = env("ADDR", String::from("127.0.0.1:9000"));
    let mode = env("MODE", String::from("classes"));
    let seconds = env("SECONDS", 6u64);
    let seed = env("SEED", 1usize) != 0;
    let access_key = env("ACCESS_KEY", String::from("benchkey"));
    let secret_key = env("SECRET_KEY", String::from("benchsecret"));
    let key = signing_key(&secret_key);

    // Create bucket.
    {
        let head = sign_head("PUT", "/bench", EMPTY_SHA256, Some(0), &access_key, &key);
        let (status, _) = Client::connect(&address).request(&head, b"");
        assert!(status == 200 || status == 409, "create bucket: {status}");
    }

    let (p4k, h4k) = make_payload(1, 4096);
    let (p64k, h64k) = make_payload(2, 64 * 1024);
    let (p1m, h1m) = make_payload(3, 1 << 20);
    let (p4m, h4m) = make_payload(4, 4 << 20);
    let (p16m, h16m) = make_payload(5, 16 << 20);

    if seed {
        seed_objects(&address, "g4k", 64, &p4k, &h4k, &access_key, &key);
        seed_objects(&address, "g64k", 32, &p64k, &h64k, &access_key, &key);
        seed_objects(&address, "g1m", 16, &p1m, &h1m, &access_key, &key);
        seed_objects(&address, "g16m", 8, &p16m, &h16m, &access_key, &key);
    }

    // Pre-sign GET headers for all seeded objects.
    let g4k_heads: Vec<Vec<u8>> = (0..64)
        .map(|i| {
            sign_head(
                "GET",
                &format!("/bench/g4k-{i}"),
                EMPTY_SHA256,
                None,
                &access_key,
                &key,
            )
        })
        .collect();
    let g64k_heads: Vec<Vec<u8>> = (0..32)
        .map(|i| {
            sign_head(
                "GET",
                &format!("/bench/g64k-{i}"),
                EMPTY_SHA256,
                None,
                &access_key,
                &key,
            )
        })
        .collect();
    let g1m_heads: Vec<Vec<u8>> = (0..16)
        .map(|i| {
            sign_head(
                "GET",
                &format!("/bench/g1m-{i}"),
                EMPTY_SHA256,
                None,
                &access_key,
                &key,
            )
        })
        .collect();
    let g16m_heads: Vec<Vec<u8>> = (0..8)
        .map(|i| {
            sign_head(
                "GET",
                &format!("/bench/g16m-{i}"),
                EMPTY_SHA256,
                None,
                &access_key,
                &key,
            )
        })
        .collect();

    let started = Instant::now();
    let deadline = started + Duration::from_secs(seconds);

    let per_worker: Vec<[ClassStats; 7]> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        match mode.as_str() {
            "classes" => {
                // 4 conns × 16 MiB PUT
                for c in 0..4 {
                    let addr = &address;
                    let body = p16m.clone();
                    let heads: Vec<Vec<u8>> = (0..2)
                        .map(|i| {
                            sign_head(
                                "PUT",
                                &format!("/bench/p16m-{c}-{i}"),
                                &h16m,
                                Some(body.len()),
                                &access_key,
                                &key,
                            )
                        })
                        .collect();
                    handles.push(scope.spawn(move || {
                        let mut cli = Client::connect(addr);
                        let mut st: [ClassStats; 7] = Default::default();
                        for i in (0..heads.len()).cycle() {
                            if Instant::now() >= deadline {
                                break;
                            }
                            let t0 = Instant::now();
                            let (code, _) = cli.request(&heads[i], &body);
                            assert_eq!(code, 200);
                            st[Class::Put16m as usize]
                                .lats_us
                                .push(t0.elapsed().as_micros() as u32);
                        }
                        st
                    }));
                }
                // 8 conns × 16 MiB GET
                for c in 0..8 {
                    let addr = &address;
                    let heads = &g16m_heads;
                    handles.push(scope.spawn(move || {
                        let mut cli = Client::connect(addr);
                        let mut st: [ClassStats; 7] = Default::default();
                        for i in (0..heads.len()).cycle().skip(c) {
                            if Instant::now() >= deadline {
                                break;
                            }
                            let t0 = Instant::now();
                            let (code, len) = cli.request(&heads[i], b"");
                            assert_eq!(code, 200);
                            assert_eq!(len, 16 << 20);
                            st[Class::Get16m as usize]
                                .lats_us
                                .push(t0.elapsed().as_micros() as u32);
                        }
                        st
                    }));
                }
                // 16 conns × 4 KiB GET
                for c in 0..16 {
                    let addr = &address;
                    let heads = &g4k_heads;
                    handles.push(scope.spawn(move || {
                        let mut cli = Client::connect(addr);
                        let mut st: [ClassStats; 7] = Default::default();
                        for i in (0..heads.len()).cycle().skip(c * 4) {
                            if Instant::now() >= deadline {
                                break;
                            }
                            let t0 = Instant::now();
                            let (code, len) = cli.request(&heads[i], b"");
                            assert_eq!(code, 200);
                            assert_eq!(len, 4096);
                            st[Class::Get4k as usize]
                                .lats_us
                                .push(t0.elapsed().as_micros() as u32);
                        }
                        st
                    }));
                }
                // 4 conns × 4 KiB PUT
                for c in 0..4 {
                    let addr = &address;
                    let body = p4k.clone();
                    let heads: Vec<Vec<u8>> = (0..16)
                        .map(|i| {
                            sign_head(
                                "PUT",
                                &format!("/bench/p4k-{c}-{i}"),
                                &h4k,
                                Some(body.len()),
                                &access_key,
                                &key,
                            )
                        })
                        .collect();
                    handles.push(scope.spawn(move || {
                        let mut cli = Client::connect(addr);
                        let mut st: [ClassStats; 7] = Default::default();
                        for i in (0..heads.len()).cycle() {
                            if Instant::now() >= deadline {
                                break;
                            }
                            let t0 = Instant::now();
                            let (code, _) = cli.request(&heads[i], &body);
                            assert_eq!(code, 200);
                            st[Class::Put4k as usize]
                                .lats_us
                                .push(t0.elapsed().as_micros() as u32);
                        }
                        st
                    }));
                }
            }
            "bimodal" => {
                for c in 0..32usize {
                    let addr = &address;
                    let g4k = &g4k_heads;
                    let g64k = &g64k_heads;
                    let g1m = &g1m_heads;
                    let g16m = &g16m_heads;
                    let b4k = p4k.clone();
                    let b4m = p4m.clone();
                    let p4k_heads: Vec<Vec<u8>> = (0..8)
                        .map(|i| {
                            sign_head(
                                "PUT",
                                &format!("/bench/bp4k-{c}-{i}"),
                                &h4k,
                                Some(b4k.len()),
                                &access_key,
                                &key,
                            )
                        })
                        .collect();
                    let p4m_heads: Vec<Vec<u8>> = (0..2)
                        .map(|i| {
                            sign_head(
                                "PUT",
                                &format!("/bench/bp4m-{c}-{i}"),
                                &h4m,
                                Some(b4m.len()),
                                &access_key,
                                &key,
                            )
                        })
                        .collect();
                    handles.push(scope.spawn(move || {
                        let mut cli = Client::connect(addr);
                        let mut st: [ClassStats; 7] = Default::default();
                        let mut rng = 0x517c_c1b7_2722_0a95u64.wrapping_mul(c as u64 + 1);
                        let mut step = 0usize;
                        while Instant::now() < deadline {
                            rng ^= rng << 13;
                            rng ^= rng >> 7;
                            rng ^= rng << 17;
                            let roll = (rng % 100) as usize;
                            let (class, head, body, expect_len): (Class, &[u8], &[u8], usize) =
                                if roll < 60 {
                                    (Class::Get4k, &g4k[step % g4k.len()], b"", 4096)
                                } else if roll < 75 {
                                    (Class::Get64k, &g64k[step % g64k.len()], b"", 64 * 1024)
                                } else if roll < 85 {
                                    (Class::Get1m, &g1m[step % g1m.len()], b"", 1 << 20)
                                } else if roll < 90 {
                                    (Class::Get16m, &g16m[step % g16m.len()], b"", 16 << 20)
                                } else if roll < 95 {
                                    (Class::Put4k, &p4k_heads[step % p4k_heads.len()], &b4k, 0)
                                } else {
                                    (Class::Put4m, &p4m_heads[step % p4m_heads.len()], &b4m, 0)
                                };
                            step += 1;
                            let t0 = Instant::now();
                            let (code, len) = cli.request(head, body);
                            assert_eq!(code, 200);
                            if expect_len > 0 {
                                assert_eq!(len, expect_len);
                            }
                            st[class as usize]
                                .lats_us
                                .push(t0.elapsed().as_micros() as u32);
                        }
                        st
                    }));
                }
            }
            other => panic!("unknown MODE `{other}` (use `classes` or `bimodal`)"),
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let elapsed = started.elapsed().as_secs_f64();
    let mut merged: [ClassStats; 7] = Default::default();
    for worker in per_worker {
        for (dst, src) in merged.iter_mut().zip(worker) {
            dst.lats_us.extend(src.lats_us);
        }
    }
    print_summary(&mut merged, elapsed);
}
