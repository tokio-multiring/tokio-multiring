//! Random-read throughput of the disk path against a block device.
//!
//! ```text
//! DEVICE=/dev/nvme0n2 MODE=polled DOMAINS=4 DEPTH=32 disk_bench
//! ```
//!
//! `MODE=fill` first writes the span the reads then cover: a drive answers
//! reads of blocks it has never been given without touching its flash.
//! Everything in the span is overwritten.
//!
//! - `MODE`: `polled` (disk rings), `interrupt` (the workers' own rings) or
//!   `fill`
//! - `DOMAINS`: how many domains to use
//! - `GROUP`: regroup the machine's CPUs into domains of this many workers
//!   and one kernel thread, ignoring the cache topology. Unset, the domains
//!   are the detected ones.
//! - `HYPERTHREADS`: `skip` or `use`
//! - `DEPTH`: reads in flight per worker
//! - `BLOCK`: bytes per read, `SPAN_GIB`: size of the region, `SECONDS`

use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_multiring::{BufferPool, Config, DiskConfig, Domain, Hyperthreads, MultiRing, Plan};

fn env<T: FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} has an invalid value")),
        Err(_) => default,
    }
}

/// CPU time the process has used, kernel submission threads included.
fn cpu_time() -> Duration {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) }, 0);
    let seconds = usage.ru_utime.tv_sec + usage.ru_stime.tv_sec;
    let micros = usage.ru_utime.tv_usec + usage.ru_stime.tv_usec;
    Duration::from_secs(seconds as u64) + Duration::from_micros(micros as u64)
}

fn main() {
    let device = std::env::var("DEVICE").expect("DEVICE names the block device to use");
    let mode = env("MODE", String::from("polled"));
    let domains = env("DOMAINS", usize::MAX);
    let depth = env("DEPTH", 32usize);
    let block = env("BLOCK", 4096usize);
    let span = env("SPAN_GIB", 16u64) << 30;
    let seconds = env("SECONDS", 5u64);
    let hyperthreads = match env("HYPERTHREADS", String::from("skip")).as_str() {
        "skip" => Hyperthreads::Skip,
        "use" => Hyperthreads::Use,
        other => panic!("HYPERTHREADS is `skip` or `use`, not `{other}`"),
    };

    let detected = Plan::detect(hyperthreads).unwrap();
    let mut available = detected.domains().to_vec();
    if let Ok(group) = std::env::var("GROUP") {
        let group: usize = group.parse().expect("GROUP has an invalid value");
        let cpus: Vec<u32> = available
            .iter()
            .flat_map(|domain| domain.workers.iter().chain(&domain.sq_thread).copied())
            .collect();
        available = cpus
            .chunks_exact(group + 1)
            .map(|chunk| Domain {
                workers: chunk[..group].to_vec(),
                sq_thread: Some(chunk[group]),
            })
            .collect();
    }
    let domains = domains.min(available.len());
    let plan = Plan::new(available[..domains].to_vec()).unwrap();
    let workers = plan.worker_count();

    let file = std::fs::File::options()
        .read(true)
        .write(mode == "fill")
        .custom_flags(libc::O_DIRECT)
        .open(&device)
        .unwrap();
    let fd = Arc::new(OwnedFd::from(file));

    let polled = match mode.as_str() {
        "polled" | "fill" => true,
        "interrupt" => false,
        other => panic!("MODE is `polled`, `interrupt` or `fill`, not `{other}`"),
    };
    let tasks = workers * depth;
    let pool = match mode.as_str() {
        "fill" => BufferPool::new(FILL_PIECE, FILL_WRITERS).unwrap(),
        _ => BufferPool::new(block, tasks).unwrap(),
    };

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_time();
    let config = Config {
        buffers: Some(pool.clone()),
        disk: polled.then(|| DiskConfig {
            sq_entries: 256,
            sq_thread_idle: Duration::from_millis(100),
            plan: plan.clone(),
        }),
        ..Config::default()
    };
    let multiring = MultiRing::install(&mut builder, config);
    let runtime = plan.build(&mut builder).unwrap();

    if mode == "fill" {
        runtime.block_on(fill(multiring, pool, fd, span));
        return;
    }
    let (latencies, cpu) = runtime.block_on(async {
        let begin = Instant::now() + Duration::from_secs(1);
        let end = begin + Duration::from_secs(seconds);
        let blocks = span / block as u64;
        let readers: Vec<_> = (0..tasks)
            .map(|task| {
                let multiring = multiring.clone();
                let pool = pool.clone();
                let fd = fd.clone();
                tokio::spawn(async move {
                    let mut random = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(task as u64 + 1);
                    let mut latencies = Vec::new();
                    let mut buf = pool.acquire().await;
                    loop {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        let offset = (random % blocks) * block as u64;

                        buf.clear();
                        let started = Instant::now();
                        let (read, returned) = if polled {
                            multiring
                                .read_polled_at(fd.clone(), buf, offset, usize::MAX)
                                .await
                        } else {
                            multiring
                                .read_fixed_at(fd.clone(), buf, offset, usize::MAX)
                                .await
                        };
                        let finished = Instant::now();
                        assert_eq!(read.unwrap(), block);
                        buf = returned;

                        if finished > end {
                            return latencies;
                        }
                        if started >= begin {
                            latencies.push((finished - started).as_micros() as u32);
                        }
                    }
                })
            })
            .collect();

        tokio::time::sleep_until(begin.into()).await;
        let cpu_before = cpu_time();
        tokio::time::sleep_until(end.into()).await;
        let cpu = cpu_time() - cpu_before;

        let mut latencies = Vec::new();
        for reader in readers {
            latencies.extend(reader.await.unwrap());
        }
        (latencies, cpu)
    });

    let mut latencies = latencies;
    latencies.sort_unstable();
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let iops = latencies.len() as f64 / seconds as f64;
    let cores = cpu.as_secs_f64() / seconds as f64;
    println!(
        "{mode:<9} domains={domains} workers={workers:<2} depth={depth:<3} \
         {:>7.0}k IOPS  p50 {:>4} us  p99 {:>5} us  {cores:>5.2} cores  {:>4.0}k IOPS/core",
        iops / 1e3,
        percentile(0.50),
        percentile(0.99),
        iops / cores / 1e3,
    );
}

/// What `fill` writes at a time.
const FILL_PIECE: usize = 1 << 20;
/// How many tasks `fill` writes with, each holding one buffer of `pool`.
const FILL_WRITERS: usize = 16;

/// Writes the whole span, in 1 MiB pieces.
async fn fill(multiring: MultiRing, pool: BufferPool, fd: Arc<OwnedFd>, span: u64) {
    const PIECE: usize = FILL_PIECE;
    const WRITERS: u64 = FILL_WRITERS as u64;

    let started = Instant::now();
    let pieces = span / PIECE as u64;
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let multiring = multiring.clone();
            let pool = pool.clone();
            let fd = fd.clone();
            tokio::spawn(async move {
                let mut buf = pool.acquire().await;
                let mut fill = writer.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                for piece in (writer..pieces).step_by(WRITERS as usize) {
                    buf.clear();
                    // Not all one value, so the drive cannot deduplicate it.
                    while buf.len() < PIECE {
                        fill ^= fill << 13;
                        fill ^= fill >> 7;
                        fill ^= fill << 17;
                        buf.extend_from_slice(&fill.to_ne_bytes());
                    }
                    let (written, returned) = multiring
                        .write_polled_at(fd.clone(), buf, piece * PIECE as u64)
                        .await;
                    assert_eq!(written.unwrap(), PIECE);
                    buf = returned;
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "filled {} GiB in {elapsed:.1} s, {:.0} MiB/s",
        span >> 30,
        (span >> 20) as f64 / elapsed
    );
}
