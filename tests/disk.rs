//! The polled disk rings.
//!
//! These run against a file in the build directory by default. That checks
//! the plumbing, but on most file systems the device then completes by
//! interrupt. To exercise real polling, point `SIDECAR_SCRATCH_DEVICE` at a
//! block device that has poll queues and whose contents may be overwritten.

use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;
use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, DiskConfig, Domain, Hyperthreads, Plan, Sidecar};

const WORKERS: usize = 4;
const BUF_SIZE: usize = 64 * 1024;

/// A plan that spreads `WORKERS` workers over domains of the given sizes,
/// on whichever CPUs this process may use.
fn plan(domain_sizes: &[usize]) -> Plan {
    let detected = Plan::detect(Hyperthreads::Use).unwrap();
    let mut cpus = detected
        .domains()
        .iter()
        .flat_map(|domain| domain.workers.iter().chain(&domain.sq_thread).copied());
    let domains = domain_sizes
        .iter()
        .map(|&size| Domain {
            workers: cpus.by_ref().take(size).collect(),
            sq_thread: None,
        })
        .collect();
    Plan::new(domains).unwrap()
}

fn runtime(plan: &Plan) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal);
    plan.build(&mut builder).unwrap()
}

/// A runtime with one domain, and the configuration for its disk rings.
fn setup() -> (tokio::runtime::Runtime, DiskConfig) {
    let plan = plan(&[WORKERS]);
    (runtime(&plan), disk_config(plan))
}

fn disk_config(plan: Plan) -> DiskConfig {
    DiskConfig {
        sq_entries: 64,
        sq_thread_idle: Duration::from_millis(50),
        plan,
    }
}

async fn start(pool: &BufferPool, disk: DiskConfig) -> std::io::Result<Sidecar> {
    Sidecar::start(Config {
        buffers: Some(pool.clone()),
        disk: Some(disk),
        ..Config::default()
    })
    .await
}

/// An `O_DIRECT` descriptor with room for `slot + 1` buffers. Tests use
/// different slots, because they may share one scratch device.
fn scratch(slot: u64) -> (Arc<OwnedFd>, u64) {
    let mut options = std::fs::File::options();
    options.read(true).write(true).custom_flags(libc::O_DIRECT);
    let file = match std::env::var_os("SIDECAR_SCRATCH_DEVICE") {
        Some(device) => options.open(device).unwrap(),
        None => {
            let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
                .join(format!("disk-{}-{slot}", std::process::id()));
            let file = options.create_new(true).open(&path).unwrap();
            std::fs::remove_file(&path).unwrap();
            file
        }
    };
    (Arc::new(OwnedFd::from(file)), slot * BUF_SIZE as u64)
}

async fn round_trip(
    sidecar: &Sidecar,
    pool: &BufferPool,
    fd: &Arc<OwnedFd>,
    offset: u64,
    fill: u8,
) {
    let mut buf = pool.acquire().await;
    buf.extend_from_slice(&[fill; BUF_SIZE]);
    let (written, mut buf) = sidecar.write_polled_at(fd.clone(), buf, offset).await;
    assert_eq!(written.unwrap(), BUF_SIZE);

    buf.clear();
    let (read, buf) = sidecar.read_polled_at(fd.clone(), buf, offset).await;
    assert_eq!(read.unwrap(), BUF_SIZE);
    assert!(buf.iter().all(|&byte| byte == fill));
}

#[test]
fn round_trip_on_a_worker() {
    let (fd, offset) = scratch(0);
    let pool = BufferPool::new(BUF_SIZE, 4).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let sidecar = start(&pool, disk).await.unwrap();
        tokio::spawn({
            let pool = pool.clone();
            async move { round_trip(&sidecar, &pool, &fd, offset, 0x11).await }
        })
        .await
        .unwrap();
    });
    assert_eq!(pool.available(), 4);
}

/// The `block_on` thread has no ring, so the operations are handed to a
/// worker's disk ring.
#[test]
fn round_trip_off_worker() {
    let (fd, offset) = scratch(1);
    let pool = BufferPool::new(BUF_SIZE, 4).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let sidecar = start(&pool, disk).await.unwrap();
        round_trip(&sidecar, &pool, &fd, offset, 0x22).await;
    });
}

/// One operation at a time, so the worker has nothing else to do and is
/// usually parked by the time the completion arrives. Nothing but the disk
/// ring's own readiness can wake it then.
#[test]
fn completions_wake_parked_workers() {
    let (fd, offset) = scratch(2);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let sidecar = start(&pool, disk).await.unwrap();
        tokio::spawn({
            let pool = pool.clone();
            async move {
                for round in 0..300u32 {
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        round_trip(&sidecar, &pool, &fd, offset, round as u8),
                    )
                    .await
                    .expect("worker was never woken");
                    // Long enough for the kernel thread to go to sleep.
                    if round % 100 == 99 {
                        tokio::time::sleep(Duration::from_millis(120)).await;
                    }
                }
            }
        })
        .await
        .unwrap();
    });
}

/// More operations in flight than either queue of a disk ring holds.
#[test]
fn many_operations_at_once() {
    const OPS: usize = 512;

    let (fd, offset) = scratch(3);
    let pool = BufferPool::new(4096, 64).unwrap();
    let plan = plan(&[2, 2]);
    runtime(&plan).block_on(async {
        let sidecar = start(&pool, disk_config(plan)).await.unwrap();

        let mut buf = pool.acquire().await;
        buf.extend_from_slice(&[0x33; 4096]);
        let (written, buf) = sidecar.write_polled_at(fd.clone(), buf, offset).await;
        assert_eq!(written.unwrap(), 4096);
        drop(buf);

        let reads: Vec<_> = (0..OPS)
            .map(|_| {
                let sidecar = sidecar.clone();
                let pool = pool.clone();
                let fd = fd.clone();
                tokio::spawn(async move {
                    let (read, buf) = sidecar
                        .read_polled_at(fd, pool.acquire().await, offset)
                        .await;
                    assert_eq!(read.unwrap(), 4096);
                    assert!(buf.iter().all(|&byte| byte == 0x33));
                })
            })
            .collect();
        for read in reads {
            read.await.unwrap();
        }
        assert_eq!(sidecar.in_flight(), 0);
    });
    assert_eq!(pool.available(), 64);
}

/// Submitted from a worker, so the write is with the kernel before the
/// future is dropped, and nothing can call it back from there.
#[test]
fn dropped_operation_runs_to_its_end() {
    let (fd, offset) = scratch(4);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let sidecar = start(&pool, disk).await.unwrap();
        tokio::spawn({
            let pool = pool.clone();
            async move {
                let mut buf = pool.acquire().await;
                buf.extend_from_slice(&[0x44; BUF_SIZE]);

                let mut write = Box::pin(sidecar.write_polled_at(fd.clone(), buf, offset));
                std::future::poll_fn(|cx| {
                    let _ = write.as_mut().poll(cx);
                    std::task::Poll::Ready(())
                })
                .await;
                drop(write);

                let buf = tokio::time::timeout(Duration::from_secs(5), pool.acquire())
                    .await
                    .expect("buffer never came back");
                let (read, buf) = sidecar.read_polled_at(fd, buf, offset).await;
                assert_eq!(read.unwrap(), BUF_SIZE);
                assert!(buf.iter().all(|&byte| byte == 0x44));
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn buffered_files_are_refused() {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("buffered-{}", std::process::id()));
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let fd = Arc::new(OwnedFd::from(file));

    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let sidecar = start(&pool, disk).await.unwrap();
        let (read, _buf) = sidecar.read_polled_at(fd, pool.acquire().await, 0).await;
        assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::Unsupported);
    });
}

#[test]
fn polled_operations_need_disk_rings() {
    let (fd, offset) = scratch(5);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, _) = setup();
    runtime.block_on(async {
        let sidecar = Sidecar::start(Config {
            buffers: Some(pool.clone()),
            ..Config::default()
        })
        .await
        .unwrap();
        let (read, _buf) = sidecar
            .read_polled_at(fd, pool.acquire().await, offset)
            .await;
        assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    });
}

#[test]
fn disk_rings_need_a_pool() {
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let error = Sidecar::start(Config {
            disk: Some(disk),
            ..Config::default()
        })
        .await
        .err()
        .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    });
}

/// The disk rings are grouped by the plan that placed the workers, so a
/// plan that did not build the runtime cannot be used.
#[test]
fn disk_rings_need_the_runtimes_plan() {
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, disk) = setup();
    runtime.block_on(async {
        let other = disk_config(plan(&[WORKERS]));
        let error = start(&pool, other).await.err().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        // The refusal left nothing behind.
        let sidecar = start(&pool, disk).await.unwrap();
        sidecar.nop().await.unwrap();
    });
}
