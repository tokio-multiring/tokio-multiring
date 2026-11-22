//! The polled disk rings.
//!
//! These run against a file in the build directory by default. That checks
//! the plumbing, but on most file systems the device then completes by
//! interrupt. To exercise real polling, point `MULTIRING_SCRATCH_DEVICE` at a
//! block device that has poll queues and whose contents may be overwritten.

use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;
use std::time::Duration;

use tokio_multiring::{BufferPool, Config, DiskConfig, Domain, Hyperthreads, MultiRing, Plan};

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

/// A runtime that `plan` builds and a multiring with these settings drives.
fn build(plan: &Plan, config: Config) -> std::io::Result<(tokio::runtime::Runtime, MultiRing)> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_time();
    let multiring = MultiRing::install(&mut builder, config);
    Ok((plan.build(&mut builder)?, multiring))
}

/// Settings for a multiring with `pool` registered and with disk rings for a
/// runtime that `plan` builds.
fn config(pool: &BufferPool, plan: &Plan) -> Config {
    Config {
        buffers: Some(pool.clone()),
        disk: Some(DiskConfig {
            sq_entries: 64,
            sq_thread_idle: Duration::from_millis(50),
            plan: plan.clone(),
        }),
        ..Config::default()
    }
}

/// A runtime with disk rings, its workers in the domains of `plan`.
fn runtime(pool: &BufferPool, plan: Plan) -> (tokio::runtime::Runtime, MultiRing) {
    build(&plan, config(pool, &plan)).unwrap()
}

/// A runtime with disk rings, its workers in one domain.
fn setup(pool: &BufferPool) -> (tokio::runtime::Runtime, MultiRing) {
    runtime(pool, plan(&[WORKERS]))
}

/// An `O_DIRECT` descriptor with room for `slot + 1` buffers. Tests use
/// different slots, because they may share one scratch device.
fn scratch(slot: u64) -> (Arc<OwnedFd>, u64) {
    let mut options = std::fs::File::options();
    options.read(true).write(true).custom_flags(libc::O_DIRECT);
    let file = match std::env::var_os("MULTIRING_SCRATCH_DEVICE") {
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
    multiring: &MultiRing,
    pool: &BufferPool,
    fd: &Arc<OwnedFd>,
    offset: u64,
    fill: u8,
) {
    let mut buf = pool.acquire().await;
    buf.extend_from_slice(&[fill; BUF_SIZE]);
    let (written, mut buf) = multiring.write_polled_at(fd.clone(), buf, offset).await;
    assert_eq!(written.unwrap(), BUF_SIZE);

    buf.clear();
    let (read, buf) = multiring
        .read_polled_at(fd.clone(), buf, offset, usize::MAX)
        .await;
    assert_eq!(read.unwrap(), BUF_SIZE);
    assert!(buf.iter().all(|&byte| byte == fill));
}

#[test]
fn round_trip_on_a_worker() {
    let (fd, offset) = scratch(0);
    let pool = BufferPool::new(BUF_SIZE, 4).unwrap();
    let (runtime, multiring) = setup(&pool);
    runtime.block_on(async {
        tokio::spawn({
            let pool = pool.clone();
            async move { round_trip(&multiring, &pool, &fd, offset, 0x11).await }
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
    let (runtime, multiring) = setup(&pool);
    runtime.block_on(async {
        round_trip(&multiring, &pool, &fd, offset, 0x22).await;
    });
}

/// One operation at a time, so the worker has nothing else to do and is
/// usually parked by the time the completion arrives. Nothing but the disk
/// ring's own readiness can wake it then.
#[test]
fn completions_wake_parked_workers() {
    let (fd, offset) = scratch(2);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, multiring) = setup(&pool);
    runtime.block_on(async {
        tokio::spawn({
            let pool = pool.clone();
            async move {
                for round in 0..300u32 {
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        round_trip(&multiring, &pool, &fd, offset, round as u8),
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
    let (runtime, multiring) = runtime(&pool, plan(&[2, 2]));
    runtime.block_on(async {
        let mut buf = pool.acquire().await;
        buf.extend_from_slice(&[0x33; 4096]);
        let (written, buf) = multiring.write_polled_at(fd.clone(), buf, offset).await;
        assert_eq!(written.unwrap(), 4096);
        drop(buf);

        let reads: Vec<_> = (0..OPS)
            .map(|_| {
                let multiring = multiring.clone();
                let pool = pool.clone();
                let fd = fd.clone();
                tokio::spawn(async move {
                    let (read, buf) = multiring
                        .read_polled_at(fd, pool.acquire().await, offset, usize::MAX)
                        .await;
                    assert_eq!(read.unwrap(), 4096);
                    assert!(buf.iter().all(|&byte| byte == 0x33));
                })
            })
            .collect();
        for read in reads {
            read.await.unwrap();
        }
        assert_eq!(multiring.in_flight(), 0);
    });
    assert_eq!(pool.available(), 64);
}

/// Submitted from a worker, so the write is with the kernel before the
/// future is dropped, and nothing can call it back from there.
#[test]
fn dropped_operation_runs_to_its_end() {
    let (fd, offset) = scratch(4);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let (runtime, multiring) = setup(&pool);
    runtime.block_on(async {
        tokio::spawn({
            let pool = pool.clone();
            async move {
                let mut buf = pool.acquire().await;
                buf.extend_from_slice(&[0x44; BUF_SIZE]);

                let mut write = Box::pin(multiring.write_polled_at(fd.clone(), buf, offset));
                std::future::poll_fn(|cx| {
                    let _ = write.as_mut().poll(cx);
                    std::task::Poll::Ready(())
                })
                .await;
                drop(write);

                let buf = tokio::time::timeout(Duration::from_secs(5), pool.acquire())
                    .await
                    .expect("buffer never came back");
                let (read, buf) = multiring.read_polled_at(fd, buf, offset, usize::MAX).await;
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
    let (runtime, multiring) = setup(&pool);
    runtime.block_on(async {
        let (read, _buf) = multiring
            .read_polled_at(fd, pool.acquire().await, 0, usize::MAX)
            .await;
        assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::Unsupported);
    });
}

#[test]
fn polled_operations_need_disk_rings() {
    let (fd, offset) = scratch(5);
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let settings = Config {
        disk: None,
        ..config(&pool, &plan(&[WORKERS]))
    };
    let (runtime, multiring) = build(&plan(&[WORKERS]), settings).unwrap();
    runtime.block_on(async {
        let (read, _buf) = multiring
            .read_polled_at(fd, pool.acquire().await, offset, usize::MAX)
            .await;
        assert_eq!(read.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    });
}

#[test]
fn disk_rings_need_a_pool() {
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let plan = plan(&[WORKERS]);
    let settings = Config {
        buffers: None,
        ..config(&pool, &plan)
    };
    let error = build(&plan, settings).err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// The disk rings are grouped by the plan that places the workers, so a
/// plan that does not build the runtime cannot be used.
#[test]
fn disk_rings_need_the_runtimes_plan() {
    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    let other = plan(&[WORKERS]);
    let error = build(&plan(&[WORKERS]), config(&pool, &other))
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
