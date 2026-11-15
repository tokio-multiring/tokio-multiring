use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, Sidecar};

const BUF_SIZE: usize = 64 * 1024;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal)
        .build()
        .unwrap()
}

#[test]
fn rejects_bad_geometry() {
    for (buf_size, count) in [(0, 1), (4095, 1), (6000, 4), (4096, 0), (2 << 30, 1)] {
        let error = BufferPool::new(buf_size, count).err().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}

#[test]
fn buffers_are_page_aligned_and_distinct() {
    let pool = BufferPool::new(BUF_SIZE, 8).unwrap();
    let mut bufs: Vec<_> = (0..8).map(|_| pool.try_acquire().unwrap()).collect();
    assert!(pool.try_acquire().is_none());

    for (index, buf) in bufs.iter_mut().enumerate() {
        assert_eq!(buf.capacity(), BUF_SIZE);
        assert!(buf.is_empty());
        assert_eq!(
            buf.extend_from_slice(&vec![index as u8; BUF_SIZE + 1]),
            BUF_SIZE
        );
        assert_eq!(buf.as_ptr() as usize % 4096, 0);
    }
    for (index, buf) in bufs.iter().enumerate() {
        assert!(buf.iter().all(|&byte| byte == index as u8));
    }

    bufs[0].truncate(3);
    assert_eq!(&bufs[0][..], [0, 0, 0]);
    bufs[0].clear();
    assert!(bufs[0].is_empty());
}

#[test]
fn dropped_buffers_come_back_empty() {
    let pool = BufferPool::new(BUF_SIZE, 2).unwrap();
    let mut first = pool.try_acquire().unwrap();
    first.extend_from_slice(b"left behind");
    let second = pool.try_acquire().unwrap();
    assert_eq!(pool.available(), 0);

    drop(first);
    assert_eq!(pool.available(), 1);
    assert!(pool.try_acquire().unwrap().is_empty());
    drop(second);
    assert_eq!(pool.available(), 2);
}

#[test]
fn acquire_waits_for_a_buffer() {
    runtime().block_on(async {
        let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
        let held = pool.acquire().await;

        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.acquire().await.capacity() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        drop(held);
        let capacity = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter was never woken")
            .unwrap();
        assert_eq!(capacity, BUF_SIZE);
        assert_eq!(pool.available(), 1);
    });
}

#[test]
fn acquire_many_waits_for_all_of_them() {
    runtime().block_on(async {
        let pool = BufferPool::new(BUF_SIZE, 3).unwrap();
        assert_eq!(pool.count(), 3);
        let held = pool.acquire_many(2).await;
        assert_eq!(held.len(), 2);

        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.acquire_many(2).await.len() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        drop(held);
        let got = tokio::time::timeout(Duration::from_secs(5), waiter).await;
        assert_eq!(got.expect("waiter was never woken").unwrap(), 2);
        assert_eq!(pool.available(), 3);
    });
}

/// Tasks that each need the whole pool take turns. Taking the buffers one
/// by one would leave them holding one apiece, waiting for the other.
#[test]
fn acquire_many_does_not_deadlock() {
    runtime().block_on(async {
        let pool = BufferPool::new(BUF_SIZE, 2).unwrap();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    for _ in 0..500 {
                        let bufs = pool.acquire_many(2).await;
                        tokio::task::yield_now().await;
                        drop(bufs);
                    }
                })
            })
            .collect();
        for task in tasks {
            tokio::time::timeout(Duration::from_secs(20), task)
                .await
                .expect("tasks are stuck")
                .unwrap();
        }
        assert_eq!(pool.available(), 2);
    });
}

/// Every worker's ring takes the pool's registration, and a second runtime
/// can take it as well.
#[test]
fn one_pool_registers_on_many_rings() {
    let pool = BufferPool::new(BUF_SIZE, 16).unwrap();
    for _ in 0..2 {
        runtime().block_on(async {
            let sidecar = Sidecar::start(Config {
                buffers: Some(pool.clone()),
                ..Config::default()
            })
            .await
            .unwrap();
            assert_eq!(sidecar.buffers().unwrap().buf_size(), BUF_SIZE);
            tokio::spawn(async move { sidecar.nop().await })
                .await
                .unwrap()
                .unwrap();
        });
    }
}

async fn sidecar_with(pool: &BufferPool) -> Sidecar {
    Sidecar::start(Config {
        buffers: Some(pool.clone()),
        ..Config::default()
    })
    .await
    .unwrap()
}

/// `O_DIRECT` is the case the pool's alignment exists for: the kernel
/// refuses buffers, lengths and offsets that are not block-aligned.
#[test]
fn direct_io_round_trip() {
    const BLOCK: usize = 4096;

    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("direct-io-{}", std::process::id()));
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path)
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let fd = Arc::new(OwnedFd::from(file));

    let pool = BufferPool::new(BUF_SIZE, 4).unwrap();
    runtime().block_on(async {
        let sidecar = sidecar_with(&pool).await;
        tokio::spawn({
            let pool = pool.clone();
            async move {
                // One full buffer, then a single block.
                let mut buf = pool.acquire().await;
                buf.extend_from_slice(&[0xa5; BUF_SIZE]);
                let (written, mut buf) = sidecar.write_fixed_at(fd.clone(), buf, 0).await;
                assert_eq!(written.unwrap(), BUF_SIZE);

                buf.clear();
                buf.extend_from_slice(&[0x5a; BLOCK]);
                let (written, mut buf) = sidecar
                    .write_fixed_at(fd.clone(), buf, BUF_SIZE as u64)
                    .await;
                assert_eq!(written.unwrap(), BLOCK);

                buf.clear();
                let (read, mut buf) = sidecar.read_fixed_at(fd.clone(), buf, 0, usize::MAX).await;
                assert_eq!(read.unwrap(), BUF_SIZE);
                assert!(buf.iter().all(|&byte| byte == 0xa5));

                // The file ends one block into this read.
                buf.clear();
                let (read, buf) = sidecar
                    .read_fixed_at(fd, buf, BUF_SIZE as u64, usize::MAX)
                    .await;
                assert_eq!(read.unwrap(), BLOCK);
                assert_eq!(buf.len(), BLOCK);
                assert!(buf.iter().all(|&byte| byte == 0x5a));
            }
        })
        .await
        .unwrap();
    });
    assert_eq!(pool.available(), 4);
}

#[test]
fn read_stops_at_its_limit() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let reader = Arc::new(OwnedFd::from(reader));
    writer.write_all(b"onetwo").unwrap();

    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    runtime().block_on(async {
        let sidecar = sidecar_with(&pool).await;
        let (read, buf) = sidecar
            .read_fixed_at(reader.clone(), pool.acquire().await, 0, 3)
            .await;
        assert_eq!(read.unwrap(), 3);
        let (read, buf) = sidecar.read_fixed_at(reader, buf, 0, usize::MAX).await;
        assert_eq!(read.unwrap(), 3);
        assert_eq!(&buf[..], b"onetwo");
    });
}

#[test]
fn read_appends_to_the_contents() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    let reader = Arc::new(OwnedFd::from(reader));
    writer.write_all(b"world").unwrap();

    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    runtime().block_on(async {
        let sidecar = sidecar_with(&pool).await;
        let mut buf = pool.acquire().await;
        buf.extend_from_slice(b"hello ");
        let (read, buf) = sidecar.read_fixed_at(reader, buf, 0, usize::MAX).await;
        assert_eq!(read.unwrap(), 5);
        assert_eq!(&buf[..], b"hello world");
    });
}

#[test]
fn cancelled_read_returns_its_buffer() {
    let (reader, _writer) = UnixStream::pair().unwrap();
    let reader = Arc::new(OwnedFd::from(reader));

    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    runtime().block_on(async {
        let sidecar = sidecar_with(&pool).await;
        let read = sidecar.read_fixed_at(reader, pool.acquire().await, 0, usize::MAX);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), read)
                .await
                .is_err()
        );
        // Not before the kernel has let go of it.
        tokio::time::timeout(Duration::from_secs(5), pool.acquire())
            .await
            .expect("buffer never came back");
    });
}

#[test]
fn buffers_from_elsewhere_are_refused() {
    let (reader, _writer) = UnixStream::pair().unwrap();
    let reader = Arc::new(OwnedFd::from(reader));

    let registered = BufferPool::new(BUF_SIZE, 1).unwrap();
    let other = BufferPool::new(BUF_SIZE, 1).unwrap();
    runtime().block_on(async {
        let sidecar = sidecar_with(&registered).await;
        let mut buf = other.acquire().await;
        buf.extend_from_slice(b"kept");

        let (result, buf) = sidecar
            .read_fixed_at(reader.clone(), buf, 0, usize::MAX)
            .await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
        let (result, buf) = sidecar.write_fixed_at(reader, buf, 0).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(&buf[..], b"kept");
        assert_eq!(sidecar.in_flight(), 0);
    });
}

#[test]
fn fixed_operations_need_a_pool() {
    let (reader, _writer) = UnixStream::pair().unwrap();
    let reader = Arc::new(OwnedFd::from(reader));

    let pool = BufferPool::new(BUF_SIZE, 1).unwrap();
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let (result, _buf) = sidecar
            .read_fixed_at(reader, pool.acquire().await, 0, usize::MAX)
            .await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    });
}
