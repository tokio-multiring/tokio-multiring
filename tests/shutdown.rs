//! Runtime teardown must reclaim kernel-owned memory and complete surviving futures.
use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio_multiring::{BufferPool, Config, MultiRing, Segment};

fn setup(pool: &BufferPool) -> (tokio::runtime::Runtime, MultiRing) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_time();
    let multiring = MultiRing::install(
        &mut builder,
        Config {
            sq_entries: 2,
            buffers: Some(pool.clone()),
            ..Config::default()
        },
    );
    (builder.build().unwrap(), multiring)
}

fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn finish<F: Future>(mut future: Pin<&mut F>) -> F::Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(result) = poll(future.as_mut()) {
            return result;
        }
        assert!(Instant::now() < deadline, "future stranded by shutdown");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "worker did not make progress");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn shutdown_returns_pending_receive_and_its_buffer() {
    let pool = BufferPool::new(4096, 1).unwrap();
    let (runtime, multiring) = setup(&pool);
    let (socket, _peer) = UnixStream::pair().unwrap();
    let fd = Arc::new(OwnedFd::from(socket));
    let mut read = Box::pin(multiring.recv(fd.clone(), pool.try_acquire().unwrap(), 4096));
    assert!(poll(read.as_mut()).is_pending());
    wait_for(|| multiring.in_flight() == 1);
    drop(runtime);
    let (result, buffer) = finish(read.as_mut());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    assert_eq!(multiring.in_flight(), 0);
    drop(buffer);
    assert_eq!(pool.available(), 1);
    assert_eq!(Arc::strong_count(&fd), 1);
}

#[test]
fn operations_started_after_shutdown_fail_and_return_buffers() {
    let pool = BufferPool::new(4096, 1).unwrap();
    let (runtime, multiring) = setup(&pool);
    drop(runtime);
    let (socket, _peer) = UnixStream::pair().unwrap();
    let fd = Arc::new(OwnedFd::from(socket));
    let mut read = Box::pin(multiring.recv(fd.clone(), pool.try_acquire().unwrap(), 4096));
    let (result, buffer) = finish(read.as_mut());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    drop(buffer);
    let mut buffer = pool.try_acquire().unwrap();
    buffer.extend_from_slice(b"hello");
    let mut send = Box::pin(multiring.send_zc_vectored(
        fd,
        vec![buffer],
        &[Segment {
            buf: 0,
            start: 0,
            len: 5,
        }],
    ));
    let (result, reclaim) = finish(send.as_mut());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    drop(finish(Box::pin(reclaim).as_mut()));
    assert_eq!(pool.available(), 1);
}

#[test]
fn shutdown_rejects_queued_operations_before_they_reach_the_ring() {
    let pool = BufferPool::new(4096, 64).unwrap();
    let (runtime, multiring) = setup(&pool);
    let (entered, entry) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    runtime.spawn(async move {
        entered.send(()).unwrap();
        blocked.recv().unwrap();
    });
    entry.recv_timeout(Duration::from_secs(5)).unwrap();
    let (socket, _peer) = UnixStream::pair().unwrap();
    let fd = Arc::new(OwnedFd::from(socket));
    let mut reads: Vec<_> = (0..64)
        .map(|_| {
            let mut read = Box::pin(multiring.recv(fd.clone(), pool.try_acquire().unwrap(), 4096));
            assert!(poll(read.as_mut()).is_pending());
            read
        })
        .collect();
    assert_eq!(multiring.in_flight(), 0);
    runtime.shutdown_background();
    release.send(()).unwrap();
    // Exercise abandonment racing the worker's rejection of unsent work.
    reads.truncate(32);
    for mut read in reads {
        let (result, buffer) = finish(read.as_mut());
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        drop(buffer);
    }
    wait_for(|| pool.available() == 64);
    assert_eq!(Arc::strong_count(&fd), 1);
}

#[test]
fn shutdown_drains_more_operations_than_the_submission_queue_holds() {
    let pool = BufferPool::new(4096, 64).unwrap();
    let (runtime, multiring) = setup(&pool);
    let (socket, mut peer) = UnixStream::pair().unwrap();
    let fd = Arc::new(OwnedFd::from(socket));
    let mut reads: Vec<_> = (0..64)
        .map(|_| {
            let mut read = Box::pin(multiring.recv(fd.clone(), pool.try_acquire().unwrap(), 4096));
            assert!(poll(read.as_mut()).is_pending());
            read
        })
        .collect();
    wait_for(|| multiring.in_flight() == 64);
    // Race real completions and future abandonment against shutdown.
    let writer = std::thread::spawn(move || {
        let _ = peer.write_all(&[42; 4096]);
        peer
    });
    reads.truncate(32);
    drop(runtime);
    let _peer = writer.join().unwrap();
    for mut read in reads {
        let (result, buffer) = finish(read.as_mut());
        if let Err(error) = result {
            assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
        }
        drop(buffer);
    }
    assert_eq!(multiring.in_flight(), 0);
    assert_eq!(pool.available(), 64);
    assert_eq!(Arc::strong_count(&fd), 1);
}

#[test]
fn shutdown_waits_for_zero_copy_buffer_release() {
    const SIZE: usize = 4 << 20;
    let pool = BufferPool::without_huge_pages(SIZE, 1).unwrap();
    let (runtime, multiring) = setup(&pool);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    let fd = Arc::new(OwnedFd::from(socket));
    let mut buffer = pool.try_acquire().unwrap();
    buffer.spare_capacity_mut().fill(42);
    buffer.grow(SIZE);
    let mut send = Box::pin(multiring.send_zc_vectored(
        fd.clone(),
        vec![buffer],
        &[Segment {
            buf: 0,
            start: 0,
            len: SIZE,
        }],
    ));
    assert!(poll(send.as_mut()).is_pending());
    wait_for(|| multiring.in_flight() == 1);
    let reader = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut bytes = [0; 65536];
        while let Ok(n) = peer.read(&mut bytes) {
            if n == 0 {
                break;
            }
            assert!(bytes[..n].iter().all(|&b| b == 42));
        }
    });
    drop(runtime);
    let (result, reclaim) = finish(send.as_mut());
    if let Err(error) = result {
        assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
    }
    drop(finish(Box::pin(reclaim).as_mut()));
    reader.join().unwrap();
    assert_eq!(multiring.in_flight(), 0);
    assert_eq!(pool.available(), 1);
    assert_eq!(Arc::strong_count(&fd), 1);
}
