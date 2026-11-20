use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, Segment, Sidecar};

/// A runtime of four workers that a sidecar drives, and the sidecar.
fn runtime(config: Config) -> (tokio::runtime::Runtime, Sidecar) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(4).enable_time();
    let sidecar = Sidecar::install(&mut builder, config);
    (builder.build().unwrap(), sidecar)
}

/// A connected pair of TCP sockets, ours first.
fn tcp_pair() -> (Arc<OwnedFd>, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let ours = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (theirs, _) = listener.accept().unwrap();
    (Arc::new(OwnedFd::from(ours)), theirs)
}

fn socket_pair() -> (Arc<OwnedFd>, UnixStream) {
    let (ours, theirs) = UnixStream::pair().unwrap();
    (Arc::new(OwnedFd::from(ours)), theirs)
}

#[test]
fn tcp_echo() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let listener = Arc::new(OwnedFd::from(listener));
    let pool = BufferPool::new(4096, 2).unwrap();

    let client = std::thread::spawn(move || {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(b"marco").unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).unwrap();
        reply
    });

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        tokio::spawn(async move {
            let stream = Arc::new(sidecar.accept(listener).await.unwrap());

            let mut request = pool.acquire().await;
            while request.len() < 5 {
                let (received, buf) = sidecar.recv(stream.clone(), request, usize::MAX).await;
                assert_ne!(received.unwrap(), 0);
                request = buf;
            }
            assert_eq!(&request[..], b"marco");

            let mut reply = pool.acquire().await;
            reply.extend_from_slice(b"polo!");
            let (sent, _) = sidecar.send(stream, reply, 0).await;
            assert_eq!(sent.unwrap(), 5);
        })
        .await
        .unwrap();
    });
    assert_eq!(&client.join().unwrap(), b"polo!");
}

/// Far more than a socket buffer holds, to a reader that takes its time.
#[test]
fn send_delivers_the_whole_buffer() {
    const SIZE: usize = 1 << 20;

    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(SIZE, 1).unwrap();

    let reader = std::thread::spawn(move || {
        let mut received = Vec::new();
        let mut chunk = [0; 16 * 1024];
        loop {
            std::thread::sleep(Duration::from_micros(200));
            match theirs.read(&mut chunk).unwrap() {
                0 => return received,
                read => received.extend_from_slice(&chunk[..read]),
            }
        }
    });

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let mut buf = pool.acquire().await;
        let pattern: Vec<u8> = (0..SIZE).map(|index| (index % 251) as u8).collect();
        buf.extend_from_slice(&pattern);
        let (sent, buf) = sidecar.send(ours, buf, 0).await;
        assert_eq!(sent.unwrap(), SIZE);
        drop(buf);
    });

    let received = reader.join().unwrap();
    assert_eq!(received.len(), SIZE);
    assert!(
        received
            .iter()
            .enumerate()
            .all(|(index, &byte)| byte == (index % 251) as u8)
    );
}

#[test]
fn recv_reports_the_end_of_the_stream() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();
    theirs.write_all(b"last words").unwrap();
    drop(theirs);

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let (received, buf) = sidecar
            .recv(ours.clone(), pool.acquire().await, usize::MAX)
            .await;
        assert_eq!(received.unwrap(), 10);
        let (received, buf) = sidecar.recv(ours, buf, usize::MAX).await;
        assert_eq!(received.unwrap(), 0);
        assert_eq!(&buf[..], b"last words");
    });
}

/// A receive takes no more than its limit, leaving the rest for the next
/// one, and a send can start part of the way into its buffer.
#[test]
fn recv_limit_and_send_offset() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();
    theirs.write_all(b"headbody").unwrap();

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let (received, buf) = sidecar.recv(ours.clone(), pool.acquire().await, 4).await;
        assert_eq!(received.unwrap(), 4);
        assert_eq!(&buf[..], b"head");
        let (received, buf) = sidecar.recv(ours.clone(), buf, usize::MAX).await;
        assert_eq!(received.unwrap(), 4);
        assert_eq!(&buf[..], b"headbody");

        let (sent, buf) = sidecar.send(ours.clone(), buf, 4).await;
        assert_eq!(sent.unwrap(), 4);
        let (sent, _) = sidecar.send(ours, buf, 8).await;
        assert_eq!(sent.unwrap(), 0);
    });

    let mut echoed = [0; 4];
    theirs.read_exact(&mut echoed).unwrap();
    assert_eq!(&echoed, b"body");
}

fn segment(buf: usize, start: usize, len: usize) -> Segment {
    Segment { buf, start, len }
}

/// Regions of two buffers go out interleaved, as one stream.
#[test]
fn send_vectored_interleaves_buffers() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 2).unwrap();

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let mut bufs = pool.acquire_many(2).await;
        bufs[0].extend_from_slice(b"ACExx");
        bufs[1].extend_from_slice(b"..BD");
        let segments = [
            segment(0, 0, 1),
            segment(1, 2, 1),
            segment(0, 1, 1),
            segment(1, 3, 1),
            segment(0, 2, 1),
        ];
        let (sent, bufs) = sidecar.send_vectored(ours.clone(), bufs, &segments).await;
        assert_eq!(sent.unwrap(), 5);
        assert_eq!((&bufs[0][..], &bufs[1][..]), (&b"ACExx"[..], &b"..BD"[..]));

        let (sent, _) = sidecar.send_vectored(ours, bufs, &[]).await;
        assert_eq!(sent.unwrap(), 0);
    });

    let mut received = [0; 5];
    theirs.read_exact(&mut received).unwrap();
    assert_eq!(&received, b"ABCDE");
}

/// A stream is dealt out over regions of two buffers, and the receive holds
/// out for all of it though it arrives in pieces.
#[test]
fn recv_vectored_fills_every_region() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 2).unwrap();

    let writer = std::thread::spawn(move || {
        for piece in [&b"01"[..], b"234", b"5", b"6789"] {
            theirs.write_all(piece).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        theirs
    });

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let bufs = pool.acquire_many(2).await;
        let segments = [segment(0, 0, 3), segment(1, 100, 4), segment(0, 3, 3)];
        let (received, mut bufs) = sidecar.recv_vectored(ours.clone(), bufs, &segments).await;
        assert_eq!(received.unwrap(), 10);
        assert!(bufs[0].is_empty() && bufs[1].is_empty());
        bufs[0].grow(6);
        bufs[1].grow(104);
        assert_eq!(&bufs[0][..], b"012789");
        assert_eq!(&bufs[1][100..], b"3456");

        // The peer stops short of what is asked for.
        let mut theirs = writer.join().unwrap();
        theirs.write_all(b"ab").unwrap();
        drop(theirs);
        let (received, bufs) = sidecar.recv_vectored(ours, bufs, &[segment(1, 0, 8)]).await;
        assert_eq!(received.unwrap(), 2);
        assert_eq!(&bufs[1][..2], b"ab");
    });
}

/// More than a socket buffer holds, through many regions.
#[test]
fn vectored_transfers_larger_than_the_socket_buffer() {
    const UNIT: usize = 128 * 1024;
    const UNITS: usize = 8;

    let (ours, theirs) = socket_pair();
    let theirs = Arc::new(OwnedFd::from(theirs));
    let pool = BufferPool::new(UNITS * UNIT, 4).unwrap();
    // Units alternate between the two buffers.
    let segments: Vec<Segment> = (0..2 * UNITS)
        .map(|unit| segment(unit % 2, unit / 2 * UNIT, UNIT))
        .collect();

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let mut sending = pool.acquire_many(2).await;
        for (index, buf) in sending.iter_mut().enumerate() {
            let fill: Vec<u8> = (0..UNITS * UNIT)
                .map(|byte| (index + byte / UNIT * 2) as u8)
                .collect();
            buf.extend_from_slice(&fill);
        }
        let receiving = pool.acquire_many(2).await;

        let send = tokio::spawn(sidecar.send_vectored(ours, sending, &segments));
        let (received, mut receiving) = sidecar.recv_vectored(theirs, receiving, &segments).await;
        assert_eq!(received.unwrap(), 2 * UNITS * UNIT);
        let (sent, sending) = send.await.unwrap();
        assert_eq!(sent.unwrap(), 2 * UNITS * UNIT);

        for (sent, received) in sending.iter().zip(&mut receiving) {
            received.grow(UNITS * UNIT);
            assert!(sent[..] == received[..]);
        }
    });
}

/// A pool of two buffers of `size` bytes, on huge pages or not. The two
/// kinds are sent in different ways: see `Sidecar::send_zc_vectored`.
fn pool_of(size: usize, huge_pages: bool) -> BufferPool {
    match huge_pages {
        true => BufferPool::new(size, 2).unwrap(),
        false => BufferPool::without_huge_pages(size, 2).unwrap(),
    }
}

/// Settings for a sidecar with `pool` registered on its rings.
fn config_with(pool: &BufferPool) -> Config {
    Config {
        buffers: Some(pool.clone()),
        ..Config::default()
    }
}

fn zero_copy_send(huge_pages: bool) {
    const UNIT: usize = 128 * 1024;
    const UNITS: usize = 8;

    let (ours, mut theirs) = tcp_pair();
    let pool = pool_of(UNITS * UNIT, huge_pages);
    // Units alternate between the two buffers, and the first is cut short.
    let mut segments: Vec<Segment> = (0..2 * UNITS)
        .map(|unit| segment(unit % 2, unit / 2 * UNIT, UNIT))
        .collect();
    segments[0] = segment(0, 5, UNIT - 5);
    let total = 2 * UNITS * UNIT - 5;

    let reader = std::thread::spawn(move || {
        let mut received = vec![0; total];
        theirs.read_exact(&mut received).unwrap();
        received
    });

    let (runtime, sidecar) = runtime(config_with(&pool));
    runtime.block_on(async {
        let mut bufs = pool.acquire_many(2).await;
        for (index, buf) in bufs.iter_mut().enumerate() {
            let fill: Vec<u8> = (0..UNITS * UNIT)
                .map(|byte| (index + byte / UNIT * 2 + byte % 7) as u8)
                .collect();
            buf.extend_from_slice(&fill);
        }
        let expected: Vec<u8> = segments
            .iter()
            .flat_map(|segment| bufs[segment.buf][segment.start..][..segment.len].to_vec())
            .collect();

        let (sent, reclaim) = sidecar.send_zc_vectored(ours, bufs, &segments).await;
        assert_eq!(sent.unwrap(), total);
        // The kernel still has the buffers, or has just handed them over:
        // either way they are not in the pool.
        assert_eq!(pool.available(), 0);

        let reclaimed = tokio::time::timeout(Duration::from_secs(5), reclaim)
            .await
            .expect("the kernel never let go of the buffers");
        assert_eq!(reclaimed.bufs.len(), 2);
        assert_eq!(reclaimed.bufs[0].len(), UNITS * UNIT);
        // Loopback delivers by copying.
        assert!(reclaimed.copied);
        assert!(reader.join().unwrap() == expected);

        drop(reclaimed);
        assert_eq!(pool.available(), 2);
        assert_eq!(sidecar.in_flight(), 0);
    });
}

#[test]
fn zero_copy_send_from_registered_buffers() {
    zero_copy_send(false);
}

#[test]
fn zero_copy_send_from_huge_pages() {
    zero_copy_send(true);
}

/// Nobody has to wait for the buffers: they find their own way back.
#[test]
fn unclaimed_zero_copy_buffers_return_to_the_pool() {
    let (ours, mut theirs) = tcp_pair();
    let pool = pool_of(4096, false);

    let (runtime, sidecar) = runtime(config_with(&pool));
    runtime.block_on(async {
        let mut bufs = pool.acquire_many(2).await;
        bufs[0].extend_from_slice(b"zero ");
        bufs[1].extend_from_slice(b"copy");
        let segments = [segment(0, 0, 5), segment(1, 0, 4)];
        let (sent, reclaim) = sidecar.send_zc_vectored(ours, bufs, &segments).await;
        assert_eq!(sent.unwrap(), 9);
        drop(reclaim);

        let mut received = [0; 9];
        theirs.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"zero copy");
        tokio::time::timeout(Duration::from_secs(5), pool.acquire_many(2))
            .await
            .expect("buffers never came back");
        assert_eq!(sidecar.in_flight(), 0);
    });
}

/// With the peer not reading, what was sent stays the kernel's, and so do
/// the buffers it is in. Sending only from buffers that have come back is
/// what keeps a sender from running ahead of its peer.
#[test]
fn zero_copy_buffers_are_held_while_the_peer_does_not_read() {
    const SIZE: usize = 1 << 20;

    let (ours, mut theirs) = tcp_pair();
    let pool = pool_of(SIZE, false);

    let (runtime, sidecar) = runtime(config_with(&pool));
    runtime.block_on(async {
        let mut bufs = pool.acquire_many(1).await;
        bufs[0].extend_from_slice(&vec![7; SIZE]);
        let whole = [segment(0, 0, SIZE)];
        let (sent, mut reclaim) = sidecar.send_zc_vectored(ours, bufs, &whole).await;
        assert_eq!(sent.unwrap(), SIZE);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(reclaim.try_take().is_none());
        assert_eq!(pool.available(), 1);
        assert_eq!(sidecar.in_flight(), 1);

        let reader = std::thread::spawn(move || {
            let mut received = vec![0; SIZE];
            theirs.read_exact(&mut received).unwrap();
            received
        });
        let reclaimed = tokio::time::timeout(Duration::from_secs(5), reclaim)
            .await
            .expect("the kernel never let go of the buffer");
        assert_eq!(reclaimed.bufs.len(), 1);
        assert!(reader.join().unwrap().iter().all(|&byte| byte == 7));
    });
}

/// The future is dropped while the send is stuck behind a peer that is not
/// reading. The send is cancelled, and the buffers come back once the
/// kernel has dropped what it had queued.
#[test]
fn dropped_zero_copy_send_returns_its_buffers() {
    const SIZE: usize = 8 << 20;

    let (ours, theirs) = tcp_pair();
    let pool = pool_of(SIZE, false);

    let (runtime, sidecar) = runtime(config_with(&pool));
    runtime.block_on(async {
        let mut bufs = pool.acquire_many(2).await;
        for buf in &mut bufs {
            buf.extend_from_slice(&vec![1; SIZE]);
        }
        // More than the socket takes with nobody reading.
        let segments = [segment(0, 0, SIZE), segment(1, 0, SIZE)];
        let send = sidecar.send_zc_vectored(ours, bufs, &segments);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), send)
                .await
                .is_err()
        );
        assert_eq!(pool.available(), 0);

        // The connection goes away, and with it what was queued on it.
        drop(theirs);
        tokio::time::timeout(Duration::from_secs(5), pool.acquire_many(2))
            .await
            .expect("buffers never came back");
        assert_eq!(sidecar.in_flight(), 0);
    });
}

/// A send that fails still accounts for its buffers.
#[test]
fn failed_zero_copy_send_hands_its_buffers_back() {
    let (ours, theirs) = tcp_pair();
    let pool = pool_of(4096, false);
    // Reset the connection: a later send finds it broken.
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    let set = unsafe {
        libc::setsockopt(
            theirs.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::from_ref(&linger).cast(),
            size_of::<libc::linger>() as u32,
        )
    };
    assert_eq!(set, 0);
    drop(theirs);

    let (runtime, sidecar) = runtime(config_with(&pool));
    runtime.block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut bufs = pool.acquire_many(1).await;
        bufs[0].extend_from_slice(b"anyone?");
        let (sent, reclaim) = sidecar
            .send_zc_vectored(ours, bufs, &[segment(0, 0, 7)])
            .await;
        assert!(sent.is_err());
        let reclaimed = tokio::time::timeout(Duration::from_secs(5), reclaim)
            .await
            .expect("the buffer never came back");
        assert_eq!(&reclaimed.bufs[0][..], b"anyone?");
        assert_eq!(sidecar.in_flight(), 0);
    });
}

/// After shutting down writing, the peer sees the end of the stream, and
/// what it sends still arrives.
#[test]
fn shutdown_of_one_direction() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        sidecar
            .shutdown(ours.clone(), std::net::Shutdown::Write)
            .await
            .unwrap();
        assert_eq!(theirs.read(&mut [0; 8]).unwrap(), 0);

        theirs.write_all(b"still open").unwrap();
        let (received, buf) = sidecar.recv(ours, pool.acquire().await, usize::MAX).await;
        assert_eq!(received.unwrap(), 10);
        assert_eq!(&buf[..], b"still open");
    });
}

/// Without `MSG_NOSIGNAL` this would kill the test process.
#[test]
fn send_to_a_closed_peer_is_an_error() {
    let (ours, theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();
    drop(theirs);

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let mut buf = pool.acquire().await;
        buf.extend_from_slice(b"anyone?");
        let (sent, buf) = sidecar.send(ours, buf, 0).await;
        assert_eq!(sent.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(&buf[..], b"anyone?");
    });
}

/// The accept completes, and its future is dropped without ever being
/// polled again. The client sees its connection closed, which it would not
/// if the accepted descriptor had been left open.
#[test]
fn dropped_accept_closes_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let listener = Arc::new(OwnedFd::from(listener));

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let (parked, accept) = tokio::sync::oneshot::channel();
        tokio::spawn({
            let sidecar = sidecar.clone();
            async move {
                let mut accept = Box::pin(sidecar.accept(listener));
                std::future::poll_fn(|cx| {
                    assert!(accept.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert!(parked.send(accept).is_ok());
            }
        });
        let accept = accept.await.unwrap();

        let mut client = TcpStream::connect(address).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while sidecar.in_flight() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("accept never completed");
        drop(accept);

        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
    });
}

#[test]
fn dropped_recv_is_cancelled() {
    let (ours, _theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let recv = sidecar.recv(ours, pool.acquire().await, usize::MAX);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), recv)
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(5), pool.acquire())
            .await
            .expect("buffer never came back");
    });
}
