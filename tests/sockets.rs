use std::future::Future;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, Sidecar};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal)
        .build()
        .unwrap()
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

/// After shutting down writing, the peer sees the end of the stream, and
/// what it sends still arrives.
#[test]
fn shutdown_of_one_direction() {
    let (ours, mut theirs) = socket_pair();
    let pool = BufferPool::new(4096, 1).unwrap();

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
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
