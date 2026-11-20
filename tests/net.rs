use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::os::fd::{AsFd, AsRawFd};

use tokio_sidecar::{BufferPool, Config, Segment, Sidecar, TcpListener, TcpStream};

/// A runtime of four workers that a sidecar drives, and the sidecar.
fn runtime(config: Config) -> (tokio::runtime::Runtime, Sidecar) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(4).enable_time();
    let sidecar = Sidecar::install(&mut builder, config);
    (builder.build().unwrap(), sidecar)
}

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

fn is_nonblocking(socket: &impl AsFd) -> bool {
    // SAFETY: the descriptor is open for the length of the call.
    let flags = unsafe { libc::fcntl(socket.as_fd().as_raw_fd(), libc::F_GETFL) };
    assert_ne!(flags, -1);
    flags & libc::O_NONBLOCK != 0
}

#[test]
fn a_stream_echoes_what_its_listener_accepted() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let pool = BufferPool::new(4096, 1).unwrap();
        let listener = TcpListener::bind(&sidecar, loopback()).unwrap();
        let address = listener.local_addr().unwrap();

        let client = std::thread::spawn(move || {
            let mut stream = std::net::TcpStream::connect(address).unwrap();
            stream.write_all(b"marco").unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).unwrap();
            (stream.local_addr().unwrap(), reply)
        });

        let stream = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        assert_eq!(stream.local_addr().unwrap(), address);
        let peer = stream.peer_addr().unwrap();

        let mut buf = pool.acquire().await;
        while buf.len() < 5 {
            let (received, back) = stream.recv(buf, usize::MAX).await;
            assert_ne!(received.unwrap(), 0);
            buf = back;
        }
        let (sent, _buf) = stream.send(buf, 0).await;
        assert_eq!(sent.unwrap(), 5);
        stream.shutdown(Shutdown::Write).await.unwrap();

        let (client_addr, reply) = client.join().unwrap();
        assert_eq!(reply, b"marco");
        assert_eq!(peer, client_addr);
    });
}

#[test]
fn clones_of_a_stream_send_and_receive_at_once() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let pool = BufferPool::new(4096, 2).unwrap();
        let listener = TcpListener::bind(&sidecar, loopback()).unwrap();
        let mut theirs = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let ours = listener.accept().await.unwrap();

        let mut bufs = pool.acquire_many(2).await;
        let (inbound, mut outbound) = (bufs.pop().unwrap(), bufs.pop().unwrap());
        outbound.extend_from_slice(b"polo");

        // The receive is under way, with nothing to receive yet, when the
        // send starts.
        let receiver = ours.clone();
        let receiving = tokio::spawn(async move { receiver.recv(inbound, usize::MAX).await });
        let segments = [Segment {
            buf: 0,
            start: 0,
            len: 4,
        }];
        let (sent, _bufs) = ours.send_vectored(vec![outbound], &segments).await;
        assert_eq!(sent.unwrap(), 4);

        let mut reply = [0; 4];
        theirs.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"polo");
        theirs.write_all(b"marco").unwrap();

        let (received, inbound) = receiving.await.unwrap();
        assert_eq!(received.unwrap(), 5);
        assert_eq!(&inbound[..], b"marco");
    });
}

#[test]
fn listeners_sharing_a_port_all_take_connections() {
    const LISTENERS: usize = 4;
    const CONNECTIONS: usize = 64;

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let first = TcpListener::bind_reuse_port(&sidecar, loopback()).unwrap();
        let address = first.local_addr().unwrap();
        let mut listeners = vec![first];
        for _ in 1..LISTENERS {
            listeners.push(TcpListener::bind_reuse_port(&sidecar, address).unwrap());
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        for (index, listener) in listeners.into_iter().enumerate() {
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    let stream = listener.accept().await.unwrap();
                    assert_eq!(stream.local_addr().unwrap(), address);
                    tx.send((index, stream.peer_addr().unwrap())).unwrap();
                }
            });
        }

        let clients: Vec<_> = (0..CONNECTIONS)
            .map(|_| std::net::TcpStream::connect(address).unwrap())
            .collect();
        let mut expected: HashSet<_> = clients
            .iter()
            .map(|client| client.local_addr().unwrap())
            .collect();

        let mut took = HashSet::new();
        for _ in 0..CONNECTIONS {
            let (index, peer) = rx.recv().await.unwrap();
            assert!(expected.remove(&peer), "{peer} was accepted twice");
            took.insert(index);
        }
        // The kernel picks a listener by a hash of the client's address.
        // That all 64 land on one of four would take odds of 4^-63.
        assert!(took.len() > 1, "one listener took every connection");
    });
}

#[test]
fn a_port_in_use_is_refused_without_reuse_port() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let first = TcpListener::bind(&sidecar, loopback()).unwrap();
        let address = first.local_addr().unwrap();

        let second = TcpListener::bind(&sidecar, address).map(drop);
        assert_eq!(second.unwrap_err().kind(), std::io::ErrorKind::AddrInUse);
        let shared = TcpListener::bind_reuse_port(&sidecar, address).map(drop);
        assert_eq!(shared.unwrap_err().kind(), std::io::ErrorKind::AddrInUse);
    });
}

#[test]
fn sockets_from_elsewhere_are_put_in_blocking_mode() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let pool = BufferPool::new(4096, 1).unwrap();

        let listener = std::net::TcpListener::bind(loopback()).unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(&sidecar, listener).unwrap();
        assert!(!is_nonblocking(&listener));

        let connecting = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        connecting.set_nonblocking(true).unwrap();
        let ours = TcpStream::from_std(&sidecar, connecting).unwrap();
        assert!(!is_nonblocking(&ours));

        // On a non-blocking socket this receive would fail with `WouldBlock`
        // instead of waiting for the peer.
        let accepted = listener.accept().await.unwrap();
        assert!(!is_nonblocking(&accepted));
        let receiving = tokio::spawn({
            let ours = ours.clone();
            let buf = pool.acquire().await;
            async move { ours.recv(buf, usize::MAX).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!receiving.is_finished());

        let mut buf = BufferPool::new(4096, 1).unwrap().acquire().await;
        buf.extend_from_slice(b"late");
        let (sent, _buf) = accepted.send(buf, 0).await;
        assert_eq!(sent.unwrap(), 4);
        let (received, buf) = receiving.await.unwrap();
        assert_eq!(received.unwrap(), 4);
        assert_eq!(&buf[..], b"late");
    });
}
