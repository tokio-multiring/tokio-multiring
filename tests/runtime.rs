//! What tokio itself gets from a runtime that a multiring drives: its own
//! sockets are watched by the rings, its workers sleep in them, and its
//! timers still fire.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_multiring::{BufferPool, Config, MultiRing};

/// A runtime with this many workers that a multiring drives, and the multiring.
fn runtime(workers: usize, config: Config) -> (tokio::runtime::Runtime, MultiRing) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(workers).enable_time();
    let multiring = MultiRing::install(&mut builder, config);
    (builder.build().unwrap(), multiring)
}

fn loopback() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Serves echo on tokio's sockets to `clients` connections, each of which
/// sends `rounds` messages and checks what comes back.
async fn echo(clients: usize, rounds: usize) {
    let listener = TcpListener::bind(loopback()).await.unwrap();
    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut connections = Vec::new();
        for _ in 0..clients {
            let (mut stream, _) = listener.accept().await.unwrap();
            connections.push(tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    let count = stream.read(&mut buf).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    stream.write_all(&buf[..count]).await.unwrap();
                }
            }));
        }
        for connection in connections {
            connection.await.unwrap();
        }
    });

    let clients: Vec<_> = (0..clients)
        .map(|client| {
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(address).await.unwrap();
                for round in 0..rounds {
                    let message = format!("client {client} round {round}");
                    stream.write_all(message.as_bytes()).await.unwrap();
                    let mut reply = vec![0u8; message.len()];
                    stream.read_exact(&mut reply).await.unwrap();
                    assert_eq!(reply, message.as_bytes());
                }
            })
        })
        .collect();
    for client in clients {
        client.await.unwrap();
    }
    server.await.unwrap();
}

#[test]
fn tokios_sockets_are_served() {
    let (runtime, _multiring) = runtime(4, Config::default());
    runtime.block_on(echo(32, 50));
}

#[test]
fn tokios_sockets_are_served_by_a_single_worker() {
    let (runtime, _multiring) = runtime(1, Config::default());
    runtime.block_on(echo(8, 20));
}

/// The listener is made and used by the `block_on` thread, which has no
/// ring, so a worker is asked to watch it. Dropping it there asks again.
#[test]
fn a_socket_of_a_thread_without_a_ring_is_watched() {
    let (runtime, _multiring) = runtime(2, Config::default());
    runtime.block_on(async {
        for _ in 0..8 {
            let listener = TcpListener::bind(loopback()).await.unwrap();
            let address = listener.local_addr().unwrap();
            let connecting = tokio::spawn(TcpStream::connect(address));
            let (mut accepted, _) = listener.accept().await.unwrap();
            let mut connected = connecting.await.unwrap().unwrap();

            connected.write_all(b"marco").await.unwrap();
            let mut heard = [0u8; 5];
            accepted.read_exact(&mut heard).await.unwrap();
            assert_eq!(&heard, b"marco");
        }
    });
}

/// A connection is watched by the ring of the worker that first used it.
/// Here another worker, or the `block_on` thread, ends up dropping it.
#[test]
fn a_socket_can_be_dropped_away_from_its_ring() {
    let (runtime, _multiring) = runtime(4, Config::default());
    runtime.block_on(async {
        let listener = TcpListener::bind(loopback()).await.unwrap();
        let address = listener.local_addr().unwrap();
        for _ in 0..64 {
            let connecting = tokio::spawn(async move {
                let mut stream = TcpStream::connect(address).await.unwrap();
                stream.write_all(b"x").await.unwrap();
                stream
            });
            let (mut accepted, _) = listener.accept().await.unwrap();
            let mut byte = [0u8; 1];
            accepted.read_exact(&mut byte).await.unwrap();
            let connected = connecting.await.unwrap();
            tokio::spawn(async move { drop(accepted) });
            drop(connected);
        }
    });
}

/// The same task uses a socket of tokio's and operations of the multiring,
/// which then share a ring.
#[test]
fn tokios_sockets_mix_with_operations() {
    let pool = BufferPool::new(4096, 2).unwrap();
    let config = Config {
        buffers: Some(pool.clone()),
        ..Config::default()
    };
    let (runtime, multiring) = runtime(2, config);
    runtime.block_on(async {
        let ours = tokio_multiring::TcpListener::bind(&multiring, loopback()).unwrap();
        let address = ours.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let stream = ours.accept().await.unwrap();
            let mut buf = pool.acquire().await;
            for _ in 0..100 {
                buf.clear();
                let (received, back) = stream.recv(buf, usize::MAX).await;
                assert_eq!(received.unwrap(), 4);
                let (sent, back) = stream.send(back, 0).await;
                assert_eq!(sent.unwrap(), 4);
                buf = back;
            }
        });
        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await.unwrap();
            for round in 0u32..100 {
                stream.write_all(&round.to_le_bytes()).await.unwrap();
                let mut reply = [0u8; 4];
                stream.read_exact(&mut reply).await.unwrap();
                assert_eq!(reply, round.to_le_bytes());
            }
        });
        client.await.unwrap();
        server.await.unwrap();
    });
}

/// Nothing but the timer can end these sleeps: no task is runnable and no
/// operation is in flight.
#[test]
fn a_timer_wakes_sleeping_workers() {
    for workers in [1, 4] {
        let (runtime, _multiring) = runtime(workers, Config::default());
        let started = Instant::now();
        runtime.block_on(async {
            tokio::spawn(tokio::time::sleep(Duration::from_millis(50)))
                .await
                .unwrap();
        });
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(50), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    }
}

/// The workers have long been asleep, with no timer to wake for, when the
/// `block_on` thread sets one. And then a sooner one.
#[test]
fn a_timer_set_while_the_workers_sleep_fires_on_time() {
    let (runtime, _multiring) = runtime(2, Config::default());
    std::thread::sleep(Duration::from_millis(50));

    let started = Instant::now();
    runtime.block_on(async {
        let late = tokio::time::sleep(Duration::from_millis(400));
        let soon = tokio::time::sleep(Duration::from_millis(40));
        tokio::pin!(late);
        tokio::select! {
            () = &mut late => panic!("the later timer fired first"),
            () = soon => {}
        }
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(40), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(300), "{elapsed:?}");
        late.await;
    });
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
}

/// Many timers at once, on workers that sleep between them.
#[test]
fn many_timers() {
    let (runtime, _multiring) = runtime(4, Config::default());
    let fired = Arc::new(AtomicUsize::new(0));
    runtime.block_on(async {
        let sleepers: Vec<_> = (0..200u64)
            .map(|index| {
                let fired = fired.clone();
                tokio::spawn(async move {
                    for _ in 0..5 {
                        tokio::time::sleep(Duration::from_millis(1 + index % 7)).await;
                    }
                    fired.fetch_add(1, Ordering::Relaxed);
                })
            })
            .collect();
        for sleeper in sleepers {
            tokio::time::timeout(Duration::from_secs(10), sleeper)
                .await
                .expect("a timer never fired")
                .unwrap();
        }
    });
    assert_eq!(fired.load(Ordering::Relaxed), 200);
}

/// A task handed in from outside the runtime runs, though every worker was
/// asleep in its ring.
#[test]
fn a_task_from_outside_wakes_a_worker() {
    let (runtime, _multiring) = runtime(2, Config::default());
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(2));
        let (tx, rx) = std::sync::mpsc::channel();
        runtime.spawn(async move { tx.send(()).unwrap() });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("no worker woke up for the task");
    }
}

/// A socket that outlives the runtime fails instead of waiting for a ring
/// that is gone.
#[test]
fn sockets_fail_once_the_runtime_is_gone() {
    let (runtime, _multiring) = runtime(2, Config::default());
    let (listener, stream, _peer) = runtime.block_on(async {
        let listener = TcpListener::bind(loopback()).await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        // Watched by now, and with nothing more to accept or to read.
        let accepting = tokio::time::timeout(Duration::from_millis(20), listener.accept());
        assert!(accepting.await.is_err());
        (listener, stream, peer)
    });
    drop(runtime);

    let other = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    other.block_on(async {
        let mut stream = stream;
        let accepted = tokio::time::timeout(Duration::from_secs(5), listener.accept());
        assert!(accepted.await.expect("the listener waits forever").is_err());
        let mut buf = [0u8; 8];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf));
        assert!(read.await.expect("the stream waits forever").is_err());
    });
}
