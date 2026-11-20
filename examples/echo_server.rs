//! TCP echo server: every byte received on a connection is sent back.
//!
//! The server side of an echo benchmark driven by an outside load generator.
//! Configured through the environment:
//!
//! - `ADDR`: listen address (default `127.0.0.1:7878`)
//! - `CPUS`: one worker per listed CPU, comma separated (default `0`)
//! - `BUF`: bytes per buffer (default 16384)
//! - `CONNS`: connections served at once (default 4096)
//! - `LISTEN`: `shared` has every worker accept from one listener,
//!   `per-worker` gives each worker a listener of its own on the address
//!   (default `shared`)
//! - `PLACE`: `steal` leaves a connection's task to tokio's scheduler,
//!   `local` keeps it on the worker that accepted it (default `steal`)
//! - `SEND`: `copy` sends from the buffer it received into and reuses it at
//!   once, `zero-copy` has the network stack send from the buffer itself and
//!   receives into a second one meanwhile (default `copy`)

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_sidecar::{
    BufferPool, Config, Domain, Plan, Reclaim, Segment, Sidecar, TcpListener, TcpStream,
};

#[derive(Clone, Copy)]
struct Mode {
    local: bool,
    zero_copy: bool,
}

fn var(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// Which of two settings the variable `name` holds, `false` for the first.
fn choice(name: &str, first: &str, second: &str) -> bool {
    let value = var(name, first);
    if value == first {
        false
    } else if value == second {
        true
    } else {
        panic!("{name} is {first} or {second}, not {value}")
    }
}

async fn echo(stream: TcpStream, pool: BufferPool) {
    let mut buf = pool.acquire().await;
    loop {
        buf.clear();
        let limit = buf.capacity();
        let (received, back) = stream.recv(buf, limit).await;
        buf = back;
        if !matches!(received, Ok(count) if count > 0) {
            return;
        }
        let (sent, back) = stream.send(buf, 0).await;
        buf = back;
        if sent.is_err() {
            return;
        }
    }
}

/// Echoes with two buffers. The kernel keeps the one a reply went out from
/// until the client has acknowledged it, so the next request is received
/// into the other, and each is taken back just before it is needed again.
async fn echo_zero_copy(stream: TcpStream, pool: BufferPool) {
    let mut bufs = pool.acquire_many(2).await;
    let mut spare = bufs.pop();
    let mut buf = bufs.pop().expect("two buffers were acquired");
    let mut in_flight: Option<Reclaim> = None;
    loop {
        buf.clear();
        let limit = buf.capacity();
        let (received, back) = stream.recv(buf, limit).await;
        let len = match received {
            Ok(count) if count > 0 => count,
            _ => return,
        };
        let whole = [Segment {
            buf: 0,
            start: 0,
            len,
        }];
        let (sent, reclaim) = stream.send_zc_vectored(vec![back], &whole).await;
        if sent.is_err() {
            return;
        }
        buf = match (spare.take(), in_flight.take()) {
            (Some(spare), _) => spare,
            (None, Some(earlier)) => match earlier.await.bufs.pop() {
                Some(buf) => buf,
                None => return,
            },
            (None, None) => unreachable!("one buffer is always spare or in flight"),
        };
        in_flight = Some(reclaim);
    }
}

async fn accept(listener: Arc<TcpListener>, pool: BufferPool, mode: Mode) {
    loop {
        let Ok(stream) = listener.accept().await else {
            continue;
        };
        stream.set_nodelay(true).ok();
        match (mode.zero_copy, mode.local) {
            (false, false) => drop(tokio::spawn(echo(stream, pool.clone()))),
            (false, true) => drop(tokio::task::spawn_worker_local(echo(stream, pool.clone()))),
            (true, false) => drop(tokio::spawn(echo_zero_copy(stream, pool.clone()))),
            (true, true) => drop(tokio::task::spawn_worker_local(echo_zero_copy(
                stream,
                pool.clone(),
            ))),
        }
    }
}

fn main() -> std::io::Result<()> {
    let address: SocketAddr = var("ADDR", "127.0.0.1:7878")
        .parse()
        .expect("ADDR is an address and a port");
    let cpus: Vec<u32> = var("CPUS", "0")
        .split(',')
        .map(|cpu| cpu.trim().parse().expect("CPUS is a list of CPU numbers"))
        .collect();
    let buf_size: usize = var("BUF", "16384").parse().expect("BUF is a byte count");
    let connections: usize = var("CONNS", "4096").parse().expect("CONNS is a count");
    let per_worker = choice("LISTEN", "shared", "per-worker");
    let mode = Mode {
        local: choice("PLACE", "steal", "local"),
        zero_copy: choice("SEND", "copy", "zero-copy"),
    };

    let plan = Plan::new(vec![Domain {
        workers: cpus,
        sq_thread: None,
    }])?;
    let per_connection = if mode.zero_copy { 2 } else { 1 };
    let pool = BufferPool::without_huge_pages(buf_size, connections * per_connection)?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let config = Config {
        sq_entries: 1024,
        buffers: Some(pool.clone()),
        ..Config::default()
    };
    let sidecar = Sidecar::install(&mut builder, config);
    let runtime = plan.build(&mut builder)?;

    runtime.block_on(async {
        if per_worker {
            // Bound here once so that an address that cannot be had is an
            // error from `main`, and kept as the first worker's listener.
            let first = Arc::new(TcpListener::bind_reuse_port(&sidecar, address)?);
            tokio::task::run_on_each_worker(move |worker| {
                let listener = match worker {
                    0 => first.clone(),
                    _ => Arc::new(
                        TcpListener::bind_reuse_port(&sidecar, address)
                            .expect("the address was bound a moment ago"),
                    ),
                };
                tokio::task::spawn_worker_local(accept(listener, pool.clone(), mode));
            });
        } else {
            let listener = Arc::new(TcpListener::bind(&sidecar, address)?);
            tokio::task::run_on_each_worker(move |_| {
                tokio::spawn(accept(listener.clone(), pool.clone(), mode));
            });
        }
        std::future::pending().await
    })
}
