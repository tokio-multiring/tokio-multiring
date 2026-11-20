//! TCP echo server: every byte received on a connection is sent back.
//!
//! The server side of an echo benchmark driven by an outside load generator.
//! Configured through the environment:
//!
//! - `ADDR`: listen address (default `127.0.0.1:7878`)
//! - `CPUS`: one worker per listed CPU, comma separated (default `0`)
//! - `BUF`: bytes per connection buffer (default 16384)
//! - `CONNS`: connections served at once (default 4096)
//! - `PLACE`: `steal` leaves a connection's task to tokio's scheduler,
//!   `local` keeps it on the worker that accepted it (default `steal`)

use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::sync::Arc;

use tokio_sidecar::{BufferPool, Config, Domain, Plan, Sidecar};

fn var(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

async fn echo(sidecar: Sidecar, socket: OwnedFd, pool: BufferPool) {
    let socket = Arc::new(socket);
    let mut buf = pool.acquire().await;
    loop {
        buf.clear();
        let limit = buf.capacity();
        let (received, back) = sidecar.recv(socket.clone(), buf, limit).await;
        buf = back;
        if !matches!(received, Ok(count) if count > 0) {
            return;
        }
        let (sent, back) = sidecar.send(socket.clone(), buf, 0).await;
        buf = back;
        if sent.is_err() {
            return;
        }
    }
}

async fn accept(sidecar: Sidecar, listener: Arc<OwnedFd>, pool: BufferPool, local: bool) {
    loop {
        let Ok(socket) = sidecar.accept(listener.clone()).await else {
            continue;
        };
        let socket = TcpStream::from(socket);
        socket.set_nodelay(true).ok();
        let connection = echo(sidecar.clone(), OwnedFd::from(socket), pool.clone());
        if local {
            tokio::task::spawn_worker_local(connection);
        } else {
            tokio::spawn(connection);
        }
    }
}

fn main() -> std::io::Result<()> {
    let cpus: Vec<u32> = var("CPUS", "0")
        .split(',')
        .map(|cpu| cpu.trim().parse().expect("CPUS is a list of CPU numbers"))
        .collect();
    let buf_size: usize = var("BUF", "16384").parse().expect("BUF is a byte count");
    let connections: usize = var("CONNS", "4096").parse().expect("CONNS is a count");
    let local = match var("PLACE", "steal").as_str() {
        "steal" => false,
        "local" => true,
        other => panic!("PLACE is steal or local, not {other}"),
    };

    let plan = Plan::new(vec![Domain {
        workers: cpus,
        sq_thread: None,
    }])?;
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal);
    let runtime = plan.build(&mut builder)?;

    let listener = TcpListener::bind(var("ADDR", "127.0.0.1:7878"))?;
    let pool = BufferPool::without_huge_pages(buf_size, connections)?;

    runtime.block_on(async {
        let sidecar = Sidecar::start(Config {
            sq_entries: 1024,
            buffers: Some(pool.clone()),
            ..Config::default()
        })
        .await?;
        let listener = Arc::new(OwnedFd::from(listener));
        tokio::task::run_on_each_worker(move |_| {
            tokio::spawn(accept(
                sidecar.clone(),
                listener.clone(),
                pool.clone(),
                local,
            ));
        });
        std::future::pending().await
    })
}
