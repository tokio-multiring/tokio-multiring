//! An S3-style object server.
//!
//! Buckets are directories and objects are files in them. HTTP is spoken
//! with ureq-proto's server state machine, and all I/O goes through a
//! [`tokio_sidecar::Sidecar`]: an object's bytes travel between the socket
//! and the device in the sidecar's registered buffers, without being copied
//! on the way.
//!
//! What it serves, in path style (`/bucket/key`):
//!
//! | Request | |
//! |---|---|
//! | `PUT /bucket` | create a bucket |
//! | `HEAD /bucket` | check that a bucket exists |
//! | `DELETE /bucket` | remove an empty bucket |
//! | `PUT /bucket/key` | store an object |
//! | `GET /bucket/key` | fetch an object, or one byte range of it |
//! | `HEAD /bucket/key` | look an object up |
//! | `DELETE /bucket/key` | remove an object |
//!
//! Requests are not authenticated, and nothing else of S3 is there:
//! listings, multipart uploads, object metadata and checksums are answered
//! with `NotImplemented` or ignored.

mod api;
mod conn;
mod date;
mod error;
mod name;
mod store;

use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_sidecar::{BufferPool, Sidecar};

use crate::conn::Conn;
use crate::store::Store;

/// Settings of a [`Server`].
#[derive(Clone, Debug)]
pub struct Config {
    /// The directory the buckets live in. It must exist, on a file system
    /// that supports `O_DIRECT`.
    pub root: PathBuf,
    /// How many connections are served at a time. Further ones wait to be
    /// accepted.
    pub connections: usize,
    /// Size of the two buffers each connection holds for requests and
    /// response headers: a multiple of 4096. A request's headers must fit
    /// in one.
    pub head_buffer: usize,
}

impl Config {
    pub fn new(root: impl Into<PathBuf>) -> Config {
        Config {
            root: root.into(),
            connections: 1024,
            head_buffer: 16 * 1024,
        }
    }
}

struct Inner {
    sidecar: Sidecar,
    store: Store,
    /// The sidecar's registered buffers, which object data moves through.
    bodies: BufferPool,
    heads: BufferPool,
    /// One permit per connection that can be served. A permit stands for
    /// two buffers of `heads`.
    slots: Arc<Semaphore>,
}

/// The server. Cheap to clone.
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

impl Server {
    /// Sets the server up over `sidecar`, which must have a buffer pool.
    /// Objects are read and written through the sidecar's polled disk rings
    /// if it has them.
    pub fn new(sidecar: Sidecar, config: Config) -> io::Result<Server> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidInput, message);
        let bodies = sidecar
            .buffers()
            .cloned()
            .ok_or_else(|| invalid("the sidecar has no buffer pool"))?;
        if config.connections == 0 {
            return Err(invalid("a server needs room for a connection"));
        }
        let heads = BufferPool::new(config.head_buffer, 2 * config.connections)?;
        let store = Store::open(sidecar.clone(), &config.root)?;
        Ok(Server {
            inner: Arc::new(Inner {
                sidecar,
                store,
                bodies,
                heads,
                slots: Arc::new(Semaphore::new(config.connections)),
            }),
        })
    }

    /// Starts serving the connections that arrive on `listener`, on every
    /// worker of the current runtime, until the runtime shuts down.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a multi-threaded runtime.
    pub fn listen(&self, listener: TcpListener) {
        let listener = Arc::new(OwnedFd::from(listener));
        let inner = self.inner.clone();
        tokio::task::run_on_each_worker(move |_| {
            tokio::spawn(accept(inner, listener));
        });
    }
}

/// Accepts connections on one worker. A connection's task starts on the
/// worker that accepted it.
async fn accept(inner: Arc<Inner>, listener: Arc<OwnedFd>) {
    loop {
        let slot = inner.slots.clone().acquire_owned().await;
        let slot = slot.expect("the semaphore is never closed");
        let socket = match inner.sidecar.accept(listener.clone()).await {
            Ok(socket) => socket,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                return log::error!("cannot accept on the listener: {error}");
            }
            Err(error) => {
                // Out of descriptors or memory, or a client that gave up
                // while it was queued.
                log::warn!("accept failed: {error}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };

        // Responses go out as a header and then data. Waiting to coalesce
        // them would only add delay.
        let socket = TcpStream::from(socket);
        if let Err(error) = socket.set_nodelay(true) {
            log::debug!("could not disable Nagle's algorithm: {error}");
        }

        let buffer = || {
            inner
                .heads
                .try_acquire()
                .expect("a slot stands for two head buffers")
        };
        let conn = Conn::new(inner.sidecar.clone(), socket.into(), buffer(), buffer());
        let inner = inner.clone();
        tokio::spawn(async move {
            api::serve(&inner, conn).await;
            drop(slot);
        });
    }
}
