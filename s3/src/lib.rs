//! An S3-style object server.
//!
//! Buckets are directories and objects are files in them. HTTP is spoken
//! with ureq-proto's server state machine, and all I/O goes through a
//! [`tokio_multiring::MultiRing`]: an object's bytes travel between the socket
//! and the drives in the multiring's registered buffers, without being copied
//! on the way.
//!
//! Given several drives, the server stripes each object over them: the
//! object is cut into units that are dealt out to the drives in turn, each
//! drive keeping its share in a file of its own. A transfer reads or writes
//! all drives at once, so one object moves at the speed of the drives
//! together. Losing a drive loses every object larger than one unit.
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
mod join;
mod name;
mod store;
mod stripe;

use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio_multiring::{BufferPool, MultiRing};

use crate::conn::Conn;
use crate::store::Store;

/// Settings of a [`Server`].
#[derive(Clone, Debug)]
pub struct Config {
    /// The directories the buckets live in, one per drive, each on a file
    /// system that supports `O_DIRECT`. They must exist.
    ///
    /// The list is part of how objects are stored: adding, removing or
    /// reordering roots makes the objects stored so far unreadable.
    pub roots: Vec<PathBuf>,
    /// Size of the units that objects are cut into when there are several
    /// roots: a multiple of 4096 that the size of the multiring's buffers is a
    /// multiple of. Part of how objects are stored, like the roots.
    pub stripe_unit: usize,
    /// How many rounds of an object a transfer keeps going at once, a round
    /// being one multiring buffer per drive: at least 1. A download reads
    /// that far ahead of what it is sending, and an upload writes that far
    /// behind what it is receiving. A transfer waits for the buffers of one
    /// round and takes the rest only while they are free.
    ///
    /// For a download sent without copying, a round is under way until the
    /// client has acknowledged it, so this is also the most that a
    /// connection has in flight to its client.
    pub window: usize,
    /// Which clients get their downloads without copying.
    pub zero_copy: ZeroCopy,
    /// How many connections are served at a time. Further ones wait to be
    /// accepted.
    pub connections: usize,
    /// Size of the two buffers each connection holds for requests and
    /// response headers: a multiple of 4096. A request's headers must fit
    /// in one.
    pub head_buffer: usize,
}

impl Config {
    pub fn new<Root: Into<PathBuf>>(roots: impl IntoIterator<Item = Root>) -> Config {
        Config {
            roots: roots.into_iter().map(Into::into).collect(),
            stripe_unit: 128 * 1024,
            window: 4,
            zero_copy: ZeroCopy::Remote,
            connections: 1024,
            head_buffer: 16 * 1024,
        }
    }
}

/// Which clients get their downloads without copying: sent straight from
/// the buffers the drives filled, where otherwise the kernel copies them
/// into the socket first. On a network card that copy is most of what a
/// download costs in processor time. Over loopback the kernel copies either
/// way, and asking it not to only adds work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZeroCopy {
    Never,
    /// Clients on other machines.
    Remote,
    /// Loopback clients too. For testing.
    Always,
}

struct Inner {
    multiring: MultiRing,
    store: Store,
    /// The multiring's registered buffers, which object data moves through.
    bodies: BufferPool,
    /// How many rounds a transfer keeps going at once.
    window: usize,
    zero_copy: ZeroCopy,
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
    /// Sets the server up over `multiring`, which must have a pool of at least
    /// as many buffers as there are roots. Objects are read and written
    /// through the multiring's polled disk rings if it has them.
    pub fn new(multiring: MultiRing, config: Config) -> io::Result<Server> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidInput, message);
        let bodies = multiring
            .buffers()
            .cloned()
            .ok_or_else(|| invalid("the multiring has no buffer pool"))?;
        if config.connections == 0 {
            return Err(invalid("a server needs room for a connection"));
        }
        if config.window == 0 {
            return Err(invalid("a transfer needs a window of at least one round"));
        }
        let heads = BufferPool::new(config.head_buffer, 2 * config.connections)?;
        let store = Store::open(multiring.clone(), &config.roots, config.stripe_unit)?;
        Ok(Server {
            inner: Arc::new(Inner {
                multiring,
                store,
                bodies,
                window: config.window,
                zero_copy: config.zero_copy,
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
        tokio::task::run_on_each_worker(|_| {
            let inner = inner.clone();
            let listener = listener.clone();
            move || {
                tokio::spawn(accept(inner, listener));
            }
        });
    }
}

/// Accepts connections on one worker. A connection's task starts on the
/// worker that accepted it.
async fn accept(inner: Arc<Inner>, listener: Arc<OwnedFd>) {
    loop {
        let slot = inner.slots.clone().acquire_owned().await;
        let slot = slot.expect("the semaphore is never closed");
        let socket = match inner.multiring.accept(listener.clone()).await {
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
        let zero_copy = match inner.zero_copy {
            ZeroCopy::Never => false,
            ZeroCopy::Always => true,
            ZeroCopy::Remote => socket
                .peer_addr()
                .is_ok_and(|peer| !peer.ip().to_canonical().is_loopback()),
        };

        let buffer = || {
            inner
                .heads
                .try_acquire()
                .expect("a slot stands for two head buffers")
        };
        let conn = Conn::new(
            inner.multiring.clone(),
            socket.into(),
            buffer(),
            buffer(),
            zero_copy,
        );
        let inner = inner.clone();
        tokio::spawn(async move {
            api::serve(&inner, conn).await;
            drop(slot);
        });
    }
}
