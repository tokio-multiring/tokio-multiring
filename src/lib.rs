//! An io_uring reactor that drives a tokio runtime, with one ring per
//! worker thread.
//!
//! The runtime's own I/O driver is not used. A worker with nothing to run
//! sleeps in its ring, the sockets and other I/O resources of tokio are
//! watched by the rings, and the operations of a [`MultiRing`] go straight
//! onto the ring of the worker that starts them. [`before_steal`] flushes a
//! worker's ring whenever it runs out of local work. Timers stay with tokio.
//!
//! A [`Plan`] pins every thread of the runtime to a CPU as it starts. With
//! [`Config::disk`], each worker also gets a polled ring for `O_DIRECT` file
//! I/O, driven by a kernel thread that it shares with the other workers of
//! its domain in the plan.
//!
//! Operation futures are `Send`. A future submits to the ring of the worker
//! that first polls it, and may then be polled, completed or dropped on any
//! thread. Buffers and file descriptors are moved into the operation and
//! handed back with the result, so the kernel never holds a pointer into
//! memory the caller can free.
//!
//! ```
//! # fn main() -> std::io::Result<()> {
//! let mut builder = tokio::runtime::Builder::new_multi_thread();
//! let multiring = tokio_multiring::MultiRing::install(&mut builder, Default::default());
//! let runtime = builder.build()?;
//! runtime.block_on(async { tokio::spawn(async move { multiring.nop().await }).await? })
//! # }
//! ```
//!
//! Requires the `io-reactor-wl` tokio fork (`tokio-multiring/tokio`) and `--cfg tokio_unstable`.

mod buffers;
mod landing;
mod net;
mod op;
mod ops;
mod plan;
mod reactor;

pub use buffers::{BufferPool, FixedBuf};
pub use net::{TcpListener, TcpStream};
pub use ops::{FileStat, Reclaim, Reclaimed, Release, Segment};
pub use plan::{Domain, Hyperthreads, Plan};
pub use reactor::{Config, DiskConfig, MultiRing, before_steal};
