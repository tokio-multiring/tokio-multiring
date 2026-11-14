//! Per-worker io_uring reactors that run beside tokio's own I/O driver.
//!
//! Tokio keeps its epoll driver. Each worker thread of a multi-threaded
//! runtime additionally owns an io_uring instance, driven by a worker-local
//! task that tokio wakes through the ring's file descriptor, and flushed by
//! [`before_steal`] whenever the worker runs out of local work. Nothing in
//! tokio's I/O path is replaced: sockets, timers and everything else built on
//! the stock driver keep working unchanged, and io_uring is used only for the
//! operations submitted through a [`Sidecar`].
//!
//! Operation futures are `Send`. A future submits to the ring of the worker
//! that first polls it, and may then be polled, completed or dropped on any
//! thread. Buffers and file descriptors are moved into the operation and
//! handed back with the result, so the kernel never holds a pointer into
//! memory the caller can free.
//!
//! ```
//! # fn main() -> std::io::Result<()> {
//! let runtime = tokio::runtime::Builder::new_multi_thread()
//!     .enable_all()
//!     .on_before_steal(tokio_sidecar::before_steal)
//!     .build()?;
//! runtime.block_on(async {
//!     let sidecar = tokio_sidecar::Sidecar::start(Default::default()).await?;
//!     tokio::spawn(async move { sidecar.nop().await }).await?
//! })
//! # }
//! ```
//!
//! Requires the `local-worker-v3-full` tokio fork and `--cfg tokio_unstable`.

mod landing;
mod op;
mod ops;
mod reactor;

pub use reactor::{Config, Sidecar, before_steal};
