//! Serve real [`axum::Router`]s (and any compatible [`tower_service::Service`])
//! over [`tokio_sidecar::Sidecar`] (`io_uring`) and `ureq-proto`.
//!
//! Ordinary Axum routes, extractors, responses, and Tower middleware work
//! unmodified. Handlers that want direct `io_uring` registered-buffer and
//! zero-copy I/O can additionally opt into:
//!
//! - [`DirectBody`]: an Axum [`FromRequest`](axum::extract::FromRequest)
//!   extractor for zero-copy `recv_vectored` into [`tokio_sidecar::FixedBuf`]s
//!   and explicit `Expect: 100-continue` approval (with transparent fallback to
//!   [`axum::body::Body`] when running on Hyper / `axum::serve`).
//! - [`DirectResponse`] / [`direct_response`] / [`DirectWrite`]: an Axum
//!   [`IntoResponse`](axum::response::IntoResponse) hook that writes straight
//!   to [`BodySink`] with vectored / zero-copy `send_zc` sends.

mod conn;
mod incoming;
mod outgoing;
mod server;

pub use incoming::{BodyError, DirectBody, SidecarDirectBody};
pub use outgoing::{BodySink, DirectResponse, DirectWrite, direct_response};
pub use server::{Config, Server, ZeroCopy};
