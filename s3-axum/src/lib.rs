//! Unified Axum S3 server running on both standard Tokio (`axum::serve`) and
//! `tokio-multiring` (`multiring-axum` over `io_uring` + `ureq-proto`).

mod app;
mod date;
mod download;
mod error;
mod join;
mod multiring_store;
mod name;
mod sigv4;
mod stripe;
mod tokio_store;
mod tokio_uring_store;
mod upload;

pub use app::{AppOptions, Backend, router, router_with_options};
pub use multiring_store::MultiRingStore;
pub use sigv4::AuthConfig;
pub use tokio_store::TokioStore;
pub use tokio_uring_store::TokioUringStore;
