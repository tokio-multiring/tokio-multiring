//! Unified Axum S3 server running on both standard Tokio (`axum::serve`) and
//! `tokio-sidecar` (`sidecar-axum` over `io_uring` + `ureq-proto`).

mod app;
mod date;
mod error;
mod join;
mod name;
mod sidecar_store;
mod sigv4;
mod stripe;
mod tokio_store;

pub use app::{AppOptions, Backend, router, router_with_options};
pub use sidecar_store::SidecarStore;
pub use sigv4::AuthConfig;
pub use tokio_store::TokioStore;
