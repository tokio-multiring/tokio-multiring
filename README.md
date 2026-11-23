# tokio-multiring

[![CI](https://github.com/tokio-multiring/tokio-multiring/actions/workflows/ci.yml/badge.svg)](https://github.com/tokio-multiring/tokio-multiring/actions/workflows/ci.yml)

Per-worker `io_uring` reactor driving a standard multi-threaded Tokio runtime — zero epoll, `Send` futures, and unmodified compatibility with the Tokio/Axum/Tower ecosystem.

## Why this exists

Most `io_uring` runtimes in Rust (`tokio-uring`, `monoio`, `compio`, `glommio`) adopt a separate, thread-per-core, `!Send` runtime model. That works well in isolation, but it splits the ecosystem: you give up Tokio's multi-threaded work-stealing scheduler, timers, `Send` futures, and drop-in compatibility with crates like `axum` and `tower`.

`tokio-multiring` takes the opposite approach. Using a pluggable `Reactor` seam in Tokio (`Builder::io_reactor`), it **replaces** Tokio's mio/epoll I/O driver directly:

- Every Tokio worker thread owns one `io_uring` ring (`SINGLE_ISSUER | COOP_TASKRUN | TASKRUN_FLAG`) and parks/sleeps directly inside `io_uring_enter`. Tokio's mio/epoll driver is never constructed.
- Standard Tokio sockets (`tokio::net`) continue to work transparently, watched via multishot poll on the workers' rings. Tokio timers stay with Tokio.
- Callers that want completion-based I/O — `O_DIRECT` files, pre-registered fixed buffers, vectored I/O, zero-copy `SEND_ZC`, and optional `IOPOLL | SQPOLL` disk rings — submit directly onto the same per-worker rings using ordinary **`Send` futures**. Buffers and file descriptors move into an operation and come back with its completion, so the kernel never holds pointers to memory the caller can drop.

To test this end-to-end against real HTTP workloads rather than echo microbenchmarks alone, the workspace also includes `multiring-axum` (serving unmodified `axum::Router`s over `tokio-multiring` and `ureq-proto`, using the vendored axum-core body API) and `s3-axum` (a single Axum S3 server that runs on `tokio-multiring`, standard Tokio/Hyper, or mainline Tokio's `io-uring` `tokio::fs` over the same on-disk format).

## Workspace crates

| Crate | Path | Description |
|---|---|---|
| **`tokio-multiring`** | `.` | Per-worker `io_uring` reactor implementing Tokio's `Reactor` seam, `Send` completion ops, `BufferPool`/`FixedBuf`, `Plan` CPU/L3 pinning, and optional polled disk rings. |
| **`multiring-axum`** | `multiring-axum/` | Serves any `axum::Router` (or compatible `tower_service::Service`) over `tokio-multiring` and `ureq-proto`. Standard extractors, responses, and Tower middleware work unmodified; handlers can opt into `DirectBody` (with transparent Hyper fallback) and `DirectResponse` / `DirectWrite` for registered buffers and `send_zc_vectored`. |
| **`multiring-s3`** | `s3/` | Hand-rolled striped S3-style object server built directly on `tokio-multiring` and `ureq-proto`, kept as a low-overhead reference baseline. |
| **`s3-axum`** | `s3-axum/` | Unified Axum S3 server (`ListBuckets`, `ListObjectsV2`, optional AWS SigV4 + `aws-chunked`, Tower middleware) selectable across `--runtime multiring`, `--runtime tokio`, and `--runtime tokio-uring`, interoperating on the exact same striped on-disk format. |

## Quick example

```rust
fn main() -> std::io::Result<()> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let multiring = tokio_multiring::MultiRing::install(&mut builder, Default::default());
    let runtime = builder.build()?;

    runtime.block_on(async {
        tokio::spawn(async move { multiring.nop().await }).await?
    })
}
```

## Architecture highlights

- **Per-worker rings, no epoll:** Each worker thread enables its ring on startup and sleeps in `io_uring_enter`. Cross-thread wakeups write to the target worker's `eventfd` (watched by a read on its ring). One sleeping worker at a time acts as the "timekeeper" for Tokio's next timer deadline.
- **Balanced flushing under load:** Rings flush from `Reactor::before_steal` (leaving `Builder::on_before_steal` free for applications), before parking, and periodically during busy loops (`ParkMode::Poll`). Busy workers submit on every look but reap completions every fourth look so local work doesn't starve Tokio's global queue.
- **Lock-free completion handoff:** Each reactor owns its in-flight operation slots and writes completions directly into the pinned future. Races between future drop/cancellation and completion delivery are arbitrated by a single atomic word per slot (`src/landing.rs`).
- **Registered buffers & zero-copy sends:** `BufferPool` allocates page-aligned `FixedBuf`s registered across every worker ring. `send_zc_vectored` exposes two-stage completion via a `Reclaim` future so buffers are reused only after the kernel releases them, automatically using registered buffers on ordinary pages and per-send pinning on huge pages (working around a Linux `io_uring` huge-page `truesize` accounting bug).
- **CPU topology & polled disk rings:** `Plan::detect` groups CPUs by L3 cache domain and pins workers via worker-local startup tasks. With `Config::disk`, each worker also gets an `IOPOLL | SQPOLL` ring sharing one kernel SQ thread per L3 domain via `ATTACH_WQ`.

## Reference benchmarks

Measured with **`s3-axum`** (identical `axum::Router` + middleware, **zero IOPOLL threads**, two Samsung PM9A3 NVMe drives with XFS, 100 GbE point-to-point link, single 4–6 s runs):

| Workload | `s3-axum` (`multiring`) | `s3-axum` (`tokio-uring`) | `s3-axum` (`tokio --direct`) | `s3-axum` (`tokio` buffered) |
|---|---|---|---|---|
| GET 16 MiB over 100G, 2w, 16c | **11.75 GiB/s** (100G line rate), p99 26.5 ms, **1.10c** | 2.14 GiB/s, p99 150 ms, 1.75c | 3.05 GiB/s, p99 133 ms, 1.86c | 2.82 GiB/s, p99 146 ms, 1.86c |
| GET 16 MiB over 100G, 1w, 16c | **8.80 GiB/s** (~74 Gbps), p99 32.5 ms, **0.85c** | 1.60 GiB/s, p99 197 ms, 0.93c | 1.73 GiB/s, p99 318 ms, 0.94c | 1.73 GiB/s, p99 219 ms, 0.93c |
| GET 16 MiB over 100G, 4w, 32c | **11.69 GiB/s** (100G line rate), p99 95 ms, **1.36c** | 3.08 GiB/s, p99 243 ms, 3.09c | 5.12 GiB/s, p99 145 ms, 3.70c | 4.32 GiB/s, p99 180 ms, 3.70c |
| PUT 16 MiB over 100G, 4w, 32c | **4.98 GiB/s**, p99 158 ms, **2.00c** | 3.15 GiB/s, p99 411 ms, 2.89c | 2.46 GiB/s, p99 659 ms, 2.86c | 2.23 GiB/s, p99 789 ms, 2.76c |
| GET 4 KiB over 100G, 4w, 32c | **90.2k req/s**, p50 347 µs, p99 491 µs | 34.4k req/s, p99 1.38 ms (page cache) | 25.7k req/s, p99 2.07 ms | 33.9k req/s, p99 1.61 ms (page cache) |
| PUT 4 KiB over 100G, 4w, 32c | **9.49k req/s**, p50 2.54 ms, p99 10.7 ms, 1.54c | 7.84k req/s, p50 3.91 ms, p99 6.72 ms, 1.06c | — | — |
| PUT 4 KiB over loopback, 4w, 32c | **10.6k req/s**, p50 2.25 ms, p99 9.67 ms, 2.71c | 7.25k req/s, p50 4.04 ms, p99 10.3 ms, 2.37c | 1.73k req/s, p99 31.3 ms, 3.29c | 1.74k req/s, p99 30.7 ms, 3.14c |
| GET 4 KiB over loopback, 4w, 32c | **82.3–84.9k req/s**, p50 371 µs, p99 508 µs | 33.3k req/s, p99 1.52 ms (page cache) | 25.5k req/s, p99 2.80 ms | 33.9k req/s, p99 1.60 ms (page cache) |

Opting into `--polled` (`IOPOLL | SQPOLL` disk rings) removes device interrupts and adds ~10–25% on small objects, at the cost of one spinning kernel SQ thread per L3 domain.

## Status & dependencies

This is an experimental systems workspace (`publish = false`, Linux-only, Rust 1.85+, `--cfg tokio_unstable` configured in `.cargo/config.toml`).

The workspace uses a Tokio fork and two vendored dependency patches.

- **[`tokio-multiring/tokio`](https://github.com/tokio-multiring/tokio)** (`io-reactor-wl` branch) — adds the unstable `runtime::reactor::Reactor` / `Builder::io_reactor` seam and worker-local placement APIs (`run_on_each_worker`, `run_on_any_worker`) on top of stock crates.io `mio`.
- **[`lthiery/ureq-proto`](https://github.com/lthiery/ureq-proto)** (`axum-proto` branch) — tracks `algesten/ureq-proto` `main` plus two small open PRs ([#60](https://github.com/algesten/ureq-proto/pull/60), [#61](https://github.com/algesten/ureq-proto/pull/61)). The vendored snapshot adds independent request decoding and response encoding.
- **axum-core 0.5.6** — the vendored snapshot adds shared downcasting for the immediate body passed to `Body::new`. This lets the adapter recognize its own body and honor middleware wrappers and replacements.

### Current constraints

- **Linux with `io_uring` only.**
- **Multi-threaded runtime without `block_in_place`:** each ring is `SINGLE_ISSUER` and tied to its worker thread; use `spawn_blocking` instead.
- **No `tokio::signal` or `tokio::process`:** Tokio's mio/epoll driver is not constructed on a `MultiRing` runtime, so APIs that require it panic.
- **Registered buffers & `RLIMIT_MEMLOCK`:** large buffer pools require an adequate `ulimit -l` (e.g. `unlimited`).
- **Polled disk rings (`--polled`):** require `O_DIRECT` on a block device/filesystem with poll queues (e.g. NVMe with `nvme.poll_queues=N`, or `null_blk` with `poll_queues`).

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="--cfg tokio_unstable -D warnings" cargo doc --workspace --no-deps
```

> **Note:** The polled disk tests (`tests/disk.rs` and the two `--polled` tests in `s3/tests/api.rs`) submit `IORING_SETUP_IOPOLL` reads under `target/tmp`. If the filesystem backing `target/tmp` does not expose poll queues (`io_poll = 0`), those tests return `EOPNOTSUPP`; see [.github/workflows/ci.yml](.github/workflows/ci.yml) for running them on a memory-backed `null_blk` mount.

### Running `s3-axum`

```sh
cargo build --release -p s3-axum

# MultiRing (io_uring + multiring-axum/ureq-proto)
target/release/s3-axum --runtime multiring --root /path/to/d0 --root /path/to/d1 \
    --listen 127.0.0.1:9000 --workers 4

# Standard Tokio (mio/epoll + Hyper/axum::serve + spawn_blocking)
target/release/s3-axum --runtime tokio --direct --root /path/to/d0 --root /path/to/d1 \
    --listen 127.0.0.1:9000 --workers 4

# Mainline Tokio io_uring fs + Hyper/axum::serve
target/release/s3-axum --runtime tokio-uring --root /path/to/d0 --root /path/to/d1 \
    --listen 127.0.0.1:9000 --workers 4
```

## License

MIT
