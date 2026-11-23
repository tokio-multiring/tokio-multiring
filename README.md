# tokio-multiring

[![CI](https://github.com/tokio-multiring/tokio-multiring/actions/workflows/ci.yml/badge.svg)](https://github.com/tokio-multiring/tokio-multiring/actions/workflows/ci.yml)

Per-worker `io_uring` reactor driving a standard multi-threaded Tokio runtime — zero epoll and `Send` futures, with Tokio sockets and an Axum HTTP adapter.

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

## Controlled benchmarks

Measured on `lounas` (16-core Zen 2 EPYC, 8 × 2-core L3 CCXs, Linux `7.0.0-43-generic`, two Samsung PM9A3 NVMe drives with XFS, ConnectX-5 100 GbE point-to-point to `lourip`) via [`bench/run.py`](bench/run.py): **5 randomized interleaved repetitions per suite**, `WARMUP=2`, `SECONDS=6`, `VERIFY=1` (client-side byte-for-byte payload verification on every GET), **zero IOPOLL threads** (`--polled` off).

Throughput is reported in binary **GiB/s** (`mib_s / 1024`; 100 GbE TCP payload line rate is `11.51 GiB/s` = `11,786 MiB/s` = `98.9 Gbps`). Values show **median (min–max)** across the 5 repetitions; `proc` is server process CPU cores and `mach` is whole-machine CPU cores on the server.

### End-to-end `s3-axum` (same `axum::Router` + middleware)

These compare complete application stacks (different HTTP engines, buffer ownership, and storage backends; see the controlled ablation below to isolate individual factors). `tokio-uring` and `tokio` (buffered) serve 4 KiB GETs from the Linux page cache; `multiring` and `tokio --direct` read from NVMe via `O_DIRECT`.

| Workload | `s3-axum` (`multiring`) | `s3-axum` (`tokio-uring`) | `s3-axum` (`tokio --direct`) | `s3-axum` (`tokio` buffered) |
|---|---|---|---|---|
| GET 16 MiB over 100G, 2w, 16c | **11.51 (11.40–11.52) GiB/s** (100G wire), p99 28.1 ms, **1.41c** (mach 2.68c) | 2.61 (2.56–2.65) GiB/s, p99 113 ms, 1.83c (mach 1.99c) | 2.37 (2.34–2.41) GiB/s, p99 169 ms, 1.89c (mach 2.04c) | 2.11 (2.04–2.12) GiB/s, p99 210 ms, 1.86c (mach 3.55c) |
| GET 16 MiB over 100G, 1w, 16c | **8.45 (8.32–8.53) GiB/s** (~73 Gbps), p99 35.4 ms, **0.99c** (mach 1.39c) | 1.63 (1.59–1.72) GiB/s, p99 189 ms, 0.99c (mach 1.22c) | 1.32 (1.30–1.34) GiB/s, p99 259 ms, 1.00c (mach 1.22c) | 1.24 (1.19–1.25) GiB/s, p99 330 ms, 0.99c (mach 1.18c) |
| GET 16 MiB over 100G, 4w, 32c | **11.48 (11.46–11.49) GiB/s** (100G wire), p99 94.6 ms, **1.54c** (mach 4.61c) | 3.44 (3.32–3.50) GiB/s, p99 178 ms, 2.80c (mach 3.00c) | 4.01 (3.87–4.19) GiB/s, p99 223 ms, 3.36c (mach 3.51c) | 3.35 (3.26–3.53) GiB/s, p99 267 ms, 3.20c (mach 3.46c) |
| PUT 16 MiB over 100G, 4w, 32c | **5.46 (5.34–5.53) GiB/s**, p99 160 ms, **3.17c** (mach 4.29c) | 3.57 (3.48–3.63) GiB/s, p99 227 ms, 3.81c (mach 4.20c) | 3.35 (3.35–3.44) GiB/s, p99 323 ms, 3.65c (mach 4.11c) | 3.26 (3.00–3.31) GiB/s, p99 318 ms, 3.73c (mach 4.22c) |
| GET 4 KiB over 100G, 4w, 32c | **91.7 (88.1–92.6)k req/s**, p50 346 µs, p99 454 µs, 3.87c | 33.2 (33.0–33.4)k req/s, p99 1.30 ms *(page cache)*, 3.11c | 42.9 (42.2–43.6)k req/s, p99 1.26 ms, 3.71c | 49.5 (48.7–50.6)k req/s, p99 1.09 ms *(page cache)*, 3.67c |
| GET 4 KiB over loopback, 4w, 32c | **86.9 (85.6–87.4)k req/s**, p50 366 µs, p99 473 µs, 3.79c | 33.8 (32.6–34.3)k req/s, p99 1.29 ms *(page cache)*, 3.21c | 41.5 (39.9–41.9)k req/s, p99 1.31 ms, 3.75c | 49.2 (48.6–50.4)k req/s, p99 1.12 ms *(page cache)*, 3.72c |
| PUT 4 KiB over 100G, 4w, 32c | **11.0 (10.9–11.0)k req/s**, p50 2.33 ms, p99 8.07 ms, 2.82c | 7.87 (7.84–8.02)k req/s, p50 4.00 ms, p99 6.43 ms, 2.59c | 9.04 (8.86–9.12)k req/s, p50 3.27 ms, p99 8.17 ms, 2.98c | 8.89 (8.73–9.02)k req/s, p50 3.05 ms, p99 10.8 ms, 2.74c |
| PUT 4 KiB over loopback, 4w, 32c | **11.1 (10.7–11.2)k req/s**, p50 2.31 ms, p99 8.00 ms, 2.84c | 7.92 (7.86–7.96)k req/s, p50 3.99 ms, p99 6.39 ms, 2.58c | 9.15 (9.00–9.18)k req/s, p50 3.21 ms, p99 8.25 ms, 3.01c | 8.82 (8.72–9.04)k req/s, p50 3.03 ms, p99 11.4 ms, 2.72c |

### Controlled ablation (`s3-axum --example ablation`, 100 GbE)

S3 disk ablations use `--direct` (`O_DIRECT`) on `TokioStore` so every stack reads from NVMe. Loopback clients are pinned away from server workers (`taskset -c 8-15,24-31`).

| Stack | S3 16 MiB GET (100G, 2w, 16c) | S3 4 KiB GET (100G, 4w, 32c) | Mem 1 MiB GET (100G, 2w, 16c) | Mem 1 MiB GET (loopback, 2w, 16c) |
|---|---|---|---|---|
| `hyper` | 2.39 GiB/s, p99 173 ms, 1.91c | 42.3k req/s, p99 1.27 ms, 3.69c | 7.36 GiB/s, p99 3.3 ms, 2.00c | **5.99 GiB/s**, p99 4.1 ms, 2.00c |
| `hyper-multiring` | 2.27 GiB/s, p99 241 ms, 1.88c | 43.6k req/s, p99 1.47 ms, 3.77c | 7.46 GiB/s, p99 2.7 ms, 2.00c | 5.97 GiB/s, p99 3.3 ms, 2.00c |
| `multiring-body` (steal, copy) | 2.20 GiB/s, p99 183 ms, 1.93c | 43.8k req/s, p99 1.41 ms, 3.75c | 7.37 GiB/s, p99 2.9 ms, 2.00c | 4.94 GiB/s, p99 4.5 ms, 2.00c |
| `multiring-body` (`--worker-local`, copy) | 2.21 GiB/s, p99 144 ms, 1.92c | 48.5k req/s, p99 1.27 ms, 3.79c | 7.36 GiB/s, p99 2.5 ms, 2.00c | 5.54 GiB/s, p99 3.3 ms, 2.00c |
| `multiring-direct` (steal, copy) | 6.76 GiB/s, p99 46.2 ms, 1.98c | 82.7k req/s, p99 595 µs, 3.97c | 6.19 GiB/s, p99 3.4 ms, 2.00c | 4.81 GiB/s, p99 4.2 ms, 2.00c |
| `multiring-direct` (`--worker-local`, copy) | 6.58 GiB/s, p99 42.4 ms, 1.98c | **90.8k req/s**, p99 456 µs, 3.85c | 6.19 GiB/s, p99 2.9 ms, 2.00c | 4.86 GiB/s, p99 3.4 ms, 2.00c |
| `multiring-direct` (`--worker-local`, zero-copy) | **11.50 GiB/s**, p99 26.0 ms, **1.24c** | — *(below 64 KiB ZC threshold)* | **11.52 GiB/s**, p99 1.6 ms, **1.45c** | 3.09 GiB/s, p99 5.4 ms, 2.00c |

Key attributions from the controlled pairs:
- **Reactor seam under Hyper (`hyper` vs `hyper-multiring`):** near parity (`-5%` to `+3%`), confirming that swapping epoll for `io_uring` readiness poll under Hyper is not what drives the end-to-end gain.
- **Worker-local placement (`steal` vs `--worker-local`):** `+10–11%` throughput and `-10%` to `-23%` p99 on 4 KiB S3 (`43.8k → 48.5k` body; `82.7k → 90.8k` direct), and consistently lower p99 across all workloads.
- **`io_uring` storage + `DirectWrite` (`multiring-body` vs `multiring-direct`, copied):** `+2.98×` on 16 MiB S3 (`2.21 → 6.58 GiB/s`) and `+1.87×` on 4 KiB S3 (`48.5k → 90.8k req/s`). On pure in-memory copied sends (`--mode mem`), `MemDirectWriter` is `-16%` slower than `Body::from(Bytes)` due to `BufferPool` lock/vectored-send overhead without zero-copy.
- **Zero-copy sends (`SEND_ZC`):** `+75%` throughput on 16 MiB S3 over 100G (`6.58 → 11.50 GiB/s`, saturating 100G) while cutting server process CPU by `37%` (`1.98c → 1.24c`). Over loopback, `--zero-copy always` is `-36%` slower (`4.86 → 3.09 GiB/s`), confirming `--zero-copy remote` as the right default.

Opting into `--polled` (`IOPOLL | SQPOLL` disk rings) removes device interrupts and adds ~10–25% on small objects, at the cost of one spinning kernel SQ thread per L3 domain.

## Reproducing controlled comparisons

Build both benchmark programs:

```sh
cargo build --release -p s3-axum --example ablation -p multiring-s3 --example load
python3 bench/run.py bench/memory.json --repetitions 7 > /tmp/results.jsonl
```

The runner requires Python 3.11 or later. It randomizes case order within each repetition with a recorded seed,
records the revision, dirty-tree status, kernel, commands and CPU affinity,
and samples process and whole-machine CPU between the client's measurement
markers. Whole-machine CPU includes unrelated activity; use an idle machine.
Process CPU alone excludes kernel worker and interrupt costs. On loopback,
whole-machine CPU includes the client too. CPU sample windows
are recorded separately from client elapsed time because receiving the markers
adds a small observation delay. Summarize the distribution of runs, not just the
fastest run. Build from a clean committed revision for publishable results.

`bench/memory.json` is a local smoke/example matrix, not a 100G result. Pin the
client away from the server for loopback tests. For network measurements, run
the client on a separate machine (the runner accepts an `ssh` client command),
set the remote environment in that command, use a remote-side `timeout`, and
use `--zero-copy remote`. The runner terminates local process groups after
`timeout_s` (default 300), but SSH termination does not guarantee remote cleanup.
Local executable hashes are recorded; record the remote binary hash separately.
Use `PAYLOAD_BYTE=66` only for the in-memory ablation, whose payload is `0x42`.
Verification is enabled by default and consumes client CPU; record `VERIFY` and
ensure the client has headroom. If measuring with `VERIFY=0`, run a separate
full `MODE=verify` pass on the same data first.

Compare these pairs separately:

| Question | Controlled pair |
|---|---|
| Reactor seam using ordinary Tokio sockets | `--stack hyper` / `--stack hyper-multiring`; same Hyper server, Router, store, workers and buffers |
| Worker-local placement | Same `multiring-body` or `multiring-direct` case, with / without `--worker-local` |
| Zero-copy sends | Same multiring case, `--zero-copy never` / `--zero-copy remote` (or `always` for loopback) |
| Application-specific fast paths | `multiring-body` / `multiring-direct`, explicitly a storage/body implementation comparison |

The ablation defaults to stealing connections and copied sends. `--workers`,
`--window`, `--zero-copy`, and `--worker-local` are explicit controls. Its
`tokio-ureq` implementation remains a minimal HTTP example that buffers request
bodies; it is not a controlled PUT comparison with the streaming servers.

For disk workloads, use `--mode s3 --root ...` with each stack. Baseline GETs
now use bounded read-ahead (`--window`, default 4), recycle read buffers, and
return stripe slices without coalescing them into another allocation. The
Tokio io-uring backend still uses its `File` seek/read API, which serializes
access within each file; multiring uses positioned reads. Both baselines now have bounded PUT write-behind and single-write small PUT
paths as well. Network send paths and buffer ownership still differ; the
baseline caches are per response, while multiring has a shared registered pool.
These remaining implementation differences must be reported alongside results.
Cancelling an entire upload leaves temporary files for startup cleanup; normal
error handling drains outstanding writes before unlinking temporary files.

The load generator has explicit phases:

```sh
# Populate every key before timed GETs; all runs must agree on SIZE/COUNT/CONNS.
MODE=prepare SIZE=16777216 COUNT=8 CONNS=16 target/release/examples/load
MODE=verify  SIZE=16777216 COUNT=8 CONNS=16 target/release/examples/load
MODE=get SIZE=16777216 COUNT=8 CONNS=16 WARMUP=2 SECONDS=10 target/release/examples/load
MODE=put SIZE=16777216 COUNT=8 CONNS=16 WARMUP=2 SECONDS=10 target/release/examples/load
```

`prepare` and `verify` visit each key once. Both `get` and `put` are timed,
cycling through those keys; `COUNT` is the working-set size per connection.
Connections, request strings and payloads are prepared before warm-up. Workers
finish warm-up before starting the shared measurement interval. Requests begun
before the deadline finish and count in both the request total and elapsed
time. Reported percentiles are closed-loop latencies at fixed concurrency,
not open-loop latency under a fixed offered request rate.

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
