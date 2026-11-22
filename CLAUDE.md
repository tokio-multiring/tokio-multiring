# tokio-sidecar, sidecar-s3, sidecar-axum and s3-axum

Four crates in one workspace:

- **`tokio-sidecar`** (repo root): an io_uring reactor that drives a tokio
  runtime in place of tokio's own I/O driver, one ring per worker. Workers
  sleep in their rings, tokio's sockets are watched by them, and the crate's
  own operations go straight onto them. Operation futures are `Send`; buffers
  and descriptors move into an operation and come back with its result.
- **`sidecar-s3`** (`s3/`): hand-rolled S3-style object server built on it,
  speaking HTTP through the server half of ureq-proto. Kept as the zero-overhead
  reference baseline.
- **`sidecar-axum`** (`sidecar-axum/`): serves any real `axum::Router`
  (`tower_service::Service<Request<Body>, Response = Response<Body>>`) over
  `tokio-sidecar` and `ureq-proto` — no Hyper and no Axum fork. Standard Axum
  extractors, responses, and Tower middleware work unmodified; handlers can opt
  into `DirectBody` (`FromRequest`, transparent fallback to `axum::body::Body`
  on Hyper) and `DirectResponse` / `DirectWrite` (`IntoResponse`) for registered
  buffers, coalesced head+body vectored sends, and `send_zc_vectored`.
- **`s3-axum`** (`s3-axum/`): unified Axum S3 app (one `axum::Router`,
  middleware, `ListBuckets`/`ListObjectsV2`, optional AWS SigV4 + `aws-chunked`)
  running on **both** `--runtime sidecar` (`tokio-sidecar` + `sidecar-axum`,
  interrupt-driven `io_uring`, no IOPOLL threads) and `--runtime tokio`
  (standard Tokio mio/epoll + `axum::serve`/Hyper + `spawn_blocking`). Both
  backends share the exact same on-disk striped format and interoperate directly.

The repository is local only: no remote, nothing published. Work in small,
focused commits. The branch `axum-s3` adds `sidecar-axum` and `s3-axum` on top
of `io-driver`; `main` is the state before the reactor-driver move.

## Build and test

```sh
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --offline --workspace
cargo test --offline --workspace
cargo fmt --all --check
cargo clippy --offline --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="--cfg tokio_unstable -D warnings" cargo doc --offline --workspace --no-deps
```

- Always pass `--offline` on lounas: cargo hangs on its network check in the
  sandbox. Fetching a new dependency needs one `cargo fetch` without it.
- `--cfg tokio_unstable` comes from `.cargo/config.toml`. rustdoc needs it in
  `RUSTDOCFLAGS` as shown.
- All of the above are clean at HEAD: 129 tests across twelve test binaries.
- Concurrency bugs here have shown up at rates near 1 in 100. Before calling
  such a change verified, loop the affected test binaries (they are in
  `target/{debug,release}/deps/`) at least 30 times in both profiles. A new
  test for a race should be shown to fail when the fix is removed.
- The integration tests write under `target/tmp/` and do not clean up.
- `tests/disk.rs` uses a file by default. `SIDECAR_SCRATCH_DEVICE=/dev/...`
  points it at a block device, whose first blocks it **overwrites**.

## Dependencies that are not from crates.io

| Crate | Source | Why |
|---|---|---|
| tokio | path `../tokio-uring-wl/tokio`, branch `byo-io-driver` (rebased onto `local-worker-v3` `c90d985b`; `[patch.crates-io]` unifies axum/hyper-util onto it) | the reactor seam (`runtime::reactor::Reactor`, `Builder::io_reactor`), upgraded worker-local tasks (`spawn_worker_local`, `run_on_each_worker`, `run_on_any_worker`, `worker_local_queue_interval`), and `Builder::on_before_steal` |
| mio | path `../mio-shared-registration`, branch `shared-registration-v3`, as a `[patch]` | `Registry::register_shared`, which that tokio branch registers sockets through |
| ureq-proto | `../ureq-proto-combined`, branch `axum-proto` (for `sidecar-axum`); `s3/` keeps `combined-0.7.1` rev `29bb652` | server state machine with HTTP/1.0 framing, strict transfer-encoding, any-final-status `Send100::reject`, and chunked finish capacity check |

## Layout

```
src/reactor.rs   Sidecar and its `Reactor` impl for tokio, per-worker `Worker`, the turn loop, sleeping, watches, disk rings
src/landing.rs   arbitration between a future and the reactor (read its module docs first)
src/op.rs        Operation enum (one variant per kind of I/O), Op future, Completion
src/ops.rs       the public operations on Sidecar
src/buffers.rs   BufferPool, FixedBuf
src/net.rs       TcpListener, TcpStream: sockets in the mode the rings need
src/plan.rs      Plan: which CPU each runtime thread runs on
examples/disk_bench.rs   raw block-device benchmark (destructive, see below)
examples/echo_server.rs  TCP echo server for outside load generators

s3/src/api.rs    one HTTP exchange: routing, upload and download paths
s3/src/store.rs  objects on disk, commit protocol
s3/src/stripe.rs layout of an object over drives, header format (pure, model-tested)
s3/src/conn.rs   buffered connection, lingering close
s3/src/name.rs   bucket rules, key -> file name, placement hash
s3/src/join.rs   join_all, and Started (poll once now, finish later)
s3/src/main.rs   the binary and its flags
s3/examples/load.rs      HTTP load generator

sidecar-axum/src/server.rs    HTTP/1.1 connection driver for axum Routers (worker-local via run_on_any_worker, idle_timeout)
sidecar-axum/src/incoming.rs  SidecarIncoming (http_body::Body) and DirectBody (FromRequest fast path + Hyper fallback)
sidecar-axum/src/outgoing.rs  DirectWrite / DirectResponse / BodySink (coalesced head+body send_vectored, send_zero_copy)
sidecar-axum/src/conn.rs      buffered connection over Sidecar

s3-axum/src/app.rs            unified Axum S3 Router, handlers, ListBuckets/ListObjectsV2, request-id/timing/SigV4 middleware
s3-axum/src/sigv4.rs          AWS4-HMAC-SHA256 verification and streaming aws-chunked decoder (signed & unsigned, trailers)
s3-axum/src/sidecar_store.rs  io_uring O_DIRECT store (inline small GET, single-write small PUT, statx overwrite fast path)
s3-axum/src/tokio_store.rs    standard Tokio spawn_blocking store (same striped layout; buffered or --direct)
s3-axum/src/main.rs           CLI launching --runtime sidecar|tokio
```

## Design decisions (settled with Louis; do not relitigate)

**Sidecar**

- The sidecar is the runtime's I/O reactor (Louis, 2026-11-20): it
  implements `tokio::runtime::reactor::Reactor`, and `Sidecar::install` puts
  it on a runtime builder. Tokio's own driver is not polled, so there is no
  epoll and `tokio::signal` and `tokio::process` panic on such a runtime.
- One ring per worker, and each worker sleeps in its own
  (`io_uring_enter` with a wait). Another thread wakes it by writing to the
  worker's eventfd, which a read on the ring is waiting on. One sleeping
  worker at a time, the "timekeeper", sleeps only until the next timer.
- Tokio's sockets are watched by a multishot poll on the ring of the worker
  that registers them, which with first-use registration is the worker that
  first uses them. A thread without a ring hands the request to a worker in
  rotation.
- Rings are flushed from `Builder::on_before_steal`, when a worker goes to
  sleep, and when the runtime has a busy worker look at its ring (a park
  with a zero timeout, every 61 tasks by default). A busy worker submits at
  every look but takes completions in only at every fourth
  (`LOOKS_PER_REAP`): handing it more tasks at every look keeps its queue
  from ever emptying, and tokio then starves its shared queue. That cost a
  55 ms 99th percentile at 2048 connections with stealing, against 4 ms.
- Rings are `SINGLE_ISSUER | COOP_TASKRUN | TASKRUN_FLAG`, created disabled
  when the runtime is built and enabled by the worker's thread as it
  starts. `DEFER_TASKRUN` was measured against it (2026-11-20, echo on
  loopback, 8 workers, 1 to 2048 connections, 256 B and 16 KiB): within
  2% on throughput, tail and CPU in every cell, so it buys nothing here and
  is not used. Gotcha for anyone retrying it: deferred completions only
  become visible to an enter that carries `GETEVENTS`, which the io-uring
  crate adds only when the ring's `TASKRUN_FLAG` is set; without that flag a
  busy worker sees its completions only when it goes to sleep, and the 99th
  percentile goes from 1 ms to 15 to 35 ms.
- A ring belongs to the thread its worker started on, so the runtime must be
  multi-threaded and must not use `block_in_place`. Both fail with a panic
  that says so.
- The reactor alone owns the in-flight slots and writes each result straight
  into the pinned future. Drop versus delivery is decided by one atomic word
  per slot (`landing.rs`). A mutex-shared slab and tokio's oneshot were both
  tried and rejected.
- Polled disk rings: one `IOPOLL | SQPOLL` ring per worker, attached with
  `ATTACH_WQ` to one kernel thread per domain of the `Plan`. `O_DIRECT` only.
- Threads are pinned from an upfront `Plan`. `Plan::build` pins each worker
  from a task that runs on it. Pinning by arrival order in `on_thread_start`
  is wrong: tokio starts workers through its blocking pool, and a blocking
  thread can end up running a worker.
- Zero-copy sends complete twice. The reactor keeps the operation in its slot
  between the result and the kernel's release notice, and the caller gets a
  `Reclaim` that yields the buffers after the second.

**Server**

- Several `--root`s means the server stripes objects itself: 128 KiB units
  dealt to the drives in turn, one file per drive. The first part is the file
  named after the key, on the drive `fnv1a("bucket/file") % drives`. It starts
  with a 4096-byte header (magic `sidecar1`, drive count, unit, size, mtime,
  128-bit id) and is renamed into place last: that rename is the commit.
  Other parts are `.part-<id>-<n>`.
- Roots, their order and the unit are part of the on-disk format. A mismatch
  is refused by the header check.
- A transfer moves in rounds: one buffer per drive, one read or write per
  drive issued together, one vectored send or receive on the socket.
- A transfer keeps up to `--window` rounds going (read-ahead on GET,
  write-behind on PUT). It waits for one round's buffers and takes more only
  while more than half the pool is free.
- Downloads to non-loopback clients are zero-copy. Buffers of a sent round
  are reused only once the client has acknowledged it, which bounds what a
  connection can pin to its window.

## Rules that are easy to break

- **An SQE may point at heap memory an operation owns, never at the
  `Operation` value itself.** Slots move when the slot vector grows.
- **Never take a pool buffer while holding one.** Take all you need at once
  with `acquire_many`, or use `try_acquire_many`. Take a scratch buffer
  before a commit lock, not under it. Two deadlocks came from this.
- **Wakers, buffer drops and readiness reports run foreign code.** Call them
  with no `RefCell` borrow or lock held (see `Worker::deliver`).
- **A worker must not sleep on work it has just made.** A turn that
  delivered a completion has woken a task. `Worker::park` returns then, and
  when `revisit` is set.
- **Sockets for the crate's own operations must be in blocking mode.**
  io_uring returns `EAGAIN` at once on `O_NONBLOCK` descriptors. Sockets from
  `std` are fine, tokio's are not: those are for tokio's own methods, which
  work on this runtime too.
- **A direct read must not ask for much more than the file holds.** The
  kernel zeroes the rest of the request first. Pass a `limit`.
- **Zero-copy with registered buffers only on ordinary pages.** On huge pages
  Linux (through 7.0 at least) charges each region to the socket as if it
  began at the start of the huge page, and sends crawl at a twentieth of the
  speed. `send_zc_vectored` already picks: registered for
  `BufferPool::without_huge_pages`, pinned per send otherwise. Notes and a
  verified kernel patch are in `~/kzc-sendzc/`. Nothing is posted upstream.
- **Never run polled I/O through device-mapper striping.** A polled request
  that crosses a dm-stripe boundary oopses the kernel (NULL dereference in
  `nvme_mpath_start_request`, 7.0.0-43). md raid does not pass polling at
  all. This is why the server stripes on its own.

## Running the server on lounas

The two demo drives are the 480 GiB namespaces of the two PM9A3s, each with
its own XFS. They are **not in fstab**; after a reboot:

```sh
sudo mount -o noatime LABEL=s3-d0 /srv/sidecar-s3/d0
sudo mount -o noatime LABEL=s3-d1 /srv/sidecar-s3/d1
```

Mount by label. NVMe device names shift between boots on this machine, so
identify a drive by serial (`S63UNG0T545456`, `S63UNG0T544890`) and size,
never by `nvmeXnY`. The other namespace on each drive is half of md0
(`/data/bio`, family data).

```sh
target/release/sidecar-s3 --root /srv/sidecar-s3/d0 --root /srv/sidecar-s3/d1 \
    --listen 127.0.0.1:19000 --domains 4 [--polled] [--zero-copy never|remote|always]
```

- The server has no authentication. Bind it to loopback, or to `10.0.0.1`
  (the point-to-point 100G link to lourip) for a test, and stop it after.
- `nvme.poll_queues=8` is on the kernel command line, which `--polled` needs.
- The memlock limit is unlimited for louis (limits.d and the bot shepherd
  unit), which pools of 8 MiB and more need.

## Benchmarks

```sh
# object load, from this machine or another (copy the binary over)
ADDR=127.0.0.1:19000 MODE=put|get SIZE=16777216 COUNT=8 CONNS=32 SECONDS=5 \
    taskset -c 8-15,24-31 target/release/examples/load
```

- `get` fetches objects named by connection and index, so run `put` first
  with the **same** `CONNS` and `COUNT`.
- Pin the load generator away from the server's workers. Over loopback the
  client is the bottleneck above about 6 GiB/s.
- For a 100G run the client goes on lourip (power it on from the lab
  dashboard; ssh refuses logins for a minute after it answers ping).
- **`examples/disk_bench.rs` works on a raw block device and `MODE=fill`
  overwrites it.** The demo namespaces now hold filesystems, so do not point
  it at them without reformatting afterwards.

Reference numbers (single runs of 4 to 6 seconds, November 2026):

| Case | Result |
|---|---|
| Either drive alone, fio | 6.47 GiB/s at 1 MiB, 1.10M IOPS at 4 KiB |
| GET 16 MiB to lourip over 100G, zero-copy, 2 workers, 16 connections | 11.7 GiB/s, 100 Gbit/s on the wire, 1.4 process cores (2.3 whole machine) |
| Same with copied sends, 2 workers | 6.9 GiB/s at 2.0 cores |
| Same, zero-copy, 1 worker | 8.8 GiB/s at 1.0 core |
| One connection over 100G | 3.1 to 4.25 GiB/s, bound by the receiver's single RX core |
| PUT 16 MiB over 100G, 4 workers, 32 connections | 5.2 GiB/s at 2.2 cores |
| GET 16 MiB over loopback, 4 workers, 32 connections | 6.3 GiB/s (client-bound) |
| GET 4 KiB over loopback, 4 workers | 57k req/s, 73k with `--polled` |
| PUT 4 KiB over loopback, 4 workers | 8.7k req/s, 13.3k with `--polled` |

`--polled` raises no device interrupts and buys 10 to 25% on small objects,
nothing on large ones, at the cost of one spinning core per domain. One
kernel SQ thread tops out near 450k 4 KiB reads per second however many
workers share it.

Unified Axum S3 (`s3-axum`, same `axum::Router` + middleware, **zero IOPOLL threads**, November 2026):

| Case | `s3-axum` (`sidecar`) | `s3-axum` (`tokio --direct`) | `s3-axum` (`tokio` buffered) |
|---|---|---|---|
| GET 16 MiB over 100G, 2 workers, 16 conns | **11.75 GiB/s** (100G wire), p99 26.5 ms, **1.10 proc cores** | 3.05 GiB/s, p99 133 ms, 1.86 cores | 2.82 GiB/s, p99 146 ms, 1.86 cores |
| GET 16 MiB over 100G, 1 worker, 16 conns | **8.80 GiB/s** (~74 Gbps), p99 32.5 ms, **0.85 proc cores** | 1.73 GiB/s, p99 318 ms, 0.94 cores | 1.73 GiB/s, p99 219 ms, 0.93 cores |
| GET 16 MiB over 100G, 4 workers, 32 conns | **11.69 GiB/s** (100G wire), p99 95 ms, **1.36 proc cores** | 5.12 GiB/s, p99 145 ms, 3.70 cores | 4.32 GiB/s, p99 180 ms, 3.70 cores |
| PUT 16 MiB over 100G, 4 workers, 32 conns | **4.98 GiB/s**, p99 158 ms, **2.00 proc cores** | 2.46 GiB/s, p99 659 ms, 2.86 cores | 2.23 GiB/s, p99 789 ms, 2.76 cores |
| GET 4 KiB over 100G, 4 workers, 32 conns | **90.2k req/s**, p50 347 µs, p99 491 µs | 25.7k req/s, p99 2.07 ms | 33.9k req/s, p99 1.61 ms (page cache) |
| PUT 4 KiB over 100G, 4 workers, 32 conns | **9.49k req/s**, p50 2.54 ms, p99 10.7 ms, 1.54 cores | — | — |
| PUT 4 KiB over loopback, 4 workers, 32 conns | **10.0k req/s**, p50 2.58 ms, p99 8.80 ms, 2.52 cores | 1.73k req/s, p99 31.3 ms, 3.29 cores | 1.74k req/s, p99 30.7 ms, 3.14 cores |
| GET 4 KiB over loopback, 4 workers, 32 conns | **84.9k req/s**, p50 371 µs, p99 508 µs, 3.72 cores | 25.5k req/s, p99 2.80 ms, 3.60 cores | 33.9k req/s, p99 1.60 ms (page cache) |

## What the server does not do

- **Durability gap:** an object's data is synced before the 200, but the
  bucket directory is not synced after the rename. A crash right then can
  lose the newest version of a key.
- **No timeouts:** an idle or slow client holds a connection slot forever.
- **No listing** (ListObjectsV2), no multipart, no stored Content-Type or
  metadata.
- **No authentication** (SigV4), and no `aws-chunked` request bodies, so the
  AWS CLI and SDKs cannot talk to it. curl and plain HTTP clients can.
- **ETag is not MD5:** it is derived from the stored time and size.
- **No TLS.**
- **Uploads copy on receive.** Zero-copy receive is a different mechanism.

Suggested order: directory sync and timeouts, then listing, then
`aws-chunked` and SigV4.

## Open technical questions

- The reactor-owned slot design measured about 5% slower than the mutex slab
  it replaced on uncontended no-op round trips. Never profiled; no benchmark
  with contention exists.
- The arbiter in `landing.rs` has race tests and a ThreadSanitizer pass (with
  `race:tokio::runtime::io::` suppressed, those reports are tokio's), but no
  loom model.
- The reference numbers above are from `main`. On `io-driver`, an
  interleaved A/B over the 100G link (2026-11-20) put GET of 16 MiB objects
  at the same 11.8 GiB/s and 1.3 cores with 2 workers, the same 8.7 GiB/s
  with 1, and PUT within noise. The small-object rows are not remeasured.
  Echo numbers, against ringline and tokio, are in `~/sidecar-echo-bench`.
- Whether the timekeeper scheme costs anything under a timer-heavy load is
  untried.
- Three `io_uring_enter` calls per echo on one connection, to ringline's
  two, and it stays that way (measured 2026-11-20). bpftrace on the echo
  server split them: the hook submits the send, which completes on the
  spot; the hook submits the next recv; the park sleeps. The park's own turn
  almost never enters, so skipping it after the hook would save nothing.
  Tried instead: the hook and the pre-sleep step only take completions in,
  and the sleeping enter carries what is queued. That gave 2.0 enters per
  echo and no gain: one connection, 512 and 2048 (steal) within noise over
  9, 3 and 3 interleaved pairs; 64 connections 2 to 3% lower in 8 of 9
  pairs, because every send then costs a trip through tokio's park instead
  of waking its task from the hook. Data and scripts (`ab.sh`, `enters.bt`,
  `ab1.tsv`) are in `~/sidecar-echo-bench`; stacks need a build with
  `-C force-frame-pointers=yes` in `RUSTFLAGS`.
- Work handed to a busy worker by another thread (a cancel, a socket
  dropped elsewhere) waits for that worker's next look at its ring.
- Uploads got about 4% slower over loopback with write-behind. Writes that
  extend a file are punted to kernel worker threads; preallocating parts when
  the length is known is untried.
- The 64 KiB threshold below which a round is copied instead of sent
  zero-copy is a guess, not a measurement.
- Small-object PUTs are bound by create, sync and rename per object. Not
  profiled.
- `BufferPool`'s free list is a mutex and a semaphore, not per-worker.
