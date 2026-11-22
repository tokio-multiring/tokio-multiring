use std::net::TcpListener;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use axum::serve::ListenerExt;
use s3_axum::{AppOptions, Backend, SidecarStore, TokioStore, router_with_options};
use sidecar_axum::{Config as HttpConfig, Server, ZeroCopy};
use tokio_sidecar::{BufferPool, DiskConfig, Domain, Hyperthreads, Plan, Sidecar};

const USAGE: &str = "\
usage: s3-axum --runtime sidecar|tokio --root DIR [--root DIR ...] [options]

  --runtime MODE        sidecar (io_uring + sidecar-axum/ureq-proto) or
                        tokio   (standard Tokio mio/epoll + axum::serve/Hyper) [sidecar]
  --root DIR            directory the buckets live in (repeatable for striping)
  --stripe-unit BYTES   stripe unit size, multiple of 4096    [131072]
  --window N            read-ahead / write-behind rounds      [4]
  --listen ADDR         address to listen on                  [127.0.0.1:9000]
  --connections N       concurrent connections (sidecar)      [1024]
  --buffers N           registered buffers (sidecar)          [256]
  --buffer-size BYTES   buffer/chunk size, multiple of unit   [1048576]
  --zero-copy WHO       never, remote, or always (sidecar)    [remote]
  --small-pages         ordinary pages for registered buffers (sidecar)
  --domains N           use only the first N cache domains    [all]
  --workers N           limit total worker threads            [domain size]
  --hyperthreads        use both SMT threads per core
  --steal-connections   spawn connection tasks with tokio::spawn instead of worker-local
  --idle-timeout SECS   close idle keep-alive connections after SECS (sidecar)
  --trace               log method, path, status, and latency via Axum middleware
  --polled              opt into IOPOLL+SQPOLL disk rings (off by default)
  --direct              use O_DIRECT in spawn_blocking (tokio runtime only)";

#[derive(Clone, Copy, PartialEq, Eq)]
enum RuntimeKind {
    Sidecar,
    Tokio,
}

struct Options {
    runtime: RuntimeKind,
    roots: Vec<PathBuf>,
    stripe_unit: usize,
    window: usize,
    zero_copy: ZeroCopy,
    small_pages: bool,
    listen: String,
    connections: usize,
    buffers: usize,
    buffer_size: usize,
    domains: Option<usize>,
    workers: Option<usize>,
    hyperthreads: Hyperthreads,
    worker_local_connections: bool,
    idle_timeout: Option<Duration>,
    trace: bool,
    polled: bool,
    direct: bool,
}

impl Options {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut opts = Options {
            runtime: RuntimeKind::Sidecar,
            roots: Vec::new(),
            stripe_unit: 128 * 1024,
            window: 4,
            zero_copy: ZeroCopy::Remote,
            small_pages: false,
            listen: String::from("127.0.0.1:9000"),
            connections: 1024,
            buffers: 256,
            buffer_size: 1 << 20,
            domains: None,
            workers: None,
            hyperthreads: Hyperthreads::Skip,
            worker_local_connections: true,
            idle_timeout: None,
            trace: false,
            polled: false,
            direct: false,
        };

        fn number(flag: &str, value: Option<String>) -> Result<usize, String> {
            let value = value.ok_or_else(|| format!("{flag} needs a value"))?;
            value
                .parse()
                .map_err(|_| format!("{flag} needs a number, not `{value}`"))
        }

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--runtime" => {
                    opts.runtime = match args.next().as_deref() {
                        Some("sidecar") => RuntimeKind::Sidecar,
                        Some("tokio") => RuntimeKind::Tokio,
                        _ => return Err("--runtime must be `sidecar` or `tokio`".into()),
                    };
                }
                "--root" => {
                    let root = args.next().ok_or("--root needs a value")?;
                    opts.roots.push(PathBuf::from(root));
                }
                "--stripe-unit" => opts.stripe_unit = number(&flag, args.next())?,
                "--window" => opts.window = number(&flag, args.next())?,
                "--listen" => opts.listen = args.next().ok_or("--listen needs a value")?,
                "--connections" => opts.connections = number(&flag, args.next())?,
                "--buffers" => opts.buffers = number(&flag, args.next())?,
                "--buffer-size" => opts.buffer_size = number(&flag, args.next())?,
                "--domains" => opts.domains = Some(number(&flag, args.next())?),
                "--workers" => opts.workers = Some(number(&flag, args.next())?),
                "--zero-copy" => {
                    opts.zero_copy = match args.next().as_deref() {
                        Some("never") => ZeroCopy::Never,
                        Some("remote") => ZeroCopy::Remote,
                        Some("always") => ZeroCopy::Always,
                        _ => return Err("--zero-copy is never, remote or always".into()),
                    };
                }
                "--small-pages" => opts.small_pages = true,
                "--hyperthreads" => opts.hyperthreads = Hyperthreads::Use,
                "--steal-connections" => opts.worker_local_connections = false,
                "--idle-timeout" => {
                    let secs = number(&flag, args.next())?;
                    opts.idle_timeout = Some(Duration::from_secs(secs as u64));
                }
                "--trace" => opts.trace = true,
                "--polled" => opts.polled = true,
                "--direct" => opts.direct = true,
                other => return Err(format!("unknown option `{other}`")),
            }
        }
        if opts.roots.is_empty() {
            return Err("--root is required".into());
        }
        Ok(opts)
    }
}

struct Stderr;

impl log::Log for Stderr {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("{}: {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

fn build_plan(opts: &Options) -> std::io::Result<Plan> {
    let detected = Plan::detect(opts.hyperthreads)?;
    let domains = detected.domains();
    let count = opts.domains.unwrap_or(domains.len()).min(domains.len());
    let mut selected: Vec<Domain> = domains[..count].to_vec();

    if let Some(limit) = opts.workers {
        let mut remaining = limit;
        let mut trimmed = Vec::new();
        for domain in selected {
            if remaining == 0 {
                break;
            }
            let take = domain.workers.len().min(remaining);
            if take > 0 {
                trimmed.push(Domain {
                    workers: domain.workers[..take].to_vec(),
                    sq_thread: domain.sq_thread,
                });
                remaining -= take;
            }
        }
        selected = trimmed;
    }
    Plan::new(selected)
}

fn run(opts: Options) -> std::io::Result<()> {
    let plan = build_plan(&opts)?;
    let workers = plan.worker_count();
    let domains = plan.domains().len();
    let listener = TcpListener::bind(&opts.listen)?;
    let address = listener.local_addr()?;
    let roots_display: Vec<_> = opts.roots.iter().map(|r| r.display()).collect();

    match opts.runtime {
        RuntimeKind::Sidecar => {
            let buffers = match opts.small_pages {
                true => BufferPool::without_huge_pages(opts.buffer_size, opts.buffers)?,
                false => BufferPool::new(opts.buffer_size, opts.buffers)?,
            };
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_time();
            let rings = tokio_sidecar::Config {
                buffers: Some(buffers),
                disk: opts.polled.then(|| DiskConfig {
                    sq_entries: 256,
                    sq_thread_idle: Duration::from_millis(100),
                    plan: plan.clone(),
                }),
                ..tokio_sidecar::Config::default()
            };
            let sidecar = Sidecar::install(&mut builder, rings);
            let runtime = plan.build(&mut builder)?;

            runtime.block_on(async {
                let store = SidecarStore::open(
                    sidecar.clone(),
                    &opts.roots,
                    opts.stripe_unit,
                    opts.window,
                )?;
                let app =
                    router_with_options(Backend::Sidecar(store), AppOptions { trace: opts.trace });
                let http_cfg = HttpConfig {
                    connections: opts.connections,
                    zero_copy: opts.zero_copy,
                    worker_local_connections: opts.worker_local_connections,
                    idle_timeout: opts.idle_timeout,
                    ..HttpConfig::default()
                };
                Server::new(sidecar, http_cfg)?.listen(listener, app);
                log::info!(
                    "s3-axum [sidecar] serving {roots_display:?} on http://{address} \
                     ({workers} workers in {domains} domains, {}, {})",
                    if opts.polled {
                        "polled disk I/O"
                    } else {
                        "interrupt-driven io_uring disk I/O"
                    },
                    if opts.worker_local_connections {
                        "worker-local conns"
                    } else {
                        "stealing conns"
                    },
                );
                std::future::pending().await
            })
        }
        RuntimeKind::Tokio => {
            listener.set_nonblocking(true)?;
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();
            let runtime = plan.build(&mut builder)?;

            runtime.block_on(async {
                let store =
                    TokioStore::open(&opts.roots, opts.stripe_unit, opts.buffer_size, opts.direct)?;
                let app =
                    router_with_options(Backend::Tokio(store), AppOptions { trace: opts.trace });
                let listener = tokio::net::TcpListener::from_std(listener)?.tap_io(|stream| {
                    let _ = stream.set_nodelay(true);
                });
                log::info!(
                    "s3-axum [tokio] serving {roots_display:?} on http://{address} \
                     ({workers} workers in {domains} domains, {})",
                    if opts.direct {
                        "spawn_blocking O_DIRECT"
                    } else {
                        "spawn_blocking buffered"
                    },
                );
                axum::serve(listener, app).await
            })
        }
    }
}

fn main() -> ExitCode {
    log::set_logger(&Stderr).expect("no logger installed yet");
    log::set_max_level(log::LevelFilter::Info);

    let opts = match Options::parse(std::env::args().skip(1)) {
        Ok(opts) => opts,
        Err(message) => {
            eprintln!("{message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(opts) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("s3-axum: {error}");
            ExitCode::FAILURE
        }
    }
}
