use std::net::TcpListener;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use sidecar_s3::{Config, Server};
use tokio_sidecar::{BufferPool, DiskConfig, Hyperthreads, Plan, Sidecar};

const USAGE: &str = "\
usage: sidecar-s3 --root DIR [options]

  --root DIR            directory the buckets live in
  --listen ADDR         address to listen on                [127.0.0.1:9000]
  --connections N       connections served at a time        [1024]
  --buffers N           buffers for object data             [256]
  --buffer-size BYTES   size of each, a multiple of 4096    [1048576]
  --domains N           use only the first N cache domains  [all]
  --hyperthreads        put workers on both threads of a core
  --polled              read and write objects through polled disk rings,
                        with a kernel thread spinning on a CPU per domain";

struct Options {
    root: PathBuf,
    listen: String,
    connections: usize,
    buffers: usize,
    buffer_size: usize,
    domains: Option<usize>,
    hyperthreads: Hyperthreads,
    polled: bool,
}

impl Options {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Options, String> {
        let mut root = None;
        let mut options = Options {
            root: PathBuf::new(),
            listen: String::from("127.0.0.1:9000"),
            connections: 1024,
            buffers: 256,
            buffer_size: 1 << 20,
            domains: None,
            hyperthreads: Hyperthreads::Skip,
            polled: false,
        };

        fn number(flag: &str, value: Option<String>) -> Result<usize, String> {
            let value = value.ok_or_else(|| format!("{flag} needs a value"))?;
            value
                .parse()
                .map_err(|_| format!("{flag} needs a number, not `{value}`"))
        }
        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--root" => root = args.next().map(PathBuf::from),
                "--listen" => {
                    options.listen = args.next().ok_or("--listen needs a value")?;
                }
                "--connections" => options.connections = number(&flag, args.next())?,
                "--buffers" => options.buffers = number(&flag, args.next())?,
                "--buffer-size" => options.buffer_size = number(&flag, args.next())?,
                "--domains" => options.domains = Some(number(&flag, args.next())?),
                "--hyperthreads" => options.hyperthreads = Hyperthreads::Use,
                "--polled" => options.polled = true,
                other => return Err(format!("unknown option `{other}`")),
            }
        }
        options.root = root.ok_or("--root is required")?;
        Ok(options)
    }
}

/// Writes log records to standard error.
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

fn run(options: Options) -> std::io::Result<()> {
    let detected = Plan::detect(options.hyperthreads)?;
    let domains = detected.domains();
    let count = options.domains.unwrap_or(domains.len()).min(domains.len());
    let plan = Plan::new(domains[..count].to_vec())?;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal);
    let runtime = plan.build(&mut builder)?;

    let listener = TcpListener::bind(&options.listen)?;
    let address = listener.local_addr()?;
    let buffers = BufferPool::new(options.buffer_size, options.buffers)?;
    let workers = plan.worker_count();
    let domains = plan.domains().len();

    runtime.block_on(async {
        let sidecar = Sidecar::start(tokio_sidecar::Config {
            buffers: Some(buffers),
            disk: options.polled.then(|| DiskConfig {
                sq_entries: 256,
                sq_thread_idle: Duration::from_millis(100),
                plan,
            }),
            ..tokio_sidecar::Config::default()
        })
        .await?;

        let mut config = Config::new(&options.root);
        config.connections = options.connections;
        Server::new(sidecar, config)?.listen(listener);
        log::info!(
            "serving {} on http://{address} with {workers} workers in {domains} domains, {}",
            options.root.display(),
            if options.polled {
                "polled disk I/O"
            } else {
                "interrupt-driven disk I/O"
            },
        );
        std::future::pending().await
    })
}

fn main() -> ExitCode {
    log::set_logger(&Stderr).expect("no logger is installed yet");
    log::set_max_level(log::LevelFilter::Info);

    let options = match Options::parse(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sidecar-s3: {error}");
            ExitCode::FAILURE
        }
    }
}
