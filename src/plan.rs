//! Where a runtime's threads run.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::runtime::{Builder, Runtime, RuntimeFlavor};

/// CPUs that are meant to work together, typically the ones sharing a
/// last-level cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Domain {
    /// One CPU per worker thread.
    pub workers: Vec<u32>,
    /// The CPU of the kernel thread that drives the disk rings of this
    /// domain's workers. `None` leaves that thread to the scheduler.
    pub sq_thread: Option<u32>,
}

/// Whether [`Plan::detect`] puts threads on both hardware threads of a core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hyperthreads {
    /// One thread per core. The other hardware thread of each core is left
    /// out of the plan, so a kernel submission thread, which spins, has its
    /// core to itself.
    Skip,
    /// One thread per hardware thread. A kernel submission thread then
    /// shares its core with a worker.
    Use,
}

struct Inner {
    domains: Vec<Domain>,
    /// Domain and CPU of each worker, by the worker's index in the runtime.
    seats: Vec<(usize, u32)>,
    /// Where the runtime's other threads may run.
    worker_cpus: Vec<u32>,
    built: AtomicBool,
}

/// Decides, before a runtime is built, which CPU each of its threads runs
/// on. Cheap to clone.
///
/// Every worker thread is pinned to a CPU of its own. The threads the
/// runtime starts later, for blocking work, may run on any of the workers'
/// CPUs, which keeps them off the ones set aside for kernel submission
/// threads.
///
/// `block_in_place` moves a worker onto a different thread, which the plan
/// knows nothing about; a runtime that hosts a sidecar cannot use it anyway.
#[derive(Clone)]
pub struct Plan {
    inner: Arc<Inner>,
}

impl Plan {
    /// A plan with the given domains.
    ///
    /// Fails if there is no domain, a domain has no workers, a CPU is listed
    /// twice, or a CPU is not one this process may run on.
    pub fn new(domains: Vec<Domain>) -> io::Result<Plan> {
        let invalid = |message| Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        if domains.is_empty() {
            return invalid("a plan needs a domain");
        }
        if domains.iter().any(|domain| domain.workers.is_empty()) {
            return invalid("a domain needs a worker");
        }

        let mut listed: Vec<u32> = domains
            .iter()
            .flat_map(|domain| domain.workers.iter().chain(&domain.sq_thread))
            .copied()
            .collect();
        listed.sort_unstable();
        if listed.windows(2).any(|pair| pair[0] == pair[1]) {
            return invalid("a plan lists a CPU twice");
        }
        let allowed = allowed_cpus()?;
        if listed.iter().any(|cpu| !allowed.contains(cpu)) {
            return invalid("a plan lists a CPU this process may not run on");
        }

        let seats: Vec<(usize, u32)> = domains
            .iter()
            .enumerate()
            .flat_map(|(index, domain)| domain.workers.iter().map(move |&cpu| (index, cpu)))
            .collect();
        Ok(Plan {
            inner: Arc::new(Inner {
                worker_cpus: seats.iter().map(|&(_, cpu)| cpu).collect(),
                seats,
                domains,
                built: AtomicBool::new(false),
            }),
        })
    }

    /// A plan for this machine: one domain per group of CPUs sharing a
    /// level-3 cache, restricted to the CPUs this process may run on.
    ///
    /// In each domain the highest-numbered CPU goes to the kernel submission
    /// thread and the others to workers. A domain with a single CPU gives it
    /// to a worker. A machine that reports no level-3 cache is one domain.
    pub fn detect(hyperthreads: Hyperthreads) -> io::Result<Plan> {
        let allowed = allowed_cpus()?;
        let mut groups: BTreeMap<Option<u32>, Vec<u32>> = BTreeMap::new();
        for &cpu in &allowed {
            if hyperthreads == Hyperthreads::Skip {
                let siblings = cpu_list(cpu, Path::new("topology/thread_siblings_list"))?;
                let lower_sibling = |sibling: &u32| *sibling < cpu && allowed.contains(sibling);
                if siblings.unwrap_or_default().iter().any(lower_sibling) {
                    continue;
                }
            }
            let cache = level_3_cache(cpu)?.and_then(|shared| shared.first().copied());
            groups.entry(cache).or_default().push(cpu);
        }

        let domains = groups
            .into_values()
            .map(|mut workers| {
                let sq_thread = if workers.len() > 1 {
                    workers.pop()
                } else {
                    None
                };
                Domain { workers, sq_thread }
            })
            .collect();
        Plan::new(domains)
    }

    pub fn domains(&self) -> &[Domain] {
        &self.inner.domains
    }

    /// Number of worker threads the plan has a CPU for.
    pub fn worker_count(&self) -> usize {
        self.inner.seats.len()
    }

    /// Builds a multi-threaded runtime from `builder` with the planned
    /// number of workers, each pinned to its CPU by the time this returns.
    ///
    /// This takes the builder's `on_thread_start` slot and its
    /// `worker_threads` setting. A plan builds one runtime; a second call
    /// fails.
    pub fn build(&self, builder: &mut Builder) -> io::Result<Runtime> {
        let invalid = |message| Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        if self.inner.built.swap(true, Ordering::Relaxed) {
            return invalid("a plan places the threads of one runtime");
        }

        // Every thread starts out confined to the workers' CPUs. If that
        // fails there is nobody to tell, and the thread keeps the CPUs of
        // the one that started it.
        let plan = self.clone();
        let runtime = builder
            .worker_threads(self.worker_count())
            .on_thread_start(move || drop(pin(&plan.inner.worker_cpus)))
            .build()?;
        if runtime.handle().runtime_flavor() != RuntimeFlavor::MultiThread {
            return invalid("a plan needs a multi-threaded runtime");
        }

        // A thread cannot tell at its start whether it will run a worker:
        // the runtime starts workers and threads for blocking work the same
        // way, and whichever thread gets to a worker's task first runs it.
        // So each worker is given its CPU by a task that runs on it.
        let (placed, results) = std::sync::mpsc::channel();
        runtime.run_on_each_worker(|worker| {
            let plan = self.clone();
            let placed = placed.clone();
            move || {
                let _ = placed.send(plan.place_current_worker(worker));
            }
        });
        drop(placed);
        for _ in 0..self.worker_count() {
            let result = results
                .recv()
                .map_err(|_| io::Error::other("a worker went away before it was placed"))?;
            result?;
        }
        Ok(runtime)
    }

    /// Pins the calling thread, which runs the worker with index `worker`,
    /// to that worker's CPU.
    fn place_current_worker(&self, worker: usize) -> io::Result<()> {
        let (_, cpu) = self.inner.seats[worker];
        pin(&[cpu])
    }

    /// Whether this plan has built a runtime, or is building one.
    pub(crate) fn has_built(&self) -> bool {
        self.inner.built.load(Ordering::Relaxed)
    }

    /// Index of the domain that the worker with index `worker` is in.
    pub(crate) fn domain_of_worker(&self, worker: usize) -> usize {
        self.inner.seats[worker].0
    }
}

impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Plan")
            .field("domains", &self.inner.domains)
            .finish()
    }
}

/// The CPUs the calling thread may run on, in ascending order.
fn allowed_cpus() -> io::Result<Vec<u32>> {
    // SAFETY: an all-zero `cpu_set_t` is a valid, empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is writable and of the size passed.
    if unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((0..libc::CPU_SETSIZE as u32)
        // SAFETY: the index is below `CPU_SETSIZE`.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu as usize, &set) })
        .collect())
}

/// Restricts the calling thread to `cpus`.
fn pin(cpus: &[u32]) -> io::Result<()> {
    // SAFETY: an all-zero `cpu_set_t` is a valid, empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &cpu in cpus {
        // SAFETY: a plan only holds CPUs the process is allowed to run on,
        // all of which are below `CPU_SETSIZE`.
        unsafe { libc::CPU_SET(cpu as usize, &mut set) };
    }
    // SAFETY: `set` is readable and of the size passed.
    if unsafe { libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Path of `file` in `cpu`'s directory in sysfs.
fn cpu_file(cpu: u32, file: &Path) -> PathBuf {
    Path::new("/sys/devices/system/cpu")
        .join(format!("cpu{cpu}"))
        .join(file)
}

/// The CPUs sharing `cpu`'s level-3 cache, if the machine reports one.
fn level_3_cache(cpu: u32) -> io::Result<Option<Vec<u32>>> {
    let entries = match std::fs::read_dir(cpu_file(cpu, Path::new("cache"))) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let cache = Path::new("cache").join(entry?.file_name());
        let level = cpu_file(cpu, &cache.join("level"));
        if std::fs::read_to_string(level).is_ok_and(|level| level.trim() == "3") {
            return cpu_list(cpu, &cache.join("shared_cpu_list"));
        }
    }
    Ok(None)
}

/// Reads a CPU list such as `0-3,16-19` from `cpu`'s directory in sysfs.
/// `None` if the machine does not have the file.
fn cpu_list(cpu: u32, file: &Path) -> io::Result<Option<Vec<u32>>> {
    let text = match std::fs::read_to_string(cpu_file(cpu, file)) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed CPU list in sysfs");
    let mut cpus = Vec::new();
    for range in text.trim().split(',').filter(|range| !range.is_empty()) {
        let (first, last) = range.split_once('-').unwrap_or((range, range));
        let first: u32 = first.parse().map_err(|_| malformed())?;
        let last: u32 = last.parse().map_err(|_| malformed())?;
        cpus.extend(first..=last);
    }
    Ok(Some(cpus))
}
