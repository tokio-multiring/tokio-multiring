use std::collections::BTreeSet;

use tokio_multiring::{Domain, Hyperthreads, Plan};

/// The CPUs the calling thread may run on.
fn affinity() -> Vec<u32> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) },
        0
    );
    (0..libc::CPU_SETSIZE as u32)
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu as usize, &set) })
        .collect()
}

fn sysfs_cpu_list(cpu: u32, file: &str) -> Option<Vec<u32>> {
    let text = std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{cpu}/{file}")).ok()?;
    let mut cpus = Vec::new();
    for range in text.trim().split(',') {
        let (first, last) = range.split_once('-').unwrap_or((range, range));
        cpus.extend(first.parse::<u32>().unwrap()..=last.parse().unwrap());
    }
    Some(cpus)
}

fn cpus_of(domain: &Domain) -> Vec<u32> {
    domain
        .workers
        .iter()
        .chain(&domain.sq_thread)
        .copied()
        .collect()
}

fn domain(workers: &[u32], sq_thread: Option<u32>) -> Domain {
    Domain {
        workers: workers.to_vec(),
        sq_thread,
    }
}

fn runtime(plan: &Plan) -> tokio::runtime::Runtime {
    plan.build(tokio::runtime::Builder::new_multi_thread().enable_all())
        .unwrap()
}

/// Two domains over the first CPUs this process may use, with workers
/// deliberately out of order and dedicated `sq_thread` CPUs set aside.
/// Uses 6 CPUs (2 domains of 2 workers + 1 sq_thread) when available,
/// falling back to 4 CPUs (2 domains of 1 worker + 1 sq_thread) on smaller
/// hosts such as 4-vCPU CI runners.
fn multi_domain_plan() -> (Plan, Vec<u32>) {
    let cpus = affinity();
    if cpus.len() >= 6 {
        let plan = Plan::new(vec![
            domain(&[cpus[2], cpus[0]], Some(cpus[4])),
            domain(&[cpus[5], cpus[1]], Some(cpus[3])),
        ])
        .unwrap();
        (plan, vec![cpus[0], cpus[1], cpus[2], cpus[5]])
    } else if cpus.len() >= 4 {
        let plan = Plan::new(vec![
            domain(&[cpus[2]], Some(cpus[0])),
            domain(&[cpus[3]], Some(cpus[1])),
        ])
        .unwrap();
        (plan, vec![cpus[2], cpus[3]])
    } else {
        panic!(
            "need at least 4 CPUs for multi-domain plan tests, got {}",
            cpus.len()
        );
    }
}

/// Blocking work is started the moment the runtime exists, so its threads
/// race the workers to start. The workers must still get the seats.
#[test]
fn every_worker_gets_a_cpu_of_its_own() {
    let (plan, expected_workers) = multi_domain_plan();
    assert_eq!(plan.worker_count(), expected_workers.len());

    let runtime = runtime(&plan);
    let blocking: Vec<_> = (0..16)
        .map(|_| runtime.spawn_blocking(|| affinity().len()))
        .collect();
    runtime.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::run_on_each_worker(|_| {
            let tx = tx.clone();
            move || tx.send(affinity()).unwrap()
        });
        drop(tx);

        let mut pinned = BTreeSet::new();
        while let Some(worker) = rx.recv().await {
            assert_eq!(worker.len(), 1, "worker may run on {worker:?}");
            assert!(pinned.insert(worker[0]), "two workers on CPU {}", worker[0]);
        }
        let planned: BTreeSet<u32> = expected_workers.iter().copied().collect();
        assert_eq!(pinned, planned);

        for thread in blocking {
            assert_eq!(thread.await.unwrap(), expected_workers.len());
        }
    });
}

/// Threads started for blocking work stay off the CPUs set aside for kernel
/// submission threads, and off everything outside the plan.
#[test]
fn other_threads_stay_on_the_workers_cpus() {
    let (plan, mut planned) = multi_domain_plan();
    planned.sort_unstable();
    runtime(&plan).block_on(async {
        let blocking: Vec<_> = (0..8)
            .map(|_| tokio::task::spawn_blocking(affinity))
            .collect();
        for thread in blocking {
            let mut allowed = thread.await.unwrap();
            allowed.sort_unstable();
            assert_eq!(allowed, planned);
        }
    });
}

#[test]
fn detected_domains_follow_the_level_3_cache() {
    let allowed = affinity();
    let plan = Plan::detect(Hyperthreads::Use).unwrap();

    let mut seen = BTreeSet::new();
    for domain in plan.domains() {
        let cpus = cpus_of(domain);
        assert!(!domain.workers.is_empty());
        assert_eq!(domain.sq_thread.is_some(), cpus.len() > 1);
        for &cpu in &cpus {
            assert!(allowed.contains(&cpu));
            assert!(seen.insert(cpu), "CPU {cpu} is in two domains");
        }
        // Any level-3 cache the machine reports is index3 in sysfs.
        if let Some(shared) = sysfs_cpu_list(cpus[0], "cache/index3/shared_cpu_list") {
            assert!(cpus.iter().all(|cpu| shared.contains(cpu)));
        }
    }
    // Nothing the process may use is left out.
    assert_eq!(seen, allowed.iter().copied().collect());
}

#[test]
fn skipping_hyperthreads_leaves_one_cpu_per_core() {
    let plan = Plan::detect(Hyperthreads::Skip).unwrap();
    let cpus: Vec<u32> = plan.domains().iter().flat_map(cpus_of).collect();

    let mut cores = BTreeSet::new();
    for &cpu in &cpus {
        let siblings = sysfs_cpu_list(cpu, "topology/thread_siblings_list").unwrap_or(vec![cpu]);
        assert!(cores.insert(siblings), "two CPUs of one core in the plan");
    }
    let with_hyperthreads = Plan::detect(Hyperthreads::Use).unwrap();
    let all: usize = with_hyperthreads
        .domains()
        .iter()
        .map(|domain| cpus_of(domain).len())
        .sum();
    assert!(cpus.len() <= all);
    assert_eq!(plan.domains().len(), with_hyperthreads.domains().len());
}

#[test]
fn bad_plans_are_refused() {
    let cpus = affinity();
    let not_allowed = (0..libc::CPU_SETSIZE as u32)
        .find(|cpu| !cpus.contains(cpu))
        .expect("a CPU number this process may not use");

    for domains in [
        vec![],
        vec![domain(&[], Some(cpus[0]))],
        vec![domain(&[cpus[0], cpus[0]], None)],
        vec![domain(&[cpus[0]], Some(cpus[0]))],
        vec![domain(&[cpus[0]], None), domain(&[cpus[1]], Some(cpus[0]))],
        vec![domain(&[not_allowed], None)],
        vec![domain(&[cpus[0]], Some(not_allowed))],
        vec![domain(&[u32::MAX], None)],
    ] {
        let error = Plan::new(domains.clone())
            .err()
            .unwrap_or_else(|| panic!("{domains:?}"));
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
}

#[test]
fn a_plan_builds_one_runtime() {
    let plan = Plan::new(vec![domain(&[affinity()[0]], None)]).unwrap();
    let _runtime = runtime(&plan);
    let error = plan
        .build(&mut tokio::runtime::Builder::new_multi_thread())
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn single_threaded_runtimes_are_refused() {
    let plan = Plan::new(vec![domain(&[affinity()[0]], None)]).unwrap();
    let error = plan
        .build(&mut tokio::runtime::Builder::new_current_thread())
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
