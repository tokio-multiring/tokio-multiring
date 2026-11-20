//! Alone in its binary, because it counts the process's kernel threads.

use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, DiskConfig, Domain, Hyperthreads, Plan, Sidecar};

/// The CPUs each kernel submission thread of this process may run on.
fn sq_thread_cpus() -> Vec<String> {
    let mut cpus: Vec<String> = std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| {
            let task = task.unwrap().path();
            let comm = std::fs::read_to_string(task.join("comm")).ok()?;
            comm.starts_with("iou-sqp").then(|| {
                let status = std::fs::read_to_string(task.join("status")).unwrap();
                let allowed = status
                    .lines()
                    .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
                    .unwrap();
                allowed.trim().to_owned()
            })
        })
        .collect();
    cpus.sort();
    cpus
}

/// Six workers' disk rings are driven by two kernel threads, not six, each
/// on the CPU its domain names.
#[test]
fn a_domain_shares_one_pinned_kernel_thread() {
    let detected = Plan::detect(Hyperthreads::Use).unwrap();
    let mut cpus = detected
        .domains()
        .iter()
        .flat_map(|domain| domain.workers.iter().chain(&domain.sq_thread).copied());
    let domains: Vec<Domain> = (0..2)
        .map(|_| Domain {
            workers: cpus.by_ref().take(3).collect(),
            sq_thread: cpus.next(),
        })
        .collect();
    let mut expected: Vec<String> = domains
        .iter()
        .map(|domain| domain.sq_thread.unwrap().to_string())
        .collect();
    expected.sort();
    let plan = Plan::new(domains).unwrap();

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let config = Config {
        buffers: Some(BufferPool::new(4096, 1).unwrap()),
        disk: Some(DiskConfig {
            sq_entries: 8,
            sq_thread_idle: Duration::from_millis(10),
            plan: plan.clone(),
        }),
        ..Config::default()
    };
    let sidecar = Sidecar::install(&mut builder, config);
    assert!(sq_thread_cpus().is_empty());

    let runtime = plan.build(&mut builder).unwrap();
    runtime.block_on(async {
        assert_eq!(sq_thread_cpus(), expected);
        sidecar.nop().await.unwrap();
    });
}
