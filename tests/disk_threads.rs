//! Alone in its binary, because it counts the process's kernel threads.

use std::time::Duration;

use tokio_sidecar::{BufferPool, Config, DiskConfig, Sidecar, SqThread};

fn sq_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter(|task| {
            let comm = task.as_ref().unwrap().path().join("comm");
            std::fs::read_to_string(comm).is_ok_and(|comm| comm.starts_with("iou-sqp"))
        })
        .count()
}

/// Six workers' disk rings are driven by two kernel threads, not six.
#[test]
fn workers_share_kernel_threads() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal)
        .build()
        .unwrap();
    let pool = BufferPool::new(4096, 1).unwrap();
    assert_eq!(sq_threads(), 0);

    runtime.block_on(async {
        let sidecar = Sidecar::start(Config {
            buffers: Some(pool),
            disk: Some(DiskConfig {
                sq_entries: 8,
                sq_thread_idle: Duration::from_millis(10),
                sq_threads: vec![
                    SqThread {
                        cpu: Some(0),
                        workers: vec![0, 1, 2],
                    },
                    SqThread {
                        cpu: None,
                        workers: vec![3, 4, 5],
                    },
                ],
            }),
            ..Config::default()
        })
        .await
        .unwrap();
        assert_eq!(sq_threads(), 2);
        sidecar.nop().await.unwrap();
    });
}
