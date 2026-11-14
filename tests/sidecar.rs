use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use tokio_sidecar::{Config, Sidecar};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn socket_pair() -> (Arc<OwnedFd>, UnixStream) {
    let (reader, writer) = UnixStream::pair().unwrap();
    (Arc::new(OwnedFd::from(reader)), writer)
}

async fn wait_for_idle(sidecar: &Sidecar) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while sidecar.in_flight() != 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("operations still in flight");
}

#[test]
fn nop_on_a_worker() {
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        tokio::spawn(async move { sidecar.nop().await })
            .await
            .unwrap()
            .unwrap();
    });
}

/// The `block_on` thread is not a worker and owns no ring, so its
/// submissions are handed to a worker.
#[test]
fn nop_off_worker() {
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        for _ in 0..16 {
            sidecar.nop().await.unwrap();
        }
    });
}

#[test]
fn file_round_trip() {
    let path = std::env::temp_dir().join(format!("tokio-sidecar-{}", std::process::id()));
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    let fd = Arc::new(OwnedFd::from(file));

    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        tokio::spawn(async move {
            let (written, _) = sidecar
                .write_at(fd.clone(), b"hello sidecar".to_vec(), 0)
                .await;
            assert_eq!(written.unwrap(), 13);

            let mut buf = Vec::with_capacity(64);
            buf.extend_from_slice(b"> ");
            let (read, buf) = sidecar.read_at(fd, buf, 6).await;
            assert_eq!(read.unwrap(), 7);
            assert_eq!(buf, b"> sidecar");
        })
        .await
        .unwrap();
    });
}

/// A completion that arrives while every worker is parked has to wake the
/// owning worker through the ring's descriptor.
#[test]
fn completion_wakes_a_parked_worker() {
    let (reader, mut writer) = socket_pair();
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let read =
            tokio::spawn(async move { sidecar.read_at(reader, Vec::with_capacity(16), 0).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        std::thread::spawn(move || writer.write_all(b"late").unwrap());

        let (result, buf) = tokio::time::timeout(Duration::from_secs(5), read)
            .await
            .expect("worker was never woken")
            .unwrap();
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buf, b"late");
    });
}

#[test]
fn drop_cancels_on_the_same_worker() {
    let (reader, _writer) = socket_pair();
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let handle = sidecar.clone();
        tokio::spawn(async move {
            let read = handle.read_at(reader, Vec::with_capacity(16), 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), read)
                    .await
                    .is_err()
            );
        })
        .await
        .unwrap();
        wait_for_idle(&sidecar).await;
    });
}

/// The read is submitted by the `block_on` thread, so both the submission
/// and the cancel cross threads.
#[test]
fn drop_cancels_from_another_thread() {
    let (reader, _writer) = socket_pair();
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let read = sidecar.read_at(reader, Vec::with_capacity(16), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), read)
                .await
                .is_err()
        );
        wait_for_idle(&sidecar).await;
    });
}

/// Tasks migrate between workers at every yield, so operations are routinely
/// polled to completion on a different thread than the one that submitted.
#[test]
fn many_tasks_across_workers() {
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let tasks: Vec<_> = (0..256)
            .map(|_| {
                let sidecar = sidecar.clone();
                tokio::spawn(async move {
                    for _ in 0..200 {
                        sidecar.nop().await.unwrap();
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(sidecar.in_flight(), 0);
    });
}

/// More submissions in one batch than the ring has entries.
#[test]
fn submission_queue_overflow() {
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config { sq_entries: 8 }).await.unwrap();
        tokio::spawn(async move {
            let nops: Vec<_> = (0..1000).map(|_| tokio::spawn(sidecar.nop())).collect();
            for nop in nops {
                nop.await.unwrap().unwrap();
            }
        })
        .await
        .unwrap();
    });
}

#[test]
fn second_sidecar_is_refused() {
    runtime().block_on(async {
        let first = Sidecar::start(Config::default()).await.unwrap();
        let second = Sidecar::start(Config::default()).await.err().unwrap();
        assert_eq!(second.kind(), std::io::ErrorKind::AlreadyExists);
        tokio::spawn(async move { first.nop().await })
            .await
            .unwrap()
            .unwrap();
    });
}
