use std::future::Future;
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio_sidecar::{Config, Sidecar};

/// A runtime of four workers that a sidecar drives, and the sidecar.
fn runtime(config: Config) -> (tokio::runtime::Runtime, Sidecar) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(4).enable_time();
    let sidecar = Sidecar::install(&mut builder, config);
    (builder.build().unwrap(), sidecar)
}

/// A runtime whose rings are flushed only when a worker goes to sleep or
/// the runtime has it look at its ring.
fn runtime_without_hook(config: Config) -> (tokio::runtime::Runtime, Sidecar) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(4).enable_time();
    let sidecar = Sidecar::install(&mut builder, config);
    builder.on_before_steal(|| ());
    (builder.build().unwrap(), sidecar)
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
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        tokio::spawn(async move { sidecar.nop().await })
            .await
            .unwrap()
            .unwrap();
    });
}

#[test]
fn works_without_the_hook() {
    let (reader, mut writer) = socket_pair();
    let (runtime, sidecar) = runtime_without_hook(Config::default());
    runtime.block_on(async {
        let read = tokio::spawn(async move {
            sidecar.nop().await.unwrap();
            sidecar.read_at(reader, Vec::with_capacity(16), 0).await
        });

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

/// Without the hook an operation only reaches the kernel as its worker goes
/// to sleep, and one that completes there and then has woken a task. The
/// worker has to run it before it sleeps: nothing else is going to wake it.
#[test]
fn a_worker_does_not_sleep_on_what_it_just_woke() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1);
    let sidecar = Sidecar::install(&mut builder, Config::default());
    builder.on_before_steal(|| ());
    let runtime = builder.build().unwrap();

    let (done, finished) = std::sync::mpsc::channel();
    runtime.spawn(async move {
        for _ in 0..100 {
            sidecar.nop().await.unwrap();
        }
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(Duration::from_secs(5))
        .expect("the worker went to sleep with a task to run");
}

/// Reschedules itself on every poll, so its worker never runs out of local
/// work and the hook never fires. (`yield_now` would not do: its wake-up is
/// deferred until the worker is otherwise idle.)
struct Spin;

impl Future for Spin {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A worker that stays busy never gets to the hook and never goes to sleep.
/// Its operations reach the kernel when the submission queue fills, and the
/// last of them when the runtime has the worker look at its ring.
#[test]
fn busy_worker_still_flushes() {
    const SQ_ENTRIES: usize = 4;
    const OPS: usize = 64;

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(1).enable_time();
    let config = Config {
        sq_entries: SQ_ENTRIES as u32,
        ..Config::default()
    };
    let sidecar = Sidecar::install(&mut builder, config);
    let runtime = builder.build().unwrap();
    runtime.block_on(async {
        tokio::spawn(async move {
            let spinner = tokio::spawn(Spin);
            let done = Arc::new(AtomicUsize::new(0));
            let nops: Vec<_> = (0..OPS)
                .map(|_| {
                    let sidecar = sidecar.clone();
                    let done = done.clone();
                    tokio::spawn(async move {
                        sidecar.nop().await.unwrap();
                        done.fetch_add(1, Ordering::Relaxed);
                    })
                })
                .collect();

            tokio::time::timeout(Duration::from_secs(5), async {
                while done.load(Ordering::Relaxed) < OPS {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("a busy worker's ring was not flushed");

            spinner.abort();
            for nop in nops {
                nop.await.unwrap();
            }
        })
        .await
        .unwrap();
    });
}

/// The `block_on` thread is not a worker and owns no ring, so its
/// submissions are handed to a worker.
#[test]
fn nop_off_worker() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
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

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
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
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
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
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
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
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let read = sidecar.read_at(reader, Vec::with_capacity(16), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), read)
                .await
                .is_err()
        );
        wait_for_idle(&sidecar).await;
    });
}

/// The read is submitted on a worker's own ring and dropped by the
/// `block_on` thread, so only the cancel crosses threads.
#[test]
fn drop_on_another_thread_cancels_a_local_submission() {
    let (reader, _writer) = socket_pair();
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let handle = sidecar.clone();
        let (parked, read) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut read = Box::pin(handle.read_at(reader, Vec::with_capacity(16), 0));
            std::future::poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(parked.send(read).is_ok());
        })
        .await
        .unwrap();
        let read = read.await.unwrap();
        assert_eq!(sidecar.in_flight(), 1);

        drop(read);
        wait_for_idle(&sidecar).await;
    });
}

/// Futures are submitted on one worker and then polled or dropped on
/// others, right as their completions are being delivered.
#[test]
fn completion_races_with_poll_and_drop_on_other_threads() {
    type Boxed = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>;

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Boxed>();

        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Boxed>();
                let consumer = tokio::spawn(async move {
                    let mut seen = 0u32;
                    while let Some(op) = rx.recv().await {
                        seen += 1;
                        if seen % 2 == 0 {
                            drop(op);
                        } else {
                            op.await.unwrap();
                        }
                    }
                });
                (tx, consumer)
            })
            .collect();
        let fan_out = tokio::spawn({
            let senders: Vec<_> = consumers.iter().map(|(tx, _)| tx.clone()).collect();
            async move {
                let mut next = 0;
                while let Some(op) = rx.recv().await {
                    senders[next % senders.len()].send(op).ok().unwrap();
                    next += 1;
                }
            }
        });

        let producers: Vec<_> = (0..3)
            .map(|_| {
                let sidecar = sidecar.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    for round in 0..20_000 {
                        let mut op: Boxed = Box::pin(sidecar.nop());
                        std::future::poll_fn(|cx| {
                            let _ = op.as_mut().poll(cx);
                            Poll::Ready(())
                        })
                        .await;
                        tx.send(op).ok().unwrap();
                        if round % 8 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect();
        drop(tx);

        for producer in producers {
            producer.await.unwrap();
        }
        fan_out.await.unwrap();
        for (tx, consumer) in consumers {
            drop(tx);
            consumer.await.unwrap();
        }
        wait_for_idle(&sidecar).await;
    });
}

/// A read is left pending on a worker's ring. Another thread then completes
/// it and, a varying few microseconds later, drops the future or polls it
/// with a new waker, so that this lands before, during and after the
/// worker's delivery of the result.
#[test]
fn delivery_races_with_drop_and_repoll() {
    type Read = Pin<Box<dyn Future<Output = (std::io::Result<usize>, Vec<u8>)> + Send>>;

    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
        for round in 0..3000u32 {
            let (reader, mut writer) = socket_pair();
            let (parked, read) = tokio::sync::oneshot::channel::<Read>();
            let handle = sidecar.clone();
            tokio::spawn(async move {
                let mut read: Read = Box::pin(handle.read_at(reader, Vec::with_capacity(4), 0));
                std::future::poll_fn(|cx| {
                    assert!(read.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert!(parked.send(read).is_ok());
            });
            let mut read = read.await.unwrap();

            writer.write_all(b"x").unwrap();
            let delay = Duration::from_micros(u64::from(round % 40));
            let start = std::time::Instant::now();
            while start.elapsed() < delay {
                std::hint::spin_loop();
            }

            if round % 2 == 0 {
                drop(read);
            } else {
                let early = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;
                let (result, buf) = match early {
                    Poll::Ready(output) => output,
                    Poll::Pending => read.await,
                };
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buf, b"x");
            }
        }
        wait_for_idle(&sidecar).await;
    });
}

/// Tasks migrate between workers at every yield, so operations are routinely
/// polled to completion on a different thread than the one that submitted.
#[test]
fn many_tasks_across_workers() {
    let (runtime, sidecar) = runtime(Config::default());
    runtime.block_on(async {
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
    let (runtime, sidecar) = runtime(Config {
        sq_entries: 8,
        ..Config::default()
    });
    runtime.block_on(async {
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
fn a_sidecar_drives_one_runtime() {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let sidecar = Sidecar::install(&mut builder, Config::default());
    let runtime = builder.build().unwrap();
    let second = builder.build().err().unwrap();
    assert_eq!(second.kind(), std::io::ErrorKind::AlreadyExists);

    runtime.block_on(async {
        tokio::spawn(async move { sidecar.nop().await })
            .await
            .unwrap()
            .unwrap();
    });
}
