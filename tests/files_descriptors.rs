//! Alone in its binary, because it counts the process's open descriptors.

use std::ffi::CString;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use tokio_sidecar::{Config, Sidecar};

/// The open completes, and its future is dropped without ever being polled
/// again. The descriptor must be closed, not leaked.
#[test]
fn dropped_open_closes_the_file() {
    fn open_descriptors() -> usize {
        std::fs::read_dir("/proc/self/fd").unwrap().count()
    }

    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("files-dropped-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("file"), b"").unwrap();
    let dir = Arc::new(OwnedFd::from(std::fs::File::open(&path).unwrap()));
    let file = CString::new("file").unwrap();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal)
        .build()
        .unwrap();
    runtime.block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        // On a worker, so that the open is with the kernel as soon as it has
        // been polled.
        tokio::spawn(async move {
            sidecar.nop().await.unwrap();
            let before = open_descriptors();

            for _ in 0..50 {
                let open = sidecar.open_at(dir.clone(), file.clone(), libc::O_RDONLY, 0);
                let mut open = Box::pin(open);
                std::future::poll_fn(|cx| {
                    assert!(open.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                assert_eq!(sidecar.in_flight(), 1);
                while sidecar.in_flight() != 0 {
                    tokio::task::yield_now().await;
                }
                drop(open);
            }
            assert_eq!(open_descriptors(), before);
        })
        .await
        .unwrap();
    });
    std::fs::remove_file(path.join("file")).unwrap();
    std::fs::remove_dir(&path).unwrap();
}
