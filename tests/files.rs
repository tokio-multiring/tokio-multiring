use std::ffi::CString;
use std::io::ErrorKind;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use tokio_sidecar::{Config, Sidecar};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_before_steal(tokio_sidecar::before_steal)
        .build()
        .unwrap()
}

/// A fresh directory, and a descriptor for it.
fn scratch_dir(name: &str) -> (std::path::PathBuf, Arc<OwnedFd>) {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("files-{name}-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    let dir = OwnedFd::from(std::fs::File::open(&path).unwrap());
    (path, Arc::new(dir))
}

fn c(path: &str) -> CString {
    CString::new(path).unwrap()
}

#[test]
fn a_files_life() {
    let (path, dir) = scratch_dir("life");
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();

        sidecar
            .mkdir_at(dir.clone(), c("sub"), 0o755)
            .await
            .unwrap();
        let again = sidecar.mkdir_at(dir.clone(), c("sub"), 0o755).await;
        assert_eq!(again.unwrap_err().kind(), ErrorKind::AlreadyExists);
        assert!(sidecar.stat_at(dir.clone(), c("sub")).await.unwrap().is_dir);

        let flags = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL;
        let file = sidecar.open_at(dir.clone(), c("sub/draft"), flags, 0o644);
        let file = Arc::new(file.await.unwrap());
        let (written, _) = sidecar
            .write_at(file.clone(), b"hello, files".to_vec(), 0)
            .await;
        assert_eq!(written.unwrap(), 12);

        sidecar.truncate(file.clone(), 5).await.unwrap();
        sidecar.fdatasync(file.clone()).await.unwrap();
        let stat = sidecar.stat(file.clone()).await.unwrap();
        assert_eq!(stat.size, 5);
        assert!(!stat.is_dir);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!((now - 60..=now + 60).contains(&stat.modified.0));

        sidecar
            .rename_at(dir.clone(), c("sub/draft"), c("sub/final"))
            .await
            .unwrap();
        let gone = sidecar.stat_at(dir.clone(), c("sub/draft")).await;
        assert_eq!(gone.unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(
            sidecar.stat_at(dir.clone(), c("sub/final")).await.unwrap(),
            stat
        );
        assert_eq!(std::fs::read(path.join("sub/final")).unwrap(), b"hello");

        let busy = sidecar.rmdir_at(dir.clone(), c("sub")).await;
        assert_eq!(busy.unwrap_err().kind(), ErrorKind::DirectoryNotEmpty);
        sidecar
            .unlink_at(dir.clone(), c("sub/final"))
            .await
            .unwrap();
        let gone = sidecar.unlink_at(dir.clone(), c("sub/final")).await;
        assert_eq!(gone.unwrap_err().kind(), ErrorKind::NotFound);
        sidecar.rmdir_at(dir.clone(), c("sub")).await.unwrap();
    });
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
    std::fs::remove_dir(&path).unwrap();
}

#[test]
fn opening_a_missing_file_fails() {
    let (path, dir) = scratch_dir("missing");
    runtime().block_on(async {
        let sidecar = Sidecar::start(Config::default()).await.unwrap();
        let missing = sidecar.open_at(dir, c("nope"), libc::O_RDONLY, 0).await;
        assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    });
    std::fs::remove_dir(&path).unwrap();
}
