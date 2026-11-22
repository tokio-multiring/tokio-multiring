use std::ffi::CString;
use std::io::ErrorKind;
use std::os::fd::OwnedFd;
use std::sync::Arc;

use tokio_multiring::{Config, MultiRing};

/// A runtime of two workers that a multiring drives, and the multiring.
fn runtime(config: Config) -> (tokio::runtime::Runtime, MultiRing) {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.worker_threads(2).enable_time();
    let multiring = MultiRing::install(&mut builder, config);
    (builder.build().unwrap(), multiring)
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
    let (runtime, multiring) = runtime(Config::default());
    runtime.block_on(async {
        multiring
            .mkdir_at(dir.clone(), c("sub"), 0o755)
            .await
            .unwrap();
        let again = multiring.mkdir_at(dir.clone(), c("sub"), 0o755).await;
        assert_eq!(again.unwrap_err().kind(), ErrorKind::AlreadyExists);
        assert!(
            multiring
                .stat_at(dir.clone(), c("sub"))
                .await
                .unwrap()
                .is_dir
        );

        let flags = libc::O_RDWR | libc::O_CREAT | libc::O_EXCL;
        let file = multiring.open_at(dir.clone(), c("sub/draft"), flags, 0o644);
        let file = Arc::new(file.await.unwrap());
        let (written, _) = multiring
            .write_at(file.clone(), b"hello, files".to_vec(), 0)
            .await;
        assert_eq!(written.unwrap(), 12);

        multiring.truncate(file.clone(), 5).await.unwrap();
        multiring.fdatasync(file.clone()).await.unwrap();
        let stat = multiring.stat(file.clone()).await.unwrap();
        assert_eq!(stat.size, 5);
        assert!(!stat.is_dir);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!((now - 60..=now + 60).contains(&stat.modified.0));

        multiring
            .rename_at(dir.clone(), c("sub/draft"), c("sub/final"))
            .await
            .unwrap();
        let gone = multiring.stat_at(dir.clone(), c("sub/draft")).await;
        assert_eq!(gone.unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(
            multiring
                .stat_at(dir.clone(), c("sub/final"))
                .await
                .unwrap(),
            stat
        );
        assert_eq!(std::fs::read(path.join("sub/final")).unwrap(), b"hello");

        let busy = multiring.rmdir_at(dir.clone(), c("sub")).await;
        assert_eq!(busy.unwrap_err().kind(), ErrorKind::DirectoryNotEmpty);
        multiring
            .unlink_at(dir.clone(), c("sub/final"))
            .await
            .unwrap();
        let gone = multiring.unlink_at(dir.clone(), c("sub/final")).await;
        assert_eq!(gone.unwrap_err().kind(), ErrorKind::NotFound);
        multiring.rmdir_at(dir.clone(), c("sub")).await.unwrap();
    });
    assert_eq!(std::fs::read_dir(&path).unwrap().count(), 0);
    std::fs::remove_dir(&path).unwrap();
}

#[test]
fn opening_a_missing_file_fails() {
    let (path, dir) = scratch_dir("missing");
    let (runtime, multiring) = runtime(Config::default());
    runtime.block_on(async {
        let missing = multiring.open_at(dir, c("nope"), libc::O_RDONLY, 0).await;
        assert_eq!(missing.unwrap_err().kind(), ErrorKind::NotFound);
    });
    std::fs::remove_dir(&path).unwrap();
}
