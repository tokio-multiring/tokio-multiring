//! Objects on disk.
//!
//! An upload is written to a file of its own in the bucket's directory and
//! renamed over the object's name when it is complete, so a reader sees
//! either the previous object or the new one, never a part of one. All file
//! data goes through `O_DIRECT` with the sidecar's registered buffers.

use std::ffi::CString;
use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio_sidecar::{FileStat, FixedBuf, Sidecar};

use crate::error::ApiError;
use crate::name::{Bucket, Object, UPLOAD_PREFIX};

/// Offsets and lengths of direct I/O are multiples of this. It is a multiple
/// of every logical block size in use, and buffer sizes are multiples of it.
pub(crate) const BLOCK: usize = 4096;

static ZEROS: [u8; BLOCK] = [0; BLOCK];

pub(crate) struct Store {
    sidecar: Sidecar,
    root: Arc<OwnedFd>,
    next_upload: AtomicU64,
}

/// An object open for reading.
pub(crate) struct Stored {
    pub(crate) file: Arc<OwnedFd>,
    pub(crate) stat: FileStat,
}

/// An object on its way in.
pub(crate) struct Upload {
    file: Arc<OwnedFd>,
    /// Where the data is while the upload lasts.
    staging: CString,
    object: CString,
    /// Bytes of the object written so far.
    size: u64,
    /// Whether the file is longer than the object, by the padding that
    /// brought the last write up to a whole block.
    padded: bool,
}

/// Why the store could not do what was asked.
pub(crate) enum StoreError {
    Api(ApiError),
    Io(io::Error),
}

impl From<io::Error> for StoreError {
    fn from(error: io::Error) -> Self {
        StoreError::Io(error)
    }
}

impl From<ApiError> for StoreError {
    fn from(error: ApiError) -> Self {
        StoreError::Api(error)
    }
}

impl Store {
    /// Opens the store rooted at the directory `root`, and removes what
    /// uploads cut short by an earlier run left behind.
    pub(crate) fn open(sidecar: Sidecar, root: &Path) -> io::Result<Store> {
        for bucket in std::fs::read_dir(root)? {
            let bucket = bucket?;
            if !bucket.file_type()?.is_dir() {
                continue;
            }
            for file in std::fs::read_dir(bucket.path())? {
                let file = file?;
                if file
                    .file_name()
                    .as_encoded_bytes()
                    .starts_with(UPLOAD_PREFIX.as_bytes())
                {
                    std::fs::remove_file(file.path())?;
                }
            }
        }
        Ok(Store {
            sidecar,
            root: Arc::new(OwnedFd::from(std::fs::File::open(root)?)),
            next_upload: AtomicU64::new(0),
        })
    }

    pub(crate) async fn create_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let created = self
            .sidecar
            .mkdir_at(self.root.clone(), bucket.path(), 0o755);
        match created.await {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                Err(ApiError::BUCKET_EXISTS.into())
            }
            other => Ok(other?),
        }
    }

    pub(crate) async fn delete_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let removed = self.sidecar.rmdir_at(self.root.clone(), bucket.path());
        match removed.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ApiError::NO_SUCH_BUCKET.into())
            }
            Err(error) if error.kind() == io::ErrorKind::DirectoryNotEmpty => {
                Err(ApiError::BUCKET_NOT_EMPTY.into())
            }
            other => Ok(other?),
        }
    }

    pub(crate) async fn head_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let stat = self.sidecar.stat_at(self.root.clone(), bucket.path());
        match stat.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ApiError::NO_SUCH_BUCKET.into())
            }
            other => Ok(other.map(drop)?),
        }
    }

    /// What a missing file means: the bucket is gone, or only the key.
    async fn missing(&self, object: &Object) -> StoreError {
        match self.head_bucket(&object.bucket).await {
            Ok(()) => ApiError::NO_SUCH_KEY.into(),
            Err(error) => error,
        }
    }

    pub(crate) async fn open_object(&self, object: &Object) -> Result<Stored, StoreError> {
        let flags = libc::O_RDONLY | libc::O_DIRECT;
        let opened = self
            .sidecar
            .open_at(self.root.clone(), object.path(), flags, 0);
        let file = match opened.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(self.missing(object).await);
            }
            other => Arc::new(other?),
        };
        let stat = self.sidecar.stat(file.clone()).await?;
        Ok(Stored { file, stat })
    }

    /// Looks the object up without opening it.
    pub(crate) async fn head_object(&self, object: &Object) -> Result<FileStat, StoreError> {
        let stat = self.sidecar.stat_at(self.root.clone(), object.path());
        match stat.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(self.missing(object).await)
            }
            other => Ok(other?),
        }
    }

    /// Removes the object. Removing one that does not exist succeeds, as it
    /// does in S3.
    pub(crate) async fn delete_object(&self, object: &Object) -> Result<(), StoreError> {
        let removed = self.sidecar.unlink_at(self.root.clone(), object.path());
        match removed.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match self.missing(object).await {
                    StoreError::Api(ApiError::NO_SUCH_KEY) => Ok(()),
                    error => Err(error),
                }
            }
            other => Ok(other?),
        }
    }

    /// Reads up to `limit` bytes from `offset`, both multiples of [`BLOCK`],
    /// into the spare capacity of `buf`.
    pub(crate) async fn read(
        &self,
        stored: &Stored,
        buf: FixedBuf,
        offset: u64,
        limit: usize,
    ) -> (io::Result<usize>, FixedBuf) {
        let file = stored.file.clone();
        if self.sidecar.has_disk_rings() {
            self.sidecar.read_polled_at(file, buf, offset, limit).await
        } else {
            self.sidecar.read_fixed_at(file, buf, offset, limit).await
        }
    }

    pub(crate) async fn begin(&self, object: &Object) -> Result<Upload, StoreError> {
        let number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let staging = object.bucket.file(&format!("{UPLOAD_PREFIX}{number}"));
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_DIRECT;
        let opened = self
            .sidecar
            .open_at(self.root.clone(), staging.clone(), flags, 0o644);
        let file = match opened.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ApiError::NO_SUCH_BUCKET.into());
            }
            other => Arc::new(other?),
        };
        Ok(Upload {
            file,
            staging,
            object: object.path(),
            size: 0,
            padded: false,
        })
    }

    /// Appends the contents of `buf` to the upload. Every call but the last
    /// must bring a whole number of blocks.
    pub(crate) async fn write(
        &self,
        upload: &mut Upload,
        mut buf: FixedBuf,
    ) -> (io::Result<()>, FixedBuf) {
        assert!(!upload.padded, "a short write ends the upload");
        let data = buf.len();
        let padding = data.next_multiple_of(BLOCK) - data;
        buf.extend_from_slice(&ZEROS[..padding]);

        // Writes start on a block boundary, so the padding of the last one
        // is never overwritten: it is cut off when the upload finishes.
        let file = upload.file.clone();
        let (written, buf) = if self.sidecar.has_disk_rings() {
            self.sidecar.write_polled_at(file, buf, upload.size).await
        } else {
            self.sidecar.write_fixed_at(file, buf, upload.size).await
        };
        let result = match written {
            Ok(written) if written == buf.len() => {
                upload.size += data as u64;
                upload.padded = padding > 0;
                Ok(())
            }
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, buf)
    }

    /// Makes the upload the object. The data is on the device by the time
    /// this returns.
    pub(crate) async fn finish(&self, upload: Upload) -> io::Result<FileStat> {
        let finished = self.publish(&upload).await;
        if finished.is_err() {
            self.abort(upload).await;
        }
        finished
    }

    async fn publish(&self, upload: &Upload) -> io::Result<FileStat> {
        if upload.padded {
            self.sidecar
                .truncate(upload.file.clone(), upload.size)
                .await?;
        }
        self.sidecar.fdatasync(upload.file.clone()).await?;
        // Renaming does not touch the modification time, so what is read
        // here is what later lookups of the object report.
        let stat = self.sidecar.stat(upload.file.clone()).await?;
        let renamed = self.sidecar.rename_at(
            self.root.clone(),
            upload.staging.clone(),
            upload.object.clone(),
        );
        renamed.await?;
        Ok(stat)
    }

    /// Throws the upload away.
    pub(crate) async fn abort(&self, upload: Upload) {
        let removed = self
            .sidecar
            .unlink_at(self.root.clone(), upload.staging.clone());
        if let Err(error) = removed.await {
            log::warn!("could not remove {:?}: {error}", upload.staging);
        }
    }
}
