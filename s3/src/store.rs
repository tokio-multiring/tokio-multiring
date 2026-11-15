//! Objects on disk.
//!
//! An object is striped over the drives as [`crate::stripe`] lays out. Its
//! first part is a file named after the key, on the drive the key hashes to.
//! Its other parts are files with names of their own, which only the header
//! in the first part knows.
//!
//! That makes the first part the whole truth about a key. An upload writes
//! every part under a temporary name, makes the others durable under their
//! final names, and then renames the first one over the key. A reader that
//! opens the key therefore finds either the previous object or the new one,
//! each with its own set of parts, never a mixture. All file data goes
//! through `O_DIRECT` with the sidecar's registered buffers.

use std::ffi::CString;
use std::io;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tokio_sidecar::{BufferPool, FixedBuf, Sidecar};

use crate::error::ApiError;
use crate::join::join_all;
use crate::name::{Bucket, Object, PART_PREFIX, UPLOAD_PREFIX};
use crate::stripe::{BLOCK, Geometry, HEADER, Header, Plan};

static ZEROS: [u8; BLOCK] = [0; BLOCK];

/// How often opening an object starts over because the object was replaced
/// between reading its header and opening its parts.
const OPEN_ATTEMPTS: usize = 8;

/// Number of locks that changes to keys are spread over.
const COMMIT_LOCKS: usize = 256;

pub(crate) struct Store {
    sidecar: Sidecar,
    buffers: BufferPool,
    /// The root directory on each drive.
    drives: Vec<Arc<OwnedFd>>,
    geometry: Geometry,
    /// Told apart from every other run, so that the names given to parts
    /// are never used twice.
    run: u64,
    next_upload: AtomicU64,
    /// Held while changing what a key names. A key always takes the same
    /// lock.
    commits: Box<[Mutex<()>]>,
}

/// What is known about an object without reading its data.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Meta {
    /// Size in bytes.
    pub(crate) size: u64,
    /// When it was stored, as seconds and nanoseconds since the Unix epoch.
    pub(crate) modified: (i64, u32),
}

/// An object open for reading.
pub(crate) struct Stored {
    /// Its parts, in order.
    files: Vec<Arc<OwnedFd>>,
    pub(crate) meta: Meta,
}

impl Stored {
    /// How many parts the object has, which is how many buffers reading it
    /// takes.
    pub(crate) fn parts(&self) -> usize {
        self.files.len()
    }
}

/// An object on its way in.
pub(crate) struct Upload {
    id: u128,
    /// What it becomes when it is complete.
    object: Object,
    /// Index of the drive its first part is on.
    first: usize,
    /// The parts written to so far.
    parts: Vec<Option<Staged>>,
    /// Bytes of the object written so far.
    size: u64,
    /// Whether the last round written was a short one, after which there
    /// can be no other.
    ended: bool,
}

/// A part that is being written.
struct Staged {
    file: Arc<OwnedFd>,
    /// Where it is while the upload lasts.
    path: CString,
    /// Whether the file is longer than its data, by the padding that
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

fn part_name(id: u128, part: usize) -> String {
    format!("{PART_PREFIX}{id:032x}-{part}")
}

fn now() -> (i64, u32) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs() as i64, now.subsec_nanos())
}

fn random() -> io::Result<u64> {
    let mut bytes = [0; 8];
    // SAFETY: the buffer is writable and of the length passed.
    let filled = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    if filled != bytes.len() as isize {
        return Err(io::Error::last_os_error());
    }
    Ok(u64::from_ne_bytes(bytes))
}

impl Store {
    /// Opens the store kept in the directories `roots`, one per drive, and
    /// removes what uploads cut short by an earlier run left behind.
    ///
    /// The order of the roots and the unit are part of how objects are laid
    /// out: objects stored under one arrangement cannot be read under
    /// another.
    pub(crate) fn open(sidecar: Sidecar, roots: &[PathBuf], unit: usize) -> io::Result<Store> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidInput, message);
        let buffers = sidecar
            .buffers()
            .cloned()
            .ok_or_else(|| invalid("the sidecar has no buffer pool"))?;
        let geometry = Geometry::new(roots.len(), unit, buffers.buf_size()).ok_or_else(|| {
            invalid(
                "a store needs a root, a stripe unit that is a multiple of 4096, \
                 and buffers that are a multiple of the unit",
            )
        })?;
        if buffers.count() < roots.len() {
            return Err(invalid("a store needs a buffer for every root"));
        }

        let mut drives = Vec::new();
        for root in roots {
            for bucket in std::fs::read_dir(root)? {
                let bucket = bucket?;
                if !bucket.file_type()?.is_dir() {
                    continue;
                }
                for file in std::fs::read_dir(bucket.path())? {
                    let file = file?;
                    let name = file.file_name();
                    if name
                        .as_encoded_bytes()
                        .starts_with(UPLOAD_PREFIX.as_bytes())
                    {
                        std::fs::remove_file(file.path())?;
                    }
                }
            }
            drives.push(Arc::new(OwnedFd::from(std::fs::File::open(root)?)));
        }
        Ok(Store {
            sidecar,
            buffers,
            drives,
            geometry,
            run: random()?,
            next_upload: AtomicU64::new(0),
            commits: (0..COMMIT_LOCKS).map(|_| Mutex::new(())).collect(),
        })
    }

    pub(crate) fn geometry(&self) -> &Geometry {
        &self.geometry
    }

    /// The root of the drive that part `part` is on, of an object whose
    /// first part is on drive `first`.
    fn drive(&self, first: usize, part: usize) -> Arc<OwnedFd> {
        self.drives[(first + part) % self.drives.len()].clone()
    }

    fn lock_of(object: &Object) -> usize {
        (object.hash() % COMMIT_LOCKS as u64) as usize
    }

    /// A bucket is a directory of the same name on every drive.
    pub(crate) async fn create_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let mut created = false;
        for drive in &self.drives {
            match self
                .sidecar
                .mkdir_at(drive.clone(), bucket.path(), 0o755)
                .await
            {
                Ok(()) => created = true,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        if created {
            Ok(())
        } else {
            Err(ApiError::BUCKET_EXISTS.into())
        }
    }

    pub(crate) async fn delete_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let mut removed = Vec::new();
        for drive in &self.drives {
            match self.sidecar.rmdir_at(drive.clone(), bucket.path()).await {
                Ok(()) => removed.push(drive),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    // The bucket stays, on every drive.
                    for drive in removed {
                        let restored = self.sidecar.mkdir_at(drive.clone(), bucket.path(), 0o755);
                        if let Err(error) = restored.await {
                            log::error!("could not restore {:?}: {error}", bucket.path());
                        }
                    }
                    return Err(match error.kind() {
                        io::ErrorKind::DirectoryNotEmpty => ApiError::BUCKET_NOT_EMPTY.into(),
                        _ => error.into(),
                    });
                }
            }
        }
        if removed.is_empty() {
            Err(ApiError::NO_SUCH_BUCKET.into())
        } else {
            Ok(())
        }
    }

    pub(crate) async fn head_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let stat = self.sidecar.stat_at(self.drives[0].clone(), bucket.path());
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

    async fn read_at(
        &self,
        file: &Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
        limit: usize,
    ) -> (io::Result<usize>, FixedBuf) {
        let file = file.clone();
        if self.sidecar.has_disk_rings() {
            self.sidecar.read_polled_at(file, buf, offset, limit).await
        } else {
            self.sidecar.read_fixed_at(file, buf, offset, limit).await
        }
    }

    /// Writes the contents of `buf`, all of them.
    async fn write_at(
        &self,
        file: &Arc<OwnedFd>,
        buf: FixedBuf,
        offset: u64,
    ) -> (io::Result<()>, FixedBuf) {
        let file = file.clone();
        let (written, buf) = if self.sidecar.has_disk_rings() {
            self.sidecar.write_polled_at(file, buf, offset).await
        } else {
            self.sidecar.write_fixed_at(file, buf, offset).await
        };
        let result = match written {
            Ok(written) if written == buf.len() => Ok(()),
            Ok(_) => Err(io::ErrorKind::WriteZero.into()),
            Err(error) => Err(error),
        };
        (result, buf)
    }

    /// Reads the header of the first part `file`, through `buf`.
    async fn read_header(
        &self,
        file: &Arc<OwnedFd>,
        mut buf: FixedBuf,
    ) -> (io::Result<Header>, FixedBuf) {
        buf.clear();
        let (read, buf) = self.read_at(file, buf, 0, HEADER).await;
        let header = read.and_then(|_| {
            Header::decode(&buf, &self.geometry).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "not an object stored with this arrangement of drives",
                )
            })
        });
        (header, buf)
    }

    /// Opens the first part of `object`.
    async fn open_first(&self, object: &Object) -> Result<Arc<OwnedFd>, StoreError> {
        let drive = self.drive(object.first_drive(self.drives.len()), 0);
        let flags = libc::O_RDONLY | libc::O_DIRECT;
        match self.sidecar.open_at(drive, object.path(), flags, 0).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(self.missing(object).await)
            }
            other => Ok(Arc::new(other?)),
        }
    }

    /// Looks the object up without opening its data.
    pub(crate) async fn head_object(&self, object: &Object) -> Result<Meta, StoreError> {
        let scratch = self.buffers.acquire().await;
        let file = self.open_first(object).await?;
        let header = self.read_header(&file, scratch).await.0?;
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    pub(crate) async fn open_object(&self, object: &Object) -> Result<Stored, StoreError> {
        let first = object.first_drive(self.drives.len());
        let mut scratch = self.buffers.acquire().await;
        'attempt: for _ in 0..OPEN_ATTEMPTS {
            let file = self.open_first(object).await?;
            let (header, returned) = self.read_header(&file, scratch).await;
            scratch = returned;
            let header = header?;

            let mut files = vec![file];
            for part in 1..self.geometry.parts(header.size) {
                let path = object.bucket.file(&part_name(header.id, part));
                let flags = libc::O_RDONLY | libc::O_DIRECT;
                let opened = self
                    .sidecar
                    .open_at(self.drive(first, part), path, flags, 0);
                match opened.await {
                    Ok(file) => files.push(Arc::new(file)),
                    // The object was replaced or removed since its header
                    // was read. See what the key names now.
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue 'attempt,
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(Stored {
                files,
                meta: Meta {
                    size: header.size,
                    modified: header.modified,
                },
            });
        }
        Err(io::Error::other("the object kept being replaced while it was opened").into())
    }

    /// Reads what `plan` asks for into `bufs`, one buffer per part of the
    /// object, from all drives at once.
    pub(crate) async fn read_round(
        &self,
        stored: &Stored,
        plan: &Plan,
        bufs: Vec<FixedBuf>,
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        let reads: Vec<_> = bufs
            .into_iter()
            .enumerate()
            .map(|(part, mut buf)| {
                let read = plan.reads.get(part).copied().flatten();
                async move {
                    let Some(read) = read else {
                        return (Ok(()), buf);
                    };
                    buf.clear();
                    let offset = read.offset + if part == 0 { HEADER as u64 } else { 0 };
                    let file = &stored.files[part];
                    let (got, buf) = self.read_at(file, buf, offset, read.limit).await;
                    let got = got.and_then(|got| match got >= read.needed {
                        true => Ok(()),
                        false => Err(io::ErrorKind::UnexpectedEof.into()),
                    });
                    (got, buf)
                }
            })
            .collect();

        let mut result = Ok(());
        let mut bufs = Vec::with_capacity(reads.len());
        for (got, buf) in join_all(reads).await {
            result = result.and(got);
            bufs.push(buf);
        }
        (result, bufs)
    }

    /// The header of what `object` names at the moment, if it names
    /// anything that has one.
    async fn current(&self, object: &Object, scratch: FixedBuf) -> (Option<Header>, FixedBuf) {
        let file = match self.open_first(object).await {
            Ok(file) => file,
            Err(StoreError::Api(_)) => return (None, scratch),
            Err(StoreError::Io(error)) => {
                log::warn!("could not open {:?}: {error}", object.path());
                return (None, scratch);
            }
        };
        let (header, scratch) = self.read_header(&file, scratch).await;
        if let Err(error) = &header {
            log::warn!("could not read the header of {:?}: {error}", object.path());
        }
        (header.ok(), scratch)
    }

    /// Removes the parts that `header` names, other than the first.
    async fn remove_parts(&self, bucket: &Bucket, first: usize, header: &Header) {
        for part in 1..self.geometry.parts(header.size) {
            let path = bucket.file(&part_name(header.id, part));
            let removed = self
                .sidecar
                .unlink_at(self.drive(first, part), path.clone());
            match removed.await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => log::warn!("could not remove {path:?}: {error}"),
            }
        }
    }

    /// Removes the object. Removing one that does not exist succeeds, as it
    /// does in S3.
    pub(crate) async fn delete_object(&self, object: &Object) -> Result<(), StoreError> {
        let first = object.first_drive(self.drives.len());
        // Only objects on several drives have other parts to look up. The
        // buffer for that is taken before the lock, as an upload does.
        let scratch = match self.drives.len() {
            1 => None,
            _ => Some(self.buffers.acquire().await),
        };
        let _commit = self.commits[Store::lock_of(object)].lock().await;
        let header = match scratch {
            Some(scratch) => self.current(object, scratch).await.0,
            None => None,
        };

        let removed = self.sidecar.unlink_at(self.drive(first, 0), object.path());
        match removed.await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return match self.missing(object).await {
                    StoreError::Api(ApiError::NO_SUCH_KEY) => Ok(()),
                    error => Err(error),
                };
            }
            other => other?,
        }
        if let Some(header) = header {
            self.remove_parts(&object.bucket, first, &header).await;
        }
        Ok(())
    }

    /// Creates the file that part `part` of `upload` is written to.
    async fn stage(&self, upload: &Upload, part: usize) -> io::Result<Staged> {
        let name = format!("{UPLOAD_PREFIX}{:032x}-{part}", upload.id);
        let path = upload.object.bucket.file(&name);
        let flags = libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_DIRECT;
        let drive = self.drive(upload.first, part);
        let file = self
            .sidecar
            .open_at(drive, path.clone(), flags, 0o644)
            .await?;
        Ok(Staged {
            file: Arc::new(file),
            path,
            padded: false,
        })
    }

    pub(crate) async fn begin(&self, object: &Object) -> Result<Upload, StoreError> {
        let number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let mut upload = Upload {
            id: u128::from(self.run) << 64 | u128::from(number),
            object: object.clone(),
            first: object.first_drive(self.drives.len()),
            parts: self.drives.iter().map(|_| None).collect(),
            size: 0,
            ended: false,
        };
        // The first part always exists: it carries the header.
        upload.parts[0] = match self.stage(&upload, 0).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ApiError::NO_SUCH_BUCKET.into());
            }
            other => Some(other?),
        };
        Ok(upload)
    }

    /// Appends a round to the upload: the next `filled` bytes of the object,
    /// laid out from the start of each of `bufs` as
    /// [`Geometry::fill`] has them. Every round but the last must be a full
    /// one. Writes to all drives at once.
    pub(crate) async fn write_round(
        &self,
        upload: &mut Upload,
        round: u64,
        bufs: Vec<FixedBuf>,
        filled: usize,
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        assert!(!upload.ended, "a short round ends the upload");
        let lens: Vec<usize> = (0..bufs.len())
            .map(|part| self.geometry.part_len(filled as u64, part) as usize)
            .collect();
        assert_eq!(lens.iter().sum::<usize>(), filled, "too few buffers");

        for (part, &len) in lens.iter().enumerate() {
            if len > 0 && upload.parts[part].is_none() {
                match self.stage(upload, part).await {
                    Ok(staged) => upload.parts[part] = Some(staged),
                    Err(error) => return (Err(error), bufs),
                }
            }
        }

        let chunk = self.geometry.chunk() as u64;
        let writes: Vec<_> = bufs
            .into_iter()
            .zip(&lens)
            .enumerate()
            .map(|(part, (mut buf, &len))| {
                let file = upload.parts[part]
                    .as_ref()
                    .map(|staged| staged.file.clone());
                async move {
                    let Some(file) = file.filter(|_| len > 0) else {
                        return (Ok(false), buf);
                    };
                    // Direct writes are whole blocks. The padding of the
                    // last one is cut off when the upload finishes.
                    let padding = len.next_multiple_of(BLOCK) - len;
                    buf.clear();
                    buf.grow(len);
                    buf.extend_from_slice(&ZEROS[..padding]);
                    let offset = round * chunk + if part == 0 { HEADER as u64 } else { 0 };
                    let (written, buf) = self.write_at(&file, buf, offset).await;
                    (written.map(|()| padding > 0), buf)
                }
            })
            .collect();

        let mut result = Ok(());
        let mut bufs = Vec::with_capacity(writes.len());
        for (part, (written, buf)) in join_all(writes).await.into_iter().enumerate() {
            bufs.push(buf);
            match (written, upload.parts[part].as_mut()) {
                (Ok(padded), Some(staged)) => staged.padded |= padded,
                (Ok(_), None) => {}
                (Err(error), _) => result = result.and(Err(error)),
            }
        }
        upload.size += filled as u64;
        upload.ended = filled < self.geometry.round_len();
        (result, bufs)
    }

    /// Makes the upload the object, using `scratch` to write its header.
    /// The data is on the drives by the time this returns.
    pub(crate) async fn finish(&self, upload: Upload, scratch: FixedBuf) -> io::Result<Meta> {
        let published = self.publish(&upload, scratch).await;
        if published.is_err() {
            self.abort(upload).await;
        }
        published
    }

    async fn publish(&self, upload: &Upload, mut scratch: FixedBuf) -> io::Result<Meta> {
        let header = Header {
            size: upload.size,
            modified: now(),
            id: upload.id,
        };
        let staged_first = upload.parts[0]
            .as_ref()
            .expect("an upload has a first part");

        // Every part is completed on its own drive, all at the same time.
        let first = async {
            if staged_first.padded {
                let len = HEADER as u64 + self.geometry.part_len(upload.size, 0);
                self.sidecar
                    .truncate(staged_first.file.clone(), len)
                    .await?;
            }
            scratch.clear();
            scratch.extend_from_slice(&header.encode(&self.geometry));
            let (written, scratch) = self.write_at(&staged_first.file, scratch, 0).await;
            written?;
            self.sidecar.fdatasync(staged_first.file.clone()).await?;
            Ok::<_, io::Error>(scratch)
        };
        let others: Vec<_> = upload
            .parts
            .iter()
            .enumerate()
            .skip(1)
            .filter_map(|(part, staged)| Some((part, staged.as_ref()?)))
            .map(|(part, staged)| async move {
                if staged.padded {
                    let len = self.geometry.part_len(upload.size, part);
                    self.sidecar.truncate(staged.file.clone(), len).await?;
                }
                self.sidecar.fdatasync(staged.file.clone()).await?;
                // Nothing refers to this name until the first part does.
                let name = upload.object.bucket.file(&part_name(upload.id, part));
                let drive = self.drive(upload.first, part);
                self.sidecar
                    .rename_at(drive, staged.path.clone(), name)
                    .await
            })
            .collect();
        let (first, others) = tokio::join!(first, join_all(others));
        let scratch = first?;
        others.into_iter().collect::<io::Result<()>>()?;

        // The moment the key changes hands. Whatever it named before has
        // parts of its own, which nothing refers to afterwards.
        let _commit = self.commits[Store::lock_of(&upload.object)].lock().await;
        let replaced = match self.drives.len() {
            1 => None,
            _ => self.current(&upload.object, scratch).await.0,
        };
        let renamed = self.sidecar.rename_at(
            self.drive(upload.first, 0),
            staged_first.path.clone(),
            upload.object.path(),
        );
        renamed.await?;
        if let Some(replaced) = replaced {
            let bucket = &upload.object.bucket;
            self.remove_parts(bucket, upload.first, &replaced).await;
        }
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    /// Throws the upload away.
    pub(crate) async fn abort(&self, upload: Upload) {
        for (part, staged) in upload.parts.iter().enumerate() {
            let Some(staged) = staged else { continue };
            // A part other than the first may already be under its final
            // name.
            let mut paths = vec![staged.path.clone()];
            if part > 0 {
                paths.push(upload.object.bucket.file(&part_name(upload.id, part)));
            }
            for path in paths {
                let removed = self
                    .sidecar
                    .unlink_at(self.drive(upload.first, part), path.clone());
                match removed.await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => log::warn!("could not remove {path:?}: {error}"),
                }
            }
        }
    }
}
