//! Standard mainline Tokio storage backend (`spawn_blocking` + `std::fs`)
//! using the exact same on-disk striped format as [`crate::sidecar_store`].
//!
//! Supports both idiomatic buffered page-cache I/O (`direct = false`, the
//! default for mainline Tokio apps) and `O_DIRECT` blocking I/O (`direct = true`)
//! for apples-to-apples NVMe comparisons without page-cache caching.

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use sidecar_axum::DirectBody;
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, spawn_blocking};

use crate::error::{ApiError, StoreError};
use crate::join::join_all;
use crate::name::{Bucket, Object, PART_PREFIX, UPLOAD_PREFIX, decode_key};
use crate::sigv4::{AwsChunkedDecoder, PayloadMode};
use crate::stripe::{BLOCK, Geometry, HEADER, Header, ListPage, ListedBucket, ListedObject, Meta};

const OPEN_ATTEMPTS: usize = 8;
const COMMIT_LOCKS: usize = 256;

/// Page-aligned heap buffer so `--direct` works cleanly with `O_DIRECT`.
struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: `AlignedBuf` exclusively owns its heap allocation.
unsafe impl Send for AlignedBuf {}
// SAFETY: Shared references only read immutable bytes.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn new(len: usize) -> Self {
        assert!(len > 0 && len % BLOCK == 0);
        let layout = Layout::from_size_align(len, BLOCK).expect("valid block layout");
        // SAFETY: layout has non-zero size and power-of-two alignment.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).expect("allocation failed");
        Self { ptr, len }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` is valid and initialized for `len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is uniquely owned and valid for `len` bytes.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, BLOCK).expect("valid block layout");
        // SAFETY: allocated with the exact same layout in `AlignedBuf::new`.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
    }
}

pub struct TokioStore {
    roots: Vec<PathBuf>,
    geometry: Geometry,
    direct: bool,
    run: u64,
    next_upload: AtomicU64,
    commits: Box<[Mutex<()>]>,
}

pub(crate) struct TokioStored {
    files: Vec<Arc<File>>,
    pub(crate) meta: Meta,
}

struct Staged {
    file: Arc<File>,
    path: PathBuf,
    padded: bool,
}

struct Upload {
    id: u128,
    object: Object,
    first: usize,
    parts: Vec<Option<Staged>>,
    size: u64,
    ended: bool,
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
    // SAFETY: `bytes` is a valid writable stack buffer.
    let filled = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    if filled != bytes.len() as isize {
        return Err(io::Error::last_os_error());
    }
    Ok(u64::from_ne_bytes(bytes))
}

fn join_io<T>(res: Result<io::Result<T>, tokio::task::JoinError>) -> io::Result<T> {
    res.map_err(io::Error::other)?
}

impl TokioStore {
    pub fn open(
        roots: &[PathBuf],
        unit: usize,
        chunk: usize,
        direct: bool,
    ) -> io::Result<Arc<Self>> {
        let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
        let geometry = Geometry::new(roots.len(), unit, chunk).ok_or_else(|| {
            invalid("invalid geometry: need roots, 4096-aligned unit, and chunk multiple of unit")
        })?;

        for root in roots {
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
        }

        Ok(Arc::new(Self {
            roots: roots.to_vec(),
            geometry,
            direct,
            run: random()?,
            next_upload: AtomicU64::new(0),
            commits: (0..COMMIT_LOCKS).map(|_| Mutex::new(())).collect(),
        }))
    }

    fn drive_root(&self, first: usize, part: usize) -> &PathBuf {
        &self.roots[(first + part) % self.roots.len()]
    }

    fn lock_of(object: &Object) -> usize {
        (object.hash() % COMMIT_LOCKS as u64) as usize
    }

    fn open_read_flags(&self) -> i32 {
        if self.direct { libc::O_DIRECT } else { 0 }
    }

    fn open_write_flags(&self) -> i32 {
        if self.direct { libc::O_DIRECT } else { 0 }
    }

    pub(crate) async fn create_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let paths: Vec<PathBuf> = self
            .roots
            .iter()
            .map(|root| root.join(bucket.as_str()))
            .collect();
        let created = spawn_blocking(move || -> io::Result<bool> {
            let mut created = false;
            for path in paths {
                match std::fs::create_dir(&path) {
                    Ok(()) => created = true,
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(created)
        })
        .await
        .map_err(io::Error::other)??;
        if created {
            Ok(())
        } else {
            Err(ApiError::BUCKET_EXISTS.into())
        }
    }

    pub(crate) async fn delete_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let paths: Vec<PathBuf> = self
            .roots
            .iter()
            .map(|root| root.join(bucket.as_str()))
            .collect();
        spawn_blocking(move || -> Result<(), StoreError> {
            let mut removed = Vec::new();
            for path in paths {
                match std::fs::remove_dir(&path) {
                    Ok(()) => removed.push(path),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        for r in removed {
                            let _ = std::fs::create_dir(r);
                        }
                        return Err(match e.kind() {
                            io::ErrorKind::DirectoryNotEmpty => ApiError::BUCKET_NOT_EMPTY.into(),
                            _ => e.into(),
                        });
                    }
                }
            }
            if removed.is_empty() {
                Err(ApiError::NO_SUCH_BUCKET.into())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn head_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let path = self.roots[0].join(bucket.as_str());
        let exists = spawn_blocking(move || std::fs::metadata(&path))
            .await
            .map_err(io::Error::other)?;
        match exists {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(ApiError::NO_SUCH_BUCKET.into()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn list_buckets(&self) -> Result<Vec<ListedBucket>, StoreError> {
        let root = self.roots[0].clone();
        let buckets = spawn_blocking(move || -> io::Result<Vec<ListedBucket>> {
            let mut out = Vec::new();
            for entry in std::fs::read_dir(&root)? {
                let entry = entry?;
                let meta = entry.metadata()?;
                if !meta.is_dir() {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                if name.starts_with('.') {
                    continue;
                }
                let modified_secs = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_secs() as i64);
                out.push(ListedBucket {
                    name,
                    modified_secs,
                });
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        })
        .await
        .map_err(io::Error::other)??;
        Ok(buckets)
    }

    pub(crate) async fn list_objects(
        &self,
        bucket: &Bucket,
        prefix: &str,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> Result<ListPage, StoreError> {
        self.head_bucket(bucket).await?;
        let roots = self.roots.clone();
        let bucket_name = bucket.as_str().to_owned();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        let geometry = self.geometry;
        let flags = self.open_read_flags();

        let page = spawn_blocking(move || -> io::Result<ListPage> {
            let mut entries = Vec::new();
            for root in &roots {
                let dir = root.join(&bucket_name);
                let rd = match std::fs::read_dir(&dir) {
                    Ok(rd) => rd,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                for entry in rd {
                    let entry = entry?;
                    let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
                        continue;
                    };
                    let Some(key) = decode_key(&file_name) else {
                        continue;
                    };
                    if !key.starts_with(&prefix) {
                        continue;
                    }
                    if let Some(ref after) = start_after
                        && key.as_str() <= after.as_str()
                    {
                        continue;
                    }
                    entries.push((key, entry.path()));
                }
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            let is_truncated = entries.len() > max_keys;
            entries.truncate(max_keys);
            let next_continuation_token = is_truncated.then(|| entries.last().unwrap().0.clone());

            let mut contents = Vec::with_capacity(entries.len());
            for (key, path) in entries {
                let file = match OpenOptions::new()
                    .read(true)
                    .custom_flags(flags)
                    .open(&path)
                {
                    Ok(f) => f,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                if let Ok(header) = Self::read_header_sync(&file, &geometry) {
                    contents.push(ListedObject {
                        key,
                        meta: Meta {
                            size: header.size,
                            modified: header.modified,
                        },
                    });
                }
            }

            Ok(ListPage {
                contents,
                is_truncated,
                next_continuation_token,
            })
        })
        .await
        .map_err(io::Error::other)??;

        Ok(page)
    }

    fn read_header_sync(file: &File, geometry: &Geometry) -> io::Result<Header> {
        let mut buf = AlignedBuf::new(HEADER);
        file.read_exact_at(buf.as_mut_slice(), 0)?;
        Header::decode(buf.as_slice(), geometry).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "not an object stored with this arrangement of drives",
            )
        })
    }

    pub(crate) async fn head_object(&self, object: &Object) -> Result<Meta, StoreError> {
        let first = object.first_drive(self.roots.len());
        let path = self.drive_root(first, 0).join(object.path_rel());
        let bucket_path = self.roots[0].join(object.bucket.as_str());
        let geometry = self.geometry;
        let flags = self.open_read_flags();

        spawn_blocking(move || -> Result<Meta, StoreError> {
            let file = match OpenOptions::new()
                .read(true)
                .custom_flags(flags)
                .open(&path)
            {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return if std::fs::metadata(&bucket_path).is_ok() {
                        Err(ApiError::NO_SUCH_KEY.into())
                    } else {
                        Err(ApiError::NO_SUCH_BUCKET.into())
                    };
                }
                Err(e) => return Err(e.into()),
            };
            let header = Self::read_header_sync(&file, &geometry)?;
            Ok(Meta {
                size: header.size,
                modified: header.modified,
            })
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn open_object(&self, object: &Object) -> Result<TokioStored, StoreError> {
        let first = object.first_drive(self.roots.len());
        let roots = self.roots.clone();
        let object = object.clone();
        let geometry = self.geometry;
        let flags = self.open_read_flags();

        spawn_blocking(move || -> Result<TokioStored, StoreError> {
            let first_path = roots[first % roots.len()].join(object.path_rel());
            let bucket_path = roots[0].join(object.bucket.as_str());

            'attempt: for _ in 0..OPEN_ATTEMPTS {
                let file = match OpenOptions::new()
                    .read(true)
                    .custom_flags(flags)
                    .open(&first_path)
                {
                    Ok(f) => Arc::new(f),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        return if std::fs::metadata(&bucket_path).is_ok() {
                            Err(ApiError::NO_SUCH_KEY.into())
                        } else {
                            Err(ApiError::NO_SUCH_BUCKET.into())
                        };
                    }
                    Err(e) => return Err(e.into()),
                };
                let header = Self::read_header_sync(&file, &geometry)?;
                let mut files = vec![file];
                for part in 1..geometry.parts(header.size) {
                    let path = roots[(first + part) % roots.len()]
                        .join(object.bucket.file_rel(&part_name(header.id, part)));
                    match OpenOptions::new()
                        .read(true)
                        .custom_flags(flags)
                        .open(&path)
                    {
                        Ok(f) => files.push(Arc::new(f)),
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue 'attempt,
                        Err(e) => return Err(e.into()),
                    }
                }
                return Ok(TokioStored {
                    files,
                    meta: Meta {
                        size: header.size,
                        modified: header.modified,
                    },
                });
            }
            Err(io::Error::other("the object kept being replaced while it was opened").into())
        })
        .await
        .map_err(io::Error::other)?
    }

    pub(crate) async fn delete_object(&self, object: &Object) -> Result<(), StoreError> {
        let _commit = self.commits[Self::lock_of(object)].lock().await;
        let first = object.first_drive(self.roots.len());
        let roots = self.roots.clone();
        let object = object.clone();
        let geometry = self.geometry;
        let flags = self.open_read_flags();

        spawn_blocking(move || -> Result<(), StoreError> {
            let first_path = roots[first % roots.len()].join(object.path_rel());
            let header = if roots.len() > 1 {
                OpenOptions::new()
                    .read(true)
                    .custom_flags(flags)
                    .open(&first_path)
                    .ok()
                    .and_then(|f| Self::read_header_sync(&f, &geometry).ok())
            } else {
                None
            };

            match std::fs::remove_file(&first_path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    let bucket_path = roots[0].join(object.bucket.as_str());
                    return if std::fs::metadata(&bucket_path).is_ok() {
                        Ok(())
                    } else {
                        Err(ApiError::NO_SUCH_BUCKET.into())
                    };
                }
                Err(e) => return Err(e.into()),
            }

            if let Some(header) = header {
                for part in 1..geometry.parts(header.size) {
                    let path = roots[(first + part) % roots.len()]
                        .join(object.bucket.file_rel(&part_name(header.id, part)));
                    let _ = std::fs::remove_file(path);
                }
            }
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn stage(&self, upload: &Upload, part: usize) -> io::Result<Staged> {
        let name = format!("{UPLOAD_PREFIX}{:032x}-{part}", upload.id);
        let path = self
            .drive_root(upload.first, part)
            .join(upload.object.bucket.file_rel(&name));
        let flags = self.open_write_flags();
        let path_clone = path.clone();
        let file = join_io(
            spawn_blocking(move || {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .custom_flags(flags)
                    .open(&path_clone)
            })
            .await,
        )?;
        Ok(Staged {
            file: Arc::new(file),
            path,
            padded: false,
        })
    }

    async fn abort(&self, upload: Upload) {
        let mut paths = Vec::new();
        for (part, staged) in upload.parts.into_iter().enumerate() {
            let Some(staged) = staged else { continue };
            paths.push(staged.path);
            if part > 0 {
                paths.push(
                    self.drive_root(upload.first, part)
                        .join(upload.object.bucket.file_rel(&part_name(upload.id, part))),
                );
            }
        }
        let _ = spawn_blocking(move || {
            for p in paths {
                let _ = std::fs::remove_file(p);
            }
        })
        .await;
    }

    async fn write_round(
        &self,
        upload: &mut Upload,
        filled: usize,
        bufs: Vec<AlignedBuf>,
    ) -> io::Result<()> {
        assert!(!upload.ended, "a short round ends the upload");
        let round = upload.size / self.geometry.round_len() as u64;
        let chunk = self.geometry.chunk() as u64;
        let direct = self.direct;

        let mut tasks = Vec::new();
        for (part, mut buf) in bufs.into_iter().enumerate() {
            let len = self.geometry.part_len(filled as u64, part) as usize;
            if len == 0 {
                break;
            }
            if upload.parts[part].is_none() {
                upload.parts[part] = Some(self.stage(upload, part).await?);
            }
            let staged = upload.parts[part].as_mut().expect("staged above");
            let write_len = if direct {
                staged.padded |= len % BLOCK != 0;
                let padded = len.next_multiple_of(BLOCK);
                buf.as_mut_slice()[len..padded].fill(0);
                padded
            } else {
                len
            };
            let file = staged.file.clone();
            let offset = round * chunk + if part == 0 { HEADER as u64 } else { 0 };
            tasks.push(spawn_blocking(move || {
                file.write_all_at(&buf.as_slice()[..write_len], offset)
            }));
        }
        upload.size += filled as u64;
        upload.ended = filled < self.geometry.round_len();

        for res in join_all(tasks).await {
            join_io(res)?;
        }
        Ok(())
    }

    pub(crate) async fn put_object(
        &self,
        object: &Object,
        body: DirectBody,
        mode: PayloadMode,
    ) -> Result<Meta, StoreError> {
        let number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let mut upload = Upload {
            id: u128::from(self.run) << 64 | u128::from(number),
            object: object.clone(),
            first: object.first_drive(self.roots.len()),
            parts: self.roots.iter().map(|_| None).collect(),
            size: 0,
            ended: false,
        };
        upload.parts[0] = match self.stage(&upload, 0).await {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(ApiError::NO_SUCH_BUCKET.into());
            }
            other => Some(other?),
        };

        let geometry = self.geometry;
        let round_len = geometry.round_len();

        let expected_sha256 = match &mode {
            PayloadMode::ExpectedSha256(d) => Some(*d),
            _ => None,
        };
        let mut hasher = expected_sha256.map(|_| Sha256::new());
        let mut decoder = match mode {
            PayloadMode::AwsChunked(spec) => Some(AwsChunkedDecoder::new(spec)),
            _ => None,
        };

        match body {
            DirectBody::Empty => {
                if decoder.is_some() {
                    self.abort(upload).await;
                    return Err(ApiError::BAD_REQUEST.into());
                }
            }
            DirectBody::Fallback(mut body) => {
                let mut bufs: Vec<AlignedBuf> = (0..geometry.drives())
                    .map(|_| AlignedBuf::new(geometry.chunk()))
                    .collect();
                let mut filled = 0usize;
                while let Some(frame) = body.frame().await {
                    let frame = match frame {
                        Ok(f) => f,
                        Err(e) => {
                            self.abort(upload).await;
                            return Err(StoreError::Io(io::Error::other(e)));
                        }
                    };
                    let Ok(data) = frame.into_data() else {
                        continue;
                    };
                    let mut slice = data.as_ref();
                    if let Some(ref mut dec) = decoder {
                        while !slice.is_empty() && !dec.is_done() {
                            let room = geometry.fill(filled..round_len);
                            let next = room.first().expect("round is not full");
                            let output =
                                &mut bufs[next.buf].as_mut_slice()[next.start..][..next.len];
                            let (used, produced) = match dec.decode(slice, output) {
                                Ok(pair) => pair,
                                Err(err) => {
                                    self.abort(upload).await;
                                    return Err(err.into());
                                }
                            };
                            slice = &slice[used..];
                            filled += produced;
                            if filled == round_len {
                                let to_write = std::mem::replace(
                                    &mut bufs,
                                    (0..geometry.drives())
                                        .map(|_| AlignedBuf::new(geometry.chunk()))
                                        .collect(),
                                );
                                if let Err(e) =
                                    self.write_round(&mut upload, filled, to_write).await
                                {
                                    self.abort(upload).await;
                                    return Err(e.into());
                                }
                                filled = 0;
                            }
                        }
                    } else {
                        if let Some(ref mut h) = hasher {
                            h.update(slice);
                        }
                        while !slice.is_empty() {
                            let take = slice.len().min(round_len - filled);
                            let segments = geometry.fill(filled..filled + take);
                            let mut offset = 0;
                            for seg in segments {
                                bufs[seg.buf].as_mut_slice()[seg.start..][..seg.len]
                                    .copy_from_slice(&slice[offset..offset + seg.len]);
                                offset += seg.len;
                            }
                            filled += take;
                            slice = &slice[take..];
                            if filled == round_len {
                                let to_write = std::mem::replace(
                                    &mut bufs,
                                    (0..geometry.drives())
                                        .map(|_| AlignedBuf::new(geometry.chunk()))
                                        .collect(),
                                );
                                if let Err(e) =
                                    self.write_round(&mut upload, filled, to_write).await
                                {
                                    self.abort(upload).await;
                                    return Err(e.into());
                                }
                                filled = 0;
                            }
                        }
                    }
                }
                if filled > 0
                    && let Err(e) = self.write_round(&mut upload, filled, bufs).await
                {
                    self.abort(upload).await;
                    return Err(e.into());
                }
            }
            DirectBody::Direct(_) => {
                self.abort(upload).await;
                return Err(StoreError::Io(io::Error::other(
                    "TokioStore received DirectBody::Direct unexpectedly",
                )));
            }
        }

        if let Some(dec) = decoder
            && let Err(err) = dec.finish()
        {
            self.abort(upload).await;
            return Err(err.into());
        }
        if let (Some(expected), Some(h)) = (expected_sha256, hasher) {
            let actual: [u8; 32] = h.finalize().into();
            if actual.ct_eq(&expected).unwrap_u8() != 1 {
                self.abort(upload).await;
                return Err(ApiError::PAYLOAD_MISMATCH.into());
            }
        }

        match self.publish(&upload).await {
            Ok(meta) => Ok(meta),
            Err(e) => {
                self.abort(upload).await;
                Err(e.into())
            }
        }
    }

    async fn publish(&self, upload: &Upload) -> io::Result<Meta> {
        let header = Header {
            size: upload.size,
            modified: now(),
            id: upload.id,
        };
        let geometry = self.geometry;
        let encoded = header.encode(&geometry);

        let mut sync_tasks = Vec::new();
        for (part, staged) in upload.parts.iter().enumerate() {
            let Some(staged) = staged else { continue };
            let file = staged.file.clone();
            let padded = staged.padded;
            let part_size = geometry.part_len(upload.size, part);
            let old_path = staged.path.clone();
            let new_path = if part > 0 {
                Some(
                    self.drive_root(upload.first, part)
                        .join(upload.object.bucket.file_rel(&part_name(upload.id, part))),
                )
            } else {
                None
            };
            sync_tasks.push(spawn_blocking(move || -> io::Result<()> {
                if padded {
                    let target_len = if part == 0 {
                        HEADER as u64 + part_size
                    } else {
                        part_size
                    };
                    file.set_len(target_len)?;
                }
                if part == 0 {
                    let mut hdr = AlignedBuf::new(HEADER);
                    hdr.as_mut_slice().copy_from_slice(&encoded);
                    file.write_all_at(hdr.as_slice(), 0)?;
                }
                file.sync_data()?;
                if let Some(new_path) = new_path {
                    std::fs::rename(old_path, new_path)?;
                }
                Ok(())
            }));
        }
        for res in join_all(sync_tasks).await {
            join_io(res)?;
        }

        let _commit = self.commits[Self::lock_of(&upload.object)].lock().await;
        let first_staged = upload.parts[0].as_ref().expect("first part exists");
        let old_first = first_staged.path.clone();
        let final_first = self
            .drive_root(upload.first, 0)
            .join(upload.object.path_rel());
        let roots = self.roots.clone();
        let object = upload.object.clone();
        let first = upload.first;
        let flags = self.open_read_flags();

        join_io(
            spawn_blocking(move || -> io::Result<()> {
                let replaced = if roots.len() > 1 {
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(flags)
                        .open(&final_first)
                        .ok()
                        .and_then(|f| Self::read_header_sync(&f, &geometry).ok())
                } else {
                    None
                };
                std::fs::rename(old_first, final_first)?;
                if let Some(replaced) = replaced {
                    for part in 1..geometry.parts(replaced.size) {
                        let path = roots[(first + part) % roots.len()]
                            .join(object.bucket.file_rel(&part_name(replaced.id, part)));
                        let _ = std::fs::remove_file(path);
                    }
                }
                Ok(())
            })
            .await,
        )?;

        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    pub(crate) fn stream_body(&self, stored: TokioStored, range: Range<u64>) -> Body {
        let rounds = self.geometry.rounds(&range);
        if rounds.is_empty() {
            return Body::empty();
        }
        Body::from_stream(TokioObjectStream {
            geometry: self.geometry,
            files: Arc::new(stored.files),
            range,
            rounds,
            in_flight: None,
        })
    }
}

struct TokioObjectStream {
    geometry: Geometry,
    files: Arc<Vec<Arc<File>>>,
    range: Range<u64>,
    rounds: Range<u64>,
    in_flight: Option<JoinHandle<io::Result<Bytes>>>,
}

impl Stream for TokioObjectStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.in_flight.is_none() {
            let Some(round) = this.rounds.next() else {
                return Poll::Ready(None);
            };
            let plan = this.geometry.plan(round, &this.range);
            let files = this.files.clone();
            this.in_flight = Some(tokio::spawn(async move {
                let mut tasks = Vec::with_capacity(plan.reads.len());
                for (part, read) in plan.reads.iter().copied().enumerate() {
                    let Some(read) = read else {
                        tasks.push(None);
                        continue;
                    };
                    let file = files[part].clone();
                    let offset = read.offset + if part == 0 { HEADER as u64 } else { 0 };
                    tasks.push(Some(spawn_blocking(move || -> io::Result<AlignedBuf> {
                        let mut buf = AlignedBuf::new(read.limit);
                        let mut done = 0;
                        while done < read.needed {
                            let n = file
                                .read_at(&mut buf.as_mut_slice()[done..], offset + done as u64)?;
                            if n == 0 {
                                return Err(io::ErrorKind::UnexpectedEof.into());
                            }
                            done += n;
                        }
                        Ok(buf)
                    })));
                }

                let mut part_bufs: Vec<Option<AlignedBuf>> = Vec::with_capacity(tasks.len());
                for task in tasks {
                    match task {
                        Some(handle) => part_bufs.push(Some(join_io(handle.await)?)),
                        None => part_bufs.push(None),
                    }
                }

                let mut out = BytesMut::with_capacity(plan.len);
                for seg in &plan.segments {
                    let buf = part_bufs[seg.buf]
                        .as_ref()
                        .expect("segment refers to read part");
                    out.extend_from_slice(&buf.as_slice()[seg.start..seg.start + seg.len]);
                }
                Ok(out.freeze())
            }));
        }

        let handle = this.in_flight.as_mut().expect("spawned above");
        match Pin::new(handle).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                this.in_flight = None;
                match res {
                    Ok(item) => Poll::Ready(Some(item)),
                    Err(e) => Poll::Ready(Some(Err(io::Error::other(e)))),
                }
            }
        }
    }
}
