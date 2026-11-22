//! Mainline Tokio `io-uring` (`tokio::fs` with `feature = "io-uring"`) storage
//! backend using the exact same on-disk striped format as [`crate::multiring_store`]
//! and [`crate::tokio_store`].
//!
//! Exercises every `io_uring`-backed `tokio::fs` operation available in mainline
//! Tokio 1.54 (`OpenOptions::open`, `File::read` / `AsyncReadExt`, `fs::write`,
//! `fs::rename`, `fs::try_exists`), while operations mainline Tokio has not yet
//! moved to `io_uring` (`File::write_all`, `sync_data`, `remove_file`,
//! `create_dir`, `remove_dir`) use Tokio's standard `tokio::fs` implementation.

use std::io::{self, SeekFrom};
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use futures_core::Stream;
use http_body_util::BodyExt;
use multiring_axum::DirectBody;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::error::{ApiError, StoreError};
use crate::join::join_all;
use crate::name::{Bucket, Object, PART_PREFIX, UPLOAD_PREFIX, decode_key};
use crate::sigv4::{AwsChunkedDecoder, PayloadMode};
use crate::stripe::{Geometry, HEADER, Header, ListPage, ListedBucket, ListedObject, Meta};

const OPEN_ATTEMPTS: usize = 8;
const COMMIT_LOCKS: usize = 256;
/// Same 16 KiB upfront inline read as `MultiRingStore` so small GETs get an
/// apples-to-apples single-read path on mainline Tokio's `io-uring` too.
const INLINE_READ: usize = 16 * 1024;

pub struct TokioUringStore {
    roots: Vec<PathBuf>,
    geometry: Geometry,
    run: u64,
    next_upload: AtomicU64,
    commits: Box<[Mutex<()>]>,
}

pub(crate) struct TokioUringStored {
    files: Vec<Arc<Mutex<File>>>,
    inline: Option<Bytes>,
    pub(crate) meta: Meta,
}

struct Staged {
    file: File,
    path: PathBuf,
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

async fn read_up_to(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

impl TokioUringStore {
    pub fn open(roots: &[PathBuf], unit: usize, chunk: usize) -> io::Result<Arc<Self>> {
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

    pub(crate) async fn create_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let mut created = false;
        for root in &self.roots {
            match fs::create_dir(root.join(bucket.as_str())).await {
                Ok(()) => created = true,
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
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
        for root in &self.roots {
            let path = root.join(bucket.as_str());
            match fs::remove_dir(&path).await {
                Ok(()) => removed.push(path),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => {
                    for r in removed {
                        let _ = fs::create_dir(r).await;
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
    }

    pub(crate) async fn head_bucket(&self, bucket: &Bucket) -> Result<(), StoreError> {
        let path = self.roots[0].join(bucket.as_str());
        if fs::try_exists(&path).await? {
            Ok(())
        } else {
            Err(ApiError::NO_SUCH_BUCKET.into())
        }
    }

    pub(crate) async fn list_buckets(&self) -> Result<Vec<ListedBucket>, StoreError> {
        let root = self.roots[0].clone();
        let buckets = tokio::task::spawn_blocking(move || -> io::Result<Vec<ListedBucket>> {
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

        let mut entries =
            tokio::task::spawn_blocking(move || -> io::Result<Vec<(String, PathBuf)>> {
                let mut found = Vec::new();
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
                        found.push((key, entry.path()));
                    }
                }
                found.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(found)
            })
            .await
            .map_err(io::Error::other)??;

        let is_truncated = entries.len() > max_keys;
        entries.truncate(max_keys);
        let next_continuation_token = is_truncated.then(|| entries.last().unwrap().0.clone());

        let mut contents = Vec::with_capacity(entries.len());
        let mut hdr_buf = vec![0u8; HEADER];
        for (key, path) in entries {
            let mut file = match OpenOptions::new().read(true).open(&path).await {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let n = read_up_to(&mut file, &mut hdr_buf).await?;
            if let Some(header) = Header::decode(&hdr_buf[..n], &self.geometry) {
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
    }

    async fn missing(&self, object: &Object) -> StoreError {
        match self.head_bucket(&object.bucket).await {
            Ok(()) => ApiError::NO_SUCH_KEY.into(),
            Err(e) => e,
        }
    }

    pub(crate) async fn head_object(&self, object: &Object) -> Result<Meta, StoreError> {
        let first = object.first_drive(self.roots.len());
        let path = self.drive_root(first, 0).join(object.path_rel());
        let mut file = match OpenOptions::new().read(true).open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(self.missing(object).await);
            }
            Err(e) => return Err(e.into()),
        };
        let mut buf = vec![0u8; HEADER];
        let n = read_up_to(&mut file, &mut buf).await?;
        let header = Header::decode(&buf[..n], &self.geometry).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "not an object stored with this arrangement of drives",
            )
        })?;
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    pub(crate) async fn open_object(
        &self,
        object: &Object,
    ) -> Result<TokioUringStored, StoreError> {
        let first = object.first_drive(self.roots.len());
        let first_path = self.drive_root(first, 0).join(object.path_rel());

        'attempt: for _ in 0..OPEN_ATTEMPTS {
            let mut file = match OpenOptions::new().read(true).open(&first_path).await {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(self.missing(object).await);
                }
                Err(e) => return Err(e.into()),
            };
            file.set_max_buf_size(self.geometry.chunk());

            let mut buf = BytesMut::zeroed(INLINE_READ);
            let n = read_up_to(&mut file, &mut buf).await?;
            let header = Header::decode(&buf[..n], &self.geometry).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "not an object stored with this arrangement of drives",
                )
            })?;

            let parts = self.geometry.parts(header.size);
            if parts == 1 && header.size > 0 && n >= HEADER + header.size as usize {
                buf.truncate(HEADER + header.size as usize);
                let inline = buf.freeze().slice(HEADER..);
                return Ok(TokioUringStored {
                    files: Vec::new(),
                    inline: Some(inline),
                    meta: Meta {
                        size: header.size,
                        modified: header.modified,
                    },
                });
            }

            let mut files = vec![Arc::new(Mutex::new(file))];
            for part in 1..parts {
                let path = self
                    .drive_root(first, part)
                    .join(object.bucket.file_rel(&part_name(header.id, part)));
                match OpenOptions::new().read(true).open(&path).await {
                    Ok(mut f) => {
                        f.set_max_buf_size(self.geometry.chunk());
                        files.push(Arc::new(Mutex::new(f)));
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue 'attempt,
                    Err(e) => return Err(e.into()),
                }
            }

            return Ok(TokioUringStored {
                files,
                inline: None,
                meta: Meta {
                    size: header.size,
                    modified: header.modified,
                },
            });
        }
        Err(io::Error::other("the object kept being replaced while it was opened").into())
    }

    async fn read_existing_header(&self, object: &Object) -> Option<Header> {
        let first = object.first_drive(self.roots.len());
        let path = self.drive_root(first, 0).join(object.path_rel());
        // Skip opening/reading when file doesn't exist or is smaller than
        // HEADER + stripe_unit (guaranteed single-part).
        match fs::metadata(&path).await {
            Ok(m) if m.len() < (HEADER + self.geometry.unit()) as u64 => return None,
            Err(_) => return None,
            Ok(_) => {}
        }
        let mut file = OpenOptions::new().read(true).open(&path).await.ok()?;
        let mut buf = vec![0u8; HEADER];
        let n = read_up_to(&mut file, &mut buf).await.ok()?;
        Header::decode(&buf[..n], &self.geometry)
    }

    async fn remove_parts(&self, bucket: &Bucket, first: usize, header: &Header) {
        for part in 1..self.geometry.parts(header.size) {
            let path = self
                .drive_root(first, part)
                .join(bucket.file_rel(&part_name(header.id, part)));
            let _ = fs::remove_file(path).await;
        }
    }

    pub(crate) async fn delete_object(&self, object: &Object) -> Result<(), StoreError> {
        let _commit = self.commits[Self::lock_of(object)].lock().await;
        let first = object.first_drive(self.roots.len());
        let header = if self.roots.len() > 1 {
            self.read_existing_header(object).await
        } else {
            None
        };
        let path = self.drive_root(first, 0).join(object.path_rel());
        match fs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return match self.missing(object).await {
                    StoreError::Api(ApiError::NO_SUCH_KEY) => Ok(()),
                    err => Err(err),
                };
            }
            Err(e) => return Err(e.into()),
        }
        if let Some(header) = header {
            self.remove_parts(&object.bucket, first, &header).await;
        }
        Ok(())
    }

    async fn stage(&self, upload: &Upload, part: usize) -> io::Result<Staged> {
        let name = format!("{UPLOAD_PREFIX}{:032x}-{part}", upload.id);
        let path = self
            .drive_root(upload.first, part)
            .join(upload.object.bucket.file_rel(&name));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.set_max_buf_size(self.geometry.chunk());
        if part == 0 {
            file.seek(SeekFrom::Start(HEADER as u64)).await?;
        }
        Ok(Staged { file, path })
    }

    async fn abort(&self, upload: Upload) {
        for (part, staged) in upload.parts.into_iter().enumerate() {
            let Some(staged) = staged else { continue };
            drop(staged.file);
            let _ = fs::remove_file(staged.path).await;
            if part > 0 {
                let path = self
                    .drive_root(upload.first, part)
                    .join(upload.object.bucket.file_rel(&part_name(upload.id, part)));
                let _ = fs::remove_file(path).await;
            }
        }
    }

    async fn write_round(
        &self,
        upload: &mut Upload,
        filled: usize,
        bufs: Vec<Vec<u8>>,
    ) -> io::Result<()> {
        assert!(!upload.ended, "a short round ends the upload");
        for part in 0..self.roots.len() {
            let len = self.geometry.part_len(filled as u64, part) as usize;
            if len == 0 {
                break;
            }
            if upload.parts[part].is_none() {
                upload.parts[part] = Some(self.stage(upload, part).await?);
            }
        }

        let mut writes = Vec::new();
        for (part, (staged_opt, buf)) in upload.parts.iter_mut().zip(bufs).enumerate() {
            let len = self.geometry.part_len(filled as u64, part) as usize;
            if len == 0 {
                break;
            }
            let staged = staged_opt.as_mut().expect("staged above");
            writes.push(async move { staged.file.write_all(&buf[..len]).await });
        }
        for res in join_all(writes).await {
            res?;
        }
        upload.size += filled as u64;
        upload.ended = filled < self.geometry.round_len();
        Ok(())
    }

    pub(crate) async fn put_object(
        &self,
        object: &Object,
        body: DirectBody,
        mode: PayloadMode,
    ) -> Result<Meta, StoreError> {
        let number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let id = u128::from(self.run) << 64 | u128::from(number);
        let first = object.first_drive(self.roots.len());

        // Single-shot fast path for small objects (<= stripe_unit) using
        // mainline Tokio's `io_uring`-backed `fs::write` + `fs::rename`!
        let mut body = match body {
            DirectBody::Empty => {
                return self
                    .put_small_uring(object, id, first, Vec::new(), mode)
                    .await;
            }
            DirectBody::Fallback(body) => body,
            DirectBody::Direct(_) => {
                return Err(StoreError::Io(io::Error::other(
                    "TokioUringStore received DirectBody::Direct unexpectedly",
                )));
            }
        };

        let exact_hint = http_body::Body::size_hint(&body).exact();
        if let Some(len) = exact_hint
            && len <= self.geometry.unit() as u64
            && !matches!(mode, PayloadMode::AwsChunked(_))
        {
            let mut data = Vec::with_capacity(HEADER + len as usize);
            data.resize(HEADER, 0);
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| StoreError::Io(io::Error::other(e)))?;
                if let Ok(chunk) = frame.into_data() {
                    data.extend_from_slice(&chunk);
                }
            }
            return self.put_small_uring(object, id, first, data, mode).await;
        }

        let mut upload = Upload {
            id,
            object: object.clone(),
            first,
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

        let mut bufs: Vec<Vec<u8>> = (0..geometry.drives())
            .map(|_| vec![0u8; geometry.chunk()])
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
                    let output = &mut bufs[next.buf][next.start..][..next.len];
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
                                .map(|_| vec![0u8; geometry.chunk()])
                                .collect(),
                        );
                        if let Err(e) = self.write_round(&mut upload, filled, to_write).await {
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
                        bufs[seg.buf][seg.start..][..seg.len]
                            .copy_from_slice(&slice[offset..offset + seg.len]);
                        offset += seg.len;
                    }
                    filled += take;
                    slice = &slice[take..];
                    if filled == round_len {
                        let to_write = std::mem::replace(
                            &mut bufs,
                            (0..geometry.drives())
                                .map(|_| vec![0u8; geometry.chunk()])
                                .collect(),
                        );
                        if let Err(e) = self.write_round(&mut upload, filled, to_write).await {
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

        match self.publish(&mut upload).await {
            Ok(meta) => Ok(meta),
            Err(e) => {
                self.abort(upload).await;
                Err(e.into())
            }
        }
    }

    async fn put_small_uring(
        &self,
        object: &Object,
        id: u128,
        first: usize,
        mut buf: Vec<u8>,
        mode: PayloadMode,
    ) -> Result<Meta, StoreError> {
        if buf.is_empty() {
            buf.resize(HEADER, 0);
        }
        let size = (buf.len() - HEADER) as u64;
        match mode {
            PayloadMode::Plain => {}
            PayloadMode::ExpectedSha256(expected) => {
                let actual: [u8; 32] = Sha256::digest(&buf[HEADER..]).into();
                if actual.ct_eq(&expected).unwrap_u8() != 1 {
                    return Err(ApiError::PAYLOAD_MISMATCH.into());
                }
            }
            PayloadMode::AwsChunked(_) => return Err(ApiError::BAD_REQUEST.into()),
        }

        let header = Header {
            size,
            modified: now(),
            id,
        };
        buf[..HEADER].copy_from_slice(&header.encode(&self.geometry));

        let staged_name = format!("{UPLOAD_PREFIX}{id:032x}-0");
        let staged_path = self
            .drive_root(first, 0)
            .join(object.bucket.file_rel(&staged_name));

        // Mainline Tokio `io_uring` `OpenAt` + `Write`!
        match fs::write(&staged_path, &buf).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(ApiError::NO_SUCH_BUCKET.into());
            }
            Err(e) => return Err(e.into()),
        }
        // Durability parity with MultiRingStore / TokioStore.
        let f = OpenOptions::new().write(true).open(&staged_path).await?;
        if let Err(e) = f.sync_data().await {
            let _ = fs::remove_file(&staged_path).await;
            return Err(e.into());
        }
        drop(f);

        let _commit = self.commits[Self::lock_of(object)].lock().await;
        let replaced = if self.roots.len() > 1 {
            self.read_existing_header(object).await
        } else {
            None
        };
        let final_path = self.drive_root(first, 0).join(object.path_rel());
        // Mainline Tokio `io_uring` `RenameAt`!
        if let Err(e) = fs::rename(&staged_path, &final_path).await {
            let _ = fs::remove_file(&staged_path).await;
            return Err(e.into());
        }
        if let Some(replaced) = replaced {
            self.remove_parts(&object.bucket, first, &replaced).await;
        }
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    async fn publish(&self, upload: &mut Upload) -> io::Result<Meta> {
        let header = Header {
            size: upload.size,
            modified: now(),
            id: upload.id,
        };
        let encoded = header.encode(&self.geometry);

        let mut tasks = Vec::new();
        for (part, staged_opt) in upload.parts.iter_mut().enumerate() {
            let Some(staged) = staged_opt.as_mut() else {
                continue;
            };
            let new_path = if part > 0 {
                Some(
                    self.drive_root(upload.first, part)
                        .join(upload.object.bucket.file_rel(&part_name(upload.id, part))),
                )
            } else {
                None
            };
            tasks.push(async move {
                if part == 0 {
                    staged.file.seek(SeekFrom::Start(0)).await?;
                    staged.file.write_all(&encoded).await?;
                }
                staged.file.sync_data().await?;
                if let Some(new_path) = new_path {
                    fs::rename(&staged.path, &new_path).await?;
                }
                Ok::<(), io::Error>(())
            });
        }
        for res in join_all(tasks).await {
            res?;
        }

        let _commit = self.commits[Self::lock_of(&upload.object)].lock().await;
        let replaced = if self.roots.len() > 1 {
            self.read_existing_header(&upload.object).await
        } else {
            None
        };
        let first_staged = upload.parts[0].as_ref().expect("first part exists");
        let final_first = self
            .drive_root(upload.first, 0)
            .join(upload.object.path_rel());
        fs::rename(&first_staged.path, &final_first).await?;
        if let Some(replaced) = replaced {
            self.remove_parts(&upload.object.bucket, upload.first, &replaced)
                .await;
        }
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    pub(crate) fn stream_body(&self, stored: TokioUringStored, range: Range<u64>) -> Body {
        if range.is_empty() {
            return Body::empty();
        }
        if let Some(inline) = stored.inline {
            let slice = inline.slice(range.start as usize..range.end as usize);
            return Body::from(slice);
        }
        let rounds = self.geometry.rounds(&range);
        if rounds.is_empty() {
            return Body::empty();
        }
        Body::from_stream(TokioUringStream {
            geometry: self.geometry,
            files: Arc::new(stored.files),
            range,
            rounds,
            in_flight: None,
        })
    }
}

struct TokioUringStream {
    geometry: Geometry,
    files: Arc<Vec<Arc<Mutex<File>>>>,
    range: Range<u64>,
    rounds: Range<u64>,
    in_flight: Option<JoinHandle<io::Result<Bytes>>>,
}

impl Stream for TokioUringStream {
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
                let mut futs = Vec::with_capacity(plan.reads.len());
                for (part, read) in plan.reads.iter().copied().enumerate() {
                    let file_opt = read.map(|r| (files[part].clone(), r));
                    futs.push(async move {
                        let Some((file, read)) = file_opt else {
                            return Ok::<Option<Vec<u8>>, io::Error>(None);
                        };
                        let offset = read.offset + if part == 0 { HEADER as u64 } else { 0 };
                        let mut guard = file.lock().await;
                        guard.seek(SeekFrom::Start(offset)).await?;
                        let mut buf = vec![0u8; read.needed];
                        // Mainline Tokio `io_uring` `Read` via `File::poll_read`!
                        guard.read_exact(&mut buf).await?;
                        Ok(Some(buf))
                    });
                }

                let mut part_bufs: Vec<Option<Vec<u8>>> = Vec::with_capacity(futs.len());
                for res in join_all(futs).await {
                    part_bufs.push(res?);
                }

                let mut out = BytesMut::with_capacity(plan.len);
                for seg in &plan.segments {
                    let buf = part_bufs[seg.buf]
                        .as_ref()
                        .expect("segment refers to read part");
                    out.extend_from_slice(&buf[seg.start..seg.start + seg.len]);
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
