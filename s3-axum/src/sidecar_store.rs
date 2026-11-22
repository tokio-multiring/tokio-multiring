//! `tokio-sidecar` (`io_uring`) storage backend with registered buffers,
//! `O_DIRECT` striped files, and zero-copy sends through `sidecar-axum`.

use std::collections::VecDeque;
use std::ffi::CString;
use std::future::Future;
use std::io;
use std::ops::Range;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use http_body_util::BodyExt;
use sidecar_axum::{BodyError, BodySink, DirectBody, DirectWrite, SidecarDirectBody};
use tokio::sync::Mutex;
use tokio_sidecar::{BufferPool, FixedBuf, Reclaim, Sidecar};

use crate::error::{ApiError, StoreError};
use crate::join::{Started, join_all};
use crate::name::{Bucket, Object, PART_PREFIX, UPLOAD_PREFIX};
use crate::stripe::{BLOCK, Geometry, HEADER, Header, Meta, Plan};

static ZEROS: [u8; BLOCK] = [0; BLOCK];
const OPEN_ATTEMPTS: usize = 8;
const COMMIT_LOCKS: usize = 256;
const ZERO_COPY_MIN: usize = 64 * 1024;

pub struct SidecarStore {
    sidecar: Sidecar,
    buffers: BufferPool,
    drives: Vec<Arc<OwnedFd>>,
    geometry: Geometry,
    window: usize,
    run: u64,
    next_upload: AtomicU64,
    commits: Box<[Mutex<()>]>,
}

pub(crate) struct SidecarStored {
    files: Vec<Arc<OwnedFd>>,
    pub(crate) meta: Meta,
}

impl SidecarStored {
    pub(crate) fn parts(&self) -> usize {
        self.files.len()
    }
}

struct Upload {
    id: u128,
    object: Object,
    first: usize,
    parts: Vec<Option<Staged>>,
    size: u64,
    ended: bool,
}

struct RoundWrite {
    round: u64,
    parts: Vec<(Arc<OwnedFd>, usize)>,
}

struct Staged {
    file: Arc<OwnedFd>,
    path: CString,
    padded: bool,
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

impl SidecarStore {
    pub fn open(
        sidecar: Sidecar,
        roots: &[PathBuf],
        unit: usize,
        window: usize,
    ) -> io::Result<Arc<Self>> {
        let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
        if window == 0 {
            return Err(invalid("window must be at least 1"));
        }
        let buffers = sidecar
            .buffers()
            .cloned()
            .ok_or_else(|| invalid("the sidecar has no buffer pool"))?;
        let geometry = Geometry::new(roots.len(), unit, buffers.buf_size()).ok_or_else(|| {
            invalid("invalid geometry: need roots, 4096-aligned unit, and buffer multiple of unit")
        })?;
        if buffers.count() < roots.len() {
            return Err(invalid("a store needs at least one buffer per root"));
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
                    if file
                        .file_name()
                        .as_encoded_bytes()
                        .starts_with(UPLOAD_PREFIX.as_bytes())
                    {
                        std::fs::remove_file(file.path())?;
                    }
                }
            }
            drives.push(Arc::new(OwnedFd::from(std::fs::File::open(root)?)));
        }

        Ok(Arc::new(Self {
            sidecar,
            buffers,
            drives,
            geometry,
            window,
            run: random()?,
            next_upload: AtomicU64::new(0),
            commits: (0..COMMIT_LOCKS).map(|_| Mutex::new(())).collect(),
        }))
    }

    fn drive(&self, first: usize, part: usize) -> Arc<OwnedFd> {
        self.drives[(first + part) % self.drives.len()].clone()
    }

    fn lock_of(object: &Object) -> usize {
        (object.hash() % COMMIT_LOCKS as u64) as usize
    }

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
                    for drive in removed {
                        let _ = self
                            .sidecar
                            .mkdir_at(drive.clone(), bucket.path(), 0o755)
                            .await;
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
        match self
            .sidecar
            .stat_at(self.drives[0].clone(), bucket.path())
            .await
        {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ApiError::NO_SUCH_BUCKET.into())
            }
            other => Ok(other.map(drop)?),
        }
    }

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

    pub(crate) async fn head_object(&self, object: &Object) -> Result<Meta, StoreError> {
        let scratch = self.buffers.acquire().await;
        let file = self.open_first(object).await?;
        let header = self.read_header(&file, scratch).await.0?;
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    pub(crate) async fn open_object(&self, object: &Object) -> Result<SidecarStored, StoreError> {
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
                match self
                    .sidecar
                    .open_at(self.drive(first, part), path, flags, 0)
                    .await
                {
                    Ok(file) => files.push(Arc::new(file)),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue 'attempt,
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(SidecarStored {
                files,
                meta: Meta {
                    size: header.size,
                    modified: header.modified,
                },
            });
        }
        Err(io::Error::other("the object kept being replaced while it was opened").into())
    }

    async fn read_round(
        &self,
        stored: &SidecarStored,
        plan: Plan,
        bufs: Vec<FixedBuf>,
    ) -> (io::Result<()>, Vec<FixedBuf>, Plan) {
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
        (result, bufs, plan)
    }

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
        (header.ok(), scratch)
    }

    async fn remove_parts(&self, bucket: &Bucket, first: usize, header: &Header) {
        for part in 1..self.geometry.parts(header.size) {
            let path = bucket.file(&part_name(header.id, part));
            let _ = self.sidecar.unlink_at(self.drive(first, part), path).await;
        }
    }

    pub(crate) async fn delete_object(&self, object: &Object) -> Result<(), StoreError> {
        let first = object.first_drive(self.drives.len());
        let scratch = match self.drives.len() {
            1 => None,
            _ => Some(self.buffers.acquire().await),
        };
        let _commit = self.commits[Self::lock_of(object)].lock().await;
        let header = match scratch {
            Some(scratch) => self.current(object, scratch).await.0,
            None => None,
        };
        match self
            .sidecar
            .unlink_at(self.drive(first, 0), object.path())
            .await
        {
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

    async fn begin(&self, object: &Object) -> Result<Upload, StoreError> {
        let number = self.next_upload.fetch_add(1, Ordering::Relaxed);
        let mut upload = Upload {
            id: u128::from(self.run) << 64 | u128::from(number),
            object: object.clone(),
            first: object.first_drive(self.drives.len()),
            parts: self.drives.iter().map(|_| None).collect(),
            size: 0,
            ended: false,
        };
        upload.parts[0] = match self.stage(&upload, 0).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ApiError::NO_SUCH_BUCKET.into());
            }
            other => Some(other?),
        };
        Ok(upload)
    }

    async fn begin_round(&self, upload: &mut Upload, filled: usize) -> io::Result<RoundWrite> {
        assert!(!upload.ended, "a short round ends the upload");
        let round = upload.size / self.geometry.round_len() as u64;
        let mut parts = Vec::new();
        for part in 0..self.drives.len() {
            let len = self.geometry.part_len(filled as u64, part) as usize;
            if len == 0 {
                break;
            }
            if upload.parts[part].is_none() {
                upload.parts[part] = Some(self.stage(upload, part).await?);
            }
            let staged = upload.parts[part].as_mut().expect("staged above");
            staged.padded |= len % BLOCK != 0;
            parts.push((staged.file.clone(), len));
        }
        upload.size += filled as u64;
        upload.ended = filled < self.geometry.round_len();
        Ok(RoundWrite { round, parts })
    }

    async fn write_round(
        &self,
        round: RoundWrite,
        bufs: Vec<FixedBuf>,
    ) -> (io::Result<()>, Vec<FixedBuf>) {
        assert!(round.parts.len() <= bufs.len(), "too few buffers");
        let chunk = self.geometry.chunk() as u64;
        let mut parts = round.parts.into_iter();
        let writes: Vec<_> = bufs
            .into_iter()
            .enumerate()
            .map(|(part, mut buf)| {
                let target = parts.next();
                async move {
                    let Some((file, len)) = target else {
                        return (Ok(()), buf);
                    };
                    let padding = len.next_multiple_of(BLOCK) - len;
                    buf.clear();
                    buf.grow(len);
                    buf.extend_from_slice(&ZEROS[..padding]);
                    let offset = round.round * chunk + if part == 0 { HEADER as u64 } else { 0 };
                    self.write_at(&file, buf, offset).await
                }
            })
            .collect();

        let mut result = Ok(());
        let mut bufs = Vec::with_capacity(writes.len());
        for (written, buf) in join_all(writes).await {
            result = result.and(written);
            bufs.push(buf);
        }
        (result, bufs)
    }

    async fn finish(&self, upload: Upload, scratch: FixedBuf) -> io::Result<Meta> {
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

        let _commit = self.commits[Self::lock_of(&upload.object)].lock().await;
        let replaced = match self.drives.len() {
            1 => None,
            _ => self.current(&upload.object, scratch).await.0,
        };
        self.sidecar
            .rename_at(
                self.drive(upload.first, 0),
                staged_first.path.clone(),
                upload.object.path(),
            )
            .await?;
        if let Some(replaced) = replaced {
            self.remove_parts(&upload.object.bucket, upload.first, &replaced)
                .await;
        }
        Ok(Meta {
            size: header.size,
            modified: header.modified,
        })
    }

    async fn abort(&self, upload: Upload) {
        for (part, staged) in upload.parts.iter().enumerate() {
            let Some(staged) = staged else { continue };
            let mut paths = vec![staged.path.clone()];
            if part > 0 {
                paths.push(upload.object.bucket.file(&part_name(upload.id, part)));
            }
            for path in paths {
                let _ = self
                    .sidecar
                    .unlink_at(self.drive(upload.first, part), path)
                    .await;
            }
        }
    }

    async fn acquire_window(&self, per_round: usize, rounds: u64) -> Vec<Vec<FixedBuf>> {
        let wanted = rounds.min(self.window as u64) as usize;
        let reserve = self.buffers.count() / 2;
        let mut window = vec![self.buffers.acquire_many(per_round).await];
        while window.len() < wanted && self.buffers.available() >= reserve + per_round {
            match self.buffers.try_acquire_many(per_round) {
                Some(bufs) => window.push(bufs),
                None => break,
            }
        }
        window
    }

    /// Stores an object from a [`DirectBody`], taking the direct io_uring
    /// `recv_vectored` fast path when served by `sidecar-axum`.
    pub(crate) async fn put_object(
        &self,
        object: &Object,
        body: DirectBody,
    ) -> Result<Meta, StoreError> {
        let mut upload = self.begin(object).await?;
        let scratch = match body {
            DirectBody::Empty => self.buffers.acquire().await,
            DirectBody::Direct(mut direct) => {
                if let Err(err) = direct.accept_100().await {
                    self.abort(upload).await;
                    return Err(match err {
                        BodyError::Io(e) => StoreError::Io(e),
                        BodyError::Malformed(_) => StoreError::Api(ApiError::BAD_REQUEST),
                    });
                }
                match self.receive_direct(&mut direct, &mut upload).await {
                    Ok(mut bufs) => bufs.swap_remove(0),
                    Err(err) => {
                        self.abort(upload).await;
                        return Err(err);
                    }
                }
            }
            DirectBody::Fallback(mut body) => {
                match self.receive_fallback(&mut body, &mut upload).await {
                    Ok(mut bufs) => bufs.swap_remove(0),
                    Err(err) => {
                        self.abort(upload).await;
                        return Err(err);
                    }
                }
            }
        };
        Ok(self.finish(upload, scratch).await?)
    }

    async fn receive_direct(
        &self,
        direct: &mut SidecarDirectBody,
        upload: &mut Upload,
    ) -> Result<Vec<FixedBuf>, StoreError> {
        let geometry = self.geometry;
        let round_len = geometry.round_len();
        let (parts, rounds) = match direct.remaining() {
            Some(left) => (geometry.parts(left), left.div_ceil(round_len as u64)),
            None => (geometry.drives(), u64::MAX),
        };
        let mut free = self.acquire_window(parts, rounds).await;
        let mut writing = VecDeque::new();

        loop {
            let mut bufs = match free.pop() {
                Some(bufs) => bufs,
                None => {
                    let oldest: Started<_> = writing.pop_front().expect("every buffer is in use");
                    let (written, bufs) = oldest.finish().await;
                    written?;
                    bufs
                }
            };
            bufs.iter_mut().for_each(FixedBuf::clear);

            let mut filled = 0usize;
            while filled < round_len && !direct.is_ended() {
                match direct.remaining() {
                    Some(left) if !direct.has_pending() => {
                        let left = usize::try_from(left).unwrap_or(usize::MAX);
                        let upto = round_len.min(filled.saturating_add(left));
                        let segments = geometry.fill(filled..upto);
                        let (received, returned) = direct.recv_vectored(bufs, &segments).await;
                        bufs = returned;
                        let received = received.map_err(|e| match e {
                            BodyError::Io(err) => StoreError::Io(err),
                            BodyError::Malformed(_) => StoreError::Api(ApiError::BAD_REQUEST),
                        })?;
                        filled += received;
                    }
                    _ => {
                        let room = geometry.fill(filled..round_len);
                        let next = room.first().expect("round is not full");
                        let output =
                            &mut bufs[next.buf].spare_capacity_mut()[next.start..][..next.len];
                        let n = direct.read_into(output).await.map_err(|e| match e {
                            BodyError::Io(err) => StoreError::Io(err),
                            BodyError::Malformed(_) => StoreError::Api(ApiError::BAD_REQUEST),
                        })?;
                        filled += n;
                    }
                }
            }
            if filled == 0 {
                free.push(bufs);
                break;
            }
            let round = self.begin_round(upload, filled).await?;
            writing.push_back(Started::start(self.write_round(round, bufs)).await);
            if direct.is_ended() {
                break;
            }
        }

        for write in writing {
            let (written, bufs) = write.finish().await;
            written?;
            free.push(bufs);
        }
        Ok(free.into_iter().flatten().collect())
    }

    async fn receive_fallback(
        &self,
        body: &mut axum::body::Body,
        upload: &mut Upload,
    ) -> Result<Vec<FixedBuf>, StoreError> {
        let geometry = self.geometry;
        let round_len = geometry.round_len();
        let mut bufs = self.buffers.acquire_many(geometry.drives()).await;
        bufs.iter_mut().for_each(FixedBuf::clear);
        let mut filled = 0usize;

        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| StoreError::Io(io::Error::other(e)))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let mut slice = data.as_ref();
            while !slice.is_empty() {
                let take = slice.len().min(round_len - filled);
                let segments = geometry.fill(filled..filled + take);
                let mut offset = 0;
                for seg in segments {
                    bufs[seg.buf].spare_capacity_mut()[seg.start..][..seg.len]
                        .copy_from_slice(&slice[offset..offset + seg.len]);
                    offset += seg.len;
                }
                filled += take;
                slice = &slice[take..];
                if filled == round_len {
                    let round = self.begin_round(upload, filled).await?;
                    let (written, returned) = self.write_round(round, bufs).await;
                    bufs = returned;
                    written?;
                    bufs.iter_mut().for_each(FixedBuf::clear);
                    filled = 0;
                }
            }
        }
        if filled > 0 {
            let round = self.begin_round(upload, filled).await?;
            let (written, returned) = self.write_round(round, bufs).await;
            bufs = returned;
            written?;
        }
        Ok(bufs)
    }

    pub(crate) fn writer(
        self: &Arc<Self>,
        stored: SidecarStored,
        range: Range<u64>,
    ) -> SidecarObjectWriter {
        SidecarObjectWriter {
            store: self.clone(),
            stored,
            range,
        }
    }
}

/// [`DirectWrite`] implementation for streaming a stored object from NVMe
/// drives to the client socket with read-ahead windowing, zero-copy `send_zc`
/// (large remote objects), and coalesced header+body vectored send (small objects).
pub(crate) struct SidecarObjectWriter {
    store: Arc<SidecarStore>,
    stored: SidecarStored,
    range: Range<u64>,
}

impl DirectWrite for SidecarObjectWriter {
    fn write_to<'a>(
        self: Box<Self>,
        sink: &'a mut BodySink<'a>,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let geometry = self.store.geometry;
            let mut rounds = geometry.rounds(&self.range);
            if rounds.is_empty() {
                return Ok(());
            }
            let parts = self.stored.parts();
            let mut free = self
                .store
                .acquire_window(parts, rounds.end - rounds.start)
                .await;
            let mut reading = VecDeque::new();
            let mut unacknowledged: VecDeque<Reclaim> = VecDeque::new();

            loop {
                while !rounds.is_empty() {
                    let Some(bufs) = free.pop() else { break };
                    let round = rounds.next().expect("checked above");
                    let plan = geometry.plan(round, &self.range);
                    let read = self.store.read_round(&self.stored, plan, bufs);
                    reading.push_back(Started::start(read).await);
                }

                let Some(oldest) = reading.pop_front() else {
                    let Some(oldest) = unacknowledged.pop_front() else {
                        debug_assert!(rounds.is_empty());
                        return Ok(());
                    };
                    let bufs = oldest.await.bufs;
                    if bufs.len() == parts {
                        free.push(bufs);
                    }
                    continue;
                };

                let (got, bufs, plan) = oldest.finish().await;
                got?;
                if sink.zero_copy() && plan.len >= ZERO_COPY_MIN {
                    let (sent, reclaim) = sink.send_zero_copy(bufs, &plan.segments).await;
                    if let Some(reclaim) = reclaim {
                        unacknowledged.push_back(reclaim);
                    }
                    sent?;
                } else {
                    let (sent, bufs) = sink.send_vectored(bufs, &plan.segments).await;
                    sent?;
                    free.push(bufs);
                }

                while let Some(reclaimed) = unacknowledged.front_mut().and_then(Reclaim::try_take) {
                    unacknowledged.pop_front();
                    if reclaimed.bufs.len() == parts {
                        free.push(reclaimed.bufs);
                    }
                }
            }
        })
    }
}
