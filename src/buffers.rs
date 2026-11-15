//! A pool of page-aligned buffers registered with io_uring.
//!
//! The pool pins its memory once, on a ring of its own that never does any
//! I/O. Every other ring gets the registration by cloning it from that ring,
//! so all rings share one set of pinned pages and agree on buffer indices.

use std::io;
use std::ops::{Deref, DerefMut};
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, PoisonError};

use io_uring::IoUring;
use tokio::sync::Semaphore;

const PAGE: usize = 4096;

/// The kernel's limit on the size of one registered buffer.
const MAX_REGISTERED: usize = 1 << 30;

struct Inner {
    base: NonNull<u8>,
    mapped: usize,
    buf_size: usize,
    /// How many buffers share one registered buffer.
    bufs_per_registered: usize,
    /// Holds the registration that other rings clone.
    registry: IoUring,
    free: Mutex<Vec<u32>>,
    /// One permit per entry of `free`.
    available: Semaphore,
}

// SAFETY: the mapping is owned by `Inner`, and the bytes of each buffer are
// only reachable through the one `FixedBuf` that holds its index.
unsafe impl Send for Inner {}
// SAFETY: as above.
unsafe impl Sync for Inner {}

impl Drop for Inner {
    fn drop(&mut self) {
        // SAFETY: the mapping was created with this address and length, and
        // no `FixedBuf` is left to point into it. Rings that still hold a
        // clone of the registration keep the pages themselves pinned.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.mapped) };
    }
}

/// A fixed set of equally sized, page-aligned buffers that rings can use
/// without mapping and pinning memory on every operation. Cheap to clone.
///
/// Pinned memory counts against `RLIMIT_MEMLOCK` unless the process has
/// `CAP_IPC_LOCK`. Sharing the registration between rings needs Linux 6.12.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<Inner>,
}

impl BufferPool {
    /// Allocates and pins `count` buffers of `buf_size` bytes each.
    ///
    /// `buf_size` must be a multiple of 4096 and at most 1 GiB. The buffers
    /// start page-aligned, as `O_DIRECT` requires, and the pool asks for
    /// transparent huge pages, which it gets where its size allows.
    pub fn new(buf_size: usize, count: usize) -> io::Result<BufferPool> {
        let invalid = |message| io::Error::new(io::ErrorKind::InvalidInput, message);
        if buf_size == 0 || buf_size % PAGE != 0 || buf_size > MAX_REGISTERED {
            return Err(invalid(
                "buffer size must be a multiple of 4096, up to 1 GiB",
            ));
        }
        let ids = u32::try_from(count)
            .ok()
            .filter(|&count| count > 0)
            .ok_or_else(|| invalid("buffer count must be between 1 and u32::MAX"))?;
        let mapped = buf_size
            .checked_mul(count)
            .ok_or_else(|| invalid("pool size overflows"))?;
        let bufs_per_registered = MAX_REGISTERED / buf_size;
        if count.div_ceil(bufs_per_registered) > usize::from(u16::MAX) {
            return Err(invalid(
                "pool needs more registered buffers than a ring can hold",
            ));
        }

        // SAFETY: a fresh anonymous mapping, not placed over anything.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        // Advice only: without huge pages the pool works the same, slower.
        // SAFETY: the range is the mapping just created.
        unsafe { libc::madvise(base, mapped, libc::MADV_HUGEPAGE) };

        // From here on `inner` owns the mapping and unmaps it on any error.
        let inner = Inner {
            base: NonNull::new(base.cast()).expect("mmap returned a null mapping"),
            mapped,
            buf_size,
            bufs_per_registered,
            registry: IoUring::new(1)?,
            free: Mutex::new((0..ids).rev().collect()),
            available: Semaphore::new(count),
        };

        let registered_size = bufs_per_registered * buf_size;
        let registered: Vec<libc::iovec> = (0..mapped)
            .step_by(registered_size)
            .map(|offset| libc::iovec {
                // SAFETY: `offset` is within the mapping.
                iov_base: unsafe { inner.base.add(offset) }.as_ptr().cast(),
                iov_len: registered_size.min(mapped - offset),
            })
            .collect();
        // SAFETY: the ranges lie within the mapping, which outlives the
        // registry ring: `Inner` drops its fields only after `drop` has
        // run, and by then nothing can start an operation on these buffers.
        unsafe { inner.registry.submitter().register_buffers(&registered) }?;

        Ok(BufferPool {
            inner: Arc::new(inner),
        })
    }

    /// Size of each buffer in bytes.
    pub fn buf_size(&self) -> usize {
        self.inner.buf_size
    }

    /// Number of buffers in the pool.
    pub fn count(&self) -> usize {
        self.inner.mapped / self.inner.buf_size
    }

    /// Number of buffers not currently handed out.
    pub fn available(&self) -> usize {
        self.inner.available.available_permits()
    }

    /// Takes a buffer, waiting for one to be returned if none is free.
    /// Waiters are served in order.
    pub async fn acquire(&self) -> FixedBuf {
        self.inner
            .available
            .acquire()
            .await
            .expect("the pool never closes its semaphore")
            .forget();
        self.take()
    }

    /// Takes `count` buffers at once, waiting until that many are free.
    ///
    /// A task that needs several buffers at a time must get them this way.
    /// Taking them one by one can leave tasks each holding some and waiting
    /// for the rest, forever.
    ///
    /// # Panics
    ///
    /// Panics if the pool has fewer than `count` buffers.
    pub async fn acquire_many(&self, count: usize) -> Vec<FixedBuf> {
        assert!(count <= self.count(), "the pool is smaller than that");
        let permits = u32::try_from(count).expect("a pool holds at most u32::MAX buffers");
        self.inner
            .available
            .acquire_many(permits)
            .await
            .expect("the pool never closes its semaphore")
            .forget();
        (0..count).map(|_| self.take()).collect()
    }

    /// Takes a buffer if one is free.
    pub fn try_acquire(&self) -> Option<FixedBuf> {
        self.inner.available.try_acquire().ok()?.forget();
        Some(self.take())
    }

    /// Hands out a buffer for a permit the caller has already taken.
    fn take(&self) -> FixedBuf {
        let id = self
            .inner
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .expect("a permit guarantees a free buffer");
        FixedBuf {
            pool: self.inner.clone(),
            id,
            len: 0,
        }
    }

    /// Gives `ring` this pool's registration. A ring that restricts
    /// registration to one thread must be passed from that thread.
    pub(crate) fn register_on(&self, ring: &IoUring) -> io::Result<()> {
        ring.submitter()
            .register_buffers_clone(self.inner.registry.as_raw_fd())
    }

    pub(crate) fn owns(&self, buf: &FixedBuf) -> bool {
        Arc::ptr_eq(&self.inner, &buf.pool)
    }
}

/// A buffer from a [`BufferPool`]. Returns to the pool when dropped.
///
/// Like a `Vec<u8>` of fixed capacity: it dereferences to the bytes written
/// so far, and reads append after them. A buffer is not wiped between uses,
/// so the bytes past its length are whatever its previous user left there.
pub struct FixedBuf {
    pool: Arc<Inner>,
    id: u32,
    len: usize,
}

impl FixedBuf {
    pub fn capacity(&self) -> usize {
        self.pool.buf_size
    }

    /// Forgets the contents.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Shortens the contents to `len` bytes. Does nothing if they are
    /// already that short.
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }

    /// Appends as much of `bytes` as fits and returns how much that was.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> usize {
        let count = bytes.len().min(self.capacity() - self.len);
        // SAFETY: the destination is `count` bytes of this buffer's spare
        // capacity, which `bytes` cannot overlap: nothing else can reach it.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.spare_ptr(), count) };
        self.len += count;
        count
    }

    /// Start of the buffer.
    pub(crate) fn ptr(&self) -> *mut u8 {
        // SAFETY: `id` names a buffer of the mapping.
        unsafe { self.pool.base.add(self.id as usize * self.pool.buf_size) }.as_ptr()
    }

    /// Start of the bytes past the contents.
    pub(crate) fn spare_ptr(&self) -> *mut u8 {
        // SAFETY: `len` never exceeds the buffer's size.
        unsafe { self.ptr().add(self.len) }
    }

    pub(crate) fn spare_len(&self) -> usize {
        self.capacity() - self.len
    }

    /// Index, on every ring that has the pool, of the registered buffer
    /// this one is part of.
    pub(crate) fn registered_index(&self) -> u16 {
        (self.id as usize / self.pool.bufs_per_registered) as u16
    }

    /// The bytes past the contents, for filling in place before a call to
    /// [`grow`](Self::grow).
    pub fn spare_capacity_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref_mut`. The spare bytes are initialized too,
        // with whatever was last written there.
        unsafe { std::slice::from_raw_parts_mut(self.spare_ptr(), self.spare_len()) }
    }

    /// Extends the contents by the next `count` bytes of the spare capacity,
    /// as they are.
    ///
    /// # Panics
    ///
    /// Panics if that is more than the spare capacity.
    pub fn grow(&mut self, count: usize) {
        assert!(count <= self.spare_len());
        self.len += count;
    }
}

impl Deref for FixedBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: the mapping starts out zeroed, so all of it is
        // initialized, and this handle is the only way to these bytes.
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len) }
    }
}

impl DerefMut for FixedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and `&mut self` makes the access unique.
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len) }
    }
}

impl Drop for FixedBuf {
    fn drop(&mut self) {
        self.pool
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.id);
        self.pool.available.add_permits(1);
    }
}
